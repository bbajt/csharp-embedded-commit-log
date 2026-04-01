using System.Threading.Channels;

namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// A bounded async channel that decouples the record router from a sink task.
/// </summary>
/// <remarks>
/// <para>
/// Block policy: <see cref="WriteAsync"/> awaits when the lane is at capacity, suspending
/// the caller until the sink task drains at least one record.
/// </para>
/// <para>
/// Spill policy: when the lane is full, overflow records are written to a local
/// <see cref="SpillFile"/> at the path supplied to the constructor. The reader loop
/// replays spill records back into the channel via <see cref="TryDrainOneSpillRecord"/>
/// during idle time and after each routed record.
/// </para>
/// <para>
/// <see cref="Count"/> is an approximate value under concurrent access. It is suitable for
/// metrics and observability but must not be used for correctness decisions.
/// </para>
/// </remarks>
public sealed class SinkLane : IDisposable
{
    private readonly Channel<LogRecord> _channel;
    private readonly string? _spillFilePath;
    private SpillFile? _spill;
    private bool _disposed;

    /// <summary>The name of the sink this lane serves.</summary>
    public string SinkName { get; }

    /// <summary>
    /// Approximate number of records currently buffered in the lane.
    /// Suitable for metrics; do not rely on this value for correctness.
    /// </summary>
    public int Count => _channel.Reader.Count;

    /// <summary>The reader side of the lane, consumed by the sink task.</summary>
    public ChannelReader<LogRecord> Reader => _channel.Reader;

    /// <summary>
    /// Creates a new <see cref="SinkLane"/> with the given name and bounded capacity.
    /// </summary>
    /// <param name="sinkName">Name of the sink this lane serves. Must not be null.</param>
    /// <param name="capacity">Maximum records that may be buffered. Must be at least 1.</param>
    /// <param name="spillFilePath">
    /// Absolute path for the spill file used by <see cref="Pipeline.BackpressurePolicy.Spill"/>.
    /// If a file already exists at this path on construction (crash recovery), a
    /// <see cref="SpillFile"/> is opened immediately so replay begins on the first
    /// <see cref="TryDrainOneSpillRecord"/> call. Pass <see langword="null"/> when
    /// spill is not in use.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="sinkName"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is less than 1.</exception>
    public SinkLane(string sinkName, int capacity, string? spillFilePath = null)
    {
        ArgumentNullException.ThrowIfNull(sinkName);

        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity), capacity, "capacity must be at least 1.");
        }

        SinkName = sinkName;
        _spillFilePath = spillFilePath;
        _channel = Channel.CreateBounded<LogRecord>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

        // Crash recovery: if a spill file already exists, open it so TryDrainOneSpillRecord
        // replays its contents before any new records are routed.
        if (spillFilePath is not null && File.Exists(spillFilePath))
        {
            _spill = new SpillFile(spillFilePath);
        }
    }

    /// <summary>
    /// Writes a record to the lane. Awaits if the lane is at capacity (Block backpressure).
    /// </summary>
    /// <param name="record">The record to enqueue. Must not be <see langword="null"/>.</param>
    /// <param name="ct">Cancellation token; cancellation unblocks the await and throws.</param>
    /// <exception cref="ObjectDisposedException">The lane has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> was cancelled while awaiting.</exception>
    public ValueTask WriteAsync(LogRecord record, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _channel.Writer.WriteAsync(record, ct);
    }

    /// <summary>
    /// Attempts to write a record without blocking.
    /// Returns <see langword="false"/> if the lane is currently at capacity.
    /// </summary>
    /// <param name="record">The record to enqueue. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> if the record was enqueued; <see langword="false"/> if the lane
    /// was at capacity and the record was not enqueued.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The lane has been disposed.</exception>
    public bool TryWrite(LogRecord record)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _channel.Writer.TryWrite(record);
    }

    /// <summary>
    /// <see langword="true"/> when a spill file is active for this lane (either because
    /// the lane overflowed or because a crash-recovery file was found at startup).
    /// Once <see langword="true"/>, all routed records go to spill (never directly to
    /// the channel) to preserve FIFO order.
    /// </summary>
    internal bool HasSpill => _spill is not null;

    /// <summary>
    /// Writes <paramref name="record"/> to the spill file.
    /// Creates the file on the first overflow. All subsequent records for this lane
    /// must also go to spill (even if the channel has space) to preserve FIFO order.
    /// </summary>
    /// <param name="record">The record to spill.</param>
    /// <exception cref="InvalidOperationException">
    /// No spill file path was supplied to the constructor.
    /// </exception>
    /// <exception cref="IOException">The spill file write failed.</exception>
    internal void Spill(LogRecord record)
    {
        _spill ??= SpillFile.Create(_spillFilePath!);
        _spill.Append(record);
    }

    /// <summary>
    /// Attempts to replay one record from the spill file into the channel.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if a record was successfully moved from spill to the channel.
    /// <see langword="false"/> if the channel is currently full (try again later),
    /// the spill file is empty, or spill replay is complete (spill file deleted).
    /// </returns>
    /// <remarks>
    /// Must only be called from the reader loop task (the sole channel writer).
    /// If the channel is full after a record was read from the file, the read position
    /// is rewound so the record is retried on the next call.
    /// </remarks>
    internal bool TryDrainOneSpillRecord()
    {
        if (_spill is null)
        {
            return false;
        }

        if (_spill.IsFullyReplayed)
        {
            _spill.DeleteAndDispose();
            _spill = null;
            return false;
        }

        long savedReadPosition = _spill.ReadPosition;

        if (!_spill.TryReadNext(out LogRecord? record) || record is null)
        {
            // EOF or corrupt tail — stop replay and reclaim disk space.
            _spill.DeleteAndDispose();
            _spill = null;
            return false;
        }

        if (!_channel.Writer.TryWrite(record))
        {
            // Channel is full — undo the read so the record is retried next time.
            _spill.SeekRead(savedReadPosition);
            return false;
        }

        // Eagerly clean up if the last record was just drained.
        if (_spill.IsFullyReplayed)
        {
            _spill.DeleteAndDispose();
            _spill = null;
        }

        return true;
    }

    /// <summary>
    /// Signals that no more records will be written to this lane.
    /// The sink task will drain remaining records and then observe channel completion.
    /// </summary>
    /// <param name="error">Optional fault to surface to the reader; null for a clean completion.</param>
    public void Complete(Exception? error = null)
        => _channel.Writer.TryComplete(error);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _channel.Writer.TryComplete();
        // Dispose file handles but do NOT delete the spill file — it must survive
        // a graceful stop so the next Start() can replay any undelivered records.
        _spill?.Dispose();
    }
}
