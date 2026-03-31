using System.Threading.Channels;

namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// A bounded async channel that decouples the record router from a sink task.
/// </summary>
/// <remarks>
/// <para>
/// Uses the Block backpressure policy: <see cref="WriteAsync"/> awaits when the lane is at
/// capacity, suspending the caller until the sink task drains at least one record.
/// </para>
/// <para>
/// <see cref="Count"/> is an approximate value under concurrent access. It is suitable for
/// metrics and observability but must not be used for correctness decisions.
/// </para>
/// </remarks>
public sealed class SinkLane : IDisposable
{
    private readonly Channel<LogRecord> _channel;
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
    /// <exception cref="ArgumentNullException"><paramref name="sinkName"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is less than 1.</exception>
    public SinkLane(string sinkName, int capacity)
    {
        ArgumentNullException.ThrowIfNull(sinkName);

        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity), capacity, "capacity must be at least 1.");
        }

        SinkName = sinkName;
        _channel = Channel.CreateBounded<LogRecord>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });
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
    }
}
