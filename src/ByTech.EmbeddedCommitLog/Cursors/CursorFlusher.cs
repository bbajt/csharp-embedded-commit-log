namespace ByTech.EmbeddedCommitLog.Cursors;

/// <summary>
/// Manages an in-memory consumer cursor position and flushes it to disk according to
/// a configurable policy: every N records processed, every T time elapsed, or both.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Advance"/> updates the in-memory position and automatically calls
/// <see cref="FlushIfDue"/> to enforce the record-count threshold. For the time-based
/// threshold, the caller must invoke <see cref="FlushIfDue"/> periodically
/// (e.g., from a background timer or the pipeline's main loop).
/// </para>
/// <para>
/// The flush uses <see cref="CursorWriter"/> internally, so the write-temp + fsync + rename
/// crash-safety guarantee applies: the on-disk cursor always reflects a committed position,
/// never a partial one.
/// </para>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item><see cref="Flush"/> is a no-op when no <see cref="Advance"/> has occurred since
///     the last flush (the cursor is not dirty).</item>
///   <item>After <see cref="Dispose"/>, all methods throw
///     <see cref="ObjectDisposedException"/>.</item>
///   <item><see cref="Dispose"/> is idempotent.</item>
///   <item>On graceful shutdown, <see cref="Dispose"/> flushes any dirty state so the
///     on-disk cursor reflects the last advanced position.</item>
/// </list>
/// <para>Failure modes: I/O exceptions from the underlying <see cref="CursorWriter"/> propagate
/// unchanged. A flush failure in <see cref="Dispose"/> also propagates.</para>
/// </remarks>
public sealed class CursorFlusher : IDisposable
{
    private readonly string _cursorsDirectory;
    private readonly string _consumerName;
    private readonly int _recordThreshold;
    private readonly TimeSpan _timeThreshold;
    private readonly Func<DateTimeOffset> _clock;

    private uint _segmentId;
    private long _offset;
    private ulong _seqNo;
    private int _recordsSinceFlush;
    private DateTimeOffset _lastFlushAt;
    private bool _dirty;
    private bool _disposed;

    /// <summary>
    /// Initialises a <see cref="CursorFlusher"/> for the named consumer.
    /// </summary>
    /// <param name="cursorsDirectory">
    /// Absolute path to the pipeline's <c>cursors/</c> subdirectory.
    /// Must exist before any flush; it is not created automatically.
    /// </param>
    /// <param name="consumerName">
    /// Name of the consumer this flusher manages. Must satisfy the same constraints
    /// as <see cref="CursorData.ConsumerName"/>.
    /// </param>
    /// <param name="recordThreshold">
    /// Number of <see cref="Advance"/> calls that trigger a flush. Must be ≥ 1.
    /// </param>
    /// <param name="timeThreshold">
    /// Maximum time between flushes when records are being processed. Must be positive.
    /// </param>
    /// <param name="initialSegmentId">Segment ID to seed the in-memory position (default 0).</param>
    /// <param name="initialOffset">Byte offset to seed the in-memory position (default 0).</param>
    /// <param name="initialSeqNo">Sequence number to seed the in-memory position (default 0).</param>
    /// <param name="clock">
    /// Time source for the time-based threshold. Defaults to <see cref="DateTimeOffset.UtcNow"/>.
    /// Inject a controlled clock in tests to avoid <c>Thread.Sleep</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="cursorsDirectory"/> or <paramref name="consumerName"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="recordThreshold"/> is less than 1, or
    /// <paramref name="timeThreshold"/> is not positive.
    /// </exception>
    public CursorFlusher(
        string cursorsDirectory,
        string consumerName,
        int recordThreshold,
        TimeSpan timeThreshold,
        uint initialSegmentId = 0u,
        long initialOffset = 0L,
        ulong initialSeqNo = 0UL,
        Func<DateTimeOffset>? clock = null)
    {
        ArgumentNullException.ThrowIfNull(cursorsDirectory);
        ArgumentNullException.ThrowIfNull(consumerName);

        if (recordThreshold < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(recordThreshold), recordThreshold,
                "Record threshold must be at least 1.");
        }

        if (timeThreshold <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeThreshold), timeThreshold,
                "Time threshold must be a positive duration.");
        }

        _cursorsDirectory = cursorsDirectory;
        _consumerName = consumerName;
        _recordThreshold = recordThreshold;
        _timeThreshold = timeThreshold;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);

        _segmentId = initialSegmentId;
        _offset = initialOffset;
        _seqNo = initialSeqNo;
        _lastFlushAt = _clock();
    }

    /// <summary>
    /// Updates the in-memory cursor position and calls <see cref="FlushIfDue"/>.
    /// </summary>
    /// <param name="segmentId">Current segment being read.</param>
    /// <param name="offset">Byte offset of the last processed record within the segment.</param>
    /// <param name="seqNo">Sequence number of the last processed record.</param>
    /// <exception cref="ObjectDisposedException">The flusher has been disposed.</exception>
    public void Advance(uint segmentId, long offset, ulong seqNo)
    {
        ThrowIfDisposed();

        _segmentId = segmentId;
        _offset = offset;
        _seqNo = seqNo;
        _recordsSinceFlush++;
        _dirty = true;

        FlushIfDue();
    }

    /// <summary>
    /// Writes the cursor to disk if the state is dirty and at least one threshold
    /// (record count or elapsed time) has been exceeded since the last flush.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The flusher has been disposed.</exception>
    public void FlushIfDue()
    {
        ThrowIfDisposed();

        if (!_dirty)
        {
            return;
        }

        DateTimeOffset now = _clock();
        bool recordThresholdMet = _recordsSinceFlush >= _recordThreshold;
        bool timeThresholdMet = (now - _lastFlushAt) >= _timeThreshold;

        if (recordThresholdMet || timeThresholdMet)
        {
            Flush();
        }
    }

    /// <summary>
    /// Writes the cursor to disk unconditionally if the state is dirty.
    /// Does nothing if no <see cref="Advance"/> has been called since the last flush.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The flusher has been disposed.</exception>
    public void Flush()
    {
        ThrowIfDisposed();
        FlushCore();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Flushes any dirty state before releasing resources. Filtered I/O exceptions
    /// (<see cref="IOException"/>, <see cref="ObjectDisposedException"/>,
    /// <see cref="InvalidOperationException"/>) are silently suppressed — the
    /// on-dispose flush is best-effort. Unfiltered exceptions propagate; the flusher
    /// is still marked disposed in either case.
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // Mark disposed before FlushCore() so that a non-filtered exception (e.g. DirectoryNotFoundException)
        // cannot leave _disposed = false, which would allow a second flush on re-entry (R05-L6).
        // FlushCore() is called directly (not Flush()) to avoid the ThrowIfDisposed() guard inside Flush()
        // inadvertently suppressing the flush now that _disposed is already true.
        _disposed = true;

        try
        {
            FlushCore();
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            // Best-effort dispose-time flush — filtered exceptions are silently suppressed.
        }
    }

    private void FlushCore()
    {
        if (!_dirty)
        {
            return;
        }

        CursorWriter.Write(_cursorsDirectory,
            new CursorData(_consumerName, _segmentId, _offset, _seqNo));

        _recordsSinceFlush = 0;
        _lastFlushAt = _clock();
        _dirty = false;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(CursorFlusher));
        }
    }
}
