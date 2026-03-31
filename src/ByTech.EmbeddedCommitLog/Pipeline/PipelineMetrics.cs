using System.Diagnostics.Metrics;

namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Exposes live operational counters for a <see cref="Pipeline"/> instance and publishes
/// them as <see cref="System.Diagnostics.Metrics"/> instruments on a named
/// <see cref="System.Diagnostics.Metrics.Meter"/>.
/// </summary>
/// <remarks>
/// <para>
/// All counter values are also updated atomically via <see cref="System.Threading.Interlocked"/>
/// and are safe to read from any thread.
/// </para>
/// <para>
/// Call <see cref="Dispose"/> (or let <see cref="Pipeline.Dispose"/> do it automatically)
/// to release the underlying <see cref="System.Diagnostics.Metrics.Meter"/>.
/// </para>
/// </remarks>
public sealed class PipelineMetrics : IDisposable
{
    private long _recordsAppended;
    private long _bytesAppended;
    private long _flushCount;
    private long _segmentRollovers;
    private long _recoveryCount;
    private long _recoveryTruncatedBytes;
    private long _segmentsDeleted;
    private long _segmentCount;
    private long _sinkDropped;

    private readonly Meter _meter;
    private readonly Counter<long> _recordsAppendedInstr;
    private readonly Counter<long> _bytesAppendedInstr;
    private readonly Counter<long> _flushCountInstr;
    private readonly Counter<long> _segmentRolloversInstr;
    private readonly Counter<long> _recoveryCountInstr;
    private readonly Counter<long> _segmentsDeletedInstr;
    private readonly Counter<long> _sinkDroppedInstr;
    private bool _observablesRegistered;
    private bool _disposed;

    /// <summary>
    /// Initialises a new <see cref="PipelineMetrics"/> and registers a
    /// <see cref="System.Diagnostics.Metrics.Meter"/> with the given name.
    /// </summary>
    /// <param name="meterName">
    /// Name passed to <see cref="System.Diagnostics.Metrics.Meter"/>. Use a unique value
    /// per pipeline instance when running multiple pipelines in the same process.
    /// </param>
    public PipelineMetrics(string meterName)
    {
        _meter = new Meter(meterName, "1.0");
        _recordsAppendedInstr = _meter.CreateCounter<long>("pecl.records.appended", "records");
        _bytesAppendedInstr = _meter.CreateCounter<long>("pecl.bytes.appended", "bytes");
        _flushCountInstr = _meter.CreateCounter<long>("pecl.flushes", "flushes");
        _segmentRolloversInstr = _meter.CreateCounter<long>("pecl.segment.rollovers", "rollovers");
        _recoveryCountInstr = _meter.CreateCounter<long>("pecl.recovery.count", "recoveries");
        _segmentsDeletedInstr = _meter.CreateCounter<long>("pecl.segments.deleted", "segments");

        _sinkDroppedInstr = _meter.CreateCounter<long>("pecl.sink.dropped", "records");
    }

    /// <summary>
    /// The <see cref="System.Diagnostics.Metrics.Meter"/> registered by this instance.
    /// Exposed for test assertion via <see cref="MeterListener"/>.
    /// </summary>
    public Meter Meter => _meter;

    /// <summary>
    /// Total number of records appended to the log since the pipeline was last started.
    /// Incremented by <see cref="Pipeline.Append"/>.
    /// </summary>
    public long RecordsAppended => Interlocked.Read(ref _recordsAppended);

    /// <summary>
    /// Total payload bytes appended to the log since the pipeline was last started.
    /// Incremented by <see cref="Pipeline.Append"/> using the raw payload length
    /// (excluding framing overhead).
    /// </summary>
    public long BytesAppended => Interlocked.Read(ref _bytesAppended);

    /// <summary>
    /// Number of times the active segment was explicitly flushed to the OS page cache.
    /// Incremented by every internal flush (explicit <see cref="Pipeline.Flush"/> call,
    /// segment rollover, or <see cref="Pipeline.Stop"/>).
    /// </summary>
    public long FlushCount => Interlocked.Read(ref _flushCount);

    /// <summary>
    /// Number of segment rollovers that have occurred since the pipeline was last started.
    /// Incremented when <see cref="Pipeline.Append"/> seals the active segment and opens
    /// a new one because <c>IsFull</c> was <see langword="true"/>.
    /// </summary>
    public long SegmentRollovers => Interlocked.Read(ref _segmentRollovers);

    /// <summary>
    /// Number of times crash recovery ran.
    /// Incremented once per successful <see cref="Pipeline.Start"/> call.
    /// </summary>
    public long RecoveryCount => Interlocked.Read(ref _recoveryCount);

    /// <summary>
    /// Total bytes removed from the tail segment during the most recent recovery scan.
    /// Zero when the log was clean (no truncation required). Reset to zero on each start.
    /// </summary>
    public long RecoveryTruncatedBytes => Interlocked.Read(ref _recoveryTruncatedBytes);

    /// <summary>
    /// Total number of segment files deleted by the GC background task since the
    /// pipeline was last started.
    /// </summary>
    public long SegmentsDeleted => Interlocked.Read(ref _segmentsDeleted);

    /// <summary>
    /// Number of segment files present in the segments directory as of the last GC
    /// pass (or pipeline start if GC has not yet run).
    /// </summary>
    public long SegmentCount => Interlocked.Read(ref _segmentCount);

    /// <summary>
    /// Total records discarded because a sink lane was full and
    /// <see cref="BackpressurePolicy.Drop"/> was active. Zero when
    /// <see cref="BackpressurePolicy.Block"/> is used.
    /// </summary>
    public long SinkDropped => Interlocked.Read(ref _sinkDropped);

    /// <summary>Atomically increments <see cref="RecordsAppended"/> and the <c>pecl.records.appended</c> counter.</summary>
    internal void IncrementRecordsAppended()
    {
        Interlocked.Increment(ref _recordsAppended);
        _recordsAppendedInstr.Add(1);
    }

    /// <summary>Atomically adds <paramref name="bytes"/> to <see cref="BytesAppended"/> and the <c>pecl.bytes.appended</c> counter.</summary>
    internal void AddBytesAppended(long bytes)
    {
        Interlocked.Add(ref _bytesAppended, bytes);
        _bytesAppendedInstr.Add(bytes);
    }

    /// <summary>Atomically increments <see cref="FlushCount"/> and the <c>pecl.flushes</c> counter.</summary>
    internal void IncrementFlushCount()
    {
        Interlocked.Increment(ref _flushCount);
        _flushCountInstr.Add(1);
    }

    /// <summary>Atomically increments <see cref="SegmentRollovers"/> and the <c>pecl.segment.rollovers</c> counter.</summary>
    internal void IncrementSegmentRollovers()
    {
        Interlocked.Increment(ref _segmentRollovers);
        _segmentRolloversInstr.Add(1);
    }

    /// <summary>Atomically increments <see cref="RecoveryCount"/> and the <c>pecl.recovery.count</c> counter.</summary>
    internal void IncrementRecoveryCount()
    {
        Interlocked.Increment(ref _recoveryCount);
        _recoveryCountInstr.Add(1);
    }

    /// <summary>Atomically replaces <see cref="RecoveryTruncatedBytes"/> with <paramref name="bytes"/>. Reset to zero at each <see cref="Pipeline.Start"/>.</summary>
    internal void SetRecoveryTruncatedBytes(long bytes) =>
        Interlocked.Exchange(ref _recoveryTruncatedBytes, bytes);

    /// <summary>Atomically increments <see cref="SegmentsDeleted"/> and the <c>pecl.segments.deleted</c> counter.</summary>
    internal void IncrementSegmentsDeleted()
    {
        Interlocked.Increment(ref _segmentsDeleted);
        _segmentsDeletedInstr.Add(1);
    }

    /// <summary>Atomically replaces <see cref="SegmentCount"/> with <paramref name="count"/>. Updated after each GC pass and at pipeline start.</summary>
    internal void SetSegmentCount(long count) =>
        Interlocked.Exchange(ref _segmentCount, count);

    /// <summary>
    /// Atomically increments <see cref="SinkDropped"/> and the <c>pecl.sink.dropped</c>
    /// counter, tagged with the name of the sink that dropped the record.
    /// </summary>
    internal void IncrementSinkDropped(string sinkName)
    {
        Interlocked.Increment(ref _sinkDropped);
        _sinkDroppedInstr.Add(1, new KeyValuePair<string, object?>("sink", sinkName));
    }

    /// <summary>
    /// Registers <see cref="ObservableGauge{T}"/> instruments for consumer lag,
    /// sink lane depth, and segment count. Idempotent — only the first call registers
    /// instruments; subsequent calls are no-ops.
    /// </summary>
    /// <param name="lagObserver">
    /// Callback that returns one <see cref="Measurement{T}"/> per push-mode consumer,
    /// tagged with <c>"consumer"</c>.
    /// </param>
    /// <param name="laneDepthObserver">
    /// Callback that returns one <see cref="Measurement{T}"/> per sink lane,
    /// tagged with <c>"consumer"</c> and <c>"sink"</c>.
    /// </param>
    /// <remarks>
    /// The <c>pecl.consumer.lag</c> gauge reports measurements for push-mode consumers
    /// only (consumers registered with at least one sink via
    /// <see cref="Pipeline.AddSink"/>). Pull-mode consumers do not set
    /// <c>LastRoutedSeqNo</c> and are excluded from this gauge. A pull-mode consumer
    /// that is far behind the log tail will not appear in this metric.
    /// </remarks>
    internal void StartObservableInstruments(
        Func<IEnumerable<Measurement<long>>> lagObserver,
        Func<IEnumerable<Measurement<long>>> laneDepthObserver)
    {
        if (_observablesRegistered)
        {
            return;
        }

        _observablesRegistered = true;

        _meter.CreateObservableGauge<long>(
            "pecl.consumer.lag", lagObserver, "records",
            "Records between a push-mode consumer cursor and the log tail.");

        _meter.CreateObservableGauge<long>(
            "pecl.sink.lane.depth", laneDepthObserver, "records",
            "Approximate records buffered in each sink lane.");

        _meter.CreateObservableGauge<long>(
            "pecl.segments.count",
            () => [new Measurement<long>(SegmentCount)],
            "segments", "Current segment file count.");
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _meter.Dispose();
    }
}
