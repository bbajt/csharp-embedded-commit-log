namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Immutable configuration for a <see cref="Pipeline"/> instance.
/// </summary>
/// <remarks>
/// All paths are resolved relative to <see cref="RootDirectory"/>. The pipeline
/// creates <c>segments/</c> and <c>cursors/</c> subdirectories automatically on first
/// <see cref="Pipeline.Start"/>.
/// </remarks>
public sealed record PipelineConfiguration
{
    /// <summary>
    /// Absolute path to the root directory that contains the pipeline's
    /// <c>segments/</c>, <c>cursors/</c>, and <c>checkpoint.dat</c>.
    /// </summary>
    public required string RootDirectory { get; init; }

    /// <summary>
    /// Maximum number of bytes a single segment file may grow to before a new segment
    /// is opened. Defaults to 64 MiB.
    /// </summary>
    public long MaxSegmentSize { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Number of records a consumer must advance before its cursor is automatically
    /// flushed to disk. Defaults to 1 000.
    /// </summary>
    public int CursorFlushRecordThreshold { get; init; } = 1_000;

    /// <summary>
    /// Maximum time that may elapse between automatic cursor flushes when the record
    /// threshold has not been reached. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan CursorFlushInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Controls when the active segment is flushed to the physical storage medium during
    /// normal pipeline operation. Defaults to <see cref="DurabilityMode.Batched"/>.
    /// </summary>
    /// <remarks>
    /// <see cref="Pipeline.Flush"/>, segment rollover, and <see cref="Pipeline.Stop"/> /
    /// <see cref="Pipeline.ForceStop"/> always call <c>FlushToDisk</c> regardless of this
    /// setting. This property only governs automatic flushing between those explicit events.
    /// </remarks>
    public DurabilityMode DurabilityMode { get; init; } = DurabilityMode.Batched;

    /// <summary>
    /// Interval in milliseconds between automatic segment flushes when
    /// <see cref="DurabilityMode"/> is <see cref="DurabilityMode.Batched"/>.
    /// Ignored for <see cref="DurabilityMode.None"/> and <see cref="DurabilityMode.Strict"/>.
    /// Defaults to 100 ms. Use a small value (e.g. 50) in tests to trigger the flush
    /// timer quickly without relying on an explicit <see cref="Pipeline.Flush"/> call.
    /// </summary>
    public int FsyncIntervalMs { get; init; } = 100;

    /// <summary>
    /// Maximum number of <see cref="Consumer.LogRecord"/> instances that may be buffered
    /// in each <see cref="Consumer.SinkLane"/> before the reader loop blocks
    /// (Block backpressure). Defaults to 4,096.
    /// </summary>
    public int SinkLaneCapacity { get; init; } = 4_096;

    /// <summary>
    /// Backpressure policy applied when a push-mode sink lane is at capacity.
    /// Defaults to <see cref="BackpressurePolicy.Block"/>.
    /// </summary>
    /// <remarks>
    /// Under <see cref="BackpressurePolicy.Block"/>, the reader loop awaits space in the lane,
    /// guaranteeing no record loss at the cost of blocking throughput. Under
    /// <see cref="BackpressurePolicy.Drop"/>, the record is discarded immediately and
    /// <see cref="PipelineMetrics.SinkDropped"/> is incremented. Pull-mode consumers are
    /// unaffected by this setting.
    /// </remarks>
    public BackpressurePolicy BackpressurePolicy { get; init; } = BackpressurePolicy.Block;

    /// <summary>
    /// Maximum time in milliseconds that <see cref="Pipeline.Stop"/> will wait for all
    /// push-mode consumer reader loops to drain before proceeding with cleanup.
    /// <c>0</c> means unbounded (the pre-MILESTONE-07 behaviour). Defaults to 30,000 ms.
    /// </summary>
    /// <remarks>
    /// When the timeout fires, a <see cref="PeclDrainTimeoutException"/> is added to the
    /// <see cref="AggregateException"/> thrown by <see cref="Pipeline.Stop"/> and cleanup
    /// proceeds immediately. Consumer cursors are persisted at their last flush position.
    /// Set to a small value (e.g., 200) in tests to avoid long waits on deliberately blocked sinks.
    /// </remarks>
    public int DrainTimeoutMs { get; init; } = 30_000;

    /// <summary>
    /// Retention policy that controls when sealed segments become eligible for GC.
    /// Defaults to <see cref="RetentionPolicy.ConsumerGated"/>.
    /// </summary>
    public RetentionPolicy RetentionPolicy { get; init; } = RetentionPolicy.ConsumerGated;

    /// <summary>
    /// Maximum age of a sealed segment in milliseconds before it becomes eligible for
    /// deletion under <see cref="RetentionPolicy.TimeBased"/>. Defaults to 604,800,000 ms
    /// (7 days). Ignored for other retention policies.
    /// </summary>
    /// <remarks>
    /// Must be &gt; 0 when <see cref="RetentionPolicy"/> is <see cref="RetentionPolicy.TimeBased"/>;
    /// <see cref="Pipeline.Start"/> throws <see cref="InvalidOperationException"/> otherwise.
    /// Use a small value (e.g. 1) in tests to force immediate expiry.
    /// </remarks>
    public long RetentionMaxAgeMs { get; init; } = 604_800_000L;

    /// <summary>
    /// Maximum total on-disk size in bytes of all segments before the oldest sealed
    /// segments are deleted under <see cref="RetentionPolicy.SizeBased"/>. Defaults to
    /// 1,073,741,824 bytes (1 GiB). Ignored for other retention policies.
    /// </summary>
    /// <remarks>
    /// Must be &gt; 0 when <see cref="RetentionPolicy"/> is <see cref="RetentionPolicy.SizeBased"/>;
    /// <see cref="Pipeline.Start"/> throws <see cref="InvalidOperationException"/> otherwise.
    /// Use a small value (e.g. 1) in tests to force all sealed segments over the limit.
    /// </remarks>
    public long RetentionMaxBytes { get; init; } = 1_073_741_824L;

    /// <summary>
    /// Approximate interval in milliseconds between GC passes. The GC background task
    /// sleeps for this duration between sweeps. Defaults to 60,000 (60 s).
    /// Use a small value (e.g. 50) in tests to avoid long sleeps.
    /// </summary>
    public int GcIntervalMs { get; init; } = 60_000;

    /// <summary>
    /// Maximum time in milliseconds that <see cref="Pipeline.Stop"/> and
    /// <see cref="Pipeline.Dispose"/> will wait for the GC background task to complete
    /// after cancellation is requested. Defaults to 5 000 ms.
    /// </summary>
    /// <remarks>
    /// If the GC task does not complete within this window (e.g., because
    /// <c>File.Delete</c> is blocking on a hung network mount), cleanup proceeds
    /// without it. The task will terminate on its own once the filesystem recovers.
    /// Set to a small value (e.g., 200) in tests to avoid long waits.
    /// </remarks>
    public int GcStopTimeoutMs { get; init; } = 5_000;

    /// <summary>
    /// Name of the <see cref="System.Diagnostics.Metrics.Meter"/> registered by this pipeline.
    /// Use a unique value when running multiple pipelines in the same process to avoid
    /// instrument name collisions. Defaults to <c>"pecl"</c>.
    /// </summary>
    public string MeterName { get; init; } = "pecl";
}
