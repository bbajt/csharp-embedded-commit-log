namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Controls when the active segment file is flushed to the physical storage medium
/// (fsync / FlushFileBuffers) during normal pipeline operation.
/// </summary>
/// <remarks>
/// Regardless of this setting, <see cref="Pipeline.Flush"/> always calls
/// <c>FlushToDisk</c>, as do segment rollover and <see cref="Pipeline.Stop"/> /
/// <see cref="Pipeline.ForceStop"/>. This enum only governs automatic flushing
/// between those explicit events.
/// </remarks>
public enum DurabilityMode
{
    /// <summary>
    /// No automatic fsync between explicit <see cref="Pipeline.Flush"/> calls,
    /// segment rollovers, and pipeline stop. Record bytes land in the OS page cache
    /// and are written to disk at the OS's discretion. Data may be lost on a hard
    /// process or OS crash before the OS flushes its cache.
    /// Suitable for development, testing, and workloads that can tolerate data loss.
    /// </summary>
    None = 0,

    /// <summary>
    /// A background timer calls <c>FlushInternal</c> directly at the interval configured
    /// by <see cref="PipelineConfiguration.FsyncIntervalMs"/> (default 100 ms), flushing
    /// the active segment to disk at a bounded frequency. Balances write throughput with
    /// bounded durability exposure.
    /// This is the default and recommended mode for most production workloads.
    /// </summary>
    Batched = 1,

    /// <summary>
    /// <c>FlushToDisk</c> is called after every <see cref="Pipeline.Append"/>.
    /// Provides the strongest durability guarantee — no appended record is lost on
    /// crash — at the cost of one fsync per record. Use for financial, audit, or
    /// compliance workloads where data loss is unacceptable.
    /// </summary>
    Strict = 2,
}
