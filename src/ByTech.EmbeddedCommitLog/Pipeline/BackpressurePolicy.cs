namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Controls what happens when a sink lane is at capacity during record routing.
/// </summary>
/// <remarks>
/// This setting only affects push-mode consumers (those with at least one sink registered via
/// <see cref="Pipeline.AddSink"/>). Pull-mode consumers are unaffected.
/// </remarks>
public enum BackpressurePolicy
{
    /// <summary>
    /// The router awaits <see cref="Consumer.SinkLane.WriteAsync"/> until space becomes
    /// available in the lane. The reader loop blocks until the slow sink catches up.
    /// This is the default and recommended policy; no records are ever silently discarded.
    /// </summary>
    Block = 0,

    /// <summary>
    /// The router calls <see cref="Consumer.SinkLane.TryWrite"/> and discards the record
    /// immediately if the lane is full. The reader loop never blocks; throughput is
    /// preserved at the cost of possible record loss.
    /// Dropped records are counted in <see cref="PipelineMetrics.SinkDropped"/> and
    /// the <c>pecl.sink.dropped</c> metric (tagged by sink name).
    /// Use only when occasional record loss is acceptable.
    /// </summary>
    Drop = 1,
}
