namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Controls what happens when a sink lane is at capacity during record routing.
/// </summary>
/// <remarks>
/// This setting only affects push-mode consumers (those with at least one sink registered via
/// <c>AddSink</c>). Pull-mode consumers are unaffected.
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
    /// the <c>pecl.sink.dropped_total</c> metric (tagged by sink name).
    /// Use only when occasional record loss is acceptable.
    /// </summary>
    Drop = 1,

    /// <summary>
    /// When a sink lane is full, overflow records are written to a local spill file
    /// (<c>{root}/spill/{consumer}-{sink}.spill</c>) and replayed into the lane when
    /// space becomes available. No records are lost; adds disk I/O overhead.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spill files use PECL record framing (<c>RecordWriter.Write</c>) and are crash-safe:
    /// if the pipeline restarts before replay completes, the spill file is re-replayed on
    /// next <c>Start()</c> (at-least-once delivery to <see cref="Sinks.ISink"/>).
    /// </para>
    /// <para>
    /// FIFO order is preserved: once a spill file exists for a lane, all subsequent
    /// records go to the spill file rather than the channel to prevent reordering.
    /// </para>
    /// </remarks>
    Spill = 2,
}
