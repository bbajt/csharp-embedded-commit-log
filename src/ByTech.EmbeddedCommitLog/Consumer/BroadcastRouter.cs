using ByTech.EmbeddedCommitLog.Pipeline;

namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// Routes every record to every registered sink lane (broadcast / fan-out).
/// </summary>
/// <remarks>
/// Under <see cref="BackpressurePolicy.Block"/>, lanes are written sequentially via
/// <see cref="SinkLane.WriteAsync"/> — a full lane blocks the router until space is available.
/// Under <see cref="BackpressurePolicy.Drop"/>, <see cref="SinkLane.TryWrite"/> is used —
/// the record is discarded immediately if the lane is full and <paramref name="onDropped"/> is invoked.
/// Under <see cref="BackpressurePolicy.Spill"/>, overflow records are written to the lane's spill
/// file; once a spill file exists all records go to spill (not directly to the channel) to
/// preserve FIFO order.
/// Parallel fan-out is a Phase 3 optimisation.
/// </remarks>
/// <param name="policy">Backpressure policy to apply when a lane is at capacity.</param>
/// <param name="onDropped">
/// Callback invoked with the sink name whenever a record is discarded under
/// <see cref="BackpressurePolicy.Drop"/>. Never called under <see cref="BackpressurePolicy.Block"/>.
/// </param>
public sealed class BroadcastRouter(BackpressurePolicy policy, Action<string> onDropped) : IRecordRouter
{
    /// <inheritdoc/>
    public async ValueTask RouteAsync(
        LogRecord record,
        IReadOnlyList<SinkLane> lanes,
        CancellationToken ct)
    {
        foreach (SinkLane lane in lanes)
        {
            if (policy == BackpressurePolicy.Drop)
            {
                if (!lane.TryWrite(record))
                {
                    onDropped(lane.SinkName);
                }
            }
            else if (policy == BackpressurePolicy.Spill)
            {
                if (lane.HasSpill || !lane.TryWrite(record))
                {
                    lane.Spill(record);
                }
            }
            else
            {
                await lane.WriteAsync(record, ct);
            }
        }
    }
}
