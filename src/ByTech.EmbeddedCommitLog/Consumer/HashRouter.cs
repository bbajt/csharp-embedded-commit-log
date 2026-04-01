using ByTech.EmbeddedCommitLog.Pipeline;

namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// Distributes records across sink lanes by computing
/// <c>((keySelector(record) % lanes.Count) + lanes.Count) % lanes.Count</c>
/// and writing to the selected lane only. This provides stable, deterministic
/// partitioning for a fixed set of lanes (lanes are frozen at pipeline start).
/// </summary>
/// <remarks>
/// <para>
/// The double-modulo formula ensures a non-negative index even when
/// <paramref name="keySelector"/> returns a negative value.
/// </para>
/// <para>
/// If the lane list is empty, <see cref="RouteAsync"/> is a no-op.
/// </para>
/// </remarks>
internal sealed class HashRouter(
    Func<LogRecord, int> keySelector,
    BackpressurePolicy policy,
    Action<string> onDropped) : IRecordRouter
{
    /// <inheritdoc/>
    public async ValueTask RouteAsync(LogRecord record, IReadOnlyList<SinkLane> lanes, CancellationToken ct)
    {
        if (lanes.Count == 0)
        {
            return;
        }

        int key = keySelector(record);
        int index = ((key % lanes.Count) + lanes.Count) % lanes.Count;
        SinkLane lane = lanes[index];

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
