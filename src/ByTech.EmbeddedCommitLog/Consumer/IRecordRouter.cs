namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// Routes a single record to one or more sink lanes according to a dispatch strategy.
/// </summary>
/// <remarks>
/// Implementations may await individual lanes when the Block backpressure policy is active.
/// The caller's reader loop is suspended until all targeted lanes accept the record.
/// </remarks>
public interface IRecordRouter
{
    /// <summary>
    /// Routes <paramref name="record"/> to the appropriate sink lanes.
    /// </summary>
    /// <param name="record">The record to route. Must not be null.</param>
    /// <param name="lanes">The set of lanes registered for this consumer. May be empty.</param>
    /// <param name="ct">Cancellation token; cancels any in-progress lane writes.</param>
    ValueTask RouteAsync(LogRecord record, IReadOnlyList<SinkLane> lanes, CancellationToken ct);
}
