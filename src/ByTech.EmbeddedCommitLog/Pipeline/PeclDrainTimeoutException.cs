namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Thrown (as an inner exception of <see cref="AggregateException"/>) by <see cref="Pipeline.Stop"/>
/// when <see cref="PipelineConfiguration.DrainTimeoutMs"/> elapses before all push-mode consumer
/// reader loops have drained to tail.
/// </summary>
/// <remarks>
/// Cleanup (GC task cancel, writer dispose, consumer dispose) proceeds after the timeout fires, so
/// the pipeline always reaches <see cref="PipelineState.Stopped"/> or <see cref="PipelineState.Error"/>
/// state. Consumer cursors are persisted at their last flush position, not at the drain-end position.
/// </remarks>
public sealed class PeclDrainTimeoutException : Exception
{
    /// <summary>The drain timeout that was exceeded, in milliseconds.</summary>
    public int TimeoutMs { get; }

    /// <summary>Names of consumers whose reader loops had not completed when the timeout fired.</summary>
    public IReadOnlyList<string> TimedOutConsumers { get; }

    /// <inheritdoc/>
    public PeclDrainTimeoutException(int timeoutMs, IReadOnlyList<string> timedOutConsumers)
        : base(
            $"Pipeline.Stop() drain timed out after {timeoutMs} ms. " +
            $"Consumers still draining: [{string.Join(", ", timedOutConsumers)}]. " +
            "Cleanup proceeded; the pipeline is in Stopped or Error state.")
    {
        TimeoutMs = timeoutMs;
        TimedOutConsumers = timedOutConsumers;
    }
}
