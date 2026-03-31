namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Thrown (as an inner exception of <c>AggregateException</c>) by <see cref="Pipeline.Stop"/>
/// when a consumer's drain phase encounters a non-<see cref="PeclErrorCode.EndOfLog"/> read error.
/// </summary>
public sealed class PeclDrainException : Exception
{
    /// <summary>Name of the consumer that experienced the drain error.</summary>
    public string ConsumerName { get; }

    /// <summary>The error returned by the segment reader during drain.</summary>
    public PeclError DrainError { get; }

    /// <inheritdoc/>
    public PeclDrainException(string consumerName, PeclError drainError)
        : base($"Consumer '{consumerName}' encountered a drain error: {drainError}")
    {
        ConsumerName = consumerName;
        DrainError = drainError;
    }
}
