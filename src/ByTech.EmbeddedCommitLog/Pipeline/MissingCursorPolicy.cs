namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Controls the starting position for a consumer that has no cursor file when
/// <see cref="Pipeline.Start"/> is called (first start, cursor deleted, or new consumer
/// registered before a previous run).
/// </summary>
/// <remarks>
/// The policy is applied only when the cursor file is absent or unreadable.
/// Consumers with a valid existing cursor file always resume from their recorded position,
/// regardless of this setting.
/// </remarks>
public enum MissingCursorPolicy
{
    /// <summary>
    /// Position the consumer at the oldest available record in the log (beginning of
    /// the earliest retained segment). The consumer replays all existing records before
    /// processing new ones. This is the default.
    /// </summary>
    FromBeginning = 0,

    /// <summary>
    /// Position the consumer at the current log tail. All records written before
    /// <see cref="Pipeline.Start"/> returns are skipped; only records appended after
    /// <c>Start</c> returns will be delivered.
    /// Use this for consumers that only care about new events (e.g. live alerting sinks
    /// that do not need historical replay).
    /// </summary>
    FromTail = 1,
}
