using ByTech.EmbeddedCommitLog.Consumer;

namespace ByTech.EmbeddedCommitLog.Sinks;

/// <summary>
/// Delivers batches of records to an external system.
/// </summary>
/// <remarks>
/// <para>
/// In the current MVP each batch contains exactly one record; future phases may group multiple
/// records into a single batch to amortise per-call overhead.
/// </para>
/// <para>
/// Implementations must not return from <see cref="WriteAsync"/> until the batch has been
/// durably committed to the external system. Any exception thrown propagates to the pipeline's
/// error handling layer and may cause the affected consumer to enter an error state.
/// </para>
/// </remarks>
public interface ISink
{
    /// <summary>
    /// Writes <paramref name="batch"/> to the external system.
    /// Must not return until the batch is durably committed (or throw on failure).
    /// </summary>
    /// <param name="batch">Records to write. Never null or empty during normal operation.</param>
    /// <param name="ct">Cancellation token; cancellation should abort the in-flight write and propagate.</param>
    Task WriteAsync(IReadOnlyList<LogRecord> batch, CancellationToken ct);
}
