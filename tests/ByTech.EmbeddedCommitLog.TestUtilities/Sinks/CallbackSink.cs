using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Sinks;

namespace ByTech.EmbeddedCommitLog.TestUtilities.Sinks;

/// <summary>
/// A configurable <see cref="ISink"/> test double that records every batch it receives.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Received"/> is not thread-safe. This type is intended for single-threaded or
/// sequentially-driven test scenarios only.
/// </para>
/// </remarks>
public sealed class CallbackSink : ISink
{
    private readonly Func<IReadOnlyList<LogRecord>, CancellationToken, Task>? _onWrite;
    private readonly List<IReadOnlyList<LogRecord>> _received = new();

    /// <summary>All batches received by this sink, in the order they were written.</summary>
    public IReadOnlyList<IReadOnlyList<LogRecord>> Received => _received;

    /// <summary>Total individual records received across all batches.</summary>
    public int RecordCount => _received.Sum(b => b.Count);

    /// <summary>
    /// Creates a new <see cref="CallbackSink"/>.
    /// </summary>
    /// <param name="onWrite">
    /// Optional async callback invoked after the batch is recorded in <see cref="Received"/>.
    /// If <see langword="null"/> the sink silently accepts all batches.
    /// </param>
    public CallbackSink(
        Func<IReadOnlyList<LogRecord>, CancellationToken, Task>? onWrite = null)
    {
        _onWrite = onWrite;
    }

    /// <inheritdoc/>
    public async Task WriteAsync(IReadOnlyList<LogRecord> batch, CancellationToken ct)
    {
        _received.Add(batch);

        if (_onWrite is not null)
        {
            await _onWrite(batch, ct);
        }
    }
}
