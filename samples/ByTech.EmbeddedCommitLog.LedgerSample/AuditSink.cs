using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Sinks;

namespace ByTech.EmbeddedCommitLog.LedgerSample;

/// <summary>
/// Push-mode sink that records every entry in the general ledger without filtering.
/// </summary>
/// <remarks>
/// Unlike <see cref="AccountSink"/>, this sink does not filter by account — it
/// receives the full broadcast and stores every record in arrival order. The result
/// is an immutable, ordered audit trail of the entire log, identical to what a
/// human auditor would see when reading the segment files directly.
/// </remarks>
public sealed class AuditSink : ISink
{
    /// <summary>Immutable snapshot of one audit log entry.</summary>
    public sealed record Entry(ulong SeqNo, LedgerEntry LedgerEntry);

    private readonly List<Entry> _entries = [];

    /// <summary>All entries received, in delivery order (which matches SeqNo order).</summary>
    public IReadOnlyList<Entry> Entries => _entries;

    /// <inheritdoc/>
    public Task WriteAsync(IReadOnlyList<LogRecord> batch, CancellationToken ct)
    {
        foreach (LogRecord record in batch)
        {
            _entries.Add(new Entry(record.Header.SeqNo, LedgerEntry.Deserialize(record.Payload)));
        }

        return Task.CompletedTask;
    }
}
