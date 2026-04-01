using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Sinks;

namespace ByTech.EmbeddedCommitLog.LedgerSample;

/// <summary>
/// Push-mode sink that projects the running balance for a single account by consuming
/// the general ledger broadcast.
/// </summary>
/// <remarks>
/// <para>
/// PECL delivers every record in the log to every registered consumer — there is no
/// server-side partitioning. This sink therefore inspects each record's
/// <see cref="LedgerEntry.AccountId"/> and silently ignores entries that belong to
/// other accounts. This is the standard event-sourcing "projection" pattern: a
/// read-side view rebuilt by replaying a shared, ordered log.
/// </para>
/// <para>
/// <see cref="Statement.SeqNo"/> is the record's global position in the pipeline —
/// the same sequence number appears in every consumer's view of the log, making it
/// straightforward to correlate entries across accounts.
/// </para>
/// <para>
/// Pass <paramref name="initialBalance"/> when restarting a pipeline from a previously
/// saved cursor position. The sink will project from that starting balance, receiving
/// only the records that were posted after the cursor was last flushed to disk.
/// </para>
/// </remarks>
public sealed class AccountSink : ISink
{
    /// <summary>Immutable snapshot of one processed ledger entry plus the resulting balance.</summary>
    public sealed record Statement(ulong SeqNo, LedgerEntry Entry, decimal RunningBalance);

    private readonly string _accountId;
    private readonly List<Statement> _history = [];
    private decimal _balance;

    /// <summary>Initialises the sink for the given account.</summary>
    /// <param name="accountId">Account ID to project (e.g. "ACC-001").</param>
    /// <param name="initialBalance">
    /// Starting balance carried forward from a previous pipeline run.
    /// Defaults to zero for a brand-new account.
    /// </param>
    public AccountSink(string accountId, decimal initialBalance = 0m)
    {
        _accountId = accountId;
        _balance = initialBalance;
    }

    /// <summary>Account identifier this sink projects.</summary>
    public string AccountId => _accountId;

    /// <summary>Current projected balance (updated synchronously on each <see cref="WriteAsync"/> call).</summary>
    public decimal Balance => _balance;

    /// <summary>Chronological list of processed entries and the running balance after each.</summary>
    public IReadOnlyList<Statement> History => _history;

    /// <inheritdoc/>
    public Task WriteAsync(IReadOnlyList<LogRecord> batch, CancellationToken ct)
    {
        foreach (LogRecord record in batch)
        {
            LedgerEntry entry = LedgerEntry.Deserialize(record.Payload);

            // Each consumer sees the full broadcast — filter for this account only.
            if (entry.AccountId != _accountId)
            {
                continue;
            }

            _balance += entry.EntryType == "CR" ? entry.Amount : -entry.Amount;
            _history.Add(new Statement(record.Header.SeqNo, entry, _balance));
        }

        return Task.CompletedTask;
    }
}
