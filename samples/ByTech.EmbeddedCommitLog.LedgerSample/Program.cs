using ByTech.EmbeddedCommitLog.LedgerSample;
using ByTech.EmbeddedCommitLog.Pipeline;

using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

// PECL persists everything to disk: segment files, per-consumer cursors, and a
// checkpoint. This directory survives process restarts; re-opening the pipeline
// with the same path resumes each consumer from its last flushed cursor position.
string dataDir = Path.Combine(Path.GetTempPath(), "pecl-ledger-" + Path.GetRandomFileName());
Directory.CreateDirectory(dataDir);

Console.WriteLine("PECL General Ledger Sample");
Console.WriteLine(new string('─', 70));
Console.WriteLine($"Data directory : {dataDir}");
Console.WriteLine();

var config = new PipelineConfiguration
{
    RootDirectory = dataDir,
    DurabilityMode = DurabilityMode.Batched,  // fsync every 100 ms (default)
};

// ── Initial accounts for Run 1 ────────────────────────────────────────────────
var accts = new (string Id, string Name, AccountSink Sink)[]
{
    ("ACC-001", "Current Account",    new AccountSink("ACC-001")),
    ("ACC-002", "Savings Account",    new AccountSink("ACC-002")),
    ("ACC-003", "Investment Account", new AccountSink("ACC-003")),
};
var audit1 = new AuditSink();
var pullEntries1 = new List<(ulong SeqNo, LedgerEntry Entry)>();

// ══════════════════════════════════════════════════════════════════════════════
// RUN 1 — initial batch
// ══════════════════════════════════════════════════════════════════════════════
Console.WriteLine("RUN 1 — posting 12 ledger entries");
Console.WriteLine();

using (var pipeline = new PeclPipeline(config))
{
    // Push-mode: one consumer per account (filtered) + one unfiltered audit consumer.
    foreach ((string id, string _, AccountSink sink) in accts)
    {
        pipeline.RegisterConsumer(id);
        pipeline.AddSink(id, "balance-projector", sink);
    }

    pipeline.RegisterConsumer("audit");
    pipeline.AddSink("audit", "ledger-log", audit1);

    // Pull-mode: no sinks. The application pulls records on demand via ReadNext.
    // Cursor starts at SeqNo 0 (MissingCursorPolicy.FromBeginning — default).
    pipeline.RegisterConsumer("report");

    pipeline.Start();

    LedgerEntry[] entries1 =
    [
        new("ACC-001", 1_000.00m, "CR", "Opening deposit",          DateTimeOffset.UtcNow),
        new("ACC-002", 2_500.00m, "CR", "Transfer from external",   DateTimeOffset.UtcNow),
        new("ACC-001",   350.00m, "DR", "Rent — April",             DateTimeOffset.UtcNow),
        new("ACC-003", 5_000.00m, "CR", "Investment purchase",      DateTimeOffset.UtcNow),
        new("ACC-001",   200.00m, "CR", "Refund — utilities",       DateTimeOffset.UtcNow),
        new("ACC-002",    50.00m, "DR", "Service fee",              DateTimeOffset.UtcNow),
        new("ACC-001",    75.00m, "DR", "Grocery store",            DateTimeOffset.UtcNow),
        new("ACC-002",   800.00m, "CR", "Freelance payment",        DateTimeOffset.UtcNow),
        new("ACC-003",   500.00m, "DR", "Fund management fee",      DateTimeOffset.UtcNow),
        new("ACC-002",   300.00m, "DR", "Insurance premium",        DateTimeOffset.UtcNow),
        new("ACC-003",   250.00m, "CR", "Dividend received",        DateTimeOffset.UtcNow),
        new("ACC-001",   500.00m, "CR", "Salary deposit — April",   DateTimeOffset.UtcNow),
    ];

    foreach (LedgerEntry entry in entries1)
    {
        pipeline.Append(entry.Serialize());
    }

    // Flush() fsyncs the active segment — all records are now visible to readers.
    pipeline.Flush();

    // D — Pull-mode: ReadNext reads directly from the segment file on the caller's
    // thread. No background loop — the application controls the read pace.
    while (true)
    {
        var result = pipeline.ReadNext("report");
        if (!result.IsSuccess)
        {
            break;
        }

        pullEntries1.Add((result.Value.Header.SeqNo, LedgerEntry.Deserialize(result.Value.Payload.Span)));
    }

    // Stop() drains all push-mode sink queues, persists cursors, then shuts down.
    pipeline.Stop();
}

// ── Print sections A–D ────────────────────────────────────────────────────────

PrintSection("A  ACCOUNT STATEMENTS  (push-mode · filtered per consumer)");
PrintAccountStatements(accts);

PrintSection("B  FULL AUDIT LOG  (push-mode · unfiltered broadcast)");
PrintLogEntries(audit1.Entries.Select(e => (e.SeqNo, e.LedgerEntry)));

PrintSection("C  CROSS-ACCOUNT RECONCILIATION  (in-memory merge of account sinks, sorted by SeqNo)");
var merged = accts
    .SelectMany(a => a.Sink.History.Select(s => (s.SeqNo, s.Entry)))
    .OrderBy(x => x.SeqNo)
    .ToList();
PrintLogEntries(merged);
decimal totalCr1 = merged.Where(x => x.Entry.EntryType == "CR").Sum(x => x.Entry.Amount);
decimal totalDr1 = merged.Where(x => x.Entry.EntryType == "DR").Sum(x => x.Entry.Amount);
Console.WriteLine($"  Totals  —  CR: +{totalCr1:N2}   DR: -{totalDr1:N2}   Net: {(totalCr1 - totalDr1):+0.00;-0.00}");
Console.WriteLine();

PrintSection("D  PULL-MODE REPORT  (\"report\" consumer · ReadNext loop · same log, different pace)");
PrintLogEntries(pullEntries1);

// ══════════════════════════════════════════════════════════════════════════════
// RUN 2 — restart, resume from cursors, post new entries, seek pull consumer
// ══════════════════════════════════════════════════════════════════════════════

// Account sinks start from the closing balances of Run 1. In a production system
// the AccountSink would load its last persisted balance from its own store on startup;
// PECL's cursor ensures it only receives records it has not yet processed.
var accts2 = new (string Id, string Name, AccountSink Sink)[]
{
    ("ACC-001", "Current Account",    new AccountSink("ACC-001", accts[0].Sink.Balance)),
    ("ACC-002", "Savings Account",    new AccountSink("ACC-002", accts[1].Sink.Balance)),
    ("ACC-003", "Investment Account", new AccountSink("ACC-003", accts[2].Sink.Balance)),
};
var audit2 = new AuditSink();
var pullEntries2 = new List<(ulong SeqNo, LedgerEntry Entry)>();

Console.WriteLine("RUN 2 — restarting pipeline; consumers resume from saved cursors");
Console.WriteLine();

using (var pipeline2 = new PeclPipeline(config))
{
    foreach ((string id, string _, AccountSink sink) in accts2)
    {
        pipeline2.RegisterConsumer(id);
        pipeline2.AddSink(id, "balance-projector", sink);
    }

    pipeline2.RegisterConsumer("audit");
    pipeline2.AddSink("audit", "ledger-log", audit2);

    pipeline2.RegisterConsumer("report");

    // Seek the pull-mode consumer back to SeqNo 4 before Start() to demonstrate
    // that any consumer can be repositioned independently to re-process history.
    // SeekConsumer must be called on a stopped pipeline (before Start).
    pipeline2.SeekConsumer("report", 4);

    pipeline2.Start();

    LedgerEntry[] entries2 =
    [
        new("ACC-001", 1_500.00m, "CR", "Salary deposit — May",  DateTimeOffset.UtcNow),
        new("ACC-002",   500.00m, "DR", "Bill payment",          DateTimeOffset.UtcNow),
        new("ACC-001",    80.00m, "DR", "Transport card top-up", DateTimeOffset.UtcNow),
    ];

    foreach (LedgerEntry entry in entries2)
    {
        pipeline2.Append(entry.Serialize());
    }

    pipeline2.Flush();

    while (true)
    {
        var result = pipeline2.ReadNext("report");
        if (!result.IsSuccess)
        {
            break;
        }

        pullEntries2.Add((result.Value.Header.SeqNo, LedgerEntry.Deserialize(result.Value.Payload.Span)));
    }

    pipeline2.Stop();
}

// ── Print sections E–G ────────────────────────────────────────────────────────

PrintSection("E  RESUMED ACCOUNT STATEMENTS  (only new entries delivered — cursors restored from disk)");
Console.WriteLine("  Account sinks resumed from their Run 1 cursor positions.");
Console.WriteLine("  Initial balance reflects the projected balance from Run 1.");
Console.WriteLine();
PrintAccountStatements(accts2, showUnchanged: true);

PrintSection("F  AUDIT DELTA  (\"audit\" consumer received only the 3 new entries)");
PrintLogEntries(audit2.Entries.Select(e => (e.SeqNo, e.LedgerEntry)));

PrintSection("G  PULL-MODE RE-PROCESSING  (\"report\" seeked to SeqNo 4 before Run 2 — replays from mid-history)");
PrintLogEntries(pullEntries2);

Console.WriteLine(new string('─', 70));
Console.WriteLine("Pipeline data persisted — reopen with the same RootDirectory to");
Console.WriteLine("resume each consumer from its last flushed cursor position.");
Console.WriteLine($"  {dataDir}");

// ── Helpers ───────────────────────────────────────────────────────────────────

static void PrintSection(string title)
{
    Console.WriteLine($"┌─ {title}");
    Console.WriteLine();
}

static void PrintAccountStatements(
    (string Id, string Name, AccountSink Sink)[] accounts,
    bool showUnchanged = false)
{
    const string Line = "  ─────────────────────────────────────────────────────────────────";

    foreach ((string id, string name, AccountSink sink) in accounts)
    {
        if (showUnchanged && sink.History.Count == 0)
        {
            Console.WriteLine($"  {id}  —  {name}");
            Console.WriteLine($"  No new entries.  Balance unchanged: {sink.Balance:N2}");
            Console.WriteLine();
            continue;
        }

        Console.WriteLine($"  {id}  —  {name}   ({sink.History.Count} entries, closing balance: {sink.Balance:N2})");
        Console.WriteLine(Line);
        Console.WriteLine($"  {"SeqNo",5}  {"Posted",10}  {"Reference",-30}  {"T",2}  {"Amount",10}  {"Balance",10}");
        Console.WriteLine(Line);

        foreach (AccountSink.Statement s in sink.History)
        {
            string sign = s.Entry.EntryType == "CR" ? "+" : "-";
            Console.WriteLine(
                $"  {s.SeqNo,5}  {s.Entry.PostedAt:yyyy-MM-dd}  {s.Entry.Reference,-30}  {s.Entry.EntryType,2}  {sign}{s.Entry.Amount,9:N2}  {s.RunningBalance,10:N2}");
        }

        Console.WriteLine(Line);
        Console.WriteLine();
    }
}

static void PrintLogEntries(IEnumerable<(ulong SeqNo, LedgerEntry Entry)> entries)
{
    const string Line = "  ─────────────────────────────────────────────────────────────────────";
    var list = entries.ToList();

    Console.WriteLine($"  {list.Count} entries");
    Console.WriteLine(Line);
    Console.WriteLine($"  {"SeqNo",5}  {"Posted",10}  {"Account",7}  {"Reference",-30}  {"T",2}  {"Amount",10}");
    Console.WriteLine(Line);

    foreach ((ulong seqNo, LedgerEntry entry) in list)
    {
        string sign = entry.EntryType == "CR" ? "+" : "-";
        Console.WriteLine(
            $"  {seqNo,5}  {entry.PostedAt:yyyy-MM-dd}  {entry.AccountId,7}  {entry.Reference,-30}  {entry.EntryType,2}  {sign}{entry.Amount,9:N2}");
    }

    Console.WriteLine(Line);
    Console.WriteLine();
}
