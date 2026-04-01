using ByTech.EmbeddedCommitLog.LedgerSample;
using ByTech.EmbeddedCommitLog.Pipeline;

using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

// ── Pipeline data directory ────────────────────────────────────────────────────
// PECL persists everything to disk: segment files, per-consumer cursors, and a
// checkpoint. This directory survives process restarts; re-opening the pipeline
// with the same path resumes from the last flushed cursor position.
string dataDir = Path.Combine(Path.GetTempPath(), "pecl-ledger-" + Path.GetRandomFileName());
Directory.CreateDirectory(dataDir);

Console.WriteLine("PECL General Ledger Sample");
Console.WriteLine(new string('─', 60));
Console.WriteLine($"Data directory : {dataDir}");
Console.WriteLine();

// ── Pipeline configuration ─────────────────────────────────────────────────────
var config = new PipelineConfiguration
{
    RootDirectory = dataDir,
    DurabilityMode = DurabilityMode.Batched,  // fsync every 100 ms (default)
};

// ── Accounts — each is an independent consumer with its own persistent cursor ──
// RegisterConsumer() creates a named cursor under {dataDir}/cursors/.
// AddSink() attaches a push-mode ISink to that consumer's delivery lane.
// All consumers receive every record in the log; the AccountSink projects only
// the entries that match its own account ID.
var accounts = new (string Id, string Name, AccountSink Sink)[]
{
    ("ACC-001", "Current Account",    new AccountSink("ACC-001")),
    ("ACC-002", "Savings Account",    new AccountSink("ACC-002")),
    ("ACC-003", "Investment Account", new AccountSink("ACC-003")),
};

using var pipeline = new PeclPipeline(config);

foreach ((string id, string _, AccountSink sink) in accounts)
{
    pipeline.RegisterConsumer(id);
    pipeline.AddSink(id, "balance-projector", sink);
}

// Start() launches the background reader loop for each consumer.
pipeline.Start();

// ── General ledger posts credit / debit entries ────────────────────────────────
// The ledger has no knowledge of the consumers: it simply appends entries.
// All entries — across all accounts — go into one ordered, durable log.
LedgerEntry[] entries =
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

Console.WriteLine($"Posting {entries.Length} ledger entries across {accounts.Length} accounts...");
Console.WriteLine();

foreach (LedgerEntry entry in entries)
{
    pipeline.Append(entry.Serialize());
}

// Flush() ensures all appended records are fsync'd to disk and visible to readers.
pipeline.Flush();

// Stop() drains all consumer queues — every AccountSink.WriteAsync completes —
// then persists cursors and shuts down the background reader loops.
pipeline.Stop();

// ── Account statements ─────────────────────────────────────────────────────────
// At this point each AccountSink has processed every entry for its account.
// SeqNo is the global sequence number in the shared log — entries from different
// accounts are interleaved by their original posting order.
const int ColRef = 28;
const string Separator = "  " + "─────────────────────────────────────────────────────────";

foreach ((string id, string name, AccountSink sink) in accounts)
{
    Console.WriteLine($"  Account : {id}  —  {name}");
    Console.WriteLine($"  Entries : {sink.History.Count}   |   Closing balance : {sink.Balance:N2}");
    Console.WriteLine(Separator);
    Console.WriteLine($"  {"SeqNo",5}  {"Posted",10}  {"Reference",-ColRef}  {"T",2}  {"Amount",10}  {"Balance",10}");
    Console.WriteLine(Separator);

    foreach (AccountSink.Statement s in sink.History)
    {
        string sign = s.Entry.EntryType == "CR" ? "+" : "-";
        Console.WriteLine(
            $"  {s.SeqNo,5}  {s.Entry.PostedAt:yyyy-MM-dd}  {s.Entry.Reference,-ColRef}  {s.Entry.EntryType,2}  {sign}{s.Entry.Amount,9:N2}  {s.RunningBalance,10:N2}");
    }

    Console.WriteLine(Separator);
    Console.WriteLine();
}

Console.WriteLine(new string('─', 60));
Console.WriteLine("Pipeline data persisted — reopen with the same RootDirectory");
Console.WriteLine("to resume each consumer from its last flushed cursor position.");
Console.WriteLine($"  {dataDir}");
