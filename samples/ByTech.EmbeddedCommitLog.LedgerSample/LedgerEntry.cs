using System.Text.Json;

namespace ByTech.EmbeddedCommitLog.LedgerSample;

/// <summary>
/// A single debit or credit posting in the general ledger.
/// Serialised as UTF-8 JSON and stored as the record payload in the PECL pipeline.
/// </summary>
/// <param name="AccountId">Target account identifier (e.g. "ACC-001").</param>
/// <param name="Amount">Absolute amount — always positive; direction is encoded in <paramref name="EntryType"/>.</param>
/// <param name="EntryType">"CR" (credit — money in) or "DR" (debit — money out).</param>
/// <param name="Reference">Human-readable description of the posting.</param>
/// <param name="PostedAt">UTC timestamp when the ledger entry was posted.</param>
public sealed record LedgerEntry(
    string AccountId,
    decimal Amount,
    string EntryType,
    string Reference,
    DateTimeOffset PostedAt)
{
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    /// <summary>Serialises this entry to a UTF-8 JSON byte array for appending to the pipeline.</summary>
    public byte[] Serialize() =>
        JsonSerializer.SerializeToUtf8Bytes(this, _jsonOptions);

    /// <summary>Deserialises a <see cref="LedgerEntry"/> from a UTF-8 JSON payload.</summary>
    /// <remarks>
    /// Accepts <see cref="byte"/>[] (push-mode <c>LogRecord.Payload</c>) and
    /// <see cref="ReadOnlySpan{T}"/> (pull-mode <c>RecordReadResult.Payload.Span</c>)
    /// without allocating an intermediate array.
    /// </remarks>
    public static LedgerEntry Deserialize(ReadOnlySpan<byte> payload) =>
        JsonSerializer.Deserialize<LedgerEntry>(payload, _jsonOptions)!;
}
