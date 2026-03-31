using System.IO.Hashing;

namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Internal helper for computing CRC-32 over the two-span PECL record data
/// (serialized header bytes followed by payload bytes).
/// </summary>
/// <remarks>
/// Uses CRC-32/ISO-HDLC (<see cref="Crc32"/>) from <c>System.IO.Hashing</c>.
/// See ADR-001 for the rationale behind choosing this algorithm over CRC-32C.
/// </remarks>
internal static class CrcUtility
{
    /// <summary>
    /// Computes the CRC-32/ISO-HDLC integrity check for a record's header and payload.
    /// </summary>
    /// <param name="header">The 24 serialized header bytes.</param>
    /// <param name="payload">The record payload bytes. May be empty.</param>
    /// <remarks>
    /// Despite the name suffix "Crc32C", this method uses CRC-32/ISO-HDLC
    /// (<see cref="Crc32"/>), not CRC-32C (Castagnoli).
    /// The method name is retained per ADR-001 to avoid a cascade of renames.
    /// Any external tool validating PECL record integrity must use CRC-32/ISO-HDLC.
    /// </remarks>
    internal static uint ComputeRecordCrc32C(ReadOnlySpan<byte> header, ReadOnlySpan<byte> payload)
    {
        var hash = new Crc32();
        hash.Append(header);
        hash.Append(payload);
        return hash.GetCurrentHashAsUInt32();
    }

    /// <summary>
    /// Computes CRC-32 over a single contiguous span of bytes.
    /// </summary>
    /// <param name="data">The bytes to checksum.</param>
    internal static uint ComputeCrc32(ReadOnlySpan<byte> data)
    {
        var hash = new Crc32();
        hash.Append(data);
        return hash.GetCurrentHashAsUInt32();
    }
}
