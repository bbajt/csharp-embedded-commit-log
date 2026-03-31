namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// The 4-byte footer appended after every PECL record payload on disk.
/// Stores a CRC-32/ISO-HDLC integrity check over the concatenation of the serialized
/// header bytes and the payload bytes.
/// </summary>
/// <param name="Crc32C">
/// CRC-32/ISO-HDLC checksum of <c>header[0..24] ++ payload[0..PayloadLength]</c>,
/// stored as a little-endian uint32. Computed by
/// <see cref="Records.CrcUtility.ComputeRecordCrc32C"/>
/// which uses <see cref="System.IO.Hashing.Crc32"/> (CRC-32/ISO-HDLC), not CRC-32C.
/// See ADR-001 in <c>.docs/DECISIONS.MD</c>.
/// </param>
public readonly record struct RecordFooter(uint Crc32C)
{
    /// <summary>Byte size of a serialized <see cref="RecordFooter"/> on disk.</summary>
    public const int SerializedSize = 4;
}
