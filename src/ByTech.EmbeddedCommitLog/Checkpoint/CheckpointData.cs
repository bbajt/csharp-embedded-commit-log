namespace ByTech.EmbeddedCommitLog.Checkpoint;

/// <summary>
/// Immutable snapshot of pipeline state written to <c>checkpoint.dat</c> for crash recovery.
/// </summary>
/// <param name="LastSegmentId">
/// Identifier of the segment that contained the last fully committed record at checkpoint time.
/// </param>
/// <param name="LastOffset">
/// Byte offset within <paramref name="LastSegmentId"/> of the last fully committed record.
/// </param>
/// <param name="LastSeqNo">
/// Sequence number of the last fully committed record.
/// </param>
/// <param name="ActiveSegmentId">
/// Identifier of the currently active (head) segment open for writes.
/// May equal <paramref name="LastSegmentId"/> when no rollover has occurred since the last checkpoint.
/// </param>
/// <remarks>
/// <para><b>On-disk format (36 bytes, all fields little-endian):</b></para>
/// <code>
/// Offset  Size  Field
///  0       4    Magic           = 0x504B4843  ("CHKP" LE)
///  4       1    Version         = 1
///  5       3    Reserved        = 0x000000
///  8       4    LastSegmentId   (uint32)
/// 12       8    LastOffset      (int64)
/// 20       8    LastSeqNo       (uint64)
/// 28       4    ActiveSegmentId (uint32)
/// 32       4    Crc32           (CRC-32/ISO-HDLC over bytes 0–31)
/// Total: 36 bytes
/// </code>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item>The CRC covers exactly the first 32 bytes; the last 4 bytes are the CRC itself.</item>
///   <item>The format is frozen; changes require a new ADR and a version bump.</item>
/// </list>
/// </remarks>
public readonly record struct CheckpointData(
    uint LastSegmentId,
    long LastOffset,
    ulong LastSeqNo,
    uint ActiveSegmentId)
{
    /// <summary>
    /// Four-byte little-endian magic value that identifies a PECL checkpoint file.
    /// Spells "CHKP" in ASCII when read as bytes <c>[0x43, 0x48, 0x4B, 0x50]</c>.
    /// </summary>
    public const uint ExpectedMagic = 0x504B4843u;

    /// <summary>Format version supported by this implementation.</summary>
    public const byte CurrentVersion = 1;

    /// <summary>Total serialized byte size of one checkpoint record on disk.</summary>
    public const int SerializedSize = 36;
}
