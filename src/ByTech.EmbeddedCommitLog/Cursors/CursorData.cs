namespace ByTech.EmbeddedCommitLog.Cursors;

/// <summary>
/// Immutable snapshot of a consumer's read position, written to <c>{ConsumerName}.cur</c>
/// in the pipeline's <c>cursors/</c> subdirectory for crash recovery.
/// </summary>
/// <param name="ConsumerName">
/// Identifies which consumer owns this cursor. Must be non-empty, valid as a filename
/// component, and encode to no more than <see cref="MaxConsumerNameBytes"/> UTF-8 bytes.
/// </param>
/// <param name="SegmentId">
/// Identifier of the segment the consumer is currently reading.
/// </param>
/// <param name="Offset">
/// Byte offset within <paramref name="SegmentId"/> of the last successfully processed record.
/// </param>
/// <param name="SeqNo">
/// Sequence number of the last successfully processed record.
/// </param>
/// <remarks>
/// <para><b>On-disk format (variable length, all fields little-endian):</b></para>
/// <code>
/// Offset           Size    Field
///  0                4      Magic    = 0x53525543  ("CURS" LE)
///  4                1      Version  = 1
///  5                1      Reserved = 0
///  6                2      NameLen  (uint16, ≤ MaxConsumerNameBytes)
///  8                NameLen ConsumerName (UTF-8, no null terminator)
///  8+NameLen        4      SegmentId   (uint32)
/// 12+NameLen        8      Offset      (int64)
/// 20+NameLen        8      SeqNo       (uint64)
/// 28+NameLen        4      Crc32       (CRC-32/ISO-HDLC over bytes 0..(27+NameLen))
/// Total: 32 + NameLen bytes
/// </code>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item>The CRC covers bytes 0 through (27 + NameLen); the last 4 bytes are the CRC itself.</item>
///   <item>The format is frozen; changes require a new ADR and a version bump.</item>
///   <item><c>NameLen</c> is always ≤ <see cref="MaxConsumerNameBytes"/>.</item>
/// </list>
/// </remarks>
public readonly record struct CursorData(
    string ConsumerName,
    uint SegmentId,
    long Offset,
    ulong SeqNo)
{
    /// <summary>
    /// Four-byte little-endian magic value that identifies a PECL cursor file.
    /// Spells "CURS" in ASCII when read as bytes <c>[0x43, 0x55, 0x52, 0x53]</c>.
    /// </summary>
    public const uint ExpectedMagic = 0x53525543u;

    /// <summary>Format version supported by this implementation.</summary>
    public const byte CurrentVersion = 1;

    /// <summary>
    /// Minimum serialized byte size (fixed portion only, when <c>NameLen = 0</c>).
    /// A zero-length name is invalid at write time; this constant is used as
    /// a floor when validating file size on read.
    /// </summary>
    public const int MinSerializedSize = 32;

    /// <summary>Maximum allowed UTF-8 byte length for <see cref="ConsumerName"/>.</summary>
    public const int MaxConsumerNameBytes = 256;

    /// <summary>
    /// Maximum possible serialized byte size
    /// (<see cref="MinSerializedSize"/> + <see cref="MaxConsumerNameBytes"/>).
    /// </summary>
    public const int MaxSerializedSize = MinSerializedSize + MaxConsumerNameBytes;
}
