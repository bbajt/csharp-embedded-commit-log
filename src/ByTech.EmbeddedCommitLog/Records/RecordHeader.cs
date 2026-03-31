namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// The 24-byte fixed-size header that precedes every PECL record payload on disk.
/// All multi-byte fields are stored in little-endian byte order.
/// </summary>
/// <remarks>
/// On-disk layout:
/// <list type="table">
/// <listheader><term>Offset</term><term>Size</term><term>Field</term></listheader>
/// <item><term>0</term><term>4 B</term><term><see cref="Magic"/></term></item>
/// <item><term>4</term><term>1 B</term><term><see cref="Version"/></term></item>
/// <item><term>5</term><term>1 B</term><term><see cref="Flags"/></term></item>
/// <item><term>6</term><term>1 B</term><term><see cref="ContentType"/></term></item>
/// <item><term>7</term><term>1 B</term><term><see cref="Reserved"/> (always 0)</term></item>
/// <item><term>8</term><term>4 B</term><term><see cref="SchemaId"/></term></item>
/// <item><term>12</term><term>8 B</term><term><see cref="SeqNo"/></term></item>
/// <item><term>20</term><term>4 B</term><term><see cref="PayloadLength"/></term></item>
/// </list>
/// Total: <see cref="SerializedSize"/> = 24 bytes.
/// </remarks>
/// <param name="Magic">Format marker. Must equal <see cref="ExpectedMagic"/> on read.</param>
/// <param name="Version">Format version. Must equal <see cref="CurrentVersion"/> on read.</param>
/// <param name="Flags">Record-level flags (block indicator, compression).</param>
/// <param name="ContentType">Advisory payload encoding hint for consumers.</param>
/// <param name="Reserved">Alignment padding — always zero. Readers must not validate this byte.</param>
/// <param name="SchemaId">Optional schema registry identifier; 0 means no schema.</param>
/// <param name="SeqNo">Globally monotonic sequence number assigned by the pipeline writer.</param>
/// <param name="PayloadLength">Byte length of the payload that immediately follows this header.</param>
public readonly record struct RecordHeader(
    uint Magic,
    byte Version,
    RecordFlags Flags,
    ContentType ContentType,
    byte Reserved,
    uint SchemaId,
    ulong SeqNo,
    uint PayloadLength)
{
    /// <summary>
    /// Expected value of <see cref="Magic"/> for all PECL records.
    /// Encodes the ASCII string "PECL" as a little-endian uint32:
    /// bytes [0x50, 0x45, 0x43, 0x4C] = 'P', 'E', 'C', 'L'.
    /// </summary>
    public const uint ExpectedMagic = 0x4C434550u;

    /// <summary>
    /// Current record format version. Readers must reject records with a different version.
    /// Increment this constant when the header layout changes in a breaking way.
    /// </summary>
    public const byte CurrentVersion = 1;

    /// <summary>Byte size of a serialized <see cref="RecordHeader"/> on disk.</summary>
    public const int SerializedSize = 24;
}
