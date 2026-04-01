namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Bit flags stored at byte offset 5 of a PECL record header.
/// Controls record-type classification and compression.
/// </summary>
/// <remarks>
/// Bit layout:
/// <list type="bullet">
/// <item>Bit 0 — <see cref="IsBlock"/>: multi-entry block record (reserved for future use)</item>
/// <item>Bit 1 — <see cref="IsCompressed"/>: payload is compressed</item>
/// <item>Bits 2–3 — <see cref="CompressionAlg0"/>/<see cref="CompressionAlg1"/>: algorithm id (00=None, 01=Brotli)</item>
/// <item>Bits 4–7 — reserved, must be zero</item>
/// </list>
/// Readers should ignore unknown bits to allow forward compatibility.
/// </remarks>
[Flags]
public enum RecordFlags : byte
{
    /// <summary>No flags set; single uncompressed record.</summary>
    None = 0,

    /// <summary>
    /// The payload is a block record: it contains multiple logical entries
    /// preceded by a block-level header. Defined for future use; Phase 1
    /// writers always produce <see cref="None"/>.
    /// </summary>
    IsBlock = 1 << 0,

    /// <summary>
    /// The payload is compressed. The compression algorithm is encoded in bits 2–3
    /// (<see cref="CompressionAlg0"/> and <see cref="CompressionAlg1"/>).
    /// Use <see cref="RecordFlagsExtensions.GetCompressionAlgorithm"/> to decode.
    /// </summary>
    IsCompressed = 1 << 1,

    /// <summary>Compression algorithm bit 0 (LSB of the 2-bit algorithm field in bits 2–3).</summary>
    CompressionAlg0 = 1 << 2,

    /// <summary>Compression algorithm bit 1 (MSB of the 2-bit algorithm field in bits 2–3).</summary>
    CompressionAlg1 = 1 << 3,
}

/// <summary>Extension helpers for <see cref="RecordFlags"/>.</summary>
internal static class RecordFlagsExtensions
{
    private const RecordFlags AlgorithmMask = RecordFlags.CompressionAlg0 | RecordFlags.CompressionAlg1;

    /// <summary>
    /// Extracts the <see cref="CompressionAlgorithm"/> encoded in bits 2–3 of <paramref name="flags"/>.
    /// Returns <see cref="CompressionAlgorithm.None"/> when <see cref="RecordFlags.IsCompressed"/> is clear.
    /// </summary>
    internal static CompressionAlgorithm GetCompressionAlgorithm(this RecordFlags flags)
    {
        if ((flags & RecordFlags.IsCompressed) == 0)
        {
            return CompressionAlgorithm.None;
        }

        int algId = (int)(flags & AlgorithmMask) >> 2;
        return (CompressionAlgorithm)algId;
    }
}
