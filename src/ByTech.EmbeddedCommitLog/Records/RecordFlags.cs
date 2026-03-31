namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Bit flags stored at byte offset 5 of a PECL record header.
/// Controls record-type classification and compression.
/// </summary>
/// <remarks>
/// Bits 2–7 are reserved and must be zero in the current format version.
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
    /// The payload is compressed. The algorithm will be indicated by the
    /// <see cref="ContentType"/> field once compression support is added
    /// in Phase 3. Phase 1 writers never set this flag.
    /// </summary>
    IsCompressed = 1 << 1,
}
