namespace ByTech.EmbeddedCommitLog;

/// <summary>Identifies the category of a PECL operation failure.</summary>
public enum PeclErrorCode
{
    /// <summary>Unspecified error — should not occur in practice.</summary>
    Unknown = 0,

    /// <summary>The stored CRC32C does not match the independently computed value.</summary>
    CrcMismatch = 1,

    /// <summary>The stream ended before a complete 24-byte record header could be read.</summary>
    TruncatedHeader = 2,

    /// <summary>The stream ended before the full payload declared by the header could be read.</summary>
    TruncatedPayload = 3,

    /// <summary>The stream ended before a complete 4-byte record footer could be read.</summary>
    TruncatedFooter = 4,

    /// <summary>The first four bytes of the record do not equal <see cref="Records.RecordHeader.ExpectedMagic"/>.</summary>
    InvalidMagic = 5,

    /// <summary>The version byte is not supported by this implementation.</summary>
    InvalidVersion = 6,

    /// <summary>
    /// The payload length declared in the header exceeds the maximum addressable array size.
    /// This indicates a corrupt or future-format record.
    /// </summary>
    PayloadTooLarge = 7,

    /// <summary>The checkpoint file (<c>checkpoint.dat</c>) does not exist in the pipeline directory.</summary>
    CheckpointMissing = 8,

    /// <summary>
    /// The checkpoint file is shorter than <see cref="Checkpoint.CheckpointData.SerializedSize"/> bytes.
    /// This indicates a truncated write.
    /// </summary>
    CheckpointTruncated = 9,

    /// <summary>The cursor file for the named consumer does not exist in the cursors directory.</summary>
    CursorMissing = 10,

    /// <summary>
    /// The cursor file is shorter than the minimum expected size, or shorter than the size implied
    /// by the <c>NameLen</c> field. This indicates a truncated write.
    /// </summary>
    CursorTruncated = 11,

    /// <summary>
    /// The <c>ConsumerName</c> stored inside the cursor file does not match the consumer name
    /// used to locate the file. This indicates a renamed or misplaced cursor file.
    /// </summary>
    CursorNameMismatch = 12,

    /// <summary>The consumer has reached the end of the written log; no more records are available.</summary>
    EndOfLog = 13,
}

/// <summary>
/// Describes a failure that occurred during a PECL I/O or validation operation.
/// </summary>
/// <param name="Code">Machine-readable error category.</param>
/// <param name="Message">Human-readable description suitable for logging.</param>
public sealed record PeclError(PeclErrorCode Code, string Message)
{
    /// <summary>Creates a <see cref="PeclErrorCode.CrcMismatch"/> error.</summary>
    /// <param name="stored">CRC32C value read from the record footer.</param>
    /// <param name="computed">CRC32C value independently computed over header + payload.</param>
    public static PeclError CrcMismatch(uint stored, uint computed) =>
        new(PeclErrorCode.CrcMismatch,
            $"CRC32C mismatch: stored=0x{stored:X8}, computed=0x{computed:X8}");

    /// <summary>Creates a <see cref="PeclErrorCode.TruncatedHeader"/> error.</summary>
    /// <param name="bytesRead">Number of header bytes that were actually available.</param>
    public static PeclError TruncatedHeader(int bytesRead) =>
        new(PeclErrorCode.TruncatedHeader,
            $"Record header truncated: read {bytesRead} of {Records.RecordHeader.SerializedSize} bytes");

    /// <summary>Creates a <see cref="PeclErrorCode.TruncatedPayload"/> error.</summary>
    /// <param name="bytesRead">Number of payload bytes that were actually available.</param>
    /// <param name="expected">Number of payload bytes declared by the record header.</param>
    public static PeclError TruncatedPayload(int bytesRead, int expected) =>
        new(PeclErrorCode.TruncatedPayload,
            $"Record payload truncated: read {bytesRead} of {expected} bytes");

    /// <summary>Creates a <see cref="PeclErrorCode.TruncatedFooter"/> error.</summary>
    /// <param name="bytesRead">Number of footer bytes that were actually available.</param>
    public static PeclError TruncatedFooter(int bytesRead) =>
        new(PeclErrorCode.TruncatedFooter,
            $"Record footer truncated: read {bytesRead} of {Records.RecordFooter.SerializedSize} bytes");

    /// <summary>Creates a <see cref="PeclErrorCode.InvalidMagic"/> error.</summary>
    /// <param name="actual">The four-byte value that was found at the record start.</param>
    public static PeclError InvalidMagic(uint actual) =>
        new(PeclErrorCode.InvalidMagic,
            $"Invalid record magic: 0x{actual:X8} (expected 0x{Records.RecordHeader.ExpectedMagic:X8})");

    /// <summary>Creates a <see cref="PeclErrorCode.InvalidVersion"/> error.</summary>
    /// <param name="actual">The version byte that was found in the record header.</param>
    public static PeclError InvalidVersion(byte actual) =>
        new(PeclErrorCode.InvalidVersion,
            $"Unsupported record version: {actual} (this implementation supports version {Records.RecordHeader.CurrentVersion})");

    /// <summary>Creates a <see cref="PeclErrorCode.PayloadTooLarge"/> error.</summary>
    /// <param name="payloadLength">The payload length declared in the corrupt header.</param>
    public static PeclError PayloadTooLarge(uint payloadLength) =>
        new(PeclErrorCode.PayloadTooLarge,
            $"Record payload length {payloadLength} exceeds the maximum addressable size ({int.MaxValue})");

    /// <summary>Creates a <see cref="PeclErrorCode.CheckpointMissing"/> error.</summary>
    /// <param name="path">The absolute path where <c>checkpoint.dat</c> was expected.</param>
    public static PeclError CheckpointMissing(string path) =>
        new(PeclErrorCode.CheckpointMissing,
            $"Checkpoint file not found: {path}");

    /// <summary>Creates a <see cref="PeclErrorCode.CheckpointTruncated"/> error.</summary>
    /// <param name="bytesRead">Number of bytes actually read from the checkpoint file.</param>
    public static PeclError CheckpointTruncated(int bytesRead) =>
        new(PeclErrorCode.CheckpointTruncated,
            $"Checkpoint file truncated: read {bytesRead} of {Checkpoint.CheckpointData.SerializedSize} bytes");

    /// <summary>Creates a <see cref="PeclErrorCode.CursorMissing"/> error.</summary>
    /// <param name="path">The absolute path where the cursor file was expected.</param>
    public static PeclError CursorMissing(string path) =>
        new(PeclErrorCode.CursorMissing,
            $"Cursor file not found: {path}");

    /// <summary>Creates a <see cref="PeclErrorCode.CursorTruncated"/> error.</summary>
    /// <param name="bytesRead">Number of bytes actually read from the cursor file.</param>
    public static PeclError CursorTruncated(int bytesRead) =>
        new(PeclErrorCode.CursorTruncated,
            $"Cursor file truncated: read {bytesRead} of at least {Cursors.CursorData.MinSerializedSize} bytes");

    /// <summary>Creates a <see cref="PeclErrorCode.CursorNameMismatch"/> error.</summary>
    /// <param name="expected">The consumer name used to locate the cursor file.</param>
    /// <param name="actual">The consumer name stored inside the cursor file.</param>
    public static PeclError CursorNameMismatch(string expected, string actual) =>
        new(PeclErrorCode.CursorNameMismatch,
            $"Cursor name mismatch: expected '{expected}', found '{actual}'");

    /// <summary>Creates a <see cref="PeclErrorCode.EndOfLog"/> error.</summary>
    /// <param name="consumerName">The name of the consumer that has reached the tail of the log.</param>
    public static PeclError EndOfLog(string consumerName) =>
        new(PeclErrorCode.EndOfLog,
            $"Consumer '{consumerName}' has reached the end of the log.");
}
