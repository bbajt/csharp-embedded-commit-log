using System.Buffers.Binary;

namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Serializes a single PECL record to a stream.
/// </summary>
/// <remarks>
/// A record consists of a 24-byte fixed header (<see cref="RecordHeader.SerializedSize"/>),
/// a variable-length payload, and a 4-byte CRC32C footer (<see cref="RecordFooter.SerializedSize"/>).
/// The CRC32C is computed over both the serialized header bytes and the payload bytes.
/// </remarks>
public static class RecordWriter
{
    /// <summary>
    /// Total fixed framing overhead per record in bytes (header + footer), excluding the payload.
    /// </summary>
    public const int FramingOverhead = RecordHeader.SerializedSize + RecordFooter.SerializedSize;

    /// <summary>
    /// Writes a single framed PECL record to <paramref name="destination"/>.
    /// </summary>
    /// <param name="destination">
    /// Writable stream. The stream position advances by the total number of bytes written.
    /// </param>
    /// <param name="payload">
    /// Opaque payload bytes. May be empty. Length must not exceed <see cref="uint.MaxValue"/>
    /// (enforced implicitly because <see cref="ReadOnlySpan{T}.Length"/> is a non-negative
    /// <see cref="int"/>).
    /// </param>
    /// <param name="seqNo">
    /// Monotonically increasing sequence number. The caller (pipeline writer task) is
    /// responsible for assigning correct values; this method stores whatever is passed.
    /// </param>
    /// <param name="contentType">Advisory payload encoding hint. Default: <see cref="ContentType.Unknown"/>.</param>
    /// <param name="flags">Record-level flags. Default: <see cref="RecordFlags.None"/>.</param>
    /// <param name="schemaId">Schema registry identifier. Pass <c>0</c> when unused.</param>
    /// <returns>
    /// Total bytes written to <paramref name="destination"/>:
    /// <c><see cref="FramingOverhead"/> + payload.Length</c>.
    /// The return type is <see langword="long"/> to avoid overflow for near-maximum payloads.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="destination"/> is <see langword="null"/>.</exception>
    public static long Write(
        Stream destination,
        ReadOnlySpan<byte> payload,
        ulong seqNo,
        ContentType contentType = ContentType.Unknown,
        RecordFlags flags = RecordFlags.None,
        uint schemaId = 0u)
    {
        ArgumentNullException.ThrowIfNull(destination);

        Span<byte> headerBuf = stackalloc byte[RecordHeader.SerializedSize];
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuf, RecordHeader.ExpectedMagic);
        headerBuf[4] = RecordHeader.CurrentVersion;
        headerBuf[5] = (byte)flags;
        headerBuf[6] = (byte)contentType;
        headerBuf[7] = 0; // Reserved — must always be zero
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuf[8..], schemaId);
        BinaryPrimitives.WriteUInt64LittleEndian(headerBuf[12..], seqNo);
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuf[20..], (uint)payload.Length);

        uint crc = CrcUtility.ComputeRecordCrc32C(headerBuf, payload);

        Span<byte> footerBuf = stackalloc byte[RecordFooter.SerializedSize];
        BinaryPrimitives.WriteUInt32LittleEndian(footerBuf, crc);

        destination.Write(headerBuf);
        destination.Write(payload);
        destination.Write(footerBuf);

        return RecordHeader.SerializedSize + (long)payload.Length + RecordFooter.SerializedSize;
    }
}
