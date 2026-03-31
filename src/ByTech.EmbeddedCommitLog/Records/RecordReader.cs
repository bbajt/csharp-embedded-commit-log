using System.Buffers.Binary;

namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Deserializes and validates a single PECL record from a stream.
/// </summary>
/// <remarks>
/// The reader validates magic, version, and CRC32C before returning a result.
/// On any failure the stream position is indeterminate; callers should not
/// attempt to resume reading from the same stream after a failure.
/// </remarks>
public static class RecordReader
{
    /// <summary>
    /// Reads and validates one PECL record from <paramref name="source"/>,
    /// starting at the stream's current position.
    /// </summary>
    /// <param name="source">Readable stream positioned at the start of a record.</param>
    /// <returns>
    /// <see cref="Result{T,TError}.IsSuccess"/> with a <see cref="RecordReadResult"/>
    /// when a complete, valid record was read; <see cref="Result{T,TError}.IsFailure"/>
    /// with a <see cref="PeclError"/> describing the problem otherwise.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Result<RecordReadResult, PeclError> Read(Stream source)
    {
        ArgumentNullException.ThrowIfNull(source);

        // ── 1. Read and validate the 24-byte header ─────────────────────────
        Span<byte> headerBuf = stackalloc byte[RecordHeader.SerializedSize];
        int headerRead = source.ReadAtLeast(
            headerBuf, RecordHeader.SerializedSize, throwOnEndOfStream: false);

        if (headerRead < RecordHeader.SerializedSize)
        {
            return Result<RecordReadResult, PeclError>.Fail(PeclError.TruncatedHeader(headerRead));
        }

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(headerBuf);
        if (magic != RecordHeader.ExpectedMagic)
        {
            return Result<RecordReadResult, PeclError>.Fail(PeclError.InvalidMagic(magic));
        }

        byte version = headerBuf[4];
        if (version != RecordHeader.CurrentVersion)
        {
            return Result<RecordReadResult, PeclError>.Fail(PeclError.InvalidVersion(version));
        }

        var flags = (RecordFlags)headerBuf[5];
        var contentType = (ContentType)headerBuf[6];
        // headerBuf[7] is Reserved — intentionally ignored on read
        uint schemaId = BinaryPrimitives.ReadUInt32LittleEndian(headerBuf[8..]);
        ulong seqNo = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf[12..]);
        uint payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(headerBuf[20..]);

        // ── 2. Guard against corrupt/future oversized payload declarations ──
        if (payloadLength > (uint)int.MaxValue)
        {
            return Result<RecordReadResult, PeclError>.Fail(PeclError.PayloadTooLarge(payloadLength));
        }

        // ── 3. Read payload ──────────────────────────────────────────────────
        byte[] payloadBytes = payloadLength == 0
            ? Array.Empty<byte>()
            : GC.AllocateUninitializedArray<byte>((int)payloadLength);

        if (payloadLength > 0)
        {
            int payloadRead = source.ReadAtLeast(
                payloadBytes, (int)payloadLength, throwOnEndOfStream: false);

            if (payloadRead < (int)payloadLength)
            {
                return Result<RecordReadResult, PeclError>.Fail(
                    PeclError.TruncatedPayload(payloadRead, (int)payloadLength));
            }
        }

        // ── 4. Read the 4-byte footer ────────────────────────────────────────
        Span<byte> footerBuf = stackalloc byte[RecordFooter.SerializedSize];
        int footerRead = source.ReadAtLeast(
            footerBuf, RecordFooter.SerializedSize, throwOnEndOfStream: false);

        if (footerRead < RecordFooter.SerializedSize)
        {
            return Result<RecordReadResult, PeclError>.Fail(PeclError.TruncatedFooter(footerRead));
        }

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(footerBuf);

        // ── 5. Verify CRC32C ─────────────────────────────────────────────────
        uint computedCrc = CrcUtility.ComputeRecordCrc32C(headerBuf, payloadBytes);
        if (storedCrc != computedCrc)
        {
            return Result<RecordReadResult, PeclError>.Fail(PeclError.CrcMismatch(storedCrc, computedCrc));
        }

        var header = new RecordHeader(
            magic,
            version,
            flags,
            contentType,
            Reserved: 0,
            schemaId,
            seqNo,
            payloadLength);

        return Result<RecordReadResult, PeclError>.Ok(new RecordReadResult(header, payloadBytes));
    }
}
