using System.Buffers.Binary;
using System.Text;
using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Cursors;

/// <summary>
/// Reads and validates the <c>{consumerName}.cur</c> file from the pipeline's cursors directory,
/// returning a <see cref="CursorData"/> snapshot or a descriptive failure.
/// </summary>
/// <remarks>
/// <para>
/// A missing cursor file is a valid, expected state (new consumer or file deleted).
/// It is surfaced as <see cref="PeclErrorCode.CursorMissing"/> rather than thrown
/// as an exception, so callers can distinguish it from an I/O error.
/// </para>
/// <para>
/// The reader ignores any <c>{name}.tmp</c> file: a stale temp file from a prior crash
/// does not affect the result.
/// </para>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item>This method never writes to or modifies any file.</item>
///   <item>It is idempotent: repeated calls return the same result for unchanged files.</item>
///   <item>A valid cursor is never returned silently if the CRC check fails.</item>
///   <item>The <see cref="CursorData.ConsumerName"/> stored in the file is validated against
///     the requested consumer name; a mismatch is surfaced as an error.</item>
/// </list>
/// </remarks>
public static class CursorReader
{
    private const string FinalSuffix = ".cur";

    /// <summary>
    /// Reads the cursor for <paramref name="consumerName"/> from <paramref name="cursorsDirectory"/>.
    /// </summary>
    /// <param name="cursorsDirectory">
    /// Absolute path to the pipeline's <c>cursors/</c> subdirectory.
    /// </param>
    /// <param name="consumerName">
    /// Name of the consumer whose cursor file to read.
    /// Used to construct the file path (<c>{consumerName}.cur</c>) and to validate
    /// the name stored inside the file.
    /// </param>
    /// <returns>
    /// <see cref="Result{T,TError}.Ok"/> with the deserialized snapshot on success;
    /// <see cref="Result{T,TError}.Fail"/> with a <see cref="PeclError"/> otherwise.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="cursorsDirectory"/> or <paramref name="consumerName"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="consumerName"/> is empty.
    /// </exception>
    public static Result<CursorData, PeclError> Read(string cursorsDirectory, string consumerName)
    {
        ArgumentNullException.ThrowIfNull(cursorsDirectory);
        ArgumentNullException.ThrowIfNull(consumerName);

        if (consumerName.Length == 0)
        {
            throw new ArgumentException("Consumer name must be non-empty.", nameof(consumerName));
        }

        string finalPath = Path.Combine(cursorsDirectory, consumerName + FinalSuffix);

        if (!File.Exists(finalPath))
        {
            return Result<CursorData, PeclError>.Fail(PeclError.CursorMissing(finalPath));
        }

        Span<byte> buf = stackalloc byte[CursorData.MaxSerializedSize];

        int bytesRead;
        using (var stream = new FileStream(
            finalPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 512))
        {
            bytesRead = stream.ReadAtLeast(
                buf,
                CursorData.MinSerializedSize,
                throwOnEndOfStream: false);
        }

        if (bytesRead < CursorData.MinSerializedSize)
        {
            return Result<CursorData, PeclError>.Fail(PeclError.CursorTruncated(bytesRead));
        }

        // Validate magic
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(buf);
        if (magic != CursorData.ExpectedMagic)
        {
            return Result<CursorData, PeclError>.Fail(PeclError.InvalidMagic(magic));
        }

        // Validate version
        byte version = buf[4];
        if (version != CursorData.CurrentVersion)
        {
            return Result<CursorData, PeclError>.Fail(PeclError.InvalidVersion(version));
        }

        // Validate NameLen
        int nameLen = BinaryPrimitives.ReadUInt16LittleEndian(buf[6..]);
        if (nameLen > CursorData.MaxConsumerNameBytes)
        {
            return Result<CursorData, PeclError>.Fail(PeclError.CursorTruncated(bytesRead));
        }

        int expectedSize = CursorData.MinSerializedSize + nameLen;
        if (bytesRead < expectedSize)
        {
            return Result<CursorData, PeclError>.Fail(PeclError.CursorTruncated(bytesRead));
        }

        int tail = 8 + nameLen;

        // Validate CRC (covers bytes 0..(tail+19), i.e., everything except the last 4 bytes)
        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(buf[(tail + 20)..]);
        uint computedCrc = CrcUtility.ComputeCrc32(buf[..(tail + 20)]);
        if (storedCrc != computedCrc)
        {
            return Result<CursorData, PeclError>.Fail(PeclError.CrcMismatch(storedCrc, computedCrc));
        }

        // Validate stored consumer name matches the requested name
        string storedName = Encoding.UTF8.GetString(buf[8..(8 + nameLen)]);
        if (storedName != consumerName)
        {
            return Result<CursorData, PeclError>.Fail(
                PeclError.CursorNameMismatch(consumerName, storedName));
        }

        // Deserialize fields
        uint segmentId = BinaryPrimitives.ReadUInt32LittleEndian(buf[tail..]);
        long offset = BinaryPrimitives.ReadInt64LittleEndian(buf[(tail + 4)..]);
        ulong seqNo = BinaryPrimitives.ReadUInt64LittleEndian(buf[(tail + 12)..]);

        return Result<CursorData, PeclError>.Ok(
            new CursorData(storedName, segmentId, offset, seqNo));
    }
}
