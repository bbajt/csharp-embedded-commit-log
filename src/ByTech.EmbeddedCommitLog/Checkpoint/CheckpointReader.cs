using System.Buffers.Binary;
using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Checkpoint;

/// <summary>
/// Reads and validates the <c>checkpoint.dat</c> file from a pipeline directory,
/// returning a <see cref="CheckpointData"/> snapshot or a descriptive failure.
/// </summary>
/// <remarks>
/// <para>
/// A missing <c>checkpoint.dat</c> is a valid, expected state (first run or no checkpoint
/// written yet). It is surfaced as <see cref="PeclErrorCode.CheckpointMissing"/> rather
/// than thrown as an exception, so callers can distinguish it from an I/O error.
/// </para>
/// <para>
/// The reader ignores <c>checkpoint.tmp</c>: a stale temp file from a prior crash
/// does not affect the result.
/// </para>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item>This method never writes to or modifies any file.</item>
///   <item>It is idempotent: repeated calls return the same result for unchanged files.</item>
///   <item>A valid checkpoint is never returned silently if the CRC check fails.</item>
/// </list>
/// </remarks>
public static class CheckpointReader
{
    private const string FinalFileName = "checkpoint.dat";

    /// <summary>
    /// Reads the checkpoint from <paramref name="pipelineDirectory"/>.
    /// </summary>
    /// <param name="pipelineDirectory">
    /// Absolute path to the pipeline root directory.
    /// </param>
    /// <returns>
    /// <see cref="Result{T,TError}.Ok"/> with the deserialized snapshot on success;
    /// <see cref="Result{T,TError}.Fail"/> with a <see cref="PeclError"/> otherwise.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="pipelineDirectory"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// The CRC check detects bit-rot corruption and complete record substitution.
    /// It cannot detect a torn write that overwrites the checkpoint record with bytes
    /// whose CRC happens to match — for example, if the page cache flushes a partial
    /// record before the atomic <c>File.Move</c> of the temp file completes. In
    /// practice, <see cref="CheckpointWriter"/> uses write-to-temp +
    /// <c>Flush(flushToDisk: true)</c> + <c>File.Move(overwrite: true)</c>, which
    /// makes torn checkpoint writes extremely unlikely on NTFS and POSIX. This is a
    /// residual theoretical risk, not a known practical defect.
    /// </remarks>
    public static Result<CheckpointData, PeclError> Read(string pipelineDirectory)
    {
        ArgumentNullException.ThrowIfNull(pipelineDirectory);

        string finalPath = Path.Combine(pipelineDirectory, FinalFileName);

        if (!File.Exists(finalPath))
        {
            return Result<CheckpointData, PeclError>.Fail(PeclError.CheckpointMissing(finalPath));
        }

        Span<byte> buf = stackalloc byte[CheckpointData.SerializedSize];

        using (var stream = new FileStream(
            finalPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 512))
        {
            int bytesRead = stream.ReadAtLeast(
                buf,
                CheckpointData.SerializedSize,
                throwOnEndOfStream: false);

            if (bytesRead < CheckpointData.SerializedSize)
            {
                return Result<CheckpointData, PeclError>.Fail(
                    PeclError.CheckpointTruncated(bytesRead));
            }
        }

        // Validate magic
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(buf);
        if (magic != CheckpointData.ExpectedMagic)
        {
            return Result<CheckpointData, PeclError>.Fail(PeclError.InvalidMagic(magic));
        }

        // Validate version
        byte version = buf[4];
        if (version != CheckpointData.CurrentVersion)
        {
            return Result<CheckpointData, PeclError>.Fail(PeclError.InvalidVersion(version));
        }

        // Validate CRC (covers the first 32 bytes; CRC is stored in the final 4)
        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(buf[32..]);
        uint computedCrc = CrcUtility.ComputeCrc32(buf[..32]);
        if (storedCrc != computedCrc)
        {
            return Result<CheckpointData, PeclError>.Fail(PeclError.CrcMismatch(storedCrc, computedCrc));
        }

        // Deserialize fields
        uint lastSegmentId = BinaryPrimitives.ReadUInt32LittleEndian(buf[8..]);
        long lastOffset = BinaryPrimitives.ReadInt64LittleEndian(buf[12..]);
        ulong lastSeqNo = BinaryPrimitives.ReadUInt64LittleEndian(buf[20..]);
        uint activeSegmentId = BinaryPrimitives.ReadUInt32LittleEndian(buf[28..]);

        return Result<CheckpointData, PeclError>.Ok(
            new CheckpointData(lastSegmentId, lastOffset, lastSeqNo, activeSegmentId));
    }
}
