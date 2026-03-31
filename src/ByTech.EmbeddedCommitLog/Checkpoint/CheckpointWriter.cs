using System.Buffers.Binary;
using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Checkpoint;

/// <summary>
/// Persists a <see cref="CheckpointData"/> snapshot to disk using a crash-safe
/// write-temp + fsync + rename strategy.
/// </summary>
/// <remarks>
/// <para>Write sequence:</para>
/// <list type="number">
///   <item>Serialize <see cref="CheckpointData"/> into a 36-byte buffer and write it to
///     <c>checkpoint.tmp</c> in the pipeline directory.</item>
///   <item>Call <c>FileStream.Flush(flushToDisk: true)</c> to fsync the temp file.</item>
///   <item>Rename <c>checkpoint.tmp</c> → <c>checkpoint.dat</c> (overwriting any existing checkpoint).</item>
/// </list>
/// <para>
/// A crash between steps 1 and 3 leaves <c>checkpoint.dat</c> unchanged.
/// A crash during step 1 leaves <c>checkpoint.tmp</c> partial or absent.
/// Neither scenario corrupts the last valid checkpoint.
/// </para>
/// <para>Failure modes: directory-not-found and I/O errors propagate to the caller unchanged.
/// A stale <c>checkpoint.tmp</c> from a prior crash is silently overwritten.</para>
/// </remarks>
public static class CheckpointWriter
{
    private const string TempFileName = "checkpoint.tmp";
    private const string FinalFileName = "checkpoint.dat";

    /// <summary>
    /// Atomically writes <paramref name="data"/> as the new checkpoint in
    /// <paramref name="pipelineDirectory"/>.
    /// </summary>
    /// <param name="pipelineDirectory">
    /// Absolute path to the pipeline root directory.
    /// The directory must exist; it is not created automatically.
    /// </param>
    /// <param name="data">The checkpoint state to persist.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="pipelineDirectory"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="DirectoryNotFoundException">
    /// <paramref name="pipelineDirectory"/> does not exist.
    /// </exception>
    public static void Write(string pipelineDirectory, CheckpointData data)
    {
        ArgumentNullException.ThrowIfNull(pipelineDirectory);

        string tmpPath = Path.Combine(pipelineDirectory, TempFileName);
        string finalPath = Path.Combine(pipelineDirectory, FinalFileName);

        Span<byte> buf = stackalloc byte[CheckpointData.SerializedSize];

        // Header: magic, version, reserved (3 bytes already zero from stackalloc)
        BinaryPrimitives.WriteUInt32LittleEndian(buf, CheckpointData.ExpectedMagic);
        buf[4] = CheckpointData.CurrentVersion;
        // buf[5..7] = 0  (Reserved — stackalloc initializes to zero)

        // Fields
        BinaryPrimitives.WriteUInt32LittleEndian(buf[8..], data.LastSegmentId);
        BinaryPrimitives.WriteInt64LittleEndian(buf[12..], data.LastOffset);
        BinaryPrimitives.WriteUInt64LittleEndian(buf[20..], data.LastSeqNo);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[28..], data.ActiveSegmentId);

        // CRC covers the first 32 bytes
        uint crc = CrcUtility.ComputeCrc32(buf[..32]);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[32..], crc);

        using (var stream = new FileStream(
            tmpPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 512))
        {
            stream.Write(buf);
            stream.Flush(flushToDisk: true);
        }

        File.Move(tmpPath, finalPath, overwrite: true);
    }
}
