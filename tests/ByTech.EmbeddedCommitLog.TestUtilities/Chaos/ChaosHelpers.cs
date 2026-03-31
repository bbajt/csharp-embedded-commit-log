namespace ByTech.EmbeddedCommitLog.TestUtilities.Chaos;

/// <summary>
/// Static helpers for simulating crash conditions in PECL integration and chaos tests.
/// All methods manipulate files directly to emulate partial writes, corruption, or
/// missing files that would result from a sudden process termination.
/// </summary>
/// <remarks>
/// The segment naming convention (<c>log-NNNNNN.seg</c>) is replicated here because
/// <c>SegmentNaming</c> is <see langword="internal"/>. The format is frozen by ADR and
/// therefore safe to duplicate in test infrastructure.
/// </remarks>
public static class ChaosHelpers
{
    // ── Segment helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Returns the absolute path of the segment file for the given
    /// <paramref name="segmentId"/> within the pipeline's segments directory.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory (contains <c>segments/</c>).</param>
    /// <param name="segmentId">The numeric segment identifier.</param>
    public static string SegmentPath(string rootDirectory, uint segmentId) =>
        Path.Combine(rootDirectory, "segments", $"log-{segmentId:D6}.seg");

    /// <summary>
    /// Returns the current on-disk byte size of the segment file for
    /// <paramref name="segmentId"/>. The file must exist.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    /// <param name="segmentId">The numeric segment identifier.</param>
    public static long GetSegmentFileSize(string rootDirectory, uint segmentId) =>
        new FileInfo(SegmentPath(rootDirectory, segmentId)).Length;

    /// <summary>
    /// Truncates the segment file for <paramref name="segmentId"/> to exactly
    /// <paramref name="length"/> bytes, simulating a crash that discarded buffered data.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    /// <param name="segmentId">The numeric segment identifier.</param>
    /// <param name="length">Target byte length. Must be ≥ 0 and ≤ current file size.</param>
    public static void TruncateSegment(string rootDirectory, uint segmentId, long length)
    {
        string path = SegmentPath(rootDirectory, segmentId);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
        fs.SetLength(length);
    }

    /// <summary>
    /// Overwrites <paramref name="bytes"/> into the segment file at a position measured
    /// <paramref name="offsetFromEnd"/> bytes from the end of the file. Used to corrupt
    /// footers (CRC) or headers of the last record.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    /// <param name="segmentId">The numeric segment identifier.</param>
    /// <param name="offsetFromEnd">
    /// Negative offset from the end of the file, e.g. 4 = last 4 bytes.
    /// </param>
    /// <param name="bytes">Replacement bytes to write.</param>
    public static void CorruptSegmentBytesAtEnd(
        string rootDirectory,
        uint segmentId,
        int offsetFromEnd,
        byte[] bytes)
    {
        string path = SegmentPath(rootDirectory, segmentId);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
        fs.Seek(-offsetFromEnd, SeekOrigin.End);
        fs.Write(bytes);
    }

    // ── Checkpoint helpers ────────────────────────────────────────────────────

    /// <summary>
    /// Returns the absolute path of the pipeline's <c>checkpoint.dat</c> file.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    public static string CheckpointPath(string rootDirectory) =>
        Path.Combine(rootDirectory, "checkpoint.dat");

    /// <summary>
    /// Deletes the pipeline's <c>checkpoint.dat</c> file, simulating a crash during
    /// the write-temp + rename sequence before the rename completed.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    public static void DeleteCheckpoint(string rootDirectory) =>
        File.Delete(CheckpointPath(rootDirectory));

    /// <summary>
    /// Overwrites the last 4 bytes of <c>checkpoint.dat</c> with <c>0xDEADBEEF</c>,
    /// producing a CRC mismatch that forces recovery to fall back to a full scan.
    /// The file must exist and be at least 4 bytes long.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    public static void CorruptCheckpointCrc(string rootDirectory)
    {
        string path = CheckpointPath(rootDirectory);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
        fs.Seek(-4, SeekOrigin.End);
        fs.Write(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });
    }

    /// <summary>
    /// Truncates <c>checkpoint.dat</c> to <paramref name="length"/> bytes, simulating a
    /// crash mid-write that left a partial checkpoint file on disk.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    /// <param name="length">Target byte length (must be less than <c>CheckpointData.SerializedSize</c> = 36).</param>
    public static void TruncateCheckpoint(string rootDirectory, long length)
    {
        string path = CheckpointPath(rootDirectory);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
        fs.SetLength(length);
    }

    // ── Cursor helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the absolute path of the cursor file for <paramref name="consumerName"/>.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    /// <param name="consumerName">The consumer name (used as the base filename).</param>
    public static string CursorPath(string rootDirectory, string consumerName) =>
        Path.Combine(rootDirectory, "cursors", $"{consumerName}.cur");

    /// <summary>
    /// Deletes the cursor file for <paramref name="consumerName"/>, simulating a crash
    /// before the cursor was ever flushed. The consumer will replay from (0, 0) on restart.
    /// </summary>
    /// <param name="rootDirectory">The pipeline root directory.</param>
    /// <param name="consumerName">The consumer whose cursor file to delete.</param>
    public static void DeleteCursor(string rootDirectory, string consumerName) =>
        File.Delete(CursorPath(rootDirectory, consumerName));
}
