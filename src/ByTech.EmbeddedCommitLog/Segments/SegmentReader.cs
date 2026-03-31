using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Segments;

/// <summary>
/// Performs a sequential forward scan of a segment file, deserialising and
/// CRC-validating each framed record via <see cref="RecordReader"/>.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="SegmentReader"/> opens the segment file for reading and advances its
/// position with each successful <see cref="ReadNext"/> call. The file is opened with
/// <see cref="FileShare.ReadWrite"/> so that a concurrent <see cref="SegmentWriter"/>
/// may continue appending to the same segment.
/// </para>
/// <para>
/// End-of-segment is signalled by a <see cref="PeclError"/> with code
/// <see cref="PeclErrorCode.TruncatedHeader"/> and zero bytes read — callers should treat
/// this as a clean stop rather than an error when scanning a complete or in-progress segment.
/// </para>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item><see cref="Position"/> is monotonically non-decreasing.</item>
///   <item>The reader never writes to the segment file.</item>
///   <item>After disposal, <see cref="ReadNext"/> throws <see cref="ObjectDisposedException"/>.</item>
/// </list>
/// <para>
/// Failure modes: file-not-found propagates from the constructor.
/// I/O errors during reading propagate from <see cref="ReadNext"/>.
/// Corruption (bad CRC, truncated header/payload) is returned as a
/// <see cref="Result{T,TError}"/> failure rather than thrown.
/// </para>
/// </remarks>
public sealed class SegmentReader : IDisposable
{
    private const int FileBufferSize = 65_536;

    private readonly FileStream _stream;
    private bool _disposed;

    /// <summary>The identifier of the segment this reader scans.</summary>
    public uint SegmentId { get; }

    /// <summary>Current byte position within the segment file.</summary>
    public long Position => _stream.Position;

    /// <summary>
    /// Opens the segment file for sequential reading, optionally seeking to
    /// <paramref name="startOffset"/> before the first <see cref="ReadNext"/> call.
    /// </summary>
    /// <param name="segmentsDirectory">
    /// Absolute path to the directory that holds segment files.
    /// </param>
    /// <param name="segmentId">Numeric identifier of the segment to read.</param>
    /// <param name="startOffset">
    /// Byte offset within the segment at which to begin reading. Defaults to 0 (start of file).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="segmentsDirectory"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="startOffset"/> is negative.</exception>
    /// <exception cref="FileNotFoundException">The segment file does not exist.</exception>
    public SegmentReader(string segmentsDirectory, uint segmentId, long startOffset = 0)
    {
        ArgumentNullException.ThrowIfNull(segmentsDirectory);

        if (startOffset < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startOffset),
                startOffset,
                "startOffset must be non-negative.");
        }

        SegmentId = segmentId;

        string path = SegmentNaming.GetFilePath(segmentsDirectory, segmentId);

        _stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            FileBufferSize,
            FileOptions.SequentialScan);

        if (startOffset > 0)
        {
            _stream.Seek(startOffset, SeekOrigin.Begin);
        }
    }

    /// <summary>
    /// Reads and validates the next framed record from the segment.
    /// </summary>
    /// <returns>
    /// <see cref="Result{T,TError}.Ok"/> with the deserialized record on success;
    /// <see cref="Result{T,TError}.Fail"/> with a <see cref="PeclError"/> describing the
    /// failure otherwise. A <see cref="PeclErrorCode.TruncatedHeader"/> failure with
    /// zero bytes read indicates end-of-segment (clean stop).
    /// </returns>
    /// <exception cref="ObjectDisposedException">The reader has been disposed.</exception>
    public Result<RecordReadResult, PeclError> ReadNext()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return RecordReader.Read(_stream);
    }

    /// <summary>Closes the underlying <see cref="FileStream"/>.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stream.Dispose();
    }
}
