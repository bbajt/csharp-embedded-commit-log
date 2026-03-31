using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Segments;

/// <summary>
/// Appends framed records to a single segment file, tracking write position and
/// signalling when the segment has reached its configured size limit.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="SegmentWriter"/> owns exactly one <see cref="FileStream"/> for the
/// segment file identified by <see cref="SegmentId"/>. It does not manage segment
/// rollover itself — callers (the Pipeline) check <see cref="IsFull"/>
/// after each <see cref="Append"/> and create a new writer for the next segment.
/// </para>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item><see cref="BytesWritten"/> is monotonically non-decreasing.</item>
///   <item><see cref="BytesWritten"/> equals the on-disk file length when first opened.</item>
///   <item><see cref="IsFull"/> is stable once <see langword="true"/>.</item>
///   <item>After <see cref="Seal"/>, <see cref="Append"/> always throws.</item>
///   <item>After disposal, all mutating methods throw <see cref="ObjectDisposedException"/>.</item>
/// </list>
/// <para>
/// Failure modes: directory-not-found and I/O errors from <see cref="FileStream"/> propagate
/// to the caller unchanged. <see cref="BytesWritten"/> is not updated if <see cref="Append"/>
/// throws.
/// </para>
/// </remarks>
public sealed class SegmentWriter : IDisposable
{
    private const int FileBufferSize = 65_536;

    private readonly FileStream _stream;
    private readonly long _maxSegmentSize;
    private bool _sealed;
    private bool _disposed;

    /// <summary>The identifier of the segment this writer manages.</summary>
    public uint SegmentId { get; }

    /// <summary>Total bytes written to this segment since it was created or last opened.</summary>
    /// <remarks>
    /// Written exclusively by the main thread (via <see cref="Append"/>); read by the reader
    /// loop background task (via <see cref="IsFull"/>). All reads use <c>Volatile.Read</c>
    /// and all writes use <c>Volatile.Write</c> to provide the required memory-ordering
    /// guarantees. The C# <c>volatile</c> keyword cannot be applied to <c>long</c> fields,
    /// so the <c>Volatile.*</c> helpers are used instead. Safe on 64-bit runtimes; not
    /// guaranteed atomic on 32-bit (PECL targets .NET 9 / x64).
    /// </remarks>
    public long BytesWritten => Volatile.Read(ref _bytesWritten);

    private long _bytesWritten;

    /// <summary>Current write position within the segment file. Equals <see cref="BytesWritten"/>.</summary>
    public long Position => BytesWritten;

    /// <summary>
    /// <see langword="true"/> when <see cref="BytesWritten"/> has reached or exceeded
    /// the configured maximum segment size. No further <see cref="Append"/> calls are permitted.
    /// </summary>
    public bool IsFull => BytesWritten >= _maxSegmentSize;

    /// <summary>
    /// Opens or creates the segment file for the given identifier and prepares it for appending.
    /// If the file already exists, writing resumes at the current end.
    /// </summary>
    /// <param name="segmentsDirectory">
    /// Absolute path to the directory that holds segment files.
    /// The directory must already exist; it is not created automatically.
    /// </param>
    /// <param name="segmentId">Numeric identifier of this segment.</param>
    /// <param name="maxSegmentSize">
    /// Maximum number of bytes this segment may hold. Must be at least
    /// <see cref="RecordWriter.FramingOverhead"/> (28 bytes — enough for one empty record).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="segmentsDirectory"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxSegmentSize"/> is less than <see cref="RecordWriter.FramingOverhead"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="segmentId"/> exceeds <see cref="SegmentNaming.MaxSegmentId"/>.
    /// PECL segment filenames use a 6-digit zero-padded ID; IDs ≥ 1,000,000 cannot be represented.
    /// </exception>
    /// <exception cref="DirectoryNotFoundException">
    /// <paramref name="segmentsDirectory"/> does not exist.
    /// </exception>
    public SegmentWriter(string segmentsDirectory, uint segmentId, long maxSegmentSize)
    {
        ArgumentNullException.ThrowIfNull(segmentsDirectory);

        if (maxSegmentSize < RecordWriter.FramingOverhead)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxSegmentSize),
                maxSegmentSize,
                $"maxSegmentSize must be at least {RecordWriter.FramingOverhead} bytes (one empty record).");
        }

        if (segmentId > SegmentNaming.MaxSegmentId)
        {
            throw new InvalidOperationException(
                $"Segment ID {segmentId} exceeds the maximum ({SegmentNaming.MaxSegmentId}). " +
                "PECL segment filenames use a 6-digit zero-padded ID (log-NNNNNN.seg). " +
                "Archive or compact the log before creating new segments.");
        }

        SegmentId = segmentId;
        _maxSegmentSize = maxSegmentSize;

        string path = SegmentNaming.GetFilePath(segmentsDirectory, segmentId);

        _stream = new FileStream(
            path,
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.Read,
            FileBufferSize,
            FileOptions.SequentialScan);

        // Seek to end so we resume correctly on an existing segment.
        _stream.Seek(0, SeekOrigin.End);
        Volatile.Write(ref _bytesWritten, _stream.Position);
    }

    /// <summary>
    /// Serialises <paramref name="payload"/> as a framed record and appends it to the segment.
    /// </summary>
    /// <param name="payload">Raw payload bytes. May be empty.</param>
    /// <param name="seqNo">Globally monotonic sequence number assigned by the writer task.</param>
    /// <param name="contentType">Advisory payload encoding hint. Defaults to <see cref="ContentType.Unknown"/>.</param>
    /// <param name="flags">Record flags. Defaults to <see cref="RecordFlags.None"/>.</param>
    /// <param name="schemaId">Optional schema identifier. Defaults to 0 (none).</param>
    /// <returns>The byte offset within this segment at which the record starts.</returns>
    /// <exception cref="ObjectDisposedException">The writer has been disposed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The segment is sealed or <see cref="IsFull"/> is <see langword="true"/>.
    /// </exception>
    public long Append(
        ReadOnlySpan<byte> payload,
        ulong seqNo,
        ContentType contentType = ContentType.Unknown,
        RecordFlags flags = RecordFlags.None,
        uint schemaId = 0u)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_sealed)
        {
            throw new InvalidOperationException("Segment is sealed.");
        }

        if (IsFull)
        {
            throw new InvalidOperationException("Segment is full.");
        }

        long offset = _bytesWritten;
        long written = RecordWriter.Write(_stream, payload, seqNo, contentType, flags, schemaId);
        // Single-writer invariant: only Pipeline.Append calls this method, always on the
        // main thread. Volatile.Read on the RHS is required for correctness under the C#
        // memory model and guards against a second writer path being added in the future.
        Volatile.Write(ref _bytesWritten, Volatile.Read(ref _bytesWritten) + written);
        return offset;
    }

    /// <summary>
    /// Flushes any buffered data to the operating system page cache.
    /// Does not guarantee durability — use an fsync at the Pipeline layer for that.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The writer has been disposed.</exception>
    public void Flush()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _stream.Flush();
    }

    /// <summary>
    /// Flushes all buffered segment data to the physical storage medium (fsync / FlushFileBuffers).
    /// Call this before writing a checkpoint to guarantee the checkpoint invariant:
    /// a checkpoint always points to data that is durable on disk.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="Flush"/>, which only flushes to the OS page cache,
    /// this method blocks until the OS confirms the data is on the storage device.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The writer has been disposed.</exception>
    public void FlushToDisk()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _stream.Flush(flushToDisk: true);
    }

    /// <summary>
    /// Seals the segment: flushes buffered data and prevents further <see cref="Append"/> calls.
    /// After sealing, the file remains open until <see cref="Dispose"/> is called.
    /// Calling <see cref="Seal"/> on an already-sealed or disposed writer is a no-op.
    /// </summary>
    public void Seal()
    {
        if (_sealed || _disposed)
        {
            return;
        }

        _stream.Flush();
        _sealed = true;
    }

    /// <summary>Seals (if not already sealed) and closes the underlying <see cref="FileStream"/>.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        Seal();
        _stream.Dispose();
    }
}
