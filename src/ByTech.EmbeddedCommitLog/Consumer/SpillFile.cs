using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// Append-only overflow store for a single <see cref="SinkLane"/>. Records written here
/// are replayed back into the lane's channel when space becomes available.
/// </summary>
/// <remarks>
/// <para>
/// The spill file uses PECL record framing (<see cref="RecordWriter.Write"/>) so that
/// crash-truncated tails are detected via CRC validation during replay.
/// </para>
/// <para>
/// Compression flags are stripped on write: <see cref="LogRecord.Payload"/> is always
/// the decoded (decompressed) payload, so the spill file stores uncompressed records.
/// </para>
/// <para>
/// Two <see cref="FileStream"/> handles are kept open simultaneously: an append-only
/// write handle and a sequential read handle. Only the reader loop task calls
/// <see cref="Append"/> and <see cref="TryReadNext"/>, so no synchronisation is needed.
/// </para>
/// </remarks>
internal sealed class SpillFile : IDisposable
{
    /// <summary>All flag bits related to compression; stripped when appending to spill.</summary>
    private const RecordFlags CompressionFlags =
        RecordFlags.IsCompressed | RecordFlags.CompressionAlg0 | RecordFlags.CompressionAlg1;

    private readonly string _path;
    private readonly FileStream _writeStream;
    private readonly FileStream _readStream;
    private bool _disposed;

    /// <summary>
    /// Opens (or creates) the spill file at <paramref name="path"/> for append and replay.
    /// If the file already exists (crash recovery), the read stream starts at offset 0 so
    /// all previously-written records are replayed on the first <see cref="TryReadNext"/> call.
    /// </summary>
    /// <param name="path">Absolute path to the spill file.</param>
    internal SpillFile(string path)
    {
        _path = path;
        _writeStream = new FileStream(
            path,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 4096,
            FileOptions.WriteThrough);
        try
        {
            _readStream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                bufferSize: 4096,
                FileOptions.SequentialScan);
        }
        catch
        {
            // Dispose the already-opened write handle to avoid leaking it
            // if the read stream cannot be opened.
            _writeStream.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Creates the parent directory (idempotent) then returns a new <see cref="SpillFile"/>
    /// at <paramref name="path"/>. If a file already exists it is opened for
    /// crash-recovery replay rather than truncated.
    /// </summary>
    /// <param name="path">Absolute path to the spill file.</param>
    /// <returns>A new <see cref="SpillFile"/> instance.</returns>
    internal static SpillFile Create(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        return new SpillFile(path);
    }

    /// <summary>
    /// Appends <paramref name="record"/> to the spill file using PECL record framing.
    /// Compression flags are stripped; the payload is stored verbatim
    /// (<see cref="LogRecord.Payload"/> is always the decoded payload).
    /// </summary>
    /// <param name="record">The record to spill.</param>
    /// <exception cref="IOException">The write or flush failed.</exception>
    internal void Append(LogRecord record)
    {
        RecordFlags spillFlags = record.Header.Flags & ~CompressionFlags;
        RecordWriter.Write(
            _writeStream,
            record.Payload,
            record.Header.SeqNo,
            record.Header.ContentType,
            spillFlags,
            record.Header.SchemaId);
        _writeStream.Flush();
    }

    /// <summary>
    /// Attempts to read the next record from the spill file.
    /// Returns <see langword="false"/> at EOF or on any framing / CRC error
    /// (corrupt tail — replay stops and the remaining bytes are abandoned).
    /// </summary>
    /// <param name="record">
    /// The reconstructed <see cref="LogRecord"/> on success; <see langword="null"/> otherwise.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if a complete, valid record was read;
    /// <see langword="false"/> at EOF or on corruption.
    /// </returns>
    internal bool TryReadNext(out LogRecord? record)
    {
        try
        {
            var result = RecordReader.Read(_readStream);
            if (result.IsFailure)
            {
                record = null;
                return false;
            }

            byte[] payload = result.Value.Payload.ToArray();
            record = new LogRecord(result.Value.Header, payload);
            return true;
        }
        catch (IOException)
        {
            // Disk I/O error during replay — treat as corrupt tail; stop replay.
            record = null;
            return false;
        }
    }

    /// <summary>
    /// <see langword="true"/> when all written records have been read back.
    /// Compares the read-stream position against the write-stream position
    /// (the current end-of-file offset after the last <see cref="Append"/> call).
    /// </summary>
    internal bool IsFullyReplayed => _readStream.Position >= _writeStream.Position;

    /// <summary>Current byte offset of the read stream.</summary>
    internal long ReadPosition => _readStream.Position;

    /// <summary>
    /// Seeks the read stream to <paramref name="position"/>.
    /// Used to undo a <see cref="TryReadNext"/> call when the channel is full,
    /// so the record is retried on the next drain attempt.
    /// </summary>
    /// <param name="position">Absolute byte offset to seek to.</param>
    internal void SeekRead(long position) => _readStream.Seek(position, SeekOrigin.Begin);

    /// <summary>
    /// Disposes both stream handles then deletes the spill file from disk.
    /// Call after full replay is confirmed to reclaim disk space.
    /// </summary>
    internal void DeleteAndDispose()
    {
        Dispose();
        File.Delete(_path);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _writeStream.Dispose();
        _readStream.Dispose();
    }
}
