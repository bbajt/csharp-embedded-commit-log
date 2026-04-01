namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>Compression algorithm applied to record payloads before writing to disk.</summary>
/// <remarks>
/// Compression is transparent to consumers —
/// <see cref="Consumer.LogRecord.Payload"/> always contains decompressed bytes.
/// Configure via <see cref="Pipeline.PipelineConfiguration.CompressionAlgorithm"/>.
/// </remarks>
public enum CompressionAlgorithm
{
    /// <summary>No compression. Default.</summary>
    None = 0,

    /// <summary>
    /// Brotli compression at quality level 1.
    /// Uses <see cref="System.IO.Compression.BrotliEncoder"/>.
    /// Payloads that do not compress (compressed size &gt;= original) are stored
    /// uncompressed automatically.
    /// </summary>
    Brotli = 1,
}
