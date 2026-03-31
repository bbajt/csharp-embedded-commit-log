namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>The deserialized content of a successfully read PECL record.</summary>
/// <param name="Header">The parsed and CRC-validated record header.</param>
/// <param name="Payload">
/// The record payload bytes. <see cref="ReadOnlyMemory{T}.IsEmpty"/> is
/// <see langword="true"/> when <see cref="RecordHeader.PayloadLength"/> is zero.
/// </param>
public readonly record struct RecordReadResult(RecordHeader Header, ReadOnlyMemory<byte> Payload);
