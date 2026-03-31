using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.TestUtilities.Records;

/// <summary>
/// Shared test fixtures for PECL record format tests.
/// Provides canonical payloads and a helper for writing complete valid records to a stream.
/// </summary>
public static class RecordTestData
{
    /// <summary>A zero-length payload for testing empty-record round-trips.</summary>
    public static readonly byte[] EmptyPayload = [];

    /// <summary>A 5-byte payload for lightweight round-trip tests.</summary>
    public static readonly byte[] SmallPayload = [0x01, 0x02, 0x03, 0x04, 0x05];

    /// <summary>A 1 024-byte repeating payload for larger allocation and round-trip tests.</summary>
    public static readonly byte[] LargePayload =
        Enumerable.Range(0, 1024).Select(i => (byte)(i % 256)).ToArray();

    /// <summary>Default sequence number used when building test records.</summary>
    public const ulong DefaultSeqNo = 42UL;

    /// <summary>Default content type used when building test records.</summary>
    public const ContentType DefaultContentType = ContentType.Json;

    /// <summary>Default record flags used when building test records.</summary>
    public const RecordFlags DefaultFlags = RecordFlags.None;

    /// <summary>Default schema ID used when building test records (0 = no schema).</summary>
    public const uint DefaultSchemaId = 0u;

    /// <summary>
    /// Writes a single valid PECL record containing <paramref name="payload"/> to a new
    /// <see cref="MemoryStream"/>, rewinds the stream to position 0, and returns it.
    /// </summary>
    /// <param name="payload">Payload bytes to embed in the record.</param>
    /// <param name="seqNo">Sequence number to assign. Defaults to <see cref="DefaultSeqNo"/>.</param>
    /// <returns>A <see cref="MemoryStream"/> positioned at 0 containing one complete record.</returns>
    public static MemoryStream BuildValidRecord(byte[] payload, ulong seqNo = DefaultSeqNo)
    {
        var ms = new MemoryStream();
        RecordWriter.Write(ms, payload, seqNo, DefaultContentType, DefaultFlags, DefaultSchemaId);
        ms.Position = 0;
        return ms;
    }
}
