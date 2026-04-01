using Xunit;
using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities;

namespace ByTech.EmbeddedCommitLog.Tests.Consumer;

/// <summary>Unit tests for <see cref="SpillFile"/> round-trip and corrupt-tail handling.</summary>
public sealed class SpillFileTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    public SpillFileTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }

    private string SpillPath() => Path.Combine(_dir, "test.spill");

    private static LogRecord MakeRecord(ulong seqNo, byte[] payload)
    {
        RecordHeader header = new(
            RecordHeader.ExpectedMagic,
            RecordHeader.CurrentVersion,
            RecordFlags.None,
            ContentType.Unknown,
            Reserved: 0,
            SchemaId: 0,
            SeqNo: seqNo,
            PayloadLength: (uint)payload.Length);
        return new LogRecord(header, payload);
    }

    [Fact]
    public void SpillFile_AppendAndReplay_RoundTrips()
    {
        string path = SpillPath();
        byte[] p1 = "hello"u8.ToArray();
        byte[] p2 = "world"u8.ToArray();

        using var spill = SpillFile.Create(path);
        spill.Append(MakeRecord(1, p1));
        spill.Append(MakeRecord(2, p2));

        Assert.True(spill.TryReadNext(out LogRecord? r1));
        Assert.Equal(1UL, r1!.Header.SeqNo);
        Assert.Equal(p1, r1.Payload);

        Assert.True(spill.TryReadNext(out LogRecord? r2));
        Assert.Equal(2UL, r2!.Header.SeqNo);
        Assert.Equal(p2, r2.Payload);

        Assert.True(spill.IsFullyReplayed);
        Assert.False(spill.TryReadNext(out _));
    }

    [Fact]
    public void SpillFile_CorruptTail_StopsReplay()
    {
        string path = SpillPath();
        byte[] payload = "data"u8.ToArray();

        // Write one valid record, then corrupt the file tail.
        using (var spill = SpillFile.Create(path))
        {
            spill.Append(MakeRecord(1, payload));
        }

        // Append garbage bytes to simulate a partial write.
        File.AppendAllBytes(path, new byte[] { 0xFF, 0xFE, 0xFD, 0x00, 0x01 });

        using var replay = new SpillFile(path);
        Assert.True(replay.TryReadNext(out LogRecord? r1));
        Assert.Equal(1UL, r1!.Header.SeqNo);

        // Second read hits corrupt tail — must return false, not throw.
        bool second = replay.TryReadNext(out LogRecord? r2);
        Assert.False(second);
        Assert.Null(r2);
    }

    [Fact]
    public void SpillFile_Create_ExistingFile_OpensForReplay()
    {
        string path = SpillPath();
        byte[] payload = "recover"u8.ToArray();

        // Simulate a crash: write a record, dispose (file preserved).
        using (var spill = SpillFile.Create(path))
        {
            spill.Append(MakeRecord(42, payload));
        }

        // Re-open as crash recovery — read stream starts at 0.
        using var recovered = new SpillFile(path);
        Assert.False(recovered.IsFullyReplayed);
        Assert.True(recovered.TryReadNext(out LogRecord? r));
        Assert.Equal(42UL, r!.Header.SeqNo);
        Assert.Equal(payload, r.Payload);
        Assert.True(recovered.IsFullyReplayed);
    }

    [Fact]
    public void SpillFile_DeleteAndDispose_RemovesFile()
    {
        string path = SpillPath();
        var spill = SpillFile.Create(path);
        spill.Append(MakeRecord(1, new byte[] { 1, 2, 3 }));

        spill.DeleteAndDispose();

        Assert.False(File.Exists(path));
    }
}
