using Xunit;
using System.Threading.Channels;
using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Tests.Consumer;

/// <summary>Unit tests for <see cref="SinkLane"/> spill integration.</summary>
public sealed class SinkLaneSpillTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    public SinkLaneSpillTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }

    private string SpillPath(string name = "lane") => Path.Combine(_dir, $"{name}.spill");

    private static LogRecord MakeRecord(ulong seqNo)
    {
        byte[] payload = new byte[] { (byte)(seqNo & 0xFF) };
        RecordHeader header = new(
            RecordHeader.ExpectedMagic,
            RecordHeader.CurrentVersion,
            RecordFlags.None,
            ContentType.Unknown,
            Reserved: 0,
            SchemaId: 0,
            SeqNo: seqNo,
            PayloadLength: 1);
        return new LogRecord(header, payload);
    }

    [Fact]
    public void SinkLane_Spill_WritesToFileWhenFull()
    {
        string path = SpillPath();
        using var lane = new SinkLane("s", capacity: 1, spillFilePath: path);

        // Fill the channel.
        Assert.True(lane.TryWrite(MakeRecord(1)));
        // Channel is full — Spill should write to disk.
        lane.Spill(MakeRecord(2));

        Assert.True(lane.HasSpill);
        Assert.True(File.Exists(path));
    }

    [Fact]
    public void SinkLane_Spill_TryDrainReturnsRecordsInOrder()
    {
        string path = SpillPath();
        using var lane = new SinkLane("s", capacity: 4, spillFilePath: path);

        // Write two records to spill.
        lane.Spill(MakeRecord(10));
        lane.Spill(MakeRecord(11));

        // Drain both into channel.
        Assert.True(lane.TryDrainOneSpillRecord());
        Assert.True(lane.TryDrainOneSpillRecord());

        // Spill file fully replayed — HasSpill should be false now.
        Assert.False(lane.HasSpill);

        // Channel should have records 10 and 11 in order.
        Assert.True(lane.Reader.TryRead(out LogRecord? r1));
        Assert.Equal(10UL, r1!.Header.SeqNo);
        Assert.True(lane.Reader.TryRead(out LogRecord? r2));
        Assert.Equal(11UL, r2!.Header.SeqNo);
    }

    [Fact]
    public void SinkLane_Spill_DeletesFileAfterFullReplay()
    {
        string path = SpillPath();
        using var lane = new SinkLane("s", capacity: 4, spillFilePath: path);

        lane.Spill(MakeRecord(1));
        Assert.True(File.Exists(path));

        // Drain the single record.
        Assert.True(lane.TryDrainOneSpillRecord());
        // Next call detects IsFullyReplayed, deletes the file, and returns false.
        Assert.False(lane.TryDrainOneSpillRecord());
        Assert.False(File.Exists(path));
        Assert.False(lane.HasSpill);
    }

    [Fact]
    public void SinkLane_Dispose_PreservesSpillFile()
    {
        string path = SpillPath();
        var lane = new SinkLane("s", capacity: 1, spillFilePath: path);
        lane.Spill(MakeRecord(99));
        lane.Dispose();

        // File must survive Dispose for crash recovery on next Start().
        Assert.True(File.Exists(path));
    }

    [Fact]
    public void SinkLane_CrashRecovery_OpensExistingSpillFile()
    {
        string path = SpillPath();

        // First run: write record to spill, then "crash" (dispose without draining).
        using (var lane = new SinkLane("s", capacity: 1, spillFilePath: path))
        {
            lane.Spill(MakeRecord(77));
        }

        // Second run: constructor detects existing file — HasSpill is true immediately.
        using var recovered = new SinkLane("s", capacity: 4, spillFilePath: path);
        Assert.True(recovered.HasSpill);

        // Drain the recovered record.
        Assert.True(recovered.TryDrainOneSpillRecord());
        Assert.True(recovered.Reader.TryRead(out LogRecord? r));
        Assert.Equal(77UL, r!.Header.SeqNo);
    }
}
