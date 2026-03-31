using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Records;
using FluentAssertions;
using Xunit;

namespace ByTech.EmbeddedCommitLog.Tests.Consumer;

public sealed class SinkLaneTests
{
    private static LogRecord MakeRecord(ulong seqNo = 0) =>
        new(new RecordHeader(RecordHeader.ExpectedMagic, RecordHeader.CurrentVersion,
            RecordFlags.None, ContentType.Unknown, 0, 0, seqNo, 0),
            Array.Empty<byte>());

    // ── Construction ──────────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullSinkName_ThrowsArgumentNullException()
    {
        Action act = () => _ = new SinkLane(null!, 10);

        act.Should().Throw<ArgumentNullException>().WithParameterName("sinkName");
    }

    [Fact]
    public void Constructor_ZeroCapacity_ThrowsArgumentOutOfRangeException()
    {
        Action act = () => _ = new SinkLane("sink", 0);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("capacity");
    }

    [Fact]
    public void Constructor_ValidArgs_SetsSinkName()
    {
        using var lane = new SinkLane("my-sink", 8);

        lane.SinkName.Should().Be("my-sink");
    }

    // ── TryWrite / Count ──────────────────────────────────────────────────────────

    [Fact]
    public void TryWrite_BelowCapacity_ReturnsTrueAndIncrementsCount()
    {
        using var lane = new SinkLane("s", 4);
        var record = MakeRecord();

        var result = lane.TryWrite(record);

        result.Should().BeTrue();
        lane.Count.Should().Be(1);
    }

    [Fact]
    public void TryWrite_WhenFull_ReturnsFalse()
    {
        using var lane = new SinkLane("s", 2);

        lane.TryWrite(MakeRecord(0));
        lane.TryWrite(MakeRecord(1));
        var result = lane.TryWrite(MakeRecord(2));

        result.Should().BeFalse();
    }

    // ── WriteAsync / backpressure ─────────────────────────────────────────────────

    [Fact]
    public async Task WriteAsync_BelowCapacity_EnqueuesRecord()
    {
        using var lane = new SinkLane("s", 4);
        var record = MakeRecord(7);

        await lane.WriteAsync(record, CancellationToken.None);

        lane.Count.Should().Be(1);
    }

    [Fact]
    public async Task WriteAsync_RecordReadableFromReader()
    {
        using var lane = new SinkLane("s", 4);
        var record = MakeRecord(99);

        await lane.WriteAsync(record, CancellationToken.None);
        var read = await lane.Reader.ReadAsync(CancellationToken.None);

        read.Should().BeSameAs(record);
    }

    [Fact]
    public async Task WriteAsync_WhenFull_UnblocksAfterReaderDrains()
    {
        using var lane = new SinkLane("s", 1);
        var first = MakeRecord(0);
        var second = MakeRecord(1);

        // Fill the lane.
        await lane.WriteAsync(first, CancellationToken.None);

        // Start blocked write in background.
        var writeTask = lane.WriteAsync(second, CancellationToken.None).AsTask();
        writeTask.IsCompleted.Should().BeFalse();

        // Drain one record — unblocks the writer.
        await lane.Reader.ReadAsync(CancellationToken.None);
        await writeTask;

        lane.Count.Should().Be(1);
    }

    [Fact]
    public async Task WriteAsync_CancelledWhileFull_ThrowsOperationCanceledException()
    {
        using var lane = new SinkLane("s", 1);
        await lane.WriteAsync(MakeRecord(0), CancellationToken.None); // fill

        using var cts = new CancellationTokenSource();
        var writeTask = lane.WriteAsync(MakeRecord(1), cts.Token).AsTask();
        cts.Cancel();

        await writeTask.Invoking(t => t).Should()
            .ThrowAsync<OperationCanceledException>();
    }

    // ── Dispose ───────────────────────────────────────────────────────────────────

    [Fact]
    public void WriteAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var lane = new SinkLane("s", 4);
        lane.Dispose();

        Action act = () => lane.WriteAsync(MakeRecord(), CancellationToken.None);

        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void TryWrite_AfterDispose_ThrowsObjectDisposedException()
    {
        var lane = new SinkLane("s", 4);
        lane.Dispose();

        Action act = () => lane.TryWrite(MakeRecord());

        act.Should().Throw<ObjectDisposedException>();
    }

    // ── Complete ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Complete_AllowsReaderToDrainThenFinish()
    {
        using var lane = new SinkLane("s", 4);
        await lane.WriteAsync(MakeRecord(0), CancellationToken.None);
        await lane.WriteAsync(MakeRecord(1), CancellationToken.None);

        lane.Complete();

        var count = 0;
        await foreach (var _ in lane.Reader.ReadAllAsync(CancellationToken.None))
        {
            count++;
        }

        count.Should().Be(2);
    }
}
