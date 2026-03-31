using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using FluentAssertions;
using Xunit;

namespace ByTech.EmbeddedCommitLog.Tests.Consumer;

public sealed class BroadcastRouterTests
{
    private static LogRecord MakeRecord(ulong seqNo = 0) =>
        new(new RecordHeader(RecordHeader.ExpectedMagic, RecordHeader.CurrentVersion,
            RecordFlags.None, ContentType.Unknown, 0, 0, seqNo, 0),
            Array.Empty<byte>());

    private static SinkLane MakeLane(string name = "s", int capacity = 8) =>
        new(name, capacity);

    // ── Empty lanes ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task RouteAsync_EmptyLaneList_Completes()
    {
        var router = new BroadcastRouter(BackpressurePolicy.Block, static _ => { });
        var record = MakeRecord();

        await router.RouteAsync(record, [], CancellationToken.None);
        // No assertion needed — must not throw.
    }

    // ── Single lane ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task RouteAsync_SingleLane_RecordEnqueued()
    {
        var router = new BroadcastRouter(BackpressurePolicy.Block, static _ => { });
        using var lane = MakeLane();
        var record = MakeRecord(1);

        await router.RouteAsync(record, [lane], CancellationToken.None);

        lane.Count.Should().Be(1);
        var read = await lane.Reader.ReadAsync(CancellationToken.None);
        read.Should().BeSameAs(record);
    }

    // ── Multiple lanes ────────────────────────────────────────────────────────────

    [Fact]
    public async Task RouteAsync_MultipleLanes_AllReceiveSameRecord()
    {
        var router = new BroadcastRouter(BackpressurePolicy.Block, static _ => { });
        using var lane1 = MakeLane("a");
        using var lane2 = MakeLane("b");
        using var lane3 = MakeLane("c");
        var record = MakeRecord(42);

        await router.RouteAsync(record, [lane1, lane2, lane3], CancellationToken.None);

        lane1.Count.Should().Be(1);
        lane2.Count.Should().Be(1);
        lane3.Count.Should().Be(1);

        (await lane1.Reader.ReadAsync(CancellationToken.None)).Should().BeSameAs(record);
        (await lane2.Reader.ReadAsync(CancellationToken.None)).Should().BeSameAs(record);
        (await lane3.Reader.ReadAsync(CancellationToken.None)).Should().BeSameAs(record);
    }

    // ── Sequential ordering (backpressure) ───────────────────────────────────────

    [Fact]
    public async Task RouteAsync_FirstLaneFull_BlocksUntilDrained()
    {
        var router = new BroadcastRouter(BackpressurePolicy.Block, static _ => { });
        using var lane1 = MakeLane("a", capacity: 1);
        using var lane2 = MakeLane("b", capacity: 8);

        // Fill lane1.
        await lane1.WriteAsync(MakeRecord(0), CancellationToken.None);

        var record = MakeRecord(1);

        // Start routing — should block on lane1.
        var routeTask = router.RouteAsync(record, [lane1, lane2], CancellationToken.None).AsTask();
        routeTask.IsCompleted.Should().BeFalse();

        // lane2 must NOT have the record yet (sequential routing — lane1 first).
        lane2.Count.Should().Be(0);

        // Drain lane1 — unblocks routing.
        await lane1.Reader.ReadAsync(CancellationToken.None);
        await routeTask;

        lane1.Count.Should().Be(1);
        lane2.Count.Should().Be(1);
    }

    // ── Cancellation ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task RouteAsync_CancelledWhileBlocked_ThrowsOperationCanceledException()
    {
        var router = new BroadcastRouter(BackpressurePolicy.Block, static _ => { });
        using var lane = MakeLane("s", capacity: 1);
        await lane.WriteAsync(MakeRecord(0), CancellationToken.None); // fill

        using var cts = new CancellationTokenSource();
        var routeTask = router.RouteAsync(MakeRecord(1), [lane], cts.Token).AsTask();
        cts.Cancel();

        await routeTask.Invoking(t => t).Should()
            .ThrowAsync<OperationCanceledException>();
    }
}
