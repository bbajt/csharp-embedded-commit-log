using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using FluentAssertions;
using Xunit;

namespace ByTech.EmbeddedCommitLog.Tests.Consumer;

/// <summary>
/// Unit tests for <see cref="HashRouter"/>.
/// </summary>
public sealed class HashRouterTests
{
    private static SinkLane MakeLane(string name = "s", int capacity = 8)
        => new(name, capacity);

    private static LogRecord MakeRecord(ulong seqNo = 1)
    {
        RecordHeader header = new(
            RecordHeader.ExpectedMagic, RecordHeader.CurrentVersion,
            RecordFlags.None, ContentType.Unknown, 0, 0, seqNo, 1);
        return new LogRecord(header, "x"u8.ToArray());
    }

    [Fact]
    public async Task SingleLane_AllRecordsRoutedToIt()
    {
        var lane = MakeLane("only");
        var router = new HashRouter(r => (int)r.Header.SeqNo, BackpressurePolicy.Drop, _ => { });

        await router.RouteAsync(MakeRecord(1), [lane], CancellationToken.None);
        await router.RouteAsync(MakeRecord(2), [lane], CancellationToken.None);
        await router.RouteAsync(MakeRecord(3), [lane], CancellationToken.None);

        lane.Count.Should().Be(3);
    }

    [Fact]
    public async Task TwoLanes_PartitionsByModulo()
    {
        var lane0 = MakeLane("s0");
        var lane1 = MakeLane("s1");
        SinkLane[] lanes = [lane0, lane1];
        var router = new HashRouter(r => (int)r.Header.SeqNo, BackpressurePolicy.Drop, _ => { });

        // SeqNo 0 → 0 % 2 = 0 → lane0
        // SeqNo 1 → 1 % 2 = 1 → lane1
        // SeqNo 2 → 2 % 2 = 0 → lane0
        await router.RouteAsync(MakeRecord(0), lanes, CancellationToken.None);
        await router.RouteAsync(MakeRecord(1), lanes, CancellationToken.None);
        await router.RouteAsync(MakeRecord(2), lanes, CancellationToken.None);

        lane0.Count.Should().Be(2, "seqNos 0 and 2 hash to lane 0");
        lane1.Count.Should().Be(1, "seqNo 1 hashes to lane 1");
    }

    [Fact]
    public async Task NegativeKey_RoutesWithoutException()
    {
        var lane0 = MakeLane("s0");
        var lane1 = MakeLane("s1");
        // key = -1 → ((-1 % 2) + 2) % 2 = ((-1) + 2) % 2 = 1 % 2 = 1 → lane1
        var router = new HashRouter(_ => -1, BackpressurePolicy.Drop, _ => { });

        Func<Task> act = async () =>
            await router.RouteAsync(MakeRecord(1), [lane0, lane1], CancellationToken.None);

        await act.Should().NotThrowAsync();
        lane1.Count.Should().Be(1, "negative key is normalised to a valid lane index");
    }

    [Fact]
    public async Task EmptyLanes_IsNoOp()
    {
        var router = new HashRouter(r => (int)r.Header.SeqNo, BackpressurePolicy.Drop, _ => { });

        Func<Task> act = async () =>
            await router.RouteAsync(MakeRecord(1), [], CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
