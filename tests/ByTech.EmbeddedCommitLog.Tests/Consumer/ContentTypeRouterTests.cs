using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using FluentAssertions;
using Xunit;

namespace ByTech.EmbeddedCommitLog.Tests.Consumer;

/// <summary>
/// Unit tests for <see cref="ContentTypeRouter"/>.
/// </summary>
public sealed class ContentTypeRouterTests
{
    private static SinkLane MakeLane(string name = "s", int capacity = 8)
        => new(name, capacity);

    private static LogRecord MakeRecord(ContentType contentType = ContentType.Unknown, ulong seqNo = 1)
    {
        RecordHeader header = new(
            RecordHeader.ExpectedMagic, RecordHeader.CurrentVersion,
            RecordFlags.None, contentType, 0, 0, seqNo, 1);
        return new LogRecord(header, "x"u8.ToArray());
    }

    [Fact]
    public async Task MatchingRecord_RoutedToCorrectLane()
    {
        var router = new ContentTypeRouter(BackpressurePolicy.Drop, _ => { });
        var lane = MakeLane("json");
        router.RegisterFilter("json", ContentType.Json);

        await router.RouteAsync(MakeRecord(ContentType.Json), [lane], CancellationToken.None);

        lane.Reader.TryRead(out LogRecord? result).Should().BeTrue();
        result!.Header.ContentType.Should().Be(ContentType.Json);
    }

    [Fact]
    public async Task NonMatchingRecord_NotRoutedToFilteredLane()
    {
        var router = new ContentTypeRouter(BackpressurePolicy.Drop, _ => { });
        var lane = MakeLane("json");
        router.RegisterFilter("json", ContentType.Json);

        await router.RouteAsync(MakeRecord(ContentType.Protobuf), [lane], CancellationToken.None);

        lane.Reader.TryRead(out _).Should().BeFalse();
    }

    [Fact]
    public async Task UnmatchedRecord_InvokesDroppedCallbackWithStar()
    {
        string? dropped = null;
        var router = new ContentTypeRouter(BackpressurePolicy.Drop, s => dropped = s);
        var lane = MakeLane("json");
        router.RegisterFilter("json", ContentType.Json);

        await router.RouteAsync(MakeRecord(ContentType.Protobuf), [lane], CancellationToken.None);

        dropped.Should().Be("*");
    }

    [Fact]
    public async Task UnfilteredLane_ReceivesEveryRecord()
    {
        // A lane with no registered filter acts as broadcast within ContentTypeRouter
        var router = new ContentTypeRouter(BackpressurePolicy.Drop, _ => { });
        var lane = MakeLane("all");
        // No RegisterFilter call for "all"

        await router.RouteAsync(MakeRecord(ContentType.Json), [lane], CancellationToken.None);
        await router.RouteAsync(MakeRecord(ContentType.Protobuf), [lane], CancellationToken.None);

        lane.Reader.TryRead(out _).Should().BeTrue();
        lane.Reader.TryRead(out _).Should().BeTrue();
    }

    [Fact]
    public async Task TwoFilteredLanes_EachReceivesOnlyOwnType()
    {
        var router = new ContentTypeRouter(BackpressurePolicy.Drop, _ => { });
        var jsonLane = MakeLane("json");
        var protoLane = MakeLane("proto");
        router.RegisterFilter("json", ContentType.Json);
        router.RegisterFilter("proto", ContentType.Protobuf);

        await router.RouteAsync(MakeRecord(ContentType.Json), [jsonLane, protoLane], CancellationToken.None);
        await router.RouteAsync(MakeRecord(ContentType.Protobuf), [jsonLane, protoLane], CancellationToken.None);

        jsonLane.Reader.TryRead(out _).Should().BeTrue("json lane should receive the Json record");
        jsonLane.Reader.TryRead(out _).Should().BeFalse("json lane should not receive the Protobuf record");

        protoLane.Reader.TryRead(out _).Should().BeTrue("proto lane should receive the Protobuf record");
        protoLane.Reader.TryRead(out _).Should().BeFalse("proto lane should not receive the Json record");
    }
}
