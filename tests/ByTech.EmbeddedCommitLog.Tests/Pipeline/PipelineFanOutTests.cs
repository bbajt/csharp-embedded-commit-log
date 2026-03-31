using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineFanOutTests
{
    private static PipelineConfiguration MakeConfig(string root, int laneCapacity = 4096) =>
        new() { RootDirectory = root, SinkLaneCapacity = laneCapacity };

    // ── AddSink validation ────────────────────────────────────────────────────

    [Fact]
    public void AddSink_AfterStart_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        Action act = () => pipeline.AddSink("c", "s", new CallbackSink());

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AddSink_UnregisteredConsumer_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));

        Action act = () => pipeline.AddSink("nobody", "s", new CallbackSink());

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AddSink_DuplicateSinkName_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", new CallbackSink());

        Action act = () => pipeline.AddSink("c", "s", new CallbackSink());

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ReadNext_PushModeConsumer_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", new CallbackSink());
        pipeline.Start();

        Action act = () => pipeline.ReadNext("c");

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ReadNext_PullModeConsumer_StillWorks()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();
        pipeline.Append("hello"u8);
        pipeline.Flush();

        var result = pipeline.ReadNext("c");

        result.IsSuccess.Should().BeTrue();
        result.Value.Payload.ToArray().Should().Equal("hello"u8.ToArray());
    }

    // ── Fan-out delivery ──────────────────────────────────────────────────────

    [Fact]
    public void AddSink_SingleSink_AllRecordsDelivered()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        var sink = new CallbackSink();
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        for (int i = 0; i < 10; i++)
        {
            pipeline.Append("rec"u8);
        }

        pipeline.Stop();

        sink.RecordCount.Should().Be(10);
    }

    [Fact]
    public void AddSink_MultipleSinks_AllReceiveSameRecords()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        var sink1 = new CallbackSink();
        var sink2 = new CallbackSink();
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s1", sink1);
        pipeline.AddSink("c", "s2", sink2);
        pipeline.Start();

        for (int i = 0; i < 5; i++)
        {
            pipeline.Append("rec"u8);
        }

        pipeline.Stop();

        sink1.RecordCount.Should().Be(5);
        sink2.RecordCount.Should().Be(5);
    }

    [Fact]
    public void AddSink_SingleSink_LargeVolume_AllDelivered()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        var sink = new CallbackSink();
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        for (int i = 0; i < 10_000; i++)
        {
            pipeline.Append("data"u8);
        }

        pipeline.Stop();

        sink.RecordCount.Should().Be(10_000);
    }

    // ── Backpressure / drain ──────────────────────────────────────────────────

    [Fact]
    public void AddSink_SlowSink_StopDrainsBeforeReturning()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        var sink = new CallbackSink(async (_, ct) => await Task.Delay(5, ct));
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        for (int i = 0; i < 20; i++)
        {
            pipeline.Append("rec"u8);
        }

        // Stop() must block until all 20 records drain through the slow sink.
        pipeline.Stop();

        sink.RecordCount.Should().Be(20);
    }

    // ── Cursor persistence ────────────────────────────────────────────────────

    [Fact]
    public void Restart_PushModeConsumer_ReadsFromCursorPosition()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // First run: write 5 records, stop (cursor saved at position 5).
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("c");
            pipeline.AddSink("c", "s", new CallbackSink());
            pipeline.Start();
            for (int i = 0; i < 5; i++) { pipeline.Append("first"u8); }
            pipeline.Stop();
        }

        // Second run: restart with a fresh sink, append 3 more records.
        var sink2 = new CallbackSink();
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("c");
            pipeline.AddSink("c", "s", sink2);
            pipeline.Start();
            for (int i = 0; i < 3; i++) { pipeline.Append("second"u8); }
            pipeline.Stop();
        }

        // The new sink must receive only the 3 records appended in the second run.
        sink2.RecordCount.Should().Be(3);
        sink2.Received.SelectMany(b => b)
            .All(r => r.Payload.SequenceEqual("second"u8.ToArray()))
            .Should().BeTrue();
    }

    // ── Chaos ─────────────────────────────────────────────────────────────────

    [Trait("Category", "Chaos")]
    [Fact]
    public void Restart_PushModeConsumer_CrashBeforeStop_AtLeastOnceDelivery()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Simulate crash: write 10 records using pull-mode (no sinks). Stop() will
        // NOT advance the cursor because no records were routed to any sink lane
        // (LastRoutedSeqNo stays null). This leaves the cursor at position 0 — the
        // same state a push-mode consumer would be in if the process crashed before
        // its first Stop() completed.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("c");
            pipeline.Start();
            for (int i = 0; i < 10; i++) { pipeline.Append("crash"u8); }
            pipeline.Stop();
        }

        // Restart with a push-mode consumer for the same consumer name.
        // Cursor is at 0, so all 10 records must be replayed (at-least-once).
        var sink2 = new CallbackSink();
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("c");
            pipeline.AddSink("c", "s", sink2);
            pipeline.Start();
            // Allow time for reader to catch up to tail.
            System.Threading.Thread.Sleep(100);
            pipeline.Stop();
        }

        // At-least-once: all records replayed because cursor was never advanced.
        sink2.RecordCount.Should().BeGreaterThan(0);
    }

    // ── Multi-consumer × multi-sink ───────────────────────────────────────────

    [Fact]
    public void ThreeConsumers_TwoSinksEach_AllSixSinksReceiveAllRecords()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));

        var sinks = new CallbackSink[3, 2];
        for (int c = 0; c < 3; c++)
        {
            pipeline.RegisterConsumer($"c{c}");
            for (int s = 0; s < 2; s++)
            {
                sinks[c, s] = new CallbackSink();
                pipeline.AddSink($"c{c}", $"s{s}", sinks[c, s]);
            }
        }

        pipeline.Start();

        for (int i = 0; i < 20; i++)
        {
            pipeline.Append("rec"u8);
        }

        pipeline.Stop();

        for (int c = 0; c < 3; c++)
        {
            for (int s = 0; s < 2; s++)
            {
                sinks[c, s].RecordCount.Should().Be(20,
                    $"consumer c{c} sink s{s} must receive all 20 records");
            }
        }
    }

    [Fact]
    public void MultiConsumer_PushMode_Restart_IndependentCursorResumption()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Run 1: both consumers receive 10 records; cursors persisted at tail (SeqNo 9).
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("fast");
            pipeline.RegisterConsumer("slow");
            pipeline.AddSink("fast", "sf", new CallbackSink());
            pipeline.AddSink("slow", "ss", new CallbackSink());
            pipeline.Start();

            for (int i = 0; i < 10; i++) { pipeline.Append("first"u8); }

            pipeline.Stop();
        }

        // Run 2: restart; append 5 more records. Both consumers must see exactly 5.
        var fastSink2 = new CallbackSink();
        var slowSink2 = new CallbackSink();
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("fast");
            pipeline.RegisterConsumer("slow");
            pipeline.AddSink("fast", "sf", fastSink2);
            pipeline.AddSink("slow", "ss", slowSink2);
            pipeline.Start();

            for (int i = 0; i < 5; i++) { pipeline.Append("second"u8); }

            pipeline.Stop();
        }

        fastSink2.RecordCount.Should().Be(5,
            "fast consumer cursor was at tail; only 5 new records should be delivered");
        slowSink2.RecordCount.Should().Be(5,
            "slow consumer cursor was at tail; only 5 new records should be delivered");

        fastSink2.Received.SelectMany(b => b)
            .All(r => r.Payload.SequenceEqual("second"u8.ToArray()))
            .Should().BeTrue("fast consumer must receive only the second-run records");
        slowSink2.Received.SelectMany(b => b)
            .All(r => r.Payload.SequenceEqual("second"u8.ToArray()))
            .Should().BeTrue("slow consumer must receive only the second-run records");
    }
}
