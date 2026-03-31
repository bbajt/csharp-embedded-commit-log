using System.Diagnostics.Metrics;
using System.Threading;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineBackpressureTests
{
    // ── Policy default ───────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that the default <see cref="BackpressurePolicy"/> is <see cref="BackpressurePolicy.Block"/>.
    /// </summary>
    [Fact]
    public void BackpressurePolicy_Default_IsBlock()
    {
        var config = new PipelineConfiguration { RootDirectory = "." };
        config.BackpressurePolicy.Should().Be(BackpressurePolicy.Block);
    }

    // ── Drop policy ──────────────────────────────────────────────────────────

    /// <summary>
    /// With <see cref="BackpressurePolicy.Drop"/> and a lane capacity of 1, appending
    /// records while the sink is blocked causes the router to discard records and
    /// increment <see cref="PipelineMetrics.SinkDropped"/>.
    /// </summary>
    [Fact]
    public void Drop_WhenLaneFull_RecordDiscardedAndCounterIncremented()
    {
        using var dir = new TempDirectory();
        using var mre = new ManualResetEventSlim(false);

        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            BackpressurePolicy = BackpressurePolicy.Drop,
            SinkLaneCapacity = 1,
            DrainTimeoutMs = 500,
            GcIntervalMs = 60_000,
        };

        // Sink blocks until the event is set, keeping the lane full for subsequent records.
        var sink = new CallbackSink((_, ct) => { mre.Wait(ct); return Task.CompletedTask; });
        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "blocker", sink);
        pipeline.Start();

        // First record fills the lane; second+ are dropped while sink is blocked.
        pipeline.Append("r1"u8);
        pipeline.Append("r2"u8);
        pipeline.Append("r3"u8);
        pipeline.Flush();

        Thread.Sleep(100); // allow reader loop to process all three records

        pipeline.Metrics.SinkDropped.Should().BeGreaterThan(0);

        mre.Set(); // unblock sink so Stop() can drain cleanly
        pipeline.Stop();
    }

    /// <summary>
    /// Verifies that the <c>pecl.sink.dropped</c> instrument measurement carries a
    /// <c>"sink"</c> tag whose value equals the name of the full sink lane.
    /// </summary>
    [Fact]
    public void Drop_MetricTaggedBySinkName()
    {
        using var dir = new TempDirectory();
        using var mre = new ManualResetEventSlim(false);

        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            BackpressurePolicy = BackpressurePolicy.Drop,
            SinkLaneCapacity = 1,
            DrainTimeoutMs = 500,
            GcIntervalMs = 60_000,
            MeterName = "pecl-drop-tag",
        };

        string? capturedSinkTag = null;
        var tagLock = new object();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == config.MeterName && instrument.Name == "pecl.sink.dropped")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "sink")
                {
                    lock (tagLock)
                    {
                        capturedSinkTag = (string?)tag.Value;
                    }
                }
            }
        });
        listener.Start();

        var sink = new CallbackSink((_, ct) => { mre.Wait(ct); return Task.CompletedTask; });
        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "my-sink", sink);
        pipeline.Start();

        pipeline.Append("r1"u8);
        pipeline.Append("r2"u8);
        pipeline.Append("r3"u8);
        pipeline.Flush();

        Thread.Sleep(100);

        lock (tagLock)
        {
            capturedSinkTag.Should().Be("my-sink");
        }

        mre.Set();
        pipeline.Stop();
    }
}
