using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineGcTests
{
    // Tiny segments (512 B) force rollovers quickly.
    // Short GC interval (50 ms) avoids long sleeps in tests.
    private static PipelineConfiguration MakeGcConfig(string root, int gcIntervalMs = 50) =>
        new()
        {
            RootDirectory = root,
            MaxSegmentSize = 512,
            GcIntervalMs = gcIntervalMs,
            SinkLaneCapacity = 4096,
        };

    // Appends records until at least targetRollovers segment rollovers have occurred.
    private static void FillSegments(PeclPipeline pipeline, int targetRollovers)
    {
        while (pipeline.Metrics.SegmentRollovers < targetRollovers)
        {
            pipeline.Append("data-payload-fill"u8);
        }
    }

    // Drains a pull-mode consumer to EndOfLog.
    private static void DrainConsumer(PeclPipeline pipeline, string consumerName)
    {
        while (true)
        {
            var result = pipeline.ReadNext(consumerName);
            if (!result.IsSuccess)
            {
                break;
            }
        }
    }

    // Sleeps long enough for at least one full GC pass to complete.
    private static void WaitForGc(int gcIntervalMs) =>
        Thread.Sleep(gcIntervalMs * 4);

    // ── Normal reclamation ────────────────────────────────────────────────────

    [Fact]
    public void GC_AllConsumersAdvanced_SegmentsReclaimed()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        FillSegments(pipeline, 3);
        pipeline.Flush();
        DrainConsumer(pipeline, "c");

        WaitForGc(50);
        pipeline.Stop();

        pipeline.Metrics.SegmentsDeleted.Should().BeGreaterThan(0);
    }

    [Fact]
    public void GC_AllConsumersAdvanced_OldSegmentFilesGone()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        FillSegments(pipeline, 3);
        pipeline.Flush();
        DrainConsumer(pipeline, "c");

        WaitForGc(50);
        pipeline.Stop();

        // Only the active segment file should remain in the segments directory.
        string segDir = System.IO.Path.Combine(dir.Path, "segments");
        string[] remaining = System.IO.Directory.GetFiles(segDir, "*.seg");
        remaining.Length.Should().Be(1);
    }

    // ── Slow consumer blocks GC ───────────────────────────────────────────────

    [Fact]
    public void GC_SlowConsumer_PreventsSegmentDeletion()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.RegisterConsumer("fast");
        pipeline.RegisterConsumer("slow"); // never reads
        pipeline.Start();

        FillSegments(pipeline, 3);
        pipeline.Flush();
        DrainConsumer(pipeline, "fast"); // fast at tail; slow still at 0

        WaitForGc(50);
        pipeline.Stop();

        // Slow consumer prevents any deletions.
        pipeline.Metrics.SegmentsDeleted.Should().Be(0);
    }

    // ── No consumers ─────────────────────────────────────────────────────────

    [Fact]
    public void GC_NoConsumers_NothingDeleted()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.Start();

        FillSegments(pipeline, 3);

        WaitForGc(50);
        pipeline.Stop();

        pipeline.Metrics.SegmentsDeleted.Should().Be(0);
    }

    // ── Metrics accuracy ─────────────────────────────────────────────────────

    [Fact]
    public void GC_MetricsAccurate_SegmentCountAndDeleted()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        // Force exactly 3 rollovers → segments 0, 1, 2, 3 (3 is active).
        FillSegments(pipeline, 3);
        pipeline.Flush();
        DrainConsumer(pipeline, "c");

        WaitForGc(50);
        pipeline.Stop();

        // Segments 0, 1, 2 should be deleted; segment 3 (active) remains.
        pipeline.Metrics.SegmentsDeleted.Should().Be(3);
        pipeline.Metrics.SegmentCount.Should().Be(1);
    }

    // ── GC during active writes ───────────────────────────────────────────────

    [Fact]
    public void GC_DuringActiveWrites_NoRecordsLost()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        const int RecordCount = 500;
        var exception = (Exception?)null;

        // Write records in a background thread.
        var writeThread = new Thread(() =>
        {
            try
            {
                for (int i = 0; i < RecordCount; i++)
                {
                    pipeline.Append("concurrent-write"u8);
                }

                pipeline.Flush();
            }
            catch (Exception ex)
            {
                exception = ex;
            }
        });

        writeThread.Start();
        writeThread.Join();

        exception.Should().BeNull("writer must not throw during concurrent GC");

        // Drain all records.
        int readCount = 0;
        while (pipeline.ReadNext("c").IsSuccess)
        {
            readCount++;
        }

        WaitForGc(50);
        pipeline.Stop();

        readCount.Should().Be(RecordCount);
    }

    // ── Push-mode GC watermark (test gap 21) ─────────────────────────────────

    /// <summary>
    /// Verifies that <c>LastFullyRoutedSegmentId</c> (the push-mode GC watermark) advances
    /// correctly after each <c>RouteAsync</c> call, allowing the GC to reclaim old sealed
    /// segments in push-mode pipelines (CRITICAL-1 fix).
    /// </summary>
    [Fact]
    public void GcPass_PushMode_WatermarkAdvancesAfterRouting()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MaxSegmentSize = 512,
            GcIntervalMs = 50,
            SinkLaneCapacity = 4096,
        };

        var sink = new CallbackSink();
        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        // Fill at least 3 segments to force rollovers.
        while (pipeline.Metrics.SegmentRollovers < 3)
        {
            pipeline.Append("fill-data"u8);
        }

        pipeline.Flush();

        int totalRecords = (int)pipeline.Metrics.RecordsAppended;

        // Wait for all records to be delivered (LastFullyRoutedSegmentId advanced for all).
        int waited = 0;
        while (sink.RecordCount < totalRecords && waited < 5000)
        {
            Thread.Sleep(20);
            waited += 20;
        }

        sink.RecordCount.Should().Be(totalRecords, "all records must be routed before checking GC");

        // Wait for at least one GC pass to run after routing completes.
        WaitForGc(50);
        pipeline.Stop();

        pipeline.Metrics.SegmentsDeleted.Should().BeGreaterThan(0,
            "GC must reclaim old segments after the push-mode consumer has routed all records");
    }

    // ── Pull-mode GC watermark (CurrentSegmentId volatile, R04-M8) ───────────

    /// <summary>
    /// Verifies that a pull-mode consumer's <c>CurrentSegmentId</c> volatile field acts
    /// as the correct GC watermark: the segment the consumer is currently reading from
    /// must NOT be deleted even after multiple GC intervals (R04-M8).
    /// </summary>
    [Fact]
    public void PullMode_GcWatermark_DoesNotDeleteActiveConsumerSegment()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        // Force at least 2 rollovers so we have segments 0, 1, 2+ on disk.
        FillSegments(pipeline, 2);
        pipeline.Flush();

        // Read exactly 2 records — consumer stays in segment 0.
        pipeline.ReadNext("c").IsSuccess.Should().BeTrue("first record must exist");
        pipeline.ReadNext("c").IsSuccess.Should().BeTrue("second record must exist");

        // Let GC run multiple passes — consumer's CurrentSegmentId = 0, so seg 0 must survive.
        WaitForGc(50);
        pipeline.Stop();

        string segDir = System.IO.Path.Combine(dir.Path, "segments");
        string seg0 = System.IO.Path.Combine(segDir, "log-000000.seg");
        System.IO.File.Exists(seg0).Should().BeTrue(
            "segment 0 must not be GC'd while the pull-mode consumer's CurrentSegmentId is still 0");
    }

    // ── Late consumer registration guard ─────────────────────────────────────

    /// <summary>
    /// RegisterConsumer after Start() must throw InvalidOperationException even
    /// when the pipeline is running with active GC. The guard must fire before
    /// any attempt to read from offset 0 of a potentially-reclaimed segment.
    /// </summary>
    [Fact]
    public void LateConsumerRegistration_WithGcdSegments_ThrowsOnRegister()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeGcConfig(dir.Path));
        pipeline.RegisterConsumer("early");
        pipeline.Start();

        // Trigger at least one rollover so the log has multiple segments.
        FillSegments(pipeline, 1);
        pipeline.Flush();
        DrainConsumer(pipeline, "early");
        WaitForGc(50);

        // RegisterConsumer while running — must throw regardless of GC state.
        Action act = () => pipeline.RegisterConsumer("late");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*late*Start*");

        pipeline.Stop();
    }

    // ── Restart after GC ─────────────────────────────────────────────────────

    [Trait("Category", "Chaos")]
    [Fact]
    public void GC_Restart_ConsumerCursorPersistedPastDeletedSegments()
    {
        using var dir = new TempDirectory();
        var config = MakeGcConfig(dir.Path);

        // Run 1: write, drain, let GC reclaim old segments, stop (cursor saved at tail).
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("c");
            pipeline.Start();
            FillSegments(pipeline, 3);
            pipeline.Flush();
            DrainConsumer(pipeline, "c");
            WaitForGc(50);
            pipeline.Stop();
        }

        // Run 2: restart — consumer cursor is already at tail; no records to read.
        // Must not throw even though old segment files are gone.
        var ex = Record.Exception(() =>
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("c");
            pipeline.Start();
            var result = pipeline.ReadNext("c");
            // EndOfLog is correct — cursor was at tail when we stopped.
            result.IsSuccess.Should().BeFalse();
            pipeline.Stop();
        });

        ex.Should().BeNull("restart after GC must not throw");
    }
}
