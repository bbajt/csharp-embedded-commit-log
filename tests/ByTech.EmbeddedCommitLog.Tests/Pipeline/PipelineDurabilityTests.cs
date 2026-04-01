using System.IO;
using System.Threading;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.Segments;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineDurabilityTests
{
    // One 4-byte-payload record occupies RecordWriter.FramingOverhead + 4 bytes.
    // Setting maxSegmentSize to exactly that value makes IsFull = true after the first record,
    // so the NEXT append triggers a rollover — the simplest possible rollover scenario.
    private static long TinySegment => RecordWriter.FramingOverhead + 4;

    private static PipelineConfiguration MakeConfig(string rootDirectory, long? maxSegmentSize = null) =>
        new() { RootDirectory = rootDirectory, MaxSegmentSize = maxSegmentSize ?? 64 * 1024 * 1024 };

    // ── Test gap 20: Rollover crash recovery ─────────────────────────────────

    /// <summary>
    /// After a rollover, both the sealed segment and the new segment are on disk.
    /// Simulates a crash (checkpoint deleted) and verifies that recovery scans all
    /// segments and finds records from both sides of the rollover boundary.
    /// </summary>
    [Fact]
    public void Rollover_RecoveryAfterCrashBetweenSealAndCheckpoint()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path, TinySegment);

        // Phase 1: write across a segment boundary, stop cleanly.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();

            pipeline.Append("fill"u8);  // fills seg0 (BytesWritten == maxSegmentSize → IsFull)
            pipeline.Append("next"u8);  // triggers rollover → seg1; WriteCheckpointCore(seg0,…)
            pipeline.Stop();
        }

        // Simulate a crash at the worst-case point: checkpoint is deleted (as if it was never
        // written after the rollover). Recovery must scan from seg0 and find both records.
        File.Delete(Path.Combine(dir.Path, "checkpoint.dat"));

        // Phase 2: recover without checkpoint — full scan from seg0.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("fill"u8.ToArray());
            r0.Value.Header.SeqNo.Should().Be(0UL);

            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("next"u8.ToArray());
            r1.Value.Header.SeqNo.Should().Be(1UL);

            tail.IsFailure.Should().BeTrue();
            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── DurabilityMode tests ─────────────────────────────────────────────────

    /// <summary>
    /// Verifies that the default <see cref="DurabilityMode"/> is <see cref="DurabilityMode.Batched"/>.
    /// </summary>
    [Fact]
    public void DurabilityMode_Default_IsBatched()
    {
        var config = new PipelineConfiguration { RootDirectory = "." };
        config.DurabilityMode.Should().Be(DurabilityMode.Batched);
    }

    /// <summary>
    /// Smoke test: <see cref="DurabilityMode.None"/> does not break basic append/read.
    /// An explicit <see cref="PeclPipeline.Flush"/> is required because <c>None</c> mode
    /// performs no automatic fsync.
    /// </summary>
    [Fact]
    public void DurabilityMode_None_WritesAndReadsSucceed()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            DurabilityMode = DurabilityMode.None,
            GcIntervalMs = 60_000,
        };

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("reader");
        pipeline.Start();

        pipeline.Append("hello"u8);
        pipeline.Flush(); // explicit flush required — None mode has no automatic fsync

        var result = pipeline.ReadNext("reader");
        result.IsSuccess.Should().BeTrue();
        result.Value.Payload.ToArray().Should().Equal("hello"u8.ToArray());

        pipeline.Stop();
    }

    /// <summary>
    /// With <see cref="DurabilityMode.Strict"/>, <c>FlushToDisk</c> is called after
    /// every <see cref="PeclPipeline.Append"/>, emptying the writer's 65536-byte
    /// <see cref="System.IO.FileStream"/> buffer so a separately opened
    /// <see cref="SegmentReader"/> can read the record without any explicit
    /// <see cref="PeclPipeline.Flush"/> call.
    /// </summary>
    [Fact]
    public void DurabilityMode_Strict_DataReadableWithoutExplicitFlush()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            DurabilityMode = DurabilityMode.Strict,
            GcIntervalMs = 60_000,
        };

        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append("strict"u8); // Strict: FlushToDisk() called immediately after

        // Open a SegmentReader directly — no Pipeline.Flush() called.
        // FlushToDisk() flushed the writer buffer to the OS, so the record is visible.
        string segmentsDir = Path.Combine(dir.Path, "segments");
        using (var reader = new SegmentReader(segmentsDir, segmentId: 0))
        {
            var result = reader.ReadNext();
            result.IsSuccess.Should().BeTrue();
            result.Value.Payload.ToArray().Should().Equal("strict"u8.ToArray());
        }

        pipeline.Stop();
    }

    /// <summary>
    /// With <see cref="DurabilityMode.Batched"/> and a short
    /// <see cref="PipelineConfiguration.FsyncIntervalMs"/>, the background timer fires
    /// <c>FlushInternal</c> automatically, incrementing
    /// <see cref="PipelineMetrics.FlushCount"/> without any explicit
    /// <see cref="PeclPipeline.Flush"/> call.
    /// </summary>
    [Fact]
    public void DurabilityMode_Batched_TimerTriggersFlushed()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            DurabilityMode = DurabilityMode.Batched,
            FsyncIntervalMs = 50,   // fast timer for testing
            GcIntervalMs = 60_000,
        };

        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append("batched"u8);

        // Poll until the timer fires — up to 5 s to tolerate slow CI runners.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (pipeline.Metrics.FlushCount == 0 && DateTime.UtcNow < deadline)
        {
            Thread.Sleep(50);
        }

        // No explicit Flush() — timer must have incremented FlushCount
        pipeline.Metrics.FlushCount.Should().BeGreaterThan(0);

        pipeline.Stop();
    }

    /// <summary>
    /// Verifies that <see cref="PeclPipeline.Start"/> throws <see cref="InvalidOperationException"/>
    /// when <see cref="DurabilityMode.Batched"/> is selected with a non-positive
    /// <see cref="PipelineConfiguration.FsyncIntervalMs"/>. A zero or negative value would
    /// cause <c>Task.Delay</c> to spin at scheduler frequency, burning CPU.
    /// </summary>
    [Fact]
    public void DurabilityMode_Batched_ZeroFsyncIntervalMs_ThrowsInvalidOperation()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            DurabilityMode = DurabilityMode.Batched,
            FsyncIntervalMs = 0,
        };
        using var pipeline = new PeclPipeline(config);
        var act = pipeline.Start;
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*FsyncIntervalMs*");
    }

    // ── Test gap 24: WriteCheckpoint throws during rollover ───────────────────

    /// <summary>
    /// When WriteCheckpointCore throws during rollover, <c>_writer</c> must still be the
    /// new valid segment writer (the fix: new writer is opened BEFORE WriteCheckpointCore).
    /// Verifies that the subsequent Append call succeeds and the pipeline shuts down cleanly.
    /// </summary>
    [Fact]
    [Trait("Category", "Chaos")]
    public void Rollover_WriteCheckpointThrows_NextAppendSucceeds()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path, TinySegment);

        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append("fill"u8); // fills seg0 (4-byte payload, exactly maxSegmentSize)
        pipeline.Flush();          // checkpoint is valid for seg0

        // Hold an exclusive lock on checkpoint.tmp so that CheckpointWriter.Write throws
        // IOException (sharing violation) when the rollover tries to create its temp file.
        string tmpPath = Path.Combine(dir.Path, "checkpoint.tmp");
        {
            using var blockHandle = new FileStream(tmpPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None);

            // The next Append triggers rollover: seal seg0 → dispose seg0 → open seg1 writer
            // → WriteCheckpointCore → IOException (checkpoint.tmp is locked).
            // With the PHASE-03-02 fix, _writer = seg1 before WriteCheckpointCore is called,
            // so the pipeline still holds a valid writer after the exception.
            // Use a 4-byte payload (max that fits in TinySegment = FramingOverhead + 4).
            Action triggerRollover = () => pipeline.Append("roll"u8);
            triggerRollover.Should().Throw<IOException>();
        }
        // blockHandle is now disposed — checkpoint writes can proceed again.

        // _writer is the new valid seg1 writer; subsequent Append must succeed.
        // Use a 2-byte payload that fits comfortably in TinySegment.
        pipeline.Append("ok"u8);
        pipeline.Flush();
        pipeline.Stop();
    }
}
