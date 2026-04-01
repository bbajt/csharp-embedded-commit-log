using System;
using System.Collections.Generic;
using System.IO;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineSeekTests
{
    // One 4-byte record fills a segment exactly; the next append triggers rollover.
    private static long TinySegment => RecordWriter.FramingOverhead + 4;

    // Three 4-byte records fill a segment exactly; the 4th append triggers rollover.
    private static long ThreeRecordSegment => (RecordWriter.FramingOverhead + 4) * 3;

    private static PipelineConfiguration MakeConfig(string root, long maxSegmentSize = 64 * 1024 * 1024) =>
        new()
        {
            RootDirectory = root,
            MaxSegmentSize = maxSegmentSize,
            GcIntervalMs = int.MaxValue, // keep GC idle — tests manage segment lifetime explicitly
        };

    private static List<ulong> ReadAll(PeclPipeline pipeline, string consumerName)
    {
        var seqNos = new List<ulong>();
        while (true)
        {
            var r = pipeline.ReadNext(consumerName);
            if (!r.IsSuccess)
            {
                break;
            }

            seqNos.Add(r.Value.Header.SeqNo);
        }

        return seqNos;
    }

    // ── SeekConsumer ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Appends 20 records into a single segment, seeks to seqNo 10, then verifies the
    /// consumer reads exactly records 10–19 on the next Start.
    /// </summary>
    [Fact]
    public void SeekConsumer_ToExistingSeqNo_ConsumerReadsFromThatPoint()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 20; i++)
        {
            pipeline.Append("fill"u8);
        }

        pipeline.Flush();
        pipeline.Stop();

        pipeline.SeekConsumer("c", 10);

        pipeline.RegisterConsumer("c");
        pipeline.Start();
        List<ulong> seqNos = ReadAll(pipeline, "c");
        pipeline.Stop();

        seqNos.Should().HaveCount(10);
        seqNos[0].Should().Be(10);
        seqNos[^1].Should().Be(19);
    }

    /// <summary>
    /// Uses TinySegment (1 record each) so seqNo 1 is the exact first record of segment 1.
    /// Seeks to that seqNo and verifies the consumer reads from there through to the end.
    /// </summary>
    [Fact]
    public void SeekConsumer_ToStartOfSegment_ConsumerReadsEntireSegment()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, TinySegment));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 5; i++)
        {
            pipeline.Append("fill"u8);
        }

        pipeline.Flush();
        pipeline.Stop();

        // With TinySegment, segments are: 0→seqNo0, 1→seqNo1, 2→seqNo2, 3→seqNo3, 4→seqNo4.
        // seqNo 1 is the first record of segment 1.
        pipeline.SeekConsumer("c", 1);

        pipeline.RegisterConsumer("c");
        pipeline.Start();
        List<ulong> seqNos = ReadAll(pipeline, "c");
        pipeline.Stop();

        seqNos[0].Should().Be(1);
        seqNos.Should().HaveCount(4);
    }

    /// <summary>
    /// Uses ThreeRecordSegment so segment 0 holds seqNos 0–2 and segment 1 holds seqNos 3–5.
    /// Seeks to seqNo 1 (inside segment 0) and verifies the consumer reads 1–5, crossing
    /// the segment boundary.
    /// </summary>
    [Fact]
    public void SeekConsumer_AcrossSegmentBoundary_FindsCorrectSegment()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, ThreeRecordSegment));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 6; i++)
        {
            pipeline.Append("fill"u8);
        }

        pipeline.Flush();
        pipeline.Stop();

        // seqNo 1 is the second record inside segment 0.
        pipeline.SeekConsumer("c", 1);

        pipeline.RegisterConsumer("c");
        pipeline.Start();
        List<ulong> seqNos = ReadAll(pipeline, "c");
        pipeline.Stop();

        seqNos[0].Should().Be(1);
        seqNos.Should().ContainInOrder(1UL, 2UL, 3UL, 4UL, 5UL);
    }

    /// <summary>
    /// Seeking past the log tail must throw <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    [Fact]
    public void SeekConsumer_BeyondTail_ThrowsArgumentOutOfRange()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 5; i++)
        {
            pipeline.Append("fill"u8);
        }

        pipeline.Flush();
        pipeline.Stop();

        Action act = () => pipeline.SeekConsumer("c", 9999);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Manually deletes segment 0 to simulate retention GC, then seeks to seqNo 0 (which
    /// was in the deleted segment). Must throw <see cref="PeclSeekException"/> with
    /// <see cref="PeclSeekException.EarliestAvailableSeqNo"/> equal to the first seqNo
    /// of the oldest remaining segment.
    /// </summary>
    [Fact]
    public void SeekConsumer_BelowRetentionFloor_ThrowsPeclSeekException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, TinySegment));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 3; i++)
        {
            pipeline.Append("fill"u8);
        }

        pipeline.Flush();
        pipeline.Stop();

        // Segments: 0→seqNo0, 1→seqNo1, 2→seqNo2.
        // Delete segment 0; seqNo 1 becomes the retention floor.
        string seg0 = Path.Combine(dir.Path, "segments", "log-000000.seg");
        File.Delete(seg0);

        Action act = () => pipeline.SeekConsumer("c", 0);
        PeclSeekException ex = act.Should().Throw<PeclSeekException>().Which;
        ex.EarliestAvailableSeqNo.Should().Be(1);
    }

    /// <summary>
    /// Calling <see cref="PeclPipeline.SeekConsumer"/> on a running pipeline must throw
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    [Fact]
    public void SeekConsumer_OnRunningPipeline_ThrowsInvalidOperation()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        Action act = () => pipeline.SeekConsumer("c", 0);
        act.Should().Throw<InvalidOperationException>();

        pipeline.Stop();
    }

    // ── ResetConsumer ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Appends 10 records, reads 5, then resets. Verifies the consumer replays all 10
    /// records from seqNo 0 on the next Start.
    /// </summary>
    [Fact]
    public void ResetConsumer_ReadsFromFirstRecord()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 10; i++)
        {
            pipeline.Append("fill"u8);
        }

        pipeline.Flush();

        // Advance consumer by reading 5 records, leaving cursor at seqNo 4.
        for (int i = 0; i < 5; i++)
        {
            pipeline.ReadNext("c");
        }

        pipeline.Stop();

        pipeline.ResetConsumer("c");

        pipeline.RegisterConsumer("c");
        pipeline.Start();
        List<ulong> seqNos = ReadAll(pipeline, "c");
        pipeline.Stop();

        seqNos[0].Should().Be(0);
        seqNos.Should().HaveCount(10);
    }

    /// <summary>
    /// Manually deletes segment 0 to simulate retention GC, then resets the consumer.
    /// <see cref="PeclPipeline.ResetConsumer"/> must not throw — it moves to the earliest
    /// available record (first record of segment 1, seqNo 1).
    /// </summary>
    [Fact]
    public void ResetConsumer_AfterGcDeletedOldSegments_ReadsFromEarliestAvailable()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, TinySegment));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 3; i++)
        {
            pipeline.Append("fill"u8);
        }

        pipeline.Flush();
        pipeline.Stop();

        string seg0 = Path.Combine(dir.Path, "segments", "log-000000.seg");
        File.Delete(seg0);

        // Must not throw even though segment 0 is gone.
        pipeline.ResetConsumer("c");

        pipeline.RegisterConsumer("c");
        pipeline.Start();
        List<ulong> seqNos = ReadAll(pipeline, "c");
        pipeline.Stop();

        seqNos[0].Should().Be(1);
        seqNos.Should().ContainInOrder(1UL, 2UL);
    }

    // ── Edge cases ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Calling <see cref="PeclPipeline.SeekConsumer"/> for a consumer name that was never
    /// registered must throw <see cref="InvalidOperationException"/>.
    /// </summary>
    [Fact]
    public void SeekConsumer_UnregisteredConsumer_ThrowsInvalidOperation()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();
        pipeline.Append("fill"u8);
        pipeline.Flush();
        pipeline.Stop();

        Action act = () => pipeline.SeekConsumer("unknown", 0);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// Calling <see cref="PeclPipeline.SeekConsumer"/> before the pipeline has ever been
    /// started (no segments directory) must throw <see cref="ArgumentOutOfRangeException"/>
    /// with the "log is empty" message.
    /// </summary>
    [Fact]
    public void SeekConsumer_EmptyLog_ThrowsArgumentOutOfRange()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        // Start() is deliberately not called — segments directory does not exist yet.
        // EnumerateSegmentIds() returns an empty list → ArgumentOutOfRangeException.

        Action act = () => pipeline.SeekConsumer("c", 0);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Calling <see cref="PeclPipeline.ResetConsumer"/> when all segments have been deleted
    /// (simulating full GC drain) must not throw. The empty-log path writes a zeroed cursor.
    /// </summary>
    [Fact]
    public void ResetConsumer_EmptyLog_DoesNotThrow()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("c");
        pipeline.Start();
        pipeline.Append("fill"u8);
        pipeline.Flush();
        pipeline.Stop();

        // Delete all segments so the segments directory is empty.
        // The cursors directory still exists (created during Start), so CursorWriter.Write works.
        string seg0 = Path.Combine(dir.Path, "segments", "log-000000.seg");
        File.Delete(seg0);

        // Must not throw — writes a zeroed cursor for the empty-log case.
        Action act = () => pipeline.ResetConsumer("c");
        act.Should().NotThrow();
    }
}
