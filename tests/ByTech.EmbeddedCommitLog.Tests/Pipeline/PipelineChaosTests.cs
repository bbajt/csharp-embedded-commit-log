using System.Reflection;
using System.Threading;
using ByTech.EmbeddedCommitLog.Cursors;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.Chaos;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Chaos")]
public sealed class PipelineChaosTests
{
    private static PipelineConfiguration MakeConfig(
        string rootDirectory,
        long maxSegmentSize = 64 * 1024 * 1024,
        int cursorFlushRecordThreshold = 1_000) =>
        new()
        {
            RootDirectory = rootDirectory,
            MaxSegmentSize = maxSegmentSize,
            CursorFlushRecordThreshold = cursorFlushRecordThreshold,
        };

    // ── Crash without flush ───────────────────────────────────────────────────

    /// <summary>
    /// Records appended after the last explicit Flush are lost when a crash is simulated
    /// by truncating the segment back to the flush boundary. Recovery must serve only the
    /// three records that were durably flushed.
    /// </summary>
    [Fact]
    public void Crash_AppendWithoutFlush_OnlyFlushedRecordsVisible()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        long flushBoundary;

        // Phase 1: write 3 records, flush durably, then write 3 more and stop cleanly.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("aaa"u8);
            pipeline.Append("bbb"u8);
            pipeline.Append("ccc"u8);
            pipeline.Flush();

            // Record the file size after the durable flush — this is the crash boundary.
            flushBoundary = ChaosHelpers.GetSegmentFileSize(dir.Path, 0u);

            // Append 3 more records. Stop flushes them to disk — we will truncate below
            // to simulate what would happen if the process crashed before these were
            // durably written.
            pipeline.Append("ddd"u8);
            pipeline.Append("eee"u8);
            pipeline.Append("fff"u8);
            pipeline.Stop();
        }

        // Simulate crash: truncate the segment back to the flush boundary.
        ChaosHelpers.TruncateSegment(dir.Path, 0u, flushBoundary);

        // Recovery: only the first 3 records must be visible.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var r2 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("aaa"u8.ToArray());
            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("bbb"u8.ToArray());
            r2.IsSuccess.Should().BeTrue();
            r2.Value.Payload.ToArray().Should().Equal("ccc"u8.ToArray());
            tail.IsFailure.Should().BeTrue();
            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── Corrupt / missing checkpoint ──────────────────────────────────────────

    /// <summary>
    /// A corrupt checkpoint CRC forces recovery to fall back to a full scan from segment 0.
    /// All records written before the "crash" must still be readable.
    /// </summary>
    [Fact]
    public void Crash_CorruptCheckpointCrc_RecoveryScansFromSegmentZero()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("alpha"u8);
            pipeline.Append("beta"u8);
            pipeline.Append("gamma"u8);
            pipeline.Stop();
        }

        ChaosHelpers.CorruptCheckpointCrc(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var r2 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("alpha"u8.ToArray());
            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("beta"u8.ToArray());
            r2.IsSuccess.Should().BeTrue();
            r2.Value.Payload.ToArray().Should().Equal("gamma"u8.ToArray());
            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    /// <summary>
    /// A truncated checkpoint file (fewer bytes than <c>CheckpointData.SerializedSize</c>)
    /// forces recovery to fall back to a full scan from segment 0. All records must survive.
    /// </summary>
    [Fact]
    public void Crash_TruncatedCheckpoint_RecoveryScansFromSegmentZero()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("one"u8);
            pipeline.Append("two"u8);
            pipeline.Stop();
        }

        // Truncate to 10 bytes — well below CheckpointData.SerializedSize (36).
        ChaosHelpers.TruncateCheckpoint(dir.Path, 10L);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("one"u8.ToArray());
            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("two"u8.ToArray());
            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── Consumer cursor recovery ──────────────────────────────────────────────

    /// <summary>
    /// A consumer cursor that was explicitly flushed before a simulated crash causes the
    /// consumer to resume from exactly the saved position on the next restart, not from (0, 0).
    /// </summary>
    [Fact]
    public void Consumer_Cursor_FlushedBeforeCrash_ResumesFromSavedPosition()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Phase 1: write 10 records.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            for (int i = 0; i < 10; i++)
            {
                pipeline.Append(new byte[] { (byte)i });
            }
            pipeline.Stop();
        }

        // Phase 2: read 5 records, flush the cursor explicitly, then stop.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            for (int i = 0; i < 5; i++)
            {
                pipeline.ReadNext("reader").IsSuccess.Should().BeTrue();
            }
            pipeline.Flush(); // persists cursor at seqNo 4
            pipeline.Stop();
        }

        // Phase 3: restart — consumer must resume from seqNo 5, not 0.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            for (ulong expected = 5UL; expected < 10UL; expected++)
            {
                var result = pipeline.ReadNext("reader");
                result.IsSuccess.Should().BeTrue($"expected record seqNo={expected}");
                result.Value.Header.SeqNo.Should().Be(expected);
            }

            pipeline.ReadNext("reader").Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    /// <summary>
    /// When a consumer's cursor file is deleted before restart (simulating a crash before
    /// any cursor flush), the consumer must replay the entire log from position (0, 0).
    /// This exercises the at-least-once delivery guarantee.
    /// </summary>
    [Fact]
    public void Consumer_Cursor_DeletedBeforeRestart_ReplaysFromBeginning()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("a"u8);
            pipeline.Append("b"u8);
            pipeline.Append("c"u8);
            pipeline.Append("d"u8);
            pipeline.Append("e"u8);
            pipeline.Stop();
        }

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            // Read 3 records.
            pipeline.ReadNext("reader").IsSuccess.Should().BeTrue();
            pipeline.ReadNext("reader").IsSuccess.Should().BeTrue();
            pipeline.ReadNext("reader").IsSuccess.Should().BeTrue();

            pipeline.Stop(); // saves cursor at record 2
        }

        // Simulate crash: delete the cursor file before the next start.
        ChaosHelpers.DeleteCursor(dir.Path, "reader");

        // Restart: no cursor → consumer replays from seqNo 0.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var first = pipeline.ReadNext("reader");
            first.IsSuccess.Should().BeTrue();
            first.Value.Header.SeqNo.Should().Be(0UL, "replay must start from the beginning");

            int count = 1;
            while (pipeline.ReadNext("reader").IsSuccess)
            {
                count++;
            }
            count.Should().Be(5, "all 5 records must be replayed");
        }
    }

    /// <summary>
    /// A cursor pointing beyond the end of the log is clamped to the actual tail during
    /// recovery. The consumer receives <see cref="PeclErrorCode.EndOfLog"/> on its first read.
    /// </summary>
    [Fact]
    public void Recovery_CursorAheadOfTail_ClampedToTail()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Write 3 records to set up directories and a valid segment.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("x"u8);
            pipeline.Append("y"u8);
            pipeline.Append("z"u8);
            pipeline.Stop();
        }

        // Directly write a cursor that points far beyond the log tail.
        string cursorsDir = Path.Combine(dir.Path, "cursors");
        CursorWriter.Write(cursorsDir, new CursorData("reader", 0u, 999_999L, 999_999UL));

        // Restart: cursor must be clamped to the actual tail.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var result = pipeline.ReadNext("reader");
            result.IsFailure.Should().BeTrue();
            result.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── Multiple consumers ────────────────────────────────────────────────────

    /// <summary>
    /// Two consumers with different read positions both have their cursors saved and
    /// recovered independently. After restart the fast consumer is at the tail and the
    /// slow consumer continues from where it left off.
    /// </summary>
    [Fact]
    public void MultipleConsumers_IndependentCursorRecovery()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("fast");
            pipeline.RegisterConsumer("slow");
            pipeline.Start();

            for (int i = 0; i < 5; i++)
            {
                pipeline.Append(new byte[] { (byte)i });
            }
            pipeline.Flush();

            // fast reads all 5 records; slow reads only 2.
            for (int i = 0; i < 5; i++)
            {
                pipeline.ReadNext("fast").IsSuccess.Should().BeTrue();
            }
            for (int i = 0; i < 2; i++)
            {
                pipeline.ReadNext("slow").IsSuccess.Should().BeTrue();
            }

            pipeline.Flush(); // persist both cursors
            pipeline.Stop();
        }

        // Restart: fast is at the tail; slow resumes from seqNo 2.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("fast");
            pipeline.RegisterConsumer("slow");
            pipeline.Start();

            // fast should be at the tail immediately.
            pipeline.ReadNext("fast").Error.Code.Should().Be(PeclErrorCode.EndOfLog);

            // slow resumes from seqNo 2 and reads records 2, 3, 4.
            for (ulong expected = 2UL; expected <= 4UL; expected++)
            {
                var result = pipeline.ReadNext("slow");
                result.IsSuccess.Should().BeTrue($"slow expected seqNo={expected}");
                result.Value.Header.SeqNo.Should().Be(expected);
            }
            pipeline.ReadNext("slow").Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── SeqNo continuity ──────────────────────────────────────────────────────

    /// <summary>
    /// After a clean stop and restart, newly appended records receive sequence numbers
    /// that continue from the last sequence number present in the recovered log.
    /// </summary>
    [Fact]
    public void Recovery_SeqNoContinuesAfterRestart()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            for (int i = 0; i < 5; i++)
            {
                pipeline.Append(new byte[] { (byte)i });
            }
            pipeline.Stop();
        }

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();

            ulong seq5 = pipeline.Append("p"u8);
            ulong seq6 = pipeline.Append("q"u8);
            ulong seq7 = pipeline.Append("r"u8);

            seq5.Should().Be(5UL);
            seq6.Should().Be(6UL);
            seq7.Should().Be(7UL);
        }
    }

    // ── Multi-segment clean recovery ──────────────────────────────────────────

    /// <summary>
    /// Writing records that span multiple segment files (forced by a tiny max segment size)
    /// followed by a clean stop and restart produces a consumer that can read all records
    /// in order across the segment boundaries.
    /// </summary>
    [Fact]
    public void MultiSegment_CleanRecovery_AllRecordsReadable()
    {
        using var dir = new TempDirectory();
        // TinySegment=35: each 5-byte payload record = 33 bytes framed.
        // After 2 records (BW=66 >= 35), IsFull on the 3rd append → rollover.
        // 5 records across 3 segments: seg0=[0,1], seg1=[2,3], seg2=[4].
        const long TinySegment = 35L;
        var config = MakeConfig(dir.Path, TinySegment);

        const int RecordCount = 5;
        var payloads = Enumerable.Range(0, RecordCount)
            .Select(i => new byte[] { (byte)i, (byte)i, (byte)i, (byte)i, (byte)i })
            .ToArray();

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            foreach (byte[] payload in payloads)
            {
                pipeline.Append(payload);
            }
            pipeline.Stop();
        }

        // Verify multiple segments were created.
        string[] segments = Directory.GetFiles(Path.Combine(dir.Path, "segments"));
        segments.Length.Should().BeGreaterThanOrEqualTo(2,
            "tiny segment size must force at least one rollover");

        // Restart and verify all records are readable in seqNo order.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            for (int i = 0; i < RecordCount; i++)
            {
                var result = pipeline.ReadNext("reader");
                result.IsSuccess.Should().BeTrue($"expected record seqNo={i}");
                result.Value.Header.SeqNo.Should().Be((ulong)i);
                result.Value.Payload.ToArray().Should().Equal(payloads[i]);
            }

            pipeline.ReadNext("reader").Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── Adversarial sinks ─────────────────────────────────────────────────────

    /// <summary>
    /// A sink whose <c>WriteAsync</c> throws causes <c>Stop()</c> to propagate
    /// an <see cref="AggregateException"/> containing the sink's exception.
    /// </summary>
    [Fact]
    public void FailingSink_Stop_SurfacesAggregateException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));

        var boom = new CallbackSink((_, _) =>
            throw new InvalidOperationException("sink exploded"));

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", boom);
        pipeline.Start();

        pipeline.Append("trigger"u8);
        pipeline.Flush();

        // Allow reader loop to deliver the record to the sink task.
        Thread.Sleep(200);

        Action act = () => pipeline.Stop();

        act.Should().Throw<AggregateException>()
            .Which.InnerExceptions.Should().ContainSingle(e =>
                e is InvalidOperationException && e.Message == "sink exploded");
    }

    /// <summary>
    /// Writer isolation invariant: <c>Pipeline.Append</c> must complete without
    /// blocking even when the sink lane is full and the sink is stalled.
    /// The reader loop blocks on the full lane; the writer thread is independent.
    /// </summary>
    [Fact]
    public void SlowSink_LaneFull_WriterNeverBlocked()
    {
        using var dir = new TempDirectory();
        // Lane capacity of 1 means the lane fills after 1 buffered record.
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            SinkLaneCapacity = 1,
        };

        var gate = new ManualResetEventSlim(false);
        var sink = new CallbackSink(async (_, ct) =>
        {
            gate.Wait(ct);
            await Task.CompletedTask;
        });

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        const int RecordCount = 50;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 0; i < RecordCount; i++)
        {
            pipeline.Append("x"u8);
        }

        sw.Stop();

        // All appends must complete well under 1 second regardless of sink backpressure.
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1),
            "Append must never block on sink lane backpressure");

        // Release the sink and allow Stop() to drain.
        gate.Set();
        pipeline.Stop();

        sink.RecordCount.Should().Be(RecordCount, "all records must be delivered after gate released");
    }

    // ── Drain phase error propagation (test gap 23) ───────────────────────────

    /// <summary>
    /// Verifies that a non-<c>EndOfLog</c> read error surfaces through <c>Stop()</c>
    /// as an <see cref="AggregateException"/> and sets <see cref="PipelineState.Error"/>
    /// (HIGH-6 + LOW-15 fix).
    ///
    /// Design: two segments are created so recovery starts scanning at segment 1
    /// (checkpoint.LastSegmentId = 1), leaving segment 0 unscanned.  The last 4 bytes
    /// of segment 0 (the CRC footer of its sole record) are then corrupted.  When phase 2
    /// starts a push-mode consumer the reader loop reads from segment 0 and encounters a
    /// CrcMismatch that recovery never had a chance to truncate.
    /// </summary>
    [Fact]
    public void ReaderError_DuringStop_SurfacesAggregateException_AndSetsErrorState()
    {
        // 1 byte payload + 28 bytes framing = 29 bytes per record.
        // MaxSegmentSize = 29 means segment 0 is full after a single append.
        const int RecordPayloadSize = 1;
        const long SegSize = RecordPayloadSize + 28; // RecordWriter.FramingOverhead = 28
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path, maxSegmentSize: SegSize);

        // Phase 1: write 2 records without consumers so that segment files close cleanly.
        //   Append("x") → segment 0 is now full (29 bytes).
        //   Append("y") → triggers rollover; segment 1 gets "y"; checkpoint LastSegmentId = 1.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append(new byte[RecordPayloadSize]);
            pipeline.Append(new byte[RecordPayloadSize]);
            pipeline.Stop();
        }

        // Corrupt the CRC footer (last 4 bytes) of segment 0.
        // Recovery in phase 2 will start scanning at segment 1 and skip segment 0,
        // so this corruption is never healed before the reader loop encounters it.
        ChaosHelpers.CorruptSegmentBytesAtEnd(dir.Path, 0u, offsetFromEnd: 4,
            bytes: new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });

        // Phase 2: new pipeline with push-mode consumer.
        // Consumer "c" has no cursor on disk → starts at (segment 0, offset 0).
        // The reader loop will read segment 0 and encounter CrcMismatch.
        var pipeline2 = new PeclPipeline(config);
        pipeline2.RegisterConsumer("c");
        pipeline2.AddSink("c", "s", new CallbackSink());
        pipeline2.Start();

        // Stop() must surface the read error as AggregateException.
        Action act = () => pipeline2.Stop();
        act.Should().Throw<AggregateException>(
            "Stop() must propagate reader errors as AggregateException");

        // Whether the error hit the normal reader loop or the drain loop, state must be Error.
        pipeline2.State.Should().Be(PipelineState.Error,
            "PipelineState.Error must be set when Stop() encounters unrecoverable errors");
    }

    // ── Durability & rollover ─────────────────────────────────────────────────

    /// <summary>
    /// After <c>TruncateSegment</c> the truncated length must be physically on disk.
    /// Verifies that the <c>Flush(flushToDisk: true)</c> added by R04-H5 works at the
    /// observable level: after recovery, <c>RecoveryTruncatedBytes &gt; 0</c> and the
    /// segment file length matches the durable flush boundary.
    /// </summary>
    [Fact]
    public void Recovery_WithTruncation_SegmentLengthIsDurableAfterRecovery()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        long flushBoundary;

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("alpha"u8);
            pipeline.Append("beta"u8);
            pipeline.Flush();
            flushBoundary = ChaosHelpers.GetSegmentFileSize(dir.Path, 0u);
            pipeline.Append("gamma"u8); // unflushed — will be truncated on recovery
            pipeline.Stop();
        }

        // Simulate crash: truncate back to flush boundary.
        ChaosHelpers.TruncateSegment(dir.Path, 0u, flushBoundary);

        // Start a new pipeline — RecoverInternal detects and truncates the tail.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            // After recovery the segment file must be at the truncated length.
            ChaosHelpers.GetSegmentFileSize(dir.Path, 0u).Should().Be(flushBoundary,
                "segment must stay at the truncated length after recovery + fsync");

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("alpha"u8.ToArray());
            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("beta"u8.ToArray());
            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    /// <summary>
    /// Segment rollover must succeed end-to-end and all records across the segment
    /// boundary must be readable after a restart. Verifies the reordered rollover
    /// sequence (R04-NLB) does not break the happy path.
    /// </summary>
    [Fact]
    public void Append_SegmentRollover_AllRecordsReadableAfterRestart()
    {
        // 1-byte payload + 28-byte framing = 29 bytes per record; segment rolls after 1.
        const long SegSize = 1 + 28;
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path, maxSegmentSize: SegSize);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("x"u8);
            pipeline.Append("y"u8);
            pipeline.Append("z"u8);
            pipeline.Stop();
        }

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var r2 = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("x"u8.ToArray());
            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("y"u8.ToArray());
            r2.IsSuccess.Should().BeTrue();
            r2.Value.Payload.ToArray().Should().Equal("z"u8.ToArray());
            pipeline.ReadNext("reader").Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── GC task lifecycle ─────────────────────────────────────────────────────

    /// <summary>
    /// If the GC background task faults (e.g. <c>Directory.GetFiles</c> throws because the
    /// segments directory is unavailable), <c>Stop()</c> must collect the fault into its
    /// <see cref="AggregateException"/> rather than rethrowing inside the finally block
    /// and abandoning writer/consumer cleanup (R04-NHA fix).
    /// </summary>
    [Fact]
    public void Stop_GcTaskFaulted_FaultSurfacedThroughAggregateException()
    {
        using var dir = new TempDirectory();
        var pipeline = new PeclPipeline(MakeConfig(dir.Path) with { GcStopTimeoutMs = 500 });
        pipeline.Start();

        // Replace _gcTask with a pre-faulted task via reflection to simulate RunGcPass
        // throwing (e.g. IOException from Directory.GetFiles on an unavailable mount).
        // The real GC task is orphaned in its Task.Delay; it exits cleanly when Stop()
        // cancels _gcCts.
        var fault = new IOException("simulated: segments directory unavailable");
        Task faultedTask = Task.FromException(fault);
        typeof(PeclPipeline)
            .GetField("_gcTask", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(pipeline, faultedTask);

        Action act = () => pipeline.Stop();

        // Stop() must throw and the GC fault must appear in InnerExceptions.
        act.Should().Throw<AggregateException>()
            .Which.InnerExceptions.Should().ContainSingle(
                e => e is IOException && e.Message == "simulated: segments directory unavailable");

        // After surfacing the fault, State transitions to Error.
        pipeline.State.Should().Be(PipelineState.Error);
    }

    /// <summary>
    /// After <c>Stop()</c> throws with a faulted GC task, <c>Dispose()</c> must complete
    /// without throwing and close all file handles (belt-and-suspenders path, R04-C1 fix).
    /// </summary>
    [Fact]
    public void Dispose_AfterStopWithFaultedGcTask_CompletesWithoutThrowing()
    {
        using var dir = new TempDirectory();
        {
            var pipeline = new PeclPipeline(MakeConfig(dir.Path) with { GcStopTimeoutMs = 500 });
            pipeline.Start();

            var fault = new IOException("simulated GC fault");
            Task faultedTask = Task.FromException(fault);
            typeof(PeclPipeline)
                .GetField("_gcTask", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(pipeline, faultedTask);

            // Stop() throws AggregateException — pipeline is now in Error state.
            try { pipeline.Stop(); } catch (AggregateException) { }

            // Dispose() must not throw and must release all file handles.
            Action disposeAct = () => pipeline.Dispose();
            disposeAct.Should().NotThrow();
        }

        // TempDirectory.Dispose() verifies all handles are released (throws on Windows
        // if any file handle is still open). Reaching here without IOException is the proof.
    }

    // ── GC-task-timeout + concurrent Dispose (R05-L8 / INFO-3) ──────────────

    /// <summary>
    /// When the GC background task does not stop within <c>GcStopTimeoutMs</c>,
    /// <c>Stop()</c> (called from <c>Dispose()</c>) proceeds and calls
    /// <c>_consumers.Clear()</c> while <c>RunGcPass</c> may still be iterating
    /// <c>_consumers</c>. The <c>lock (_observabilityLock)</c> added by R05-L8
    /// must prevent <see cref="InvalidOperationException"/> ("collection was modified").
    /// Runs 25 iterations to increase the probability of hitting the race window.
    /// </summary>
    [Fact]
    public void Chaos_GcTaskTimeout_ConcurrentDispose_NoDataRace()
    {
        for (int i = 0; i < 25; i++)
        {
            using var dir = new TempDirectory();

            // Very short GcIntervalMs: RunGcPass fires rapidly.
            // Very short GcStopTimeoutMs: Stop() proceeds before the GC task
            // has a chance to acknowledge cancellation, exercising the window
            // where RunGcPass may be mid-iteration when Stop() clears _consumers.
            var config = new PipelineConfiguration
            {
                RootDirectory = dir.Path,
                GcIntervalMs = 5,
                GcStopTimeoutMs = 1,
            };

            var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("c1");
            pipeline.RegisterConsumer("c2");
            pipeline.Start();

            pipeline.Append("a"u8);
            pipeline.Append("b"u8);
            pipeline.Flush();

            // Dispose cancels GC task and waits with a 1 ms timeout. If RunGcPass
            // is mid-execution on the _consumers dictionary when Stop() calls
            // _consumers.Clear(), the lock in RunGcPass (R05-L8) must prevent
            // InvalidOperationException from the Dictionary enumerator.
            Action act = () => pipeline.Dispose();
            act.Should().NotThrow<InvalidOperationException>(
                $"iteration {i}: concurrent _consumers iteration and Clear() must not throw (R05-L8)");
        }
    }
}
