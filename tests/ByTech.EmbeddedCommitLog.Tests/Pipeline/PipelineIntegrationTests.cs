using System.Diagnostics;
using System.Text;
using System.Threading;
using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.Chaos;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineIntegrationTests
{
    private static PipelineConfiguration MakeConfig(string rootDirectory, long maxSegmentSize = 64 * 1024 * 1024) =>
        new() { RootDirectory = rootDirectory, MaxSegmentSize = maxSegmentSize };

    // ── Startup ───────────────────────────────────────────────────────────────

    [Fact]
    public void Start_EmptyDirectory_CreatesDirectoriesAndTransitionsToRunning()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));

        pipeline.Start();

        pipeline.State.Should().Be(PipelineState.Running);
        Directory.Exists(Path.Combine(dir.Path, "segments")).Should().BeTrue();
        Directory.Exists(Path.Combine(dir.Path, "cursors")).Should().BeTrue();
    }

    // ── Append ────────────────────────────────────────────────────────────────

    [Fact]
    public void Append_AssignsMonotonicSeqNos()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.Start();

        ulong seq0 = pipeline.Append("hello"u8);
        ulong seq1 = pipeline.Append("world"u8);
        ulong seq2 = pipeline.Append("pecl"u8);

        seq0.Should().Be(0UL);
        seq1.Should().Be(1UL);
        seq2.Should().Be(2UL);
    }

    [Fact]
    public void Append_PayloadPlusFramingExceedsMaxSegmentSize_ThrowsArgumentException()
    {
        // 28 B framing overhead + 10 B payload = 38 B. MaxSegmentSize = 38 B fits exactly one
        // 10-byte record. An 11-byte payload (39 B total) must be rejected.
        const long MaxSize = 38L;
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, maxSegmentSize: MaxSize));
        pipeline.Start();

        byte[] oversized = new byte[11];
        Action act = () => pipeline.Append(oversized);

        act.Should().Throw<ArgumentException>().WithParameterName("payload");
    }

    [Fact]
    public void Append_PayloadPlusFramingExactlyEqualsMaxSegmentSize_Succeeds()
    {
        // 28 B framing + 10 B payload = 38 B = MaxSegmentSize — must succeed.
        const long MaxSize = 38L;
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, maxSegmentSize: MaxSize));
        pipeline.Start();

        ulong seq = pipeline.Append(new byte[10]);

        seq.Should().Be(0UL);
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    [Fact]
    public void ReadNext_AfterAppend_ReturnsRecordPayload()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("reader");
        pipeline.Start();

        byte[] payload = Encoding.UTF8.GetBytes("hello pecl");
        pipeline.Append(payload);
        pipeline.Flush();

        var result = pipeline.ReadNext("reader");

        result.IsSuccess.Should().BeTrue();
        result.Value.Payload.ToArray().Should().Equal(payload);
    }

    [Fact]
    public void ReadNext_AtTail_ReturnsEndOfLog()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.RegisterConsumer("reader");
        pipeline.Start();

        pipeline.Append("record"u8);
        pipeline.Flush();

        // Consume the only record.
        pipeline.ReadNext("reader").IsSuccess.Should().BeTrue();

        // Next read must return EndOfLog.
        var result = pipeline.ReadNext("reader");
        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
    }

    // ── Recovery — clean shutdown ─────────────────────────────────────────────

    [Fact]
    public void Stop_ThenRestart_CleanRecovery_AllRecordsPresent()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Write three records and stop cleanly.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("alpha"u8);
            pipeline.Append("beta"u8);
            pipeline.Append("gamma"u8);
            pipeline.Stop();
        }

        // Restart and read back all records.
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
            r0.Value.Header.SeqNo.Should().Be(0UL);

            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("beta"u8.ToArray());
            r1.Value.Header.SeqNo.Should().Be(1UL);

            r2.IsSuccess.Should().BeTrue();
            r2.Value.Payload.ToArray().Should().Equal("gamma"u8.ToArray());
            r2.Value.Header.SeqNo.Should().Be(2UL);

            tail.IsFailure.Should().BeTrue();
            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── Recovery — missing checkpoint ─────────────────────────────────────────

    [Fact]
    public void Stop_ThenRestart_MissingCheckpoint_ScanFromBeginning()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Write two records and stop.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("first"u8);
            pipeline.Append("second"u8);
            pipeline.Stop();
        }

        // Delete the checkpoint to simulate a crash scenario.
        File.Delete(Path.Combine(dir.Path, "checkpoint.dat"));

        // Restart — recovery must scan from segment 0.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("first"u8.ToArray());

            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("second"u8.ToArray());

            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);
        }
    }

    // ── Recovery — corrupt tail ───────────────────────────────────────────────

    [Fact]
    public void Stop_ThenRestart_CorruptTailTruncated()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Write two records and stop.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("good"u8);
            pipeline.Append("also-good"u8);
            pipeline.Stop();
        }

        // Corrupt the tail of the segment (overwrite last 4 bytes with garbage).
        string segPath = Directory.GetFiles(Path.Combine(dir.Path, "segments")).Single();
        long segLen = new FileInfo(segPath).Length;
        using (var fs = new FileStream(segPath, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(-4, SeekOrigin.End);
            fs.Write(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });
        }

        // The checkpoint's LastOffset already covers the corrupted bytes — recovery
        // would skip re-scanning them. Delete the checkpoint to force a full scan
        // from offset 0 so the corruption is detected and truncated.
        ChaosHelpers.DeleteCheckpoint(dir.Path);

        // Restart — corrupt second record must be truncated; first record must still be readable.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("good"u8.ToArray());

            // After truncation, only one record is recoverable.
            tail.IsFailure.Should().BeTrue();

            // Segment must have been physically truncated.
            new FileInfo(segPath).Length.Should().BeLessThan(segLen);
        }
    }

    // ── Segment rollover ──────────────────────────────────────────────────────

    [Fact]
    public void Append_SegmentRollover_ReaderSpansSegments()
    {
        using var dir = new TempDirectory();
        // Tiny segment size to force rollover after a few records.
        // RecordWriter.FramingOverhead = 28; each "data" payload = 4 bytes → 32 bytes/record.
        // Set max to 35 bytes so each segment holds exactly one record.
        const long TinySegment = 35L;
        var config = MakeConfig(dir.Path, TinySegment);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            ulong seq0 = pipeline.Append("aaaa"u8);
            ulong seq1 = pipeline.Append("bbbb"u8);
            ulong seq2 = pipeline.Append("cccc"u8);
            pipeline.Flush();

            seq0.Should().Be(0UL);
            seq1.Should().Be(1UL);
            seq2.Should().Be(2UL);

            // Three records across (potentially) three segments.
            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var r2 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("aaaa"u8.ToArray());

            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("bbbb"u8.ToArray());

            r2.IsSuccess.Should().BeTrue();
            r2.Value.Payload.ToArray().Should().Equal("cccc"u8.ToArray());

            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);

            // At least two segment files must exist.
            Directory.GetFiles(Path.Combine(dir.Path, "segments")).Length.Should().BeGreaterThanOrEqualTo(2);
        }
    }

    // ── ArrayPool WriteBuffer reuse (R04-M6) ─────────────────────────────────

    /// <summary>
    /// Verifies that the pooled <c>WriteBuffer</c> (R04-M6) passes the same
    /// <see cref="IReadOnlyList{T}"/> array instance to every <c>WriteAsync</c> call,
    /// eliminating per-record heap allocation in <c>RunSinkTaskAsync</c>.
    /// </summary>
    [Fact]
    public void SinkSlot_WriteBuffer_IsReusedAcrossRecords()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration { RootDirectory = dir.Path };

        var capturedBatches = new List<IReadOnlyList<LogRecord>>();
        var sink = new CallbackSink((batch, _) =>
        {
            capturedBatches.Add(batch);
            return Task.CompletedTask;
        });

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        const int N = 3;
        for (int i = 0; i < N; i++)
        {
            pipeline.Append("x"u8);
        }

        pipeline.Flush();

        int waited = 0;
        while (sink.RecordCount < N && waited < 5000)
        {
            Thread.Sleep(20);
            waited += 20;
        }

        sink.RecordCount.Should().Be(N, "all records must be delivered");
        capturedBatches.Should().HaveCount(N);

        // All WriteAsync calls must receive the same array instance — the pooled WriteBuffer.
        for (int i = 1; i < capturedBatches.Count; i++)
        {
            capturedBatches[i].Should().BeSameAs(capturedBatches[0],
                "WriteBuffer must be reused across WriteAsync calls (R04-M6)");
        }
    }

    /// <summary>
    /// Verifies that pushed records arrive at the sink with correct payloads in the
    /// correct order after the <c>WriteBuffer</c> pooling change (regression guard).
    /// Payloads are copied synchronously inside <c>WriteAsync</c> — before the buffer
    /// slot is overwritten by the next record — so the captured bytes must be stable.
    /// </summary>
    [Fact]
    public void PushMode_SinkDelivery_MatchesAppendedRecords()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration { RootDirectory = dir.Path };

        var deliveredPayloads = new List<byte[]>();
        var sink = new CallbackSink((batch, _) =>
        {
            // Copy payload bytes during WriteAsync before WriteBuffer[0] is overwritten.
            foreach (LogRecord record in batch)
            {
                deliveredPayloads.Add(record.Payload.ToArray());
            }

            return Task.CompletedTask;
        });

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        byte[][] payloads =
        [
            "first"u8.ToArray(),
            "second"u8.ToArray(),
            "third"u8.ToArray(),
        ];

        foreach (byte[] p in payloads)
        {
            pipeline.Append(p);
        }

        pipeline.Flush();

        int waited = 0;
        while (deliveredPayloads.Count < payloads.Length && waited < 5000)
        {
            Thread.Sleep(20);
            waited += 20;
        }

        deliveredPayloads.Should().HaveCount(payloads.Length);
        for (int i = 0; i < payloads.Length; i++)
        {
            deliveredPayloads[i].Should().Equal(payloads[i],
                $"record {i} payload must be correct after WriteBuffer reuse");
        }
    }

    // ── Polling-phase error context (R05-M2 / INFO-1) ────────────────────────

    /// <summary>
    /// A non-<c>EndOfLog</c> read error encountered during the push-mode reader loop's
    /// <em>polling phase</em> must surface through <c>Stop()</c> as an
    /// <see cref="AggregateException"/> containing a <see cref="PeclDrainException"/>
    /// with the original <see cref="PeclError"/> preserved (R05-M2 fix).
    /// Before the fix the error was thrown as a bare <see cref="InvalidOperationException"/>
    /// discarding the full <see cref="PeclError"/> context.
    /// </summary>
    [Fact]
    public void ReaderLoop_PollingPhaseError_SurfacesAsPeclDrainExceptionWithErrorContext()
    {
        // 1-byte payload + 28-byte framing = 29 bytes. Segment 0 fills after 1 record.
        const int RecordPayloadSize = 1;
        const long SegSize = RecordPayloadSize + 28;
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path, maxSegmentSize: SegSize);

        // Phase 1: write 3 records (forces two rollovers). Checkpoint.LastSegmentId = 2
        // so recovery in phase 2 scans from segment 2, leaving segment 1 unhealed.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append(new byte[RecordPayloadSize]);  // r0 → seg0
            pipeline.Append(new byte[RecordPayloadSize]);  // r1 → seg1
            pipeline.Append(new byte[RecordPayloadSize]);  // r2 → seg2
            pipeline.Stop();
        }

        // Corrupt the CRC footer (last 4 bytes) of segment 1.
        // Phase 2 recovery starts at segment 2 (checkpoint), so segment 1 corruption
        // survives intact. The push-mode consumer starts at (seg 0, offset 0), routes
        // r0 successfully, then encounters CrcMismatch in segment 1 (R06-INFO-2).
        ChaosHelpers.CorruptSegmentBytesAtEnd(dir.Path, 1u, offsetFromEnd: 4,
            bytes: new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });

        // Phase 2: push-mode consumer with no prior cursor → reads from (seg 0, offset 0).
        // ManualResetEventSlim gives thread-safe delivery confirmation (CallbackSink._received
        // is a plain List<T> — not safe to poll from a different thread without a barrier).
        using var latch = new ManualResetEventSlim(false);
        var sink = new CallbackSink((_, _) => { latch.Set(); return Task.CompletedTask; });
        using var pipeline2 = new PeclPipeline(config);
        pipeline2.RegisterConsumer("c");
        pipeline2.AddSink("c", "s", sink);
        pipeline2.Start();

        // Wait deterministically for record 0 to be routed (proves reader is past seg0
        // and will hit CrcMismatch in seg1 on the next iteration).
        bool latched = latch.Wait(5000, TestContext.Current.CancellationToken);
        latched.Should().BeTrue("record 0 must be delivered before CrcMismatch in segment 1");

        // Wait deterministically for the reader loop to record the CrcMismatch drain error.
        // DrainErrorSignal is set by SetDrainError() — no polling, no Thread.Sleep (R07-INFO-1).
        bool drainErrorSet = pipeline2.WaitForConsumerDrainError(
            "c", 5_000, TestContext.Current.CancellationToken);
        drainErrorSet.Should().BeTrue("reader loop must record CrcMismatch before Stop() is called");

        // Stop() must throw AggregateException.
        AggregateException ae = Assert.Throws<AggregateException>(() => pipeline2.Stop());

        // The inner exception must be PeclDrainException (not InvalidOperationException),
        // proving that the full PeclError context was preserved via SetDrainError (R05-M2).
        ae.InnerExceptions.Should().ContainSingle(
            e => e is PeclDrainException,
            "polling-phase error must surface as PeclDrainException, not InvalidOperationException");

        PeclDrainException drainEx = ae.InnerExceptions.OfType<PeclDrainException>().Single();
        drainEx.DrainError.Code.Should().Be(PeclErrorCode.CrcMismatch,
            "PeclDrainException must carry the original PeclError with CrcMismatch code");
        drainEx.ConsumerName.Should().Be("c");
    }

    // ── Drain Timeout (PHASE-07-01) ────────────────────────────────────────────

    /// <summary>
    /// When <see cref="PipelineConfiguration.DrainTimeoutMs"/> elapses before all reader
    /// loops complete, <see cref="Pipeline.Stop"/> must throw <see cref="AggregateException"/>
    /// containing exactly one <see cref="PeclDrainTimeoutException"/> and must finish cleanup
    /// (pipeline reaches Stopped or Error, not Draining).
    /// </summary>
    /// <remarks>
    /// The timeout is triggered by filling the sink lane (capacity=1) with a permanently-blocking
    /// sink (released by <c>sinkReleaser</c>) so the reader loop is stuck in
    /// <c>RouteAsync(CancellationToken.None)</c> and cannot observe the cancelled
    /// CancellationToken. Because the sink never completes on its own, <c>Stop()</c> is run on a
    /// background task (it blocks on <c>Task.WaitAll(sinkTasks)</c> with no timeout after the
    /// drain fires). A 1 000 ms sleep gives the drain timeout generous headroom even on a
    /// heavily-loaded 2-CPU CI runner before the releaser unblocks the sink.
    /// </remarks>
    [Fact]
    public async Task Stop_DrainTimeoutExceeded_ThrowsAggregateContainingPeclDrainTimeoutException()
    {
        // Mechanism: SinkLaneCapacity=1 fills the lane so the reader loop blocks in
        // RouteAsync(CancellationToken.None) and cannot observe CTS cancellation.
        // DrainTimeoutMs=50 fires because the sink never finishes until sinkReleaser is set.
        //
        // Stop() is called on a background Task because after the drain timeout it calls
        // Task.WaitAll(sinkTasks) with no timeout — it would deadlock the test thread if
        // the sink is still blocking.
        //
        // Synchronisation: deliveryLatch fires on the first sink call so we know the lane
        // is occupied (SinkLaneCapacity=1) and the reader is blocked in WriteAsync(CT.None)
        // trying to route the next record. At that point the drain is guaranteed to time out.
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            SinkLaneCapacity = 1,
            DrainTimeoutMs = 50,
            GcIntervalMs = 50,
            GcStopTimeoutMs = 200,
        };

        // Blocking sink: signals on first delivery then blocks until sinkReleaser is set.
        using var deliveryLatch = new ManualResetEventSlim(false);
        var sinkReleaser = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sink = new CallbackSink(async (_, _) =>
        {
            deliveryLatch.Set();
            await sinkReleaser.Task;
        });

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        for (int i = 0; i < 5; i++)
        {
            pipeline.Append("r"u8);
        }
        pipeline.Flush();

        // Wait for first delivery: the lane is now occupied and RunSinkTaskAsync is blocked.
        // The reader has routed record 1 and is blocked in WriteAsync(record 2, CT.None).
        bool latched = deliveryLatch.Wait(10_000, TestContext.Current.CancellationToken);
        latched.Should().BeTrue("sink must receive at least one record before Stop() is called");

        // Run Stop() on a background task — it will block on Task.WaitAll(sinkTasks) after
        // the drain timeout fires, so it must not run on the test thread.
        AggregateException? stopException = null;
        Task stopTask = Task.Run(() =>
        {
            try
            {
                pipeline.Stop();
            }
            catch (AggregateException ex)
            {
                stopException = ex;
            }
        }, TestContext.Current.CancellationToken);

        // Give the drain timeout (50 ms) generous headroom on a loaded 2-CPU CI runner.
        // K×50 ms must be < 1 000 ms for any realistic scheduling multiplier K.
        await Task.Delay(1_000, TestContext.Current.CancellationToken);

        // Release the sink so Stop() can finish Task.WaitAll(sinkTasks) and complete cleanup.
        sinkReleaser.TrySetResult();

        // WaitAsync throws TimeoutException if Stop() does not complete — that failure is clear.
        await stopTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        stopException.Should().NotBeNull("Stop() must throw AggregateException when drain times out");
        AggregateException ae = stopException!;

        ae.InnerExceptions.Should().ContainSingle(
            e => e is PeclDrainTimeoutException,
            "drain timeout must surface as PeclDrainTimeoutException inside the AggregateException");

        PeclDrainTimeoutException timeoutEx = ae.InnerExceptions.OfType<PeclDrainTimeoutException>().Single();
        timeoutEx.TimeoutMs.Should().Be(50);
        timeoutEx.TimedOutConsumers.Should().ContainSingle(name => name == "c",
            "the timed-out consumer list must identify consumer 'c'");

        pipeline.State.Should().BeOneOf(
            new[] { PipelineState.Stopped, PipelineState.Error },
            "cleanup must complete and pipeline must leave Draining state regardless of the timeout");
    }

    [Fact]
    public void Stop_DrainTimeoutZero_UnboundedWait_StopSucceeds()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            DrainTimeoutMs = 0,   // 0 = unbounded (preserves pre-MILESTONE-07 behaviour)
            GcIntervalMs = 50,
            GcStopTimeoutMs = 200,
        };

        using var latch = new ManualResetEventSlim(false);
        var sink = new CallbackSink((_, _) => { latch.Set(); return Task.CompletedTask; });

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        pipeline.Append("hello"u8);
        pipeline.Flush();

        bool latched = latch.Wait(5_000, TestContext.Current.CancellationToken);
        latched.Should().BeTrue("record must be delivered before Stop()");

        pipeline.Stop();

        pipeline.State.Should().Be(PipelineState.Stopped,
            "DrainTimeoutMs=0 must preserve unbounded-wait behaviour and stop cleanly");
    }

    [Fact]
    public void ForceStop_FromRunning_TransitionsToStopped()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            GcIntervalMs = 50,
            GcStopTimeoutMs = 200,
        };

        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append("hello"u8);
        pipeline.Flush();

        pipeline.ForceStop();

        pipeline.State.Should().Be(PipelineState.Stopped,
            "ForceStop() on a running pipeline with no consumers must transition to Stopped");
    }

    [Fact]
    public void ForceStop_FromRunning_WithConsumer_DoesNotDrain()
    {
        // Sink blocks until we signal sinkUnblock. After ForceStop() the lane is TryComplete'd
        // (via SinkLane.Dispose()), so unblocking the TCS lets the sink task drain cleanly.
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            DrainTimeoutMs = 200,
            GcIntervalMs = 50,
            GcStopTimeoutMs = 200,
        };

        using var deliveryLatch = new ManualResetEventSlim(false);
        var sinkUnblock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var sink = new CallbackSink(async (_, _) =>
        {
            deliveryLatch.Set();
            await sinkUnblock.Task;
        });

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        pipeline.Append("hello"u8);
        pipeline.Flush();

        bool latched = deliveryLatch.Wait(5_000, TestContext.Current.CancellationToken);
        latched.Should().BeTrue("sink must receive the record before ForceStop() is called");

        var sw = Stopwatch.StartNew();
        pipeline.ForceStop();
        sw.Stop();

        // Unblock the stuck sink task so it exits cleanly — avoids leaked background threads.
        sinkUnblock.TrySetResult(true);

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1),
            "ForceStop() must not wait for the DrainTimeoutMs duration (200 ms)");
        pipeline.State.Should().BeOneOf(new[] { PipelineState.Stopped, PipelineState.Error },
            "pipeline must be fully torn down after ForceStop()");
    }
}
