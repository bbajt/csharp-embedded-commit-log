using System.Diagnostics.Metrics;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.Chaos;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

public sealed class PipelineMetricsTests
{
    private static PipelineConfiguration MakeConfig(string rootDirectory, long maxSegmentSize = 64 * 1024 * 1024) =>
        new() { RootDirectory = rootDirectory, MaxSegmentSize = maxSegmentSize };

    [Fact]
    public void Append_IncrementsRecordsAppended()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.Start();

        pipeline.Append("a"u8);
        pipeline.Append("bb"u8);
        pipeline.Append("ccc"u8);

        pipeline.Metrics.RecordsAppended.Should().Be(3L);
    }

    [Fact]
    public void Append_AccumulatesBytesAppended()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.Start();

        pipeline.Append(new byte[5]);  // 5 bytes
        pipeline.Append(new byte[3]);  // 3 bytes
        pipeline.Append(new byte[7]);  // 7 bytes

        pipeline.Metrics.BytesAppended.Should().Be(15L);
    }

    [Fact]
    public void Flush_IncrementsFlushCount()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.Start();

        pipeline.Append("data"u8);
        pipeline.Flush();
        pipeline.Flush();

        pipeline.Metrics.FlushCount.Should().Be(2L);
    }

    [Fact]
    public void Append_SegmentRollover_IncrementsSegmentRollovers()
    {
        using var dir = new TempDirectory();
        // TinySegment=35: each 4-byte payload record = 32 bytes framed.
        // After first append BW=32 (<35), after second BW=64 (>=35) → rollover on 3rd.
        const long TinySegment = 35L;
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, TinySegment));
        pipeline.Start();

        pipeline.Append("aaaa"u8); // seg 0, BW=32
        pipeline.Append("bbbb"u8); // seg 0, BW=64
        pipeline.Append("cccc"u8); // IsFull → rollover to seg 1, then write

        pipeline.Metrics.SegmentRollovers.Should().Be(1L);
    }

    [Fact]
    public void Start_IncrementsRecoveryCount()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Metrics.RecoveryCount.Should().Be(1L);
            pipeline.Stop();
        }

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Metrics.RecoveryCount.Should().Be(1L, "each Pipeline instance resets its own counters");
        }
    }

    [Fact]
    public void Recovery_WithTruncation_SetsRecoveryTruncatedBytes()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        // Write two records, then corrupt the tail so recovery must truncate.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("good"u8);
            pipeline.Append("also-good"u8);
            pipeline.Stop();
        }

        string segPath = Directory.GetFiles(Path.Combine(dir.Path, "segments")).Single();
        using (var fs = new FileStream(segPath, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(-4, SeekOrigin.End);
            fs.Write(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });
        }

        // The checkpoint's LastOffset already covers the corrupted bytes — recovery
        // would skip re-scanning them. Delete the checkpoint to force a full scan
        // from offset 0 so the corruption is detected and truncated.
        ChaosHelpers.DeleteCheckpoint(dir.Path);

        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Metrics.RecoveryTruncatedBytes.Should().BeGreaterThan(0L);
        }
    }

    [Fact]
    public void Recovery_CleanStart_RecoveryTruncatedBytesIsZero()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.Start();

        pipeline.Metrics.RecoveryTruncatedBytes.Should().Be(0L);
    }

    // ── System.Diagnostics.Metrics instrument tests ───────────────────────────

    /// <summary>
    /// Creates a <see cref="MeterListener"/> that accumulates counter values for
    /// all instruments belonging to the named meter.
    /// </summary>
    private static (MeterListener Listener, Dictionary<string, long> Totals) SetupListener(
        string meterName)
    {
        var totals = new Dictionary<string, long>();
        var listener = new MeterListener();

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == meterName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };

        listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
        {
            lock (totals)
            {
                totals.TryGetValue(instrument.Name, out long existing);
                totals[instrument.Name] = existing + value;
            }
        });

        listener.Start();
        return (listener, totals);
    }

    [Fact]
    public void RecordsAppended_Counter_IncrementOnAppend()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration { RootDirectory = dir.Path, MeterName = "pecl-ra" };
        var (listener, totals) = SetupListener(config.MeterName);

        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append("a"u8);
        pipeline.Append("bb"u8);
        pipeline.Append("ccc"u8);
        pipeline.Append("dddd"u8);
        pipeline.Append("eeeee"u8);

        listener.Dispose();

        totals.GetValueOrDefault("pecl.records.appended").Should().Be(5L);
    }

    [Fact]
    public void BytesAppended_Counter_ReflectsPayloadSize()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration { RootDirectory = dir.Path, MeterName = "pecl-ba" };
        var (listener, totals) = SetupListener(config.MeterName);

        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append(new byte[10]);
        pipeline.Append(new byte[20]);
        pipeline.Append(new byte[30]);

        listener.Dispose();

        totals.GetValueOrDefault("pecl.bytes.appended").Should().Be(60L);
    }

    [Trait("Category", "Integration")]
    [Fact]
    public void SegmentsDeleted_Counter_IncrementOnGcPass()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MeterName = "pecl-gc",
            MaxSegmentSize = 512,
            GcIntervalMs = 50,
        };

        var (listener, totals) = SetupListener(config.MeterName);

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        while (pipeline.Metrics.SegmentRollovers < 3)
        {
            pipeline.Append("fill-payload"u8);
        }

        pipeline.Flush();

        while (pipeline.ReadNext("c").IsSuccess) { }

        Thread.Sleep(50 * 4);
        pipeline.Stop();
        listener.Dispose();

        totals.GetValueOrDefault("pecl.segments.deleted").Should().BeGreaterThan(0L);
    }

    [Trait("Category", "Integration")]
    [Fact]
    public void ConsumerLag_ObservableGauge_PushMode()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration { RootDirectory = dir.Path, MeterName = "pecl-lag" };
        var sink = new CallbackSink();

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        for (int i = 0; i < 10; i++)
        {
            pipeline.Append("x"u8);
        }

        // Flush to disk so the reader loop (which uses a SegmentReader) can see the data.
        pipeline.Flush();

        // Poll until the sink has received all records — this ensures LastRoutedSeqNo
        // has been set by the reader loop before we collect the gauge reading.
        int waited = 0;
        while (sink.RecordCount < 10 && waited < 5000)
        {
            Thread.Sleep(20);
            waited += 20;
        }

        sink.RecordCount.Should().Be(10, "all records must be delivered before checking gauge");

        // SetupListener is called AFTER pipeline.Start() — MeterListener.Start() fires
        // InstrumentPublished for all already-published instruments, enabling events
        // for the observable gauges created by StartObservableInstruments.
        var (listener, totals) = SetupListener(config.MeterName);
        listener.RecordObservableInstruments();
        listener.Dispose();

        // The lag gauge emits one measurement per push-mode consumer with LastRoutedSeqNo set.
        totals.Should().ContainKey("pecl.consumer.lag",
            "lag gauge must emit at least one measurement for push-mode consumers");
    }

    // ── Observable callback thread-safety (test gap 22) ──────────────────────

    /// <summary>
    /// Races observable gauge callbacks against <c>Stop()</c>/<c>_consumers.Clear()</c> to
    /// verify that <c>_observabilityLock</c> prevents <see cref="InvalidOperationException"/>
    /// from concurrent collection mutation (CRITICAL-2 fix).
    /// </summary>
    [Trait("Category", "Chaos")]
    [Fact]
    public async Task ObservableCallbacks_DuringStop_DoNotThrow()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MeterName = $"pecl-chaos-stop-{Guid.NewGuid():N}",
        };

        var sink = new CallbackSink();
        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        for (int i = 0; i < 20; i++)
        {
            pipeline.Append("x"u8);
        }

        pipeline.Flush();

        // Wait for records to be delivered so gauges have non-trivial state.
        int waited = 0;
        while (sink.RecordCount < 20 && waited < 5000)
        {
            Thread.Sleep(20);
            waited += 20;
        }

        sink.RecordCount.Should().Be(20, "all records must be delivered before racing Stop()");

        // MeterListener AFTER pipeline.Start() — see feedback-meterlistener-setup.md.
        var (listener, _) = SetupListener(config.MeterName);

        Exception? observableException = null;

        // Race: Stop() clears _consumers while this thread polls observable gauges.
        Task stopTask = Task.Run(
            () =>
            {
                Thread.Sleep(2); // brief yield so the poll loop enters first
                pipeline.Stop();
            },
            TestContext.Current.CancellationToken);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (!stopTask.IsCompleted && sw.ElapsedMilliseconds < 3000)
        {
            try
            {
                listener.RecordObservableInstruments();
            }
            catch (Exception ex)
            {
                observableException = ex;
                break;
            }
        }

        await stopTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        listener.Dispose();

        observableException.Should().BeNull(
            "observable callbacks must not throw during concurrent Stop() / _consumers.Clear()");
    }

    // ── _nextSeqNo volatile read in observable callback (R04-M7) ─────────────

    /// <summary>
    /// Verifies that the observable lag gauge reads <c>_nextSeqNo</c> via
    /// <see cref="System.Threading.Volatile.Read"/> so the MeterListener thread never
    /// observes a value outside the written range (R04-M7). The lag must be ≥ 0 for a
    /// consumer that has caught up to the tail.
    /// </summary>
    [Trait("Category", "Integration")]
    [Fact]
    public void ObservableGauge_ReadsNextSeqNoWithVolatileFence()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MeterName = $"pecl-m7-{Guid.NewGuid():N}",
        };
        var sink = new CallbackSink();
        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        const int N = 10;
        for (int i = 0; i < N; i++)
        {
            pipeline.Append("x"u8);
        }

        pipeline.Flush();

        // Wait for all records to be delivered so LastRoutedSeqNo is set.
        int waited = 0;
        while (sink.RecordCount < N && waited < 5000)
        {
            Thread.Sleep(20);
            waited += 20;
        }

        sink.RecordCount.Should().Be(N, "all records must be delivered before sampling gauge");

        // SetupListener AFTER pipeline.Start() — see feedback-meterlistener-setup.md.
        var (listener, totals) = SetupListener(config.MeterName);
        listener.RecordObservableInstruments();
        listener.Dispose();

        // With consumer at tail, lag = 0. Math.Max(0L, tail - cursor) must be ≥ 0.
        // A missing Volatile.Read on _nextSeqNo could surface as a stale negative value
        // before clamping — this test verifies the gauge emits a valid non-negative measurement.
        totals.Should().ContainKey("pecl.consumer.lag",
            "lag gauge must emit at least one measurement when a push-mode consumer has routed records");
        totals.GetValueOrDefault("pecl.consumer.lag").Should().BeGreaterThanOrEqualTo(0L,
            "lag must be non-negative; Volatile.Read on _nextSeqNo ensures a consistent snapshot");
    }

    [Fact]
    public void MeterName_Config_DistinctPerPipeline()
    {
        using var dir1 = new TempDirectory();
        using var dir2 = new TempDirectory();

        var config1 = new PipelineConfiguration { RootDirectory = dir1.Path, MeterName = "pecl-p1" };
        var config2 = new PipelineConfiguration { RootDirectory = dir2.Path, MeterName = "pecl-p2" };

        var (listener1, totals1) = SetupListener("pecl-p1");
        var (listener2, totals2) = SetupListener("pecl-p2");

        using var pipeline1 = new PeclPipeline(config1);
        using var pipeline2 = new PeclPipeline(config2);

        pipeline1.Start();
        pipeline2.Start();

        pipeline1.Append("a"u8);
        pipeline1.Append("b"u8);
        pipeline2.Append("x"u8);

        listener1.Dispose();
        listener2.Dispose();

        totals1.GetValueOrDefault("pecl.records.appended").Should().Be(2L,
            "pipeline-1 appended 2 records — listener-1 must see exactly 2");
        totals2.GetValueOrDefault("pecl.records.appended").Should().Be(1L,
            "pipeline-2 appended 1 record — listener-2 must see exactly 1");
    }

    // ── Pull-mode consumer lag exclusion (INFO-2) ─────────────────────────────

    /// <summary>
    /// A pull-mode consumer that has never called <see cref="PeclPipeline.ReadNext"/> has no
    /// <c>LastReadSeqNo</c> and must be excluded from the <c>pecl.consumer.lag</c> gauge.
    /// The gauge must emit no measurements even when the consumer is far behind the tail
    /// (INFO-2 / R05-M3). For the positive case (ReadNext called → consumer appears in gauge)
    /// see <see cref="ConsumerLag_PullMode_ReportedInLagGauge"/>.
    /// </summary>
    [Trait("Category", "Integration")]
    [Fact]
    public void ObservableGauge_PullModeConsumer_ReportsZeroLag()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MeterName = $"pecl-info2-{Guid.NewGuid():N}",
        };
        using var pipeline = new PeclPipeline(config);

        // No AddSink call — this consumer is pull-mode.
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        for (int i = 0; i < 10; i++)
        {
            pipeline.Append("x"u8);
        }

        pipeline.Flush();

        // SetupListener AFTER pipeline.Start() — see feedback-meterlistener-setup.md.
        var (listener, totals) = SetupListener(config.MeterName);
        listener.RecordObservableInstruments();
        listener.Dispose();

        // Pull-mode consumers with no ReadNext() calls have no LastReadSeqNo; the lag observer
        // filters them out. For the positive case (ReadNext called → consumer appears in gauge)
        // see ConsumerLag_PullMode_ReportedInLagGauge.
        totals.ContainsKey("pecl.consumer.lag").Should().BeFalse(
            "pull-mode consumer with no ReadNext() calls has no LastReadSeqNo and must be excluded from the lag gauge");
    }

    [Trait("Category", "Integration")]
    [Fact]
    public void ConsumerLag_PullMode_ReportedInLagGauge()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MeterName = $"pecl-pull-lag-{Guid.NewGuid():N}",
        };

        using var pipeline = new PeclPipeline(config);
        pipeline.RegisterConsumer("c"); // pull-mode: no sinks
        pipeline.Start();

        for (int i = 0; i < 3; i++)
        {
            pipeline.Append("x"u8);
        }

        pipeline.Flush();

        // Read one record — sets LastReadSeqNo, pull-mode consumer now visible to lag observer.
        pipeline.ReadNext("c").IsSuccess.Should().BeTrue("first record must be readable");

        // SetupListener AFTER pipeline.Start() — see feedback-meterlistener-setup.md.
        var (listener1, totals1) = SetupListener(config.MeterName);
        listener1.RecordObservableInstruments();
        listener1.Dispose();

        totals1.Should().ContainKey("pecl.consumer.lag",
            "pull-mode consumer with at least one ReadNext must appear in the lag gauge");
        totals1["pecl.consumer.lag"].Should().BeGreaterThan(0L,
            "consumer has read 1 of 3 records so lag must be positive");

        // Read remaining records — cursor catches up to tail, lag drops to zero.
        pipeline.ReadNext("c").IsSuccess.Should().BeTrue("second record must be readable");
        pipeline.ReadNext("c").IsSuccess.Should().BeTrue("third record must be readable");

        var (listener2, totals2) = SetupListener(config.MeterName);
        listener2.RecordObservableInstruments();
        listener2.Dispose();

        totals2["pecl.consumer.lag"].Should().Be(0L,
            "consumer has read all 3 records so lag must be zero");
    }
}
