using BenchmarkDotNet.Attributes;
using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Sinks;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Benchmarks;

/// <summary>
/// Measures fan-out throughput: records routed through a broadcast router to one or
/// three <see cref="ISink"/> instances per consumer. Each iteration appends
/// <see cref="RecordsPerBatch"/> records then calls <c>Stop()</c> to drain all lanes.
/// </summary>
/// <remarks>
/// Run in Release mode only:
/// <code>dotnet run --project benchmarks/ByTech.EmbeddedCommitLog.Benchmarks -c Release</code>
/// Record results in <c>.docs/PERFORMANCE-BASELINE.MD</c>.
/// </remarks>
[MemoryDiagnoser]
public class FanOutBenchmark
{
    /// <summary>Number of records written per benchmark iteration.</summary>
    public const int RecordsPerBatch = 1_000;

    /// <summary>64-byte payload — representative of a small-to-medium telemetry event.</summary>
    private static readonly byte[] _payload = new byte[64];

    private string? _rootDirectory;
    private PeclPipeline? _pipeline;

    /// <summary>Creates a fresh pipeline in a unique temp directory before each iteration.</summary>
    [IterationSetup(Targets = [nameof(FanOut_1Sink)])]
    public void Setup1Sink()
    {
        _rootDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_rootDirectory);

        _pipeline = new PeclPipeline(new PipelineConfiguration
        {
            RootDirectory = _rootDirectory,
            MaxSegmentSize = 512L * 1024 * 1024, // 512 MiB — avoid rollover
        });
        _pipeline.RegisterConsumer("c");
        _pipeline.AddSink("c", "s1", new NoOpSink());
        _pipeline.Start();
    }

    /// <summary>Creates a fresh pipeline in a unique temp directory before each iteration.</summary>
    [IterationSetup(Targets = [nameof(FanOut_3Sinks)])]
    public void Setup3Sinks()
    {
        _rootDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_rootDirectory);

        _pipeline = new PeclPipeline(new PipelineConfiguration
        {
            RootDirectory = _rootDirectory,
            MaxSegmentSize = 512L * 1024 * 1024,
        });
        _pipeline.RegisterConsumer("c");
        _pipeline.AddSink("c", "s1", new NoOpSink());
        _pipeline.AddSink("c", "s2", new NoOpSink());
        _pipeline.AddSink("c", "s3", new NoOpSink());
        _pipeline.Start();
    }

    /// <summary>Disposes the pipeline and deletes the temp directory after each iteration.</summary>
    [IterationCleanup]
    public void Cleanup()
    {
        _pipeline?.Dispose();
        _pipeline = null;

        if (_rootDirectory is not null && Directory.Exists(_rootDirectory))
        {
            Directory.Delete(_rootDirectory, recursive: true);
        }
    }

    /// <summary>
    /// Appends <see cref="RecordsPerBatch"/> records and calls <c>Stop()</c> to drain
    /// one sink lane. Measures end-to-end fan-out latency for a single-sink consumer.
    /// </summary>
    [Benchmark]
    public void FanOut_1Sink()
    {
        for (int i = 0; i < RecordsPerBatch; i++)
        {
            _pipeline!.Append(_payload);
        }
        _pipeline!.Stop();
    }

    /// <summary>
    /// Appends <see cref="RecordsPerBatch"/> records and calls <c>Stop()</c> to drain
    /// three sink lanes. Measures broadcast fan-out overhead vs. single-sink baseline.
    /// </summary>
    [Benchmark]
    public void FanOut_3Sinks()
    {
        for (int i = 0; i < RecordsPerBatch; i++)
        {
            _pipeline!.Append(_payload);
        }
        _pipeline!.Stop();
    }

    /// <summary>A no-op <see cref="ISink"/> that discards all records immediately.</summary>
    private sealed class NoOpSink : ISink
    {
        /// <inheritdoc/>
        public Task WriteAsync(IReadOnlyList<LogRecord> batch, CancellationToken ct) =>
            Task.CompletedTask;
    }
}
