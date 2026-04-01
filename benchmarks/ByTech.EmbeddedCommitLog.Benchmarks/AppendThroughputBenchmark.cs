using BenchmarkDotNet.Attributes;
using ByTech.EmbeddedCommitLog.Pipeline;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Benchmarks;

/// <summary>
/// Measures sustained append throughput for a PECL pipeline using Batched durability:
/// <see cref="RecordsPerBatch"/> records are written then flushed as a group.
/// </summary>
/// <remarks>
/// Run in Release mode only:
/// <code>dotnet run --project benchmarks/ByTech.EmbeddedCommitLog.Benchmarks -c Release</code>
/// Record results in <c>.docs/PERFORMANCE-BASELINE.MD</c>.
/// </remarks>
[MemoryDiagnoser]
public class AppendThroughputBenchmark
{
    /// <summary>Number of records written per benchmark iteration (one batch).</summary>
    public const int RecordsPerBatch = 100_000;

    /// <summary>64-byte payload — representative of a small-to-medium telemetry event.</summary>
    private static readonly byte[] _payload = new byte[64];

    private string? _rootDirectory;
    private PeclPipeline? _pipeline;

    /// <summary>Creates a fresh pipeline in a unique temp directory before each iteration.</summary>
    [IterationSetup]
    public void Setup()
    {
        _rootDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_rootDirectory);

        _pipeline = new PeclPipeline(new PipelineConfiguration
        {
            RootDirectory = _rootDirectory,
            MaxSegmentSize = 512L * 1024 * 1024, // 512 MiB — avoid rollover during benchmark
        });
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
    /// Appends <see cref="RecordsPerBatch"/> records then flushes once (Batched durability).
    /// This is the primary throughput scenario: high append rate with periodic group commits.
    /// </summary>
    [Benchmark]
    public void AppendBatch()
    {
        for (int i = 0; i < RecordsPerBatch; i++)
        {
            _pipeline!.Append(_payload);
        }
        _pipeline!.Flush();
    }
}
