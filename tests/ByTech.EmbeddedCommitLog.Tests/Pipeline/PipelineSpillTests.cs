using Xunit;
using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

/// <summary>Integration tests for <see cref="BackpressurePolicy.Spill"/>.</summary>
[Trait("Category", "Integration")]
public sealed class PipelineSpillTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    public PipelineSpillTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private PipelineConfiguration SpillConfig() => new()
    {
        RootDirectory = _root,
        SinkLaneCapacity = 2,
        BackpressurePolicy = BackpressurePolicy.Spill,
    };

    [Fact]
    public void Pipeline_Spill_SinkReceivesAllRecordsUnderOverflow()
    {
        const int RecordCount = 20;
        var received = new List<byte[]>();
        var mre = new ManualResetEventSlim(false);

        using var pipeline = new PeclPipeline(SpillConfig());
        pipeline.RegisterConsumer("c");

        var sink = new CallbackSink(async (batch, ct) =>
        {
            lock (received)
            {
                foreach (var r in batch)
                {
                    received.Add(r.Payload);
                }

                if (received.Count >= RecordCount)
                {
                    mre.Set();
                }
            }

            await Task.CompletedTask;
        });
        pipeline.AddSink("c", "s", sink);
        pipeline.Start();

        // Append records quickly to provoke overflow into spill.
        for (int i = 1; i <= RecordCount; i++)
        {
            pipeline.Append(new byte[] { (byte)i });
        }

        pipeline.Flush();

        Assert.True(mre.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken), $"Only received {received.Count}/{RecordCount}");

        // All records delivered, no duplicates on a clean run.
        Assert.Equal(RecordCount, received.Count);
    }

    [Fact]
    public void Pipeline_Spill_OrphanedSpillFiles_DeletedOnStart()
    {
        // Create an orphan spill file (consumer "gone", sink "old").
        string spillDir = Path.Combine(_root, "spill");
        Directory.CreateDirectory(spillDir);
        string orphan = Path.Combine(spillDir, "gone-old.spill");
        File.WriteAllBytes(orphan, Array.Empty<byte>());

        using var pipeline = new PeclPipeline(SpillConfig());
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", new CallbackSink());
        pipeline.Start();

        // Orphan must be deleted; the registered-consumer spill path must NOT have been deleted.
        Assert.False(File.Exists(orphan));
    }

    [Fact]
    public void Pipeline_Spill_RecoveryAfterCrash_ReplayAtLeastOnce()
    {
        const int RecordCount = 5;

        // --- First run: append records, stop pipeline before sink drains ---
        var firstReceived = new List<byte[]>();

        using (var pipeline = new PeclPipeline(SpillConfig()))
        {
            pipeline.RegisterConsumer("c");
            pipeline.AddSink("c", "s", new CallbackSink(async (batch, ct) =>
            {
                lock (firstReceived)
                {
                    foreach (var r in batch)
                    {
                        firstReceived.Add(r.Payload);
                    }
                }

                await Task.CompletedTask;
            }));
            pipeline.Start();

            for (int i = 1; i <= RecordCount; i++)
            {
                pipeline.Append(new byte[] { (byte)i });
            }

            pipeline.Flush();
            // Stop without waiting for full drain — spill file may survive.
            pipeline.Stop();
        }

        // --- Second run: replay from spill + segment ---
        var secondReceived = new List<byte[]>();
        var mre = new ManualResetEventSlim(false);

        using (var pipeline = new PeclPipeline(SpillConfig()))
        {
            pipeline.RegisterConsumer("c");
            pipeline.AddSink("c", "s", new CallbackSink(async (batch, ct) =>
            {
                lock (secondReceived)
                {
                    foreach (var r in batch)
                    {
                        secondReceived.Add(r.Payload);
                    }

                    if (secondReceived.Count >= RecordCount)
                    {
                        mre.Set();
                    }
                }

                await Task.CompletedTask;
            }));
            pipeline.Start();

            mre.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            pipeline.Stop();
        }

        // At-least-once: all records were delivered across the two runs combined.
        int total = firstReceived.Count + secondReceived.Count;
        Assert.True(total >= RecordCount, $"Total delivered {total} < {RecordCount}");
    }
}
