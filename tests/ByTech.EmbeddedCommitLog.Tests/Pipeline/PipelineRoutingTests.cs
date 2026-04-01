using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

/// <summary>
/// Integration tests for <c>ContentTypeRouter</c> and <c>HashRouter</c> via
/// the content-type and hash-routing <c>AddSink</c> overloads.
/// </summary>
public sealed class PipelineRoutingTests
{
    // ── Guard: mixing strategies ─────────────────────────────────────────────

    [Fact]
    public void AddSink_MixBroadcastThenContentType_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s1", new CallbackSink((_, _) => Task.CompletedTask));

        Action act = () => pipeline.AddSink("c", "s2",
            new CallbackSink((_, _) => Task.CompletedTask), ContentType.Json);

        act.Should().Throw<InvalidOperationException>().WithMessage("*routing strategy*");
    }

    [Fact]
    public void AddSink_MixContentTypeThenBroadcast_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s1",
            new CallbackSink((_, _) => Task.CompletedTask), ContentType.Json);

        Action act = () => pipeline.AddSink("c", "s2",
            new CallbackSink((_, _) => Task.CompletedTask));

        act.Should().Throw<InvalidOperationException>().WithMessage("*routing strategy*");
    }

    [Fact]
    public void AddSink_MixContentTypeThenHash_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });
        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s1",
            new CallbackSink((_, _) => Task.CompletedTask), ContentType.Json);

        Action act = () => pipeline.AddSink("c", "s2",
            new CallbackSink((_, _) => Task.CompletedTask),
            (LogRecord r) => (int)r.Header.SeqNo);

        act.Should().Throw<InvalidOperationException>().WithMessage("*routing strategy*");
    }

    // ── ContentType routing ──────────────────────────────────────────────────

    [Trait("Category", "Integration")]
    [Fact]
    public void AddSink_ContentTypeFilter_SinkReceivesOnlyMatchingRecords()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });

        var received = new List<LogRecord>();
        var mre = new System.Threading.ManualResetEventSlim(false);

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "json-only", new CallbackSink((batch, _) =>
        {
            lock (received)
            {
                foreach (LogRecord r in batch)
                {
                    received.Add(r);
                }

                if (received.Count >= 1)
                {
                    mre.Set();
                }
            }

            return Task.CompletedTask;
        }), ContentType.Json);
        pipeline.Start();

        pipeline.Append("json-payload"u8.ToArray(), ContentType.Json);
        pipeline.Append("proto-payload"u8.ToArray(), ContentType.Protobuf);
        pipeline.Flush();

        mre.Wait(TestContext.Current.CancellationToken);
        // Give the loop a moment to route any second record (it should not arrive)
        System.Threading.Thread.Sleep(100);

        lock (received)
        {
            received.Should().HaveCount(1, "only the Json record should reach the json-only sink");
            received[0].Header.ContentType.Should().Be(ContentType.Json);
        }
    }

    [Trait("Category", "Integration")]
    [Fact]
    public void AddSink_ContentTypeFilter_TwoSinks_EachReceivesOwnType()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });

        var jsonReceived = new List<LogRecord>();
        var protoReceived = new List<LogRecord>();
        var jsonMre = new System.Threading.ManualResetEventSlim(false);
        var protoMre = new System.Threading.ManualResetEventSlim(false);

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "json-sink", new CallbackSink((batch, _) =>
        {
            lock (jsonReceived)
            {
                foreach (LogRecord r in batch)
                {
                    jsonReceived.Add(r);
                }

                if (jsonReceived.Count >= 1)
                {
                    jsonMre.Set();
                }
            }

            return Task.CompletedTask;
        }), ContentType.Json);
        pipeline.AddSink("c", "proto-sink", new CallbackSink((batch, _) =>
        {
            lock (protoReceived)
            {
                foreach (LogRecord r in batch)
                {
                    protoReceived.Add(r);
                }

                if (protoReceived.Count >= 1)
                {
                    protoMre.Set();
                }
            }

            return Task.CompletedTask;
        }), ContentType.Protobuf);
        pipeline.Start();

        pipeline.Append("j"u8.ToArray(), ContentType.Json);
        pipeline.Append("p"u8.ToArray(), ContentType.Protobuf);
        pipeline.Flush();

        jsonMre.Wait(TestContext.Current.CancellationToken);
        protoMre.Wait(TestContext.Current.CancellationToken);

        lock (jsonReceived)
        {
            jsonReceived.Should().HaveCount(1);
        }

        lock (protoReceived)
        {
            protoReceived.Should().HaveCount(1);
        }
    }

    // ── Hash routing ─────────────────────────────────────────────────────────

    [Trait("Category", "Integration")]
    [Fact]
    public void AddSink_HashRouting_PartitionsAcrossTwoSinks()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });

        var sink0SeqNos = new List<ulong>();
        var sink1SeqNos = new List<ulong>();
        const int TotalExpected = 4;
        int totalReceived = 0;
        var mre = new System.Threading.ManualResetEventSlim(false);

        // Routing key = SeqNo (first record is SeqNo 0)
        Func<LogRecord, int> keySelector = r => (int)r.Header.SeqNo;

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s0", new CallbackSink((batch, _) =>
        {
            lock (sink0SeqNos)
            {
                foreach (LogRecord r in batch)
                {
                    sink0SeqNos.Add(r.Header.SeqNo);
                }
            }

            if (System.Threading.Interlocked.Add(ref totalReceived, batch.Count) >= TotalExpected)
            {
                mre.Set();
            }

            return Task.CompletedTask;
        }), keySelector);
        pipeline.AddSink("c", "s1", new CallbackSink((batch, _) =>
        {
            lock (sink1SeqNos)
            {
                foreach (LogRecord r in batch)
                {
                    sink1SeqNos.Add(r.Header.SeqNo);
                }
            }

            if (System.Threading.Interlocked.Add(ref totalReceived, batch.Count) >= TotalExpected)
            {
                mre.Set();
            }

            return Task.CompletedTask;
        }), keySelector);
        pipeline.Start();

        // Append 4 records; SeqNos will be 0,1,2,3
        // 0 % 2 = 0 → s0, 1 % 2 = 1 → s1, 2 % 2 = 0 → s0, 3 % 2 = 1 → s1
        for (int i = 0; i < TotalExpected; i++)
        {
            pipeline.Append("x"u8.ToArray());
        }

        pipeline.Flush();

        mre.Wait(TestContext.Current.CancellationToken);

        lock (sink0SeqNos)
        {
            sink0SeqNos.Should().HaveCount(2, "even SeqNos route to s0");
        }

        lock (sink1SeqNos)
        {
            sink1SeqNos.Should().HaveCount(2, "odd SeqNos route to s1");
        }
    }
}
