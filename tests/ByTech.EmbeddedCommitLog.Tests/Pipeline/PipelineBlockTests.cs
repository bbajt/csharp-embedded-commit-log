using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

/// <summary>
/// Tests for <see cref="PeclPipeline.AppendBlock"/> and the transparent block-record
/// decoding in the reader loop.
/// </summary>
public sealed class PipelineBlockTests
{
    // ── Guard tests ───────────────────────────────────────────────────────────

    [Fact]
    public void AppendBlock_NullEntries_ThrowsArgumentNullException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });
        pipeline.Start();

        Action act = () => pipeline.AppendBlock(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AppendBlock_EmptyEntries_ThrowsArgumentException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });
        pipeline.Start();

        Action act = () => pipeline.AppendBlock(Array.Empty<ReadOnlyMemory<byte>>());

        act.Should().Throw<ArgumentException>().WithMessage("*at least one entry*");
    }

    // ── BlockPayloadWriter / BlockPayloadReader round-trip ────────────────────

    [Fact]
    public void BlockPayloadWriter_RoundTrips_WithBlockPayloadReader()
    {
        var entries = new List<ReadOnlyMemory<byte>>
        {
            "hello"u8.ToArray(),
            "world"u8.ToArray(),
            "!"u8.ToArray(),
        };

        int size = BlockPayloadWriter.ComputeSize(entries);
        byte[] buf = new byte[size];
        BlockPayloadWriter.Write(entries, buf);

        var reader = new BlockPayloadReader(buf);
        reader.EntryCount.Should().Be(3);

        reader.TryReadNext(out ReadOnlySpan<byte> e1).Should().BeTrue();
        e1.ToArray().Should().Equal("hello"u8.ToArray());

        reader.TryReadNext(out ReadOnlySpan<byte> e2).Should().BeTrue();
        e2.ToArray().Should().Equal("world"u8.ToArray());

        reader.TryReadNext(out ReadOnlySpan<byte> e3).Should().BeTrue();
        e3.ToArray().Should().Equal("!"u8.ToArray());

        reader.TryReadNext(out _).Should().BeFalse();
    }

    [Fact]
    public void BlockPayloadReader_CorruptPayload_ReturnsFalse()
    {
        // Payload too short to contain even the 4-byte count prefix
        var reader = new BlockPayloadReader(new byte[] { 0x01 });
        reader.EntryCount.Should().Be(0);
        reader.TryReadNext(out _).Should().BeFalse();
    }

    // ── Integration: pull-mode receives raw block record ─────────────────────

    [Trait("Category", "Integration")]
    [Fact]
    public void AppendBlock_SingleEntry_PullModeReceivesBlockRecord()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        pipeline.AppendBlock(new[] { (ReadOnlyMemory<byte>)"hello"u8.ToArray() });
        pipeline.Flush();

        var result = pipeline.ReadNext("c");
        result.IsSuccess.Should().BeTrue();
        result.Value.Header.Flags.HasFlag(RecordFlags.IsBlock).Should().BeTrue(
            "pull-mode ReadNext must return the raw block record with IsBlock flag set");

        // No more records
        pipeline.ReadNext("c").IsSuccess.Should().BeFalse();
    }

    // ── Integration: push-mode receives individual records ────────────────────

    [Trait("Category", "Integration")]
    [Fact]
    public void AppendBlock_MultipleEntries_SinkReceivesIndividualRecords()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration { RootDirectory = dir.Path };
        using var pipeline = new PeclPipeline(config);

        var received = new List<byte[]>();
        var mre = new System.Threading.ManualResetEventSlim(false);

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", new CallbackSink((batch, _) =>
        {
            lock (received)
            {
                foreach (var r in batch)
                {
                    received.Add(r.Payload);
                }

                if (received.Count >= 3)
                {
                    mre.Set();
                }
            }

            return Task.CompletedTask;
        }));
        pipeline.Start();

        var entries = new List<ReadOnlyMemory<byte>>
        {
            "aa"u8.ToArray(),
            "bb"u8.ToArray(),
            "cc"u8.ToArray(),
        };
        pipeline.AppendBlock(entries);
        pipeline.Flush();

        mre.Wait(TestContext.Current.CancellationToken);

        lock (received)
        {
            received.Should().HaveCount(3, "ISink must receive one LogRecord per block entry");
            received[0].Should().Equal("aa"u8.ToArray());
            received[1].Should().Equal("bb"u8.ToArray());
            received[2].Should().Equal("cc"u8.ToArray());
        }
    }

    [Trait("Category", "Integration")]
    [Fact]
    public void AppendBlock_MultipleEntries_ContentTypePreservedPerEntry()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });

        var headers = new List<RecordHeader>();
        var mre = new System.Threading.ManualResetEventSlim(false);

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", new CallbackSink((batch, _) =>
        {
            lock (headers)
            {
                foreach (var r in batch)
                {
                    headers.Add(r.Header);
                }

                if (headers.Count >= 2)
                {
                    mre.Set();
                }
            }

            return Task.CompletedTask;
        }));
        pipeline.Start();

        var entries = new List<ReadOnlyMemory<byte>>
        {
            "x"u8.ToArray(),
            "y"u8.ToArray(),
        };
        pipeline.AppendBlock(entries, ContentType.Json, 42u);
        pipeline.Flush();

        mre.Wait(TestContext.Current.CancellationToken);

        lock (headers)
        {
            headers.Should().HaveCount(2);
            foreach (var h in headers)
            {
                h.ContentType.Should().Be(ContentType.Json, "block entries inherit the block frame's ContentType");
                h.SchemaId.Should().Be(42u, "block entries inherit the block frame's SchemaId");
                h.Flags.Should().Be(RecordFlags.None, "synthesised entry records must not have IsBlock set");
            }
        }
    }

    [Trait("Category", "Integration")]
    [Fact]
    public void AppendBlock_MultipleEntries_SeqNoSharedAcrossEntries()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(new PipelineConfiguration { RootDirectory = dir.Path });

        var seqNos = new List<ulong>();
        var mre = new System.Threading.ManualResetEventSlim(false);

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", new CallbackSink((batch, _) =>
        {
            lock (seqNos)
            {
                foreach (var r in batch)
                {
                    seqNos.Add(r.Header.SeqNo);
                }

                if (seqNos.Count >= 3)
                {
                    mre.Set();
                }
            }

            return Task.CompletedTask;
        }));
        pipeline.Start();

        ulong blockSeqNo = pipeline.AppendBlock(new[]
        {
            (ReadOnlyMemory<byte>)"a"u8.ToArray(),
            (ReadOnlyMemory<byte>)"b"u8.ToArray(),
            (ReadOnlyMemory<byte>)"c"u8.ToArray(),
        });
        pipeline.Flush();

        mre.Wait(TestContext.Current.CancellationToken);

        lock (seqNos)
        {
            seqNos.Should().HaveCount(3);
            foreach (ulong sn in seqNos)
            {
                sn.Should().Be(blockSeqNo, "all entries within a block share the block frame's SeqNo");
            }
        }
    }
}
