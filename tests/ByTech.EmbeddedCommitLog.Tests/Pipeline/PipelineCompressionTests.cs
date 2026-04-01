using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineCompressionTests
{
    // Highly compressible JSON-like payload (~80 bytes, all ASCII, repeated chars).
    private static readonly byte[] _compressiblePayload =
        Encoding.UTF8.GetBytes("{\"event\":\"record\",\"value\":0,\"tags\":[\"a\",\"b\",\"c\"],\"extra\":\"xxxxxxxxxxxxxxxxxx\"}");

    private static PipelineConfiguration MakeConfig(
        string root,
        CompressionAlgorithm compression = CompressionAlgorithm.None,
        long maxSegmentSize = 64 * 1024 * 1024) =>
        new()
        {
            RootDirectory = root,
            MaxSegmentSize = maxSegmentSize,
            GcIntervalMs = int.MaxValue,
            CompressionAlgorithm = compression,
        };

    private static List<byte[]> ReadAllPayloads(PeclPipeline pipeline, string consumerName)
    {
        var payloads = new List<byte[]>();
        while (true)
        {
            var r = pipeline.ReadNext(consumerName);
            if (!r.IsSuccess)
            {
                break;
            }

            payloads.Add(r.Value.Payload.ToArray());
        }

        return payloads;
    }

    // ── None (baseline) ───────────────────────────────────────────────────────

    /// <summary>
    /// With <see cref="CompressionAlgorithm.None"/> the pipeline writes uncompressed records.
    /// Each payload read back must be byte-for-byte identical to the original.
    /// </summary>
    [Fact]
    public void Compression_None_RoundTrip_PayloadUnchanged()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.None));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        byte[] original = _compressiblePayload;
        for (int i = 0; i < 10; i++)
        {
            pipeline.Append(original);
        }

        pipeline.Flush();

        List<byte[]> payloads = ReadAllPayloads(pipeline, "c");
        pipeline.Stop();

        payloads.Should().HaveCount(10);
        foreach (byte[] payload in payloads)
        {
            payload.Should().Equal(original);
        }
    }

    // ── Brotli ────────────────────────────────────────────────────────────────

    /// <summary>
    /// With <see cref="CompressionAlgorithm.Brotli"/> and a compressible payload,
    /// each record read back must contain the original uncompressed bytes.
    /// </summary>
    [Fact]
    public void Compression_Brotli_RoundTrip_PayloadDecompressedCorrectly()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        byte[] original = _compressiblePayload;
        for (int i = 0; i < 10; i++)
        {
            pipeline.Append(original);
        }

        pipeline.Flush();

        List<byte[]> payloads = ReadAllPayloads(pipeline, "c");
        pipeline.Stop();

        payloads.Should().HaveCount(10);
        foreach (byte[] payload in payloads)
        {
            payload.Should().Equal(original);
        }
    }

    /// <summary>
    /// An incompressible payload (random bytes) must be stored without the
    /// <see cref="RecordFlags.IsCompressed"/> flag — i.e., compression falls back
    /// to uncompressed storage automatically.
    /// Verified by reading the raw segment file and inspecting the flags byte.
    /// </summary>
    [Fact]
    public void Compression_Brotli_IncompressiblePayload_StoredUncompressed()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        // 200 random bytes are not compressible by Brotli at quality 1.
        byte[] random = new byte[200];
        new Random(42).NextBytes(random);
        pipeline.Append(random);
        pipeline.Flush();
        pipeline.Stop();

        // Read the segment file directly and check the flags byte (offset 5 in the header).
        string segPath = Path.Combine(dir.Path, "segments", "log-000000.seg");
        byte[] rawSegment = File.ReadAllBytes(segPath);
        byte flagsByte = rawSegment[5]; // byte offset 5 = RecordFlags in header
        var flags = (RecordFlags)flagsByte;
        (flags & RecordFlags.IsCompressed).Should().Be(RecordFlags.None,
            "random bytes should not compress; writer must fall back to uncompressed");
    }

    /// <summary>
    /// An empty payload must be stored uncompressed even when Brotli is configured.
    /// The consumer must receive a zero-length payload.
    /// </summary>
    [Fact]
    public void Compression_Brotli_EmptyPayload_StoredUncompressed()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        pipeline.Append(ReadOnlySpan<byte>.Empty);
        pipeline.Flush();

        List<byte[]> payloads = ReadAllPayloads(pipeline, "c");
        pipeline.Stop();

        payloads.Should().ContainSingle();
        payloads[0].Should().BeEmpty();
    }

    /// <summary>
    /// A push-mode sink consumer must receive raw (decompressed) bytes even when
    /// the pipeline is configured with <see cref="CompressionAlgorithm.Brotli"/>.
    /// </summary>
    [Fact]
    public void Compression_Brotli_ConsumerReceivesDecompressedBytes()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli));

        byte[] original = _compressiblePayload;
        var received = new List<byte[]>();
        using var latch = new System.Threading.ManualResetEventSlim(false);

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "sink", new TestUtilities.Sinks.CallbackSink(async (batch, ct) =>
        {
            foreach (var record in batch)
            {
                received.Add(record.Payload.ToArray());
            }

            latch.Set();
            await System.Threading.Tasks.Task.CompletedTask;
        }));

        pipeline.Start();
        pipeline.Append(original);
        pipeline.Flush();
        latch.Wait(Xunit.TestContext.Current.CancellationToken);
        pipeline.Stop();

        received.Should().ContainSingle();
        received[0].Should().Equal(original);
    }

    /// <summary>
    /// Writing compressed records across a segment boundary must round-trip correctly.
    /// Verifies that the reader handles compressed records in both the first and subsequent segments.
    /// </summary>
    [Fact]
    public void Compression_Brotli_CrossSegmentRoundTrip()
    {
        using var dir = new TempDirectory();
        // Small segment: forces rollover after a few records.
        long segmentSize = (RecordWriter.FramingOverhead + _compressiblePayload.Length) * 3;
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli, segmentSize));
        pipeline.RegisterConsumer("c");
        pipeline.Start();

        byte[] original = _compressiblePayload;
        const int RecordCount = 9; // 3 full segments with 3 records each
        for (int i = 0; i < RecordCount; i++)
        {
            pipeline.Append(original);
        }

        pipeline.Flush();

        List<byte[]> payloads = ReadAllPayloads(pipeline, "c");
        pipeline.Stop();

        payloads.Should().HaveCount(RecordCount);
        foreach (byte[] payload in payloads)
        {
            payload.Should().Equal(original);
        }
    }

    /// <summary>
    /// Simulates a crash by truncating the last compressed record mid-frame, then
    /// restarts the pipeline. Recovery must succeed and all complete records before
    /// the truncation point must be readable.
    /// </summary>
    [Fact]
    public void Compression_Brotli_CrashRecovery_TruncatedCompressedRecord()
    {
        using var dir = new TempDirectory();
        byte[] original = _compressiblePayload;

        // Write 5 compressed records, flush, then truncate the last one.
        using (var pipeline = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli)))
        {
            pipeline.RegisterConsumer("c");
            pipeline.Start();
            for (int i = 0; i < 5; i++)
            {
                pipeline.Append(original);
            }

            pipeline.Flush();
            pipeline.Stop();
        }

        // Truncate the segment file by 8 bytes to corrupt the last record.
        string segPath = Path.Combine(dir.Path, "segments", "log-000000.seg");
        long fullSize = new FileInfo(segPath).Length;
        using (FileStream fs = new FileStream(segPath, FileMode.Open, FileAccess.Write))
        {
            fs.SetLength(fullSize - 8);
        }

        // Re-open: recovery should truncate at the corruption point.
        using var recovered = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli));
        recovered.RegisterConsumer("c");
        recovered.Start();

        List<byte[]> payloads = ReadAllPayloads(recovered, "c");
        recovered.Stop();

        // At least the first 4 complete records must be readable.
        payloads.Should().HaveCountGreaterThanOrEqualTo(4);
        foreach (byte[] payload in payloads)
        {
            payload.Should().Equal(original);
        }
    }

    /// <summary>
    /// Verifies the ADR-002 format self-description invariant: records written with
    /// <see cref="CompressionAlgorithm.Brotli"/> must be readable by a pipeline configured
    /// with <see cref="CompressionAlgorithm.None"/>. <see cref="RecordReader"/> is
    /// flag-driven and decompresses regardless of pipeline configuration.
    /// </summary>
    [Fact]
    public void Compression_Brotli_WrittenRecordsReadableByNonePipeline()
    {
        using var dir = new TempDirectory();
        byte[] original = _compressiblePayload;

        // Write 5 records with Brotli compression.
        using (var writer = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.Brotli)))
        {
            writer.RegisterConsumer("c");
            writer.Start();
            for (int i = 0; i < 5; i++)
            {
                writer.Append(original);
            }

            writer.Flush();
            writer.Stop();
        }

        // Re-open with CompressionAlgorithm.None — RecordReader is flag-driven and must
        // decompress regardless of pipeline config (ADR-002 self-description invariant).
        using var reader = new PeclPipeline(MakeConfig(dir.Path, CompressionAlgorithm.None));
        reader.RegisterConsumer("c");
        reader.Start();
        List<byte[]> payloads = ReadAllPayloads(reader, "c");
        reader.Stop();

        payloads.Should().HaveCount(5);
        foreach (byte[] p in payloads)
        {
            p.Should().Equal(original);
        }
    }
}
