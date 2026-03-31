using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.Chaos;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineRecoveryTests
{
    private static PipelineConfiguration MakeConfig(string rootDirectory) =>
        new() { RootDirectory = rootDirectory };

    /// <summary>
    /// Recovery starting at <c>chk.LastOffset</c> (PHASE-03-05 T01 optimization) must
    /// still produce exactly the right record set after a simulated crash where the
    /// segment is truncated back to the flush boundary.
    ///
    /// Design: write 5 records and flush durably (checkpoint at <c>LastOffset = X</c>).
    /// Write 3 more and stop (updates checkpoint to <c>LastOffset = Y</c>). Truncate
    /// the segment back to <c>X</c>. On recovery, <c>scanStart = Y</c> which is past
    /// the truncated file end — recovery treats this as a clean tail and uses the
    /// checkpoint's <c>LastSeqNo</c> to set <c>_nextSeqNo</c>. The consumer must read
    /// exactly the 5 durably-flushed records.
    /// </summary>
    [Fact]
    public void Recovery_StartsAtCheckpointOffset_SkipsAlreadyValidRecords()
    {
        using var dir = new TempDirectory();
        var config = MakeConfig(dir.Path);

        long flushBoundary;

        // Phase 1: write 5 records and flush durably, then write 3 more and stop.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.Start();
            pipeline.Append("rec-a"u8);
            pipeline.Append("rec-b"u8);
            pipeline.Append("rec-c"u8);
            pipeline.Append("rec-d"u8);
            pipeline.Append("rec-e"u8);
            pipeline.Flush();

            // Checkpoint now points to LastOffset = file size after 5 records.
            flushBoundary = ChaosHelpers.GetSegmentFileSize(dir.Path, 0u);

            // Append 3 more. Stop writes them to disk — we will truncate below to
            // simulate a crash that discarded the unflushed data.
            pipeline.Append("rec-f"u8);
            pipeline.Append("rec-g"u8);
            pipeline.Append("rec-h"u8);
            pipeline.Stop();
        }

        // Simulate crash: truncate the segment back to the flush boundary.
        // After this the checkpoint's LastOffset points beyond the file end.
        // Recovery must handle this correctly: scanStart > file size → clean
        // tail found → 5 durably-flushed records remain readable.
        ChaosHelpers.TruncateSegment(dir.Path, 0u, flushBoundary);

        // Phase 2: recover — exactly the 5 durably-flushed records must be visible.
        {
            using var pipeline = new PeclPipeline(config);
            pipeline.RegisterConsumer("reader");
            pipeline.Start();

            var r0 = pipeline.ReadNext("reader");
            var r1 = pipeline.ReadNext("reader");
            var r2 = pipeline.ReadNext("reader");
            var r3 = pipeline.ReadNext("reader");
            var r4 = pipeline.ReadNext("reader");
            var tail = pipeline.ReadNext("reader");

            r0.IsSuccess.Should().BeTrue();
            r0.Value.Payload.ToArray().Should().Equal("rec-a"u8.ToArray());
            r1.IsSuccess.Should().BeTrue();
            r1.Value.Payload.ToArray().Should().Equal("rec-b"u8.ToArray());
            r2.IsSuccess.Should().BeTrue();
            r2.Value.Payload.ToArray().Should().Equal("rec-c"u8.ToArray());
            r3.IsSuccess.Should().BeTrue();
            r3.Value.Payload.ToArray().Should().Equal("rec-d"u8.ToArray());
            r4.IsSuccess.Should().BeTrue();
            r4.Value.Payload.ToArray().Should().Equal("rec-e"u8.ToArray());
            tail.IsFailure.Should().BeTrue();
            tail.Error.Code.Should().Be(PeclErrorCode.EndOfLog);

            pipeline.Stop();
        }
    }
}
