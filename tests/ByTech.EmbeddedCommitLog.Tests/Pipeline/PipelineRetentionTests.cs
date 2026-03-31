using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using FluentAssertions;
using System.Threading;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineRetentionTests
{
    // One 4-byte-payload record fills a segment sized to exactly FramingOverhead + 4 bytes.
    // The NEXT append then triggers a rollover — simplest way to create a sealed segment.
    private static long TinySegment => RecordWriter.FramingOverhead + 4;

    // ── Defaults ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that the default <see cref="RetentionPolicy"/> is
    /// <see cref="RetentionPolicy.ConsumerGated"/>.
    /// </summary>
    [Fact]
    public void RetentionPolicy_Default_IsConsumerGated()
    {
        var config = new PipelineConfiguration { RootDirectory = "." };
        config.RetentionPolicy.Should().Be(RetentionPolicy.ConsumerGated);
    }

    // ── Validation ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that <see cref="PeclPipeline.Start"/> throws
    /// <see cref="InvalidOperationException"/> when <see cref="RetentionPolicy.TimeBased"/>
    /// is selected with a non-positive <see cref="PipelineConfiguration.RetentionMaxAgeMs"/>.
    /// </summary>
    [Fact]
    public void RetentionPolicy_TimeBased_ZeroMaxAge_ThrowsOnStart()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            RetentionPolicy = RetentionPolicy.TimeBased,
            RetentionMaxAgeMs = 0,
        };
        using var pipeline = new PeclPipeline(config);
        var act = pipeline.Start;
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*RetentionMaxAgeMs*");
    }

    /// <summary>
    /// Verifies that <see cref="PeclPipeline.Start"/> throws
    /// <see cref="InvalidOperationException"/> when <see cref="RetentionPolicy.SizeBased"/>
    /// is selected with a non-positive <see cref="PipelineConfiguration.RetentionMaxBytes"/>.
    /// </summary>
    [Fact]
    public void RetentionPolicy_SizeBased_ZeroMaxBytes_ThrowsOnStart()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            RetentionPolicy = RetentionPolicy.SizeBased,
            RetentionMaxBytes = 0,
        };
        using var pipeline = new PeclPipeline(config);
        var act = pipeline.Start;
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*RetentionMaxBytes*");
    }

    // ── TimeBased ────────────────────────────────────────────────────────────────

    /// <summary>
    /// With <see cref="RetentionPolicy.TimeBased"/> and <c>RetentionMaxAgeMs = 1</c>,
    /// a sealed segment is older than 1 ms by the time the first GC pass runs
    /// (GcIntervalMs = 50 ms), so it is deleted and
    /// <see cref="PipelineMetrics.SegmentsDeleted"/> is incremented.
    /// </summary>
    [Fact]
    public void RetentionPolicy_TimeBased_DeletesOldSegmentsOnGcPass()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MaxSegmentSize = TinySegment,
            RetentionPolicy = RetentionPolicy.TimeBased,
            RetentionMaxAgeMs = 1,        // 1 ms — any sealed segment qualifies
            GcIntervalMs = 50,
            DurabilityMode = DurabilityMode.None,
        };
        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append("fill"u8); // fills seg0 (IsFull after this write)
        pipeline.Append("next"u8); // triggers rollover → seg0 sealed, seg1 active
        pipeline.Flush();

        Thread.Sleep(200); // allow ≥ 3 GC passes (GcIntervalMs = 50 ms)

        pipeline.Metrics.SegmentsDeleted.Should().BeGreaterThan(0);
        pipeline.Stop();
    }

    // ── SizeBased ────────────────────────────────────────────────────────────────

    /// <summary>
    /// With <see cref="RetentionPolicy.SizeBased"/> and <c>RetentionMaxBytes = 1</c>,
    /// total log size always exceeds the limit, so the oldest sealed segment is deleted
    /// on the first GC pass.
    /// </summary>
    [Fact]
    public void RetentionPolicy_SizeBased_DeletesWhenOverLimit()
    {
        using var dir = new TempDirectory();
        var config = new PipelineConfiguration
        {
            RootDirectory = dir.Path,
            MaxSegmentSize = TinySegment,
            RetentionPolicy = RetentionPolicy.SizeBased,
            RetentionMaxBytes = 1,        // 1 byte — any real segment exceeds this
            GcIntervalMs = 50,
            DurabilityMode = DurabilityMode.None,
        };
        using var pipeline = new PeclPipeline(config);
        pipeline.Start();

        pipeline.Append("fill"u8); // seals seg0
        pipeline.Append("next"u8); // seg1 active
        pipeline.Flush();

        Thread.Sleep(200); // allow ≥ 3 GC passes

        pipeline.Metrics.SegmentsDeleted.Should().BeGreaterThan(0);
        pipeline.Stop();
    }
}
