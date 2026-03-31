using System.Threading;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Sinks;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineLifecycleTests
{
    private static PipelineConfiguration MakeConfig(string rootDirectory) =>
        new() { RootDirectory = rootDirectory };

    /// <summary>
    /// Stop() on a pipeline in Draining state (indicates a prior Stop() threw) must
    /// throw InvalidOperationException rather than silently returning, so callers get
    /// a diagnostic pointing them to Dispose().
    /// </summary>
    [Fact]
    public void Stop_WhenStateIsDraining_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        var pipeline = new PeclPipeline(MakeConfig(dir.Path));

        // Force pipeline into Draining state via reflection to test the guard.
        typeof(PeclPipeline)
            .GetProperty("State")!
            .SetValue(pipeline, PipelineState.Draining);

        Action act = () => pipeline.Stop();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Draining*");
    }

    /// <summary>
    /// When a sink throws during Stop(), all file handles must be closed even though
    /// Stop() propagates an AggregateException. The temp directory must be deletable
    /// after the call, proving no handles remain open.
    /// </summary>
    [Fact]
    [Trait("Category", "Chaos")]
    public void Stop_WhenSinkThrows_AllHandlesClosedAfterStop()
    {
        using var dir = new TempDirectory();
        var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        var boom = new CallbackSink((_, _) => throw new InvalidOperationException("sink exploded"));

        pipeline.RegisterConsumer("c");
        pipeline.AddSink("c", "s", boom);
        pipeline.Start();
        pipeline.Append("trigger"u8);
        pipeline.Flush();
        Thread.Sleep(200);

        Action act = () => pipeline.Stop();

        // Stop() must throw because the sink faulted.
        act.Should().Throw<AggregateException>()
            .Which.InnerExceptions.Should().ContainSingle(
                e => e is InvalidOperationException && e.Message == "sink exploded");

        // After Stop() throws due to sink error, State must be Error.
        pipeline.State.Should().Be(PipelineState.Error);

        // CRITICAL: temp directory must be deletable — proves all file handles are closed.
        // TempDirectory.Dispose() (via the using declaration above) throws IOException
        // on Windows if any handle is still open. Reaching the end of this test exercises it.
    }

    /// <summary>
    /// RegisterConsumer after Start() must throw InvalidOperationException.
    /// Consumers must be registered before the pipeline starts.
    /// </summary>
    [Fact]
    public void RegisterConsumer_AfterStart_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.Start();

        Action act = () => pipeline.RegisterConsumer("late");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*late*Start*");

        pipeline.Stop();
    }

    /// <summary>
    /// RegisterConsumer on a stopped pipeline must succeed.
    /// State == Stopped is the only valid state for registration.
    /// </summary>
    [Fact]
    public void RegisterConsumer_WhenStopped_Succeeds()
    {
        using var dir = new TempDirectory();
        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));
        pipeline.Start();
        pipeline.Stop();

        // State is now Stopped — RegisterConsumer must not throw.
        Action act = () => pipeline.RegisterConsumer("after-stop");
        act.Should().NotThrow();
    }

    /// <summary>
    /// If <c>Start()</c> throws before reaching <see cref="PipelineState.Running"/> (e.g.
    /// <c>EnsureDirectories</c> fails because a file blocks the path), <c>State</c> must
    /// revert to <see cref="PipelineState.Stopped"/> — never permanently left at
    /// <c>Starting</c> (R04-L9 fix).
    /// </summary>
    [Fact]
    public void Start_WhenSetupThrows_StateIsStoppedAfterException()
    {
        using var dir = new TempDirectory();

        // Block Directory.CreateDirectory for the segments/ subdirectory by placing
        // a regular file at the path it would occupy.
        File.WriteAllText(Path.Combine(dir.Path, "segments"), "not a directory");

        var pipeline = new PeclPipeline(MakeConfig(dir.Path));

        Action act = () => pipeline.Start();

        // Start() must propagate the filesystem exception.
        act.Should().Throw<Exception>("EnsureDirectories cannot create segments/ when a file blocks it");

        // Critical: State must revert to Stopped — not stuck at Starting.
        pipeline.State.Should().Be(PipelineState.Stopped);
    }

    /// <summary>
    /// After a failed <c>Start()</c>, the same pipeline instance must be reusable once the
    /// root cause is fixed. <c>State</c> is <c>Stopped</c> and a subsequent <c>Start()</c>
    /// must succeed (R04-L9 fix).
    /// </summary>
    [Fact]
    public void Start_AfterFailedStart_CanRestartAfterFix()
    {
        using var dir = new TempDirectory();
        string blockedPath = Path.Combine(dir.Path, "segments");
        File.WriteAllText(blockedPath, "not a directory");

        using var pipeline = new PeclPipeline(MakeConfig(dir.Path));

        // First Start() fails.
        try { pipeline.Start(); } catch { /* expected */ }

        pipeline.State.Should().Be(PipelineState.Stopped,
            "state must revert so the same instance can be retried after fixing the cause");

        // Fix: remove the blocking file so Directory.CreateDirectory can succeed.
        File.Delete(blockedPath);

        // Second Start() on the same instance must succeed.
        pipeline.Start();
        pipeline.State.Should().Be(PipelineState.Running);

        pipeline.Stop();
    }

    /// <summary>
    /// Dispose() on a pipeline whose sink throws must not itself throw, and must
    /// close all file handles so the temp directory can be cleaned up.
    /// </summary>
    [Fact]
    [Trait("Category", "Chaos")]
    public void Dispose_WhenSinkThrows_AllHandlesClosedAfterDispose()
    {
        using var dir = new TempDirectory();
        {
            var pipeline = new PeclPipeline(MakeConfig(dir.Path));
            var boom = new CallbackSink((_, _) => throw new InvalidOperationException("boom"));

            pipeline.RegisterConsumer("c");
            pipeline.AddSink("c", "s", boom);
            pipeline.Start();
            pipeline.Append("x"u8);
            pipeline.Flush();
            Thread.Sleep(200);

            // Dispose swallows exceptions — must not throw.
            Action act = () => pipeline.Dispose();
            act.Should().NotThrow();
        }

        // dir.Dispose() (implicit at end of using) throws IOException on Windows if any
        // handle is still open. Reaching here without IOException proves all handles closed.
    }
}
