using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Cursors;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;

namespace ByTech.EmbeddedCommitLog.Tests.Cursors;

public sealed class CursorFlusherTests
{
    private const string ConsumerName = "influxdb";
    private static readonly TimeSpan _longTimeout = TimeSpan.FromHours(1);
    private static readonly TimeSpan _shortTimeout = TimeSpan.FromSeconds(5);

    // ── Record threshold ──────────────────────────────────────────────────────

    [Fact]
    public void Advance_BelowRecordThreshold_DoesNotFlush()
    {
        using var dir = new TempDirectory();
        using var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 5, _longTimeout);

        for (int i = 0; i < 4; i++)
        {
            flusher.Advance(1u, i * 100L, (ulong)i);
        }

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeFalse();
    }

    [Fact]
    public void Advance_AtRecordThreshold_Flushes()
    {
        using var dir = new TempDirectory();
        using var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 5, _longTimeout);

        for (int i = 0; i < 5; i++)
        {
            flusher.Advance(1u, i * 100L, (ulong)i);
        }

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeTrue();
    }

    [Fact]
    public void Advance_CounterResetsAfterFlush()
    {
        using var dir = new TempDirectory();
        using var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 5, _longTimeout);

        // First batch: triggers flush at 5
        for (int i = 0; i < 5; i++)
        {
            flusher.Advance(1u, i * 100L, (ulong)i);
        }

        string curPath = Path.Combine(dir.Path, "influxdb.cur");
        long mtimeAfterFirstFlush = new FileInfo(curPath).LastWriteTimeUtc.Ticks;

        // Second batch: only 4 advances — should NOT trigger another flush
        for (int i = 5; i < 9; i++)
        {
            flusher.Advance(1u, i * 100L, (ulong)i);
        }

        new FileInfo(curPath).LastWriteTimeUtc.Ticks.Should().Be(mtimeAfterFirstFlush);
    }

    // ── Time threshold ────────────────────────────────────────────────────────

    [Fact]
    public void FlushIfDue_BelowTimeThreshold_DoesNotFlush()
    {
        using var dir = new TempDirectory();
        DateTimeOffset fixedTime = DateTimeOffset.UtcNow;
        using var flusher = new CursorFlusher(
            dir.Path, ConsumerName, recordThreshold: 1000, _shortTimeout,
            clock: () => fixedTime);

        flusher.Advance(1u, 100L, 1UL);
        flusher.FlushIfDue(); // time has not advanced — still below threshold

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeFalse();
    }

    [Fact]
    public void FlushIfDue_AtTimeThreshold_Flushes()
    {
        using var dir = new TempDirectory();
        DateTimeOffset startTime = DateTimeOffset.UtcNow;
        DateTimeOffset advancedTime = startTime + _shortTimeout;
        int call = 0;
        using var flusher = new CursorFlusher(
            dir.Path, ConsumerName, recordThreshold: 1000, _shortTimeout,
            clock: () => call++ == 0 ? startTime : advancedTime);

        flusher.Advance(1u, 100L, 1UL);       // call 0 — below time threshold, record < 1000
        flusher.FlushIfDue();                   // call 1 — time threshold met

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeTrue();
    }

    // ── Explicit flush ────────────────────────────────────────────────────────

    [Fact]
    public void Flush_WhenDirty_WritesFile()
    {
        using var dir = new TempDirectory();
        using var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1000, _longTimeout);

        flusher.Advance(1u, 100L, 1UL);
        flusher.Flush();

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeTrue();
    }

    [Fact]
    public void Flush_WhenNotDirty_NoWrite()
    {
        using var dir = new TempDirectory();
        using var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1, _longTimeout);

        // Trigger a flush via record threshold
        flusher.Advance(1u, 100L, 1UL);
        string curPath = Path.Combine(dir.Path, "influxdb.cur");
        long mtimeAfterFirstFlush = new FileInfo(curPath).LastWriteTimeUtc.Ticks;

        // Explicit flush with no intervening Advance — must be a no-op
        flusher.Flush();

        new FileInfo(curPath).LastWriteTimeUtc.Ticks.Should().Be(mtimeAfterFirstFlush);
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    [Fact]
    public void Dispose_WhenDirty_Flushes()
    {
        using var dir = new TempDirectory();
        var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1000, _longTimeout);

        flusher.Advance(2u, 512L, 7UL);
        flusher.Dispose();

        var result = CursorReader.Read(dir.Path, ConsumerName);
        result.IsSuccess.Should().BeTrue();
        result.Value.SegmentId.Should().Be(2u);
        result.Value.Offset.Should().Be(512L);
        result.Value.SeqNo.Should().Be(7UL);
    }

    [Fact]
    public void Dispose_WhenNotDirty_NoWrite()
    {
        using var dir = new TempDirectory();
        var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1000, _longTimeout);

        flusher.Dispose();

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeFalse();
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        using var dir = new TempDirectory();
        var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1000, _longTimeout);

        var act = () =>
        {
            flusher.Dispose();
            flusher.Dispose();
        };

        act.Should().NotThrow();
    }

    // ── Post-dispose guards ───────────────────────────────────────────────────

    [Fact]
    public void AfterDispose_Advance_ThrowsObjectDisposedException()
    {
        using var dir = new TempDirectory();
        var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1000, _longTimeout);
        flusher.Dispose();

        var act = () => flusher.Advance(1u, 0L, 0UL);

        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void AfterDispose_Flush_ThrowsObjectDisposedException()
    {
        using var dir = new TempDirectory();
        var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1000, _longTimeout);
        flusher.Dispose();

        var act = () => flusher.Flush();

        act.Should().Throw<ObjectDisposedException>();
    }

    // ── Position accuracy ─────────────────────────────────────────────────────

    [Fact]
    public void Advance_UpdatesPositionCorrectly()
    {
        using var dir = new TempDirectory();
        using var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1, _longTimeout);

        flusher.Advance(5u, 8192L, 42UL);

        var result = CursorReader.Read(dir.Path, ConsumerName);
        result.IsSuccess.Should().BeTrue();
        result.Value.SegmentId.Should().Be(5u);
        result.Value.Offset.Should().Be(8192L);
        result.Value.SeqNo.Should().Be(42UL);
    }

    [Fact]
    public void InitialPosition_ReflectedAfterFirstFlush()
    {
        using var dir = new TempDirectory();
        var flusher = new CursorFlusher(
            dir.Path, ConsumerName, recordThreshold: 1000, _longTimeout,
            initialSegmentId: 3u, initialOffset: 2048L, initialSeqNo: 99UL);

        flusher.Flush(); // not dirty — no write
        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeFalse();

        flusher.Advance(3u, 2048L, 100UL); // advance one record — triggers nothing (threshold 1000)
        flusher.Flush(); // force flush

        var result = CursorReader.Read(dir.Path, ConsumerName);
        result.IsSuccess.Should().BeTrue();
        result.Value.SegmentId.Should().Be(3u);
        result.Value.Offset.Should().Be(2048L);
        result.Value.SeqNo.Should().Be(100UL);
        flusher.Dispose();
    }

    // ── Constructor guards ────────────────────────────────────────────────────

    [Fact]
    public void Constructor_RecordThresholdZero_ThrowsArgumentOutOfRangeException()
    {
        using var dir = new TempDirectory();

        var act = () => new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 0, _longTimeout);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Constructor_TimeThresholdZero_ThrowsArgumentOutOfRangeException()
    {
        using var dir = new TempDirectory();

        var act = () => new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 1, TimeSpan.Zero);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    // ── Dispose idempotency (R05-L6) ──────────────────────────────────────────

    /// <summary>
    /// Calling <see cref="CursorFlusher.Dispose"/> twice must be a no-op on the second call.
    /// The first <c>Dispose</c> flushes dirty state; the second must not attempt another flush.
    /// Verifies that <c>_disposed = true</c> is set before <c>FlushCore()</c> so a concurrent
    /// or re-entrant second call returns immediately (R05-L6 fix).
    /// </summary>
    [Fact]
    public void Dispose_CalledTwice_SecondCallIsNoOp()
    {
        using var dir = new TempDirectory();
        var flusher = new CursorFlusher(dir.Path, ConsumerName, recordThreshold: 100, _longTimeout);

        // Advance to make the flusher dirty without triggering a threshold flush.
        flusher.Advance(0u, 100L, 1UL);

        // First Dispose: must write the cursor file (dirty state flushed).
        flusher.Dispose();

        string curPath = Path.Combine(dir.Path, $"{ConsumerName}.cur");
        File.Exists(curPath).Should().BeTrue("first Dispose must flush dirty state");
        long mtimeAfterFirstDispose = new FileInfo(curPath).LastWriteTimeUtc.Ticks;

        // Small delay to ensure any second write would produce a different mtime.
        System.Threading.Thread.Sleep(10);

        // Second Dispose: must be a no-op — cursor file must not be touched.
        flusher.Dispose();

        long mtimeAfterSecondDispose = new FileInfo(curPath).LastWriteTimeUtc.Ticks;
        mtimeAfterSecondDispose.Should().Be(mtimeAfterFirstDispose,
            "second Dispose must not flush again (R05-L6: _disposed set before FlushCore)");
    }
}
