using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Cursors;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;

namespace ByTech.EmbeddedCommitLog.Tests.Cursors;

/// <summary>
/// Chaos tests that verify the crash-safe atomicity guarantees of the cursor
/// write-temp + fsync + rename strategy, and the at-least-once replay guarantee
/// when the cursor lags behind actual processing position.
/// </summary>
/// <remarks>
/// These tests do not actually crash the process. They simulate crash artifacts by
/// directly manipulating files and then asserting that the system behaves correctly
/// when it encounters those artifacts on the next operation.
/// </remarks>
[Trait("Category", "Chaos")]
public sealed class CursorChaosTests
{
    private const string ConsumerName = "influxdb";
    private static readonly CursorData _dataD1 = new(ConsumerName, 1u, 1024L, 100UL);
    private static readonly CursorData _dataD2 = new(ConsumerName, 2u, 2048L, 200UL);

    // ── Crash before any flush ────────────────────────────────────────────────

    /// <summary>
    /// Simulates a consumer that advanced position but never flushed its cursor before crashing.
    /// On recovery the cursor file is absent — the consumer must replay from the beginning.
    /// </summary>
    [Fact]
    public void Chaos_CrashBeforeAnyFlush_CursorFileAbsent()
    {
        using var dir = new TempDirectory();
        using var flusher = new CursorFlusher(
            dir.Path, ConsumerName, recordThreshold: 1000, TimeSpan.FromHours(1));

        // Process 3 records, but threshold is 1000 — no flush happens
        for (int i = 0; i < 3; i++)
        {
            flusher.Advance(1u, i * 100L, (ulong)i);
        }

        // Simulate crash: flusher goes out of scope without Dispose being called
        // (process killed — Dispose does not run)
        GC.SuppressFinalize(flusher);

        var result = CursorReader.Read(dir.Path, ConsumerName);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CursorMissing);
    }

    // ── Flush lag: cursor behind actual position ──────────────────────────────

    /// <summary>
    /// Simulates the common case: D1 was committed and flushed; D2 was processed but the
    /// cursor flush did not occur before the crash. On recovery the cursor returns D1,
    /// causing D2 to be replayed (at-least-once semantics).
    /// </summary>
    [Fact]
    public void Chaos_CrashMidWindow_CursorLagsActualPosition()
    {
        using var dir = new TempDirectory();

        // D1 is committed and flushed
        CursorWriter.Write(dir.Path, _dataD1);

        // D2 is processed in memory (simulate via a new flusher seeded at D1 position
        // that advances to D2 but doesn't flush)
        var flusher = new CursorFlusher(
            dir.Path, ConsumerName, recordThreshold: 1000, TimeSpan.FromHours(1),
            initialSegmentId: _dataD1.SegmentId,
            initialOffset: _dataD1.Offset,
            initialSeqNo: _dataD1.SeqNo);
        flusher.Advance(_dataD2.SegmentId, _dataD2.Offset, _dataD2.SeqNo);
        GC.SuppressFinalize(flusher); // crash — Dispose not called

        // Recovery: cursor still reflects D1 → D2 will be replayed
        var result = CursorReader.Read(dir.Path, ConsumerName);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD1);
    }

    // ── Crash during temp write ───────────────────────────────────────────────

    /// <summary>
    /// Simulates a crash during the write of <c>influxdb.tmp</c>.
    /// The temp file is left corrupt; <c>influxdb.cur</c> must survive unchanged.
    /// </summary>
    [Fact]
    public void Chaos_CrashDuringTempWrite_OldCursorSurvives()
    {
        using var dir = new TempDirectory();

        // D1 is safely committed
        CursorWriter.Write(dir.Path, _dataD1);

        // Simulate crash: corrupt temp file written, then process died
        File.WriteAllBytes(Path.Combine(dir.Path, "influxdb.tmp"), [0xDE, 0xAD, 0xBE]);

        // influxdb.cur must still return D1
        var result = CursorReader.Read(dir.Path, ConsumerName);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD1);
    }

    // ── Crash after fsync, before rename ─────────────────────────────────────

    /// <summary>
    /// Simulates a crash after the temp file was fsynced but before rename completed.
    /// Both <c>influxdb.tmp</c> (valid D2) and <c>influxdb.cur</c> (valid D1) exist.
    /// The reader must return D1 (reads <c>.cur</c>, ignores <c>.tmp</c>).
    /// </summary>
    [Fact]
    public void Chaos_CrashAfterFsyncBeforeRename_OldCursorSurvives()
    {
        using var dir = new TempDirectory();

        // Commit D1 safely
        CursorWriter.Write(dir.Path, _dataD1);

        // Simulate the state after fsync+before-rename: valid D2 tmp exists
        WriteRawCursor(Path.Combine(dir.Path, "influxdb.tmp"), _dataD2);

        // Reader sees D1 (the .cur file)
        var result = CursorReader.Read(dir.Path, ConsumerName);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD1);
    }

    // ── Writer recovery after stale tmp ──────────────────────────────────────

    /// <summary>
    /// Verifies that <see cref="CursorWriter.Write"/> succeeds even when a stale
    /// <c>influxdb.tmp</c> is present, and that the resulting <c>influxdb.cur</c> is valid.
    /// </summary>
    [Fact]
    public void Chaos_WriterRecovery_StaleTmpDoesNotPreventWrite()
    {
        using var dir = new TempDirectory();

        // Pre-existing stale tmp
        File.WriteAllBytes(Path.Combine(dir.Path, "influxdb.tmp"), [0xAB, 0xCD, 0xEF]);

        CursorWriter.Write(dir.Path, _dataD2);

        var result = CursorReader.Read(dir.Path, ConsumerName);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD2);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Writes a fully valid raw cursor file at <paramref name="path"/>
    /// without using <see cref="CursorWriter"/> (so we can place it at an arbitrary path,
    /// such as <c>.tmp</c>, to simulate mid-crash state).
    /// </summary>
    private static void WriteRawCursor(string path, CursorData data)
    {
        string tempDir = Path.Combine(Path.GetDirectoryName(path)!, "__rawtemp__");
        Directory.CreateDirectory(tempDir);

        try
        {
            CursorWriter.Write(tempDir, data);
            File.Copy(
                Path.Combine(tempDir, data.ConsumerName + ".cur"),
                path,
                overwrite: true);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }
}
