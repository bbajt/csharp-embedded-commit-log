using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Checkpoint;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;

namespace ByTech.EmbeddedCommitLog.Tests.Checkpoint;

/// <summary>
/// Chaos tests that verify the crash-safe atomicity guarantees of the checkpoint
/// write-temp + fsync + rename strategy under simulated failure scenarios.
/// </summary>
/// <remarks>
/// These tests do not actually crash the process. They simulate crash artifacts by
/// directly manipulating files and then asserting that the system behaves correctly
/// when it encounters those artifacts on the next operation.
/// </remarks>
[Trait("Category", "Chaos")]
public sealed class CheckpointChaosTests
{
    private static readonly CheckpointData _dataD1 = new(
        LastSegmentId: 1u, LastOffset: 1024L, LastSeqNo: 100UL, ActiveSegmentId: 1u);

    private static readonly CheckpointData _dataD2 = new(
        LastSegmentId: 2u, LastOffset: 2048L, LastSeqNo: 200UL, ActiveSegmentId: 2u);

    // ── Crash during temp write ───────────────────────────────────────────────

    /// <summary>
    /// Simulates a crash during the write of <c>checkpoint.tmp</c>.
    /// The temp file is left partial/corrupt; <c>checkpoint.dat</c> must survive.
    /// </summary>
    [Fact]
    public void Chaos_CrashDuringTempWrite_OldCheckpointDatSurvives()
    {
        using var dir = new TempDirectory();

        // D1 is safely committed in checkpoint.dat
        CheckpointWriter.Write(dir.Path, _dataD1);

        // Simulate crash: a corrupt temp file was written, then process died
        File.WriteAllBytes(Path.Combine(dir.Path, "checkpoint.tmp"), [0xDE, 0xAD, 0xBE]);

        // checkpoint.dat must still return D1 (tmp is not read by CheckpointReader)
        var result = CheckpointReader.Read(dir.Path);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD1);
    }

    // ── Crash after fsync, before rename ─────────────────────────────────────

    /// <summary>
    /// Simulates a crash after the temp file was fsynced but before rename completed.
    /// Both <c>checkpoint.tmp</c> (valid D2) and <c>checkpoint.dat</c> (valid D1) exist.
    /// The reader must return D1 (reads <c>.dat</c>, ignores <c>.tmp</c>).
    /// </summary>
    [Fact]
    public void Chaos_CrashAfterFsyncBeforeRename_OldCheckpointDatSurvives()
    {
        using var dir = new TempDirectory();

        // Commit D1 safely
        CheckpointWriter.Write(dir.Path, _dataD1);

        // Simulate the state after fsync+before-rename: a valid D2 tmp file exists
        WriteRawCheckpoint(Path.Combine(dir.Path, "checkpoint.tmp"), _dataD2);

        // Reader sees D1 (the .dat file)
        var result = CheckpointReader.Read(dir.Path);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD1);
    }

    // ── No checkpoint.dat, only stale tmp ────────────────────────────────────

    /// <summary>
    /// Simulates first run: no <c>checkpoint.dat</c>, only a stale <c>checkpoint.tmp</c>
    /// from a prior incomplete write. The reader must return <see cref="PeclErrorCode.CheckpointMissing"/>.
    /// </summary>
    [Fact]
    public void Chaos_NoCheckpointDat_StaleTmpOnly_ReturnsCheckpointMissing()
    {
        using var dir = new TempDirectory();

        // Only a stale temp file exists (crash before first rename ever completed)
        WriteRawCheckpoint(Path.Combine(dir.Path, "checkpoint.tmp"), _dataD1);

        var result = CheckpointReader.Read(dir.Path);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CheckpointMissing);
    }

    // ── Writer recovery after stale tmp ──────────────────────────────────────

    /// <summary>
    /// Verifies that <see cref="CheckpointWriter.Write"/> succeeds even when a stale
    /// <c>checkpoint.tmp</c> is present, and that the resulting <c>checkpoint.dat</c> is valid.
    /// </summary>
    [Fact]
    public void Chaos_WriterRecovery_StaleTmpDoesNotPreventWrite()
    {
        using var dir = new TempDirectory();

        // Pre-existing stale tmp
        File.WriteAllBytes(Path.Combine(dir.Path, "checkpoint.tmp"), [0xAB, 0xCD, 0xEF]);

        CheckpointWriter.Write(dir.Path, _dataD2);

        var result = CheckpointReader.Read(dir.Path);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD2);
    }

    // ── Round-trip after corrupt checkpoint ──────────────────────────────────

    /// <summary>
    /// Simulates external corruption of <c>checkpoint.dat</c> (e.g. partial overwrite by
    /// another process). Verifies that a subsequent successful write produces a valid checkpoint.
    /// </summary>
    [Fact]
    public void Chaos_RoundTrip_WriteAfterCrashSimulation()
    {
        using var dir = new TempDirectory();

        // Commit D1, then corrupt it
        CheckpointWriter.Write(dir.Path, _dataD1);
        string datPath = Path.Combine(dir.Path, "checkpoint.dat");
        byte[] bytes = File.ReadAllBytes(datPath);
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(datPath, bytes);

        // Verify corruption is detected
        CheckpointReader.Read(dir.Path).IsFailure.Should().BeTrue();

        // Now commit D2 — must produce a valid checkpoint
        CheckpointWriter.Write(dir.Path, _dataD2);

        var result = CheckpointReader.Read(dir.Path);
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_dataD2);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Writes a fully valid raw checkpoint file at <paramref name="path"/>
    /// without using <see cref="CheckpointWriter"/> (so we can put it in .tmp directly).
    /// </summary>
    private static void WriteRawCheckpoint(string path, CheckpointData data)
    {
        // Use CheckpointWriter in a temp location, then copy to the target path.
        // This avoids reimplementing the serialization here.
        string tempDir = Path.Combine(Path.GetDirectoryName(path)!, "__rawtemp__");
        Directory.CreateDirectory(tempDir);

        try
        {
            CheckpointWriter.Write(tempDir, data);
            File.Copy(Path.Combine(tempDir, "checkpoint.dat"), path, overwrite: true);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }
}
