using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Checkpoint;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;

namespace ByTech.EmbeddedCommitLog.Tests.Checkpoint;

public sealed class CheckpointWriterTests
{
    private static readonly CheckpointData _sampleData = new(
        LastSegmentId: 3u,
        LastOffset: 8192L,
        LastSeqNo: 1000UL,
        ActiveSegmentId: 4u);

    // ── File creation ────────────────────────────────────────────────────────

    [Fact]
    public void Write_CreatesCheckpointDatFile()
    {
        using var dir = new TempDirectory();

        CheckpointWriter.Write(dir.Path, _sampleData);

        File.Exists(Path.Combine(dir.Path, "checkpoint.dat")).Should().BeTrue();
    }

    [Fact]
    public void Write_NoTempFileAfterSuccess()
    {
        using var dir = new TempDirectory();

        CheckpointWriter.Write(dir.Path, _sampleData);

        File.Exists(Path.Combine(dir.Path, "checkpoint.tmp")).Should().BeFalse();
    }

    [Fact]
    public void Write_CheckpointDatIsExactlySerializedSizeBytes()
    {
        using var dir = new TempDirectory();

        CheckpointWriter.Write(dir.Path, _sampleData);

        new FileInfo(Path.Combine(dir.Path, "checkpoint.dat")).Length
            .Should().Be(CheckpointData.SerializedSize);
    }

    // ── Overwrite ────────────────────────────────────────────────────────────

    [Fact]
    public void Write_OverwritesExistingCheckpoint()
    {
        using var dir = new TempDirectory();
        var first = new CheckpointData(1u, 100L, 10UL, 1u);
        var second = new CheckpointData(2u, 200L, 20UL, 2u);

        CheckpointWriter.Write(dir.Path, first);
        CheckpointWriter.Write(dir.Path, second);

        // Verify by reading back raw bytes — LastSegmentId at offset 8
        byte[] bytes = File.ReadAllBytes(Path.Combine(dir.Path, "checkpoint.dat"));
        uint lastSegId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8));
        lastSegId.Should().Be(second.LastSegmentId);
    }

    // ── Stale temp file resilience ────────────────────────────────────────────

    [Fact]
    public void Write_StaleTempFile_Succeeds()
    {
        using var dir = new TempDirectory();

        // Simulate a stale temp file left by a previous crash.
        File.WriteAllBytes(Path.Combine(dir.Path, "checkpoint.tmp"), [0xDE, 0xAD, 0xBE, 0xEF]);

        var act = () => CheckpointWriter.Write(dir.Path, _sampleData);

        act.Should().NotThrow();
        File.Exists(Path.Combine(dir.Path, "checkpoint.dat")).Should().BeTrue();
        File.Exists(Path.Combine(dir.Path, "checkpoint.tmp")).Should().BeFalse();
    }

    // ── Parameter guards ─────────────────────────────────────────────────────

    [Fact]
    public void Write_NullDirectory_ThrowsArgumentNullException()
    {
        var act = () => CheckpointWriter.Write(null!, _sampleData);

        act.Should().Throw<ArgumentNullException>();
    }

    // ── CheckpointData ───────────────────────────────────────────────────────

    [Fact]
    public void CheckpointData_DefaultValues_AreAllZero()
    {
        var data = default(CheckpointData);

        data.LastSegmentId.Should().Be(0u);
        data.LastOffset.Should().Be(0L);
        data.LastSeqNo.Should().Be(0UL);
        data.ActiveSegmentId.Should().Be(0u);
    }

    [Fact]
    public void CheckpointData_SerializedSizeIs36()
    {
        CheckpointData.SerializedSize.Should().Be(36);
    }

    [Fact]
    public void CheckpointData_ExpectedMagicMatchesChkpAscii()
    {
        // "CHKP" bytes: C=0x43, H=0x48, K=0x4B, P=0x50 → LE uint32 = 0x504B4843
        CheckpointData.ExpectedMagic.Should().Be(0x504B4843u);
    }
}
