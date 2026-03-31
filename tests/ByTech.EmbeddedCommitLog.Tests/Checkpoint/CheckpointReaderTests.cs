using System.Buffers.Binary;
using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Checkpoint;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;

namespace ByTech.EmbeddedCommitLog.Tests.Checkpoint;

public sealed class CheckpointReaderTests
{
    private static readonly CheckpointData _sampleData = new(
        LastSegmentId: 7u,
        LastOffset: 131072L,
        LastSeqNo: 5000UL,
        ActiveSegmentId: 8u);

    // ── Happy path ───────────────────────────────────────────────────────────

    [Fact]
    public void Read_ValidCheckpoint_ReturnsSuccess()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        var result = CheckpointReader.Read(dir.Path);

        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public void Read_LastSegmentIdRoundTrips()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        var result = CheckpointReader.Read(dir.Path);

        result.Value.LastSegmentId.Should().Be(_sampleData.LastSegmentId);
    }

    [Fact]
    public void Read_LastOffsetRoundTrips()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        var result = CheckpointReader.Read(dir.Path);

        result.Value.LastOffset.Should().Be(_sampleData.LastOffset);
    }

    [Fact]
    public void Read_LastSeqNoRoundTrips()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        var result = CheckpointReader.Read(dir.Path);

        result.Value.LastSeqNo.Should().Be(_sampleData.LastSeqNo);
    }

    [Fact]
    public void Read_ActiveSegmentIdRoundTrips()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        var result = CheckpointReader.Read(dir.Path);

        result.Value.ActiveSegmentId.Should().Be(_sampleData.ActiveSegmentId);
    }

    [Fact]
    public void Read_AllZeroData_RoundTrips()
    {
        using var dir = new TempDirectory();
        var zero = new CheckpointData(0u, 0L, 0UL, 0u);
        CheckpointWriter.Write(dir.Path, zero);

        var result = CheckpointReader.Read(dir.Path);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(zero);
    }

    // ── Failure paths ────────────────────────────────────────────────────────

    [Fact]
    public void Read_MissingFile_ReturnsCheckpointMissingError()
    {
        using var dir = new TempDirectory();

        var result = CheckpointReader.Read(dir.Path);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CheckpointMissing);
    }

    [Fact]
    public void Read_TruncatedFile_ReturnsCheckpointTruncatedError()
    {
        using var dir = new TempDirectory();
        File.WriteAllBytes(Path.Combine(dir.Path, "checkpoint.dat"), [0x01, 0x02, 0x03, 0x04, 0x05]);

        var result = CheckpointReader.Read(dir.Path);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CheckpointTruncated);
    }

    [Fact]
    public void Read_CorruptCrc_ReturnsCrcMismatchError()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        string path = Path.Combine(dir.Path, "checkpoint.dat");
        byte[] bytes = File.ReadAllBytes(path);
        bytes[^1] ^= 0xFF; // flip last byte of CRC
        File.WriteAllBytes(path, bytes);

        var result = CheckpointReader.Read(dir.Path);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CrcMismatch);
    }

    [Fact]
    public void Read_WrongMagic_ReturnsInvalidMagicError()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        string path = Path.Combine(dir.Path, "checkpoint.dat");
        byte[] bytes = File.ReadAllBytes(path);
        // Overwrite magic bytes with garbage
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0), 0xDEADBEEFu);
        File.WriteAllBytes(path, bytes);

        var result = CheckpointReader.Read(dir.Path);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.InvalidMagic);
    }

    [Fact]
    public void Read_WrongVersion_ReturnsInvalidVersionError()
    {
        using var dir = new TempDirectory();
        CheckpointWriter.Write(dir.Path, _sampleData);

        string path = Path.Combine(dir.Path, "checkpoint.dat");
        byte[] bytes = File.ReadAllBytes(path);
        bytes[4] = 99; // overwrite version byte
        // Recompute CRC so the file is structurally valid except for version
        uint newCrc = ComputeCrc32OverFirstBytes(bytes, 32);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(32), newCrc);
        File.WriteAllBytes(path, bytes);

        var result = CheckpointReader.Read(dir.Path);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.InvalidVersion);
    }

    // ── Parameter guards ─────────────────────────────────────────────────────

    [Fact]
    public void Read_NullDirectory_ThrowsArgumentNullException()
    {
        var act = () => CheckpointReader.Read(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    /// <summary>Computes CRC-32/ISO-HDLC the same way CheckpointWriter does, for test manipulation.</summary>
    private static uint ComputeCrc32OverFirstBytes(byte[] data, int count)
    {
        var hash = new System.IO.Hashing.Crc32();
        hash.Append(data.AsSpan(0, count));
        return hash.GetCurrentHashAsUInt32();
    }
}
