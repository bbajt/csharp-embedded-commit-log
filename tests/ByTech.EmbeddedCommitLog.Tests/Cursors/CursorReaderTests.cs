using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Cursors;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;

namespace ByTech.EmbeddedCommitLog.Tests.Cursors;

public sealed class CursorReaderTests
{
    private static readonly CursorData _sampleData = new(
        ConsumerName: "influxdb",
        SegmentId: 3u,
        Offset: 4096L,
        SeqNo: 500UL);

    // ── Success path ──────────────────────────────────────────────────────────

    [Fact]
    public void Read_ValidCursor_ReturnsSuccess()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public void Read_ConsumerNameRoundTrips()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.Value.ConsumerName.Should().Be("influxdb");
    }

    [Fact]
    public void Read_SegmentIdRoundTrips()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.Value.SegmentId.Should().Be(3u);
    }

    [Fact]
    public void Read_OffsetRoundTrips()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.Value.Offset.Should().Be(4096L);
    }

    [Fact]
    public void Read_SeqNoRoundTrips()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.Value.SeqNo.Should().Be(500UL);
    }

    [Fact]
    public void Read_LongConsumerName_RoundTrips()
    {
        using var dir = new TempDirectory();
        string longName = new('x', 100);
        var data = new CursorData(longName, 7u, 8192L, 999UL);
        CursorWriter.Write(dir.Path, data);

        var result = CursorReader.Read(dir.Path, longName);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(data);
    }

    // ── Failure paths ─────────────────────────────────────────────────────────

    [Fact]
    public void Read_MissingFile_ReturnsCursorMissingError()
    {
        using var dir = new TempDirectory();

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CursorMissing);
    }

    [Fact]
    public void Read_TruncatedFile_ReturnsCursorTruncatedError()
    {
        using var dir = new TempDirectory();
        File.WriteAllBytes(Path.Combine(dir.Path, "influxdb.cur"), [0x01, 0x02, 0x03, 0x04, 0x05]);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CursorTruncated);
    }

    [Fact]
    public void Read_CorruptCrc_ReturnsCrcMismatchError()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);
        string curPath = Path.Combine(dir.Path, "influxdb.cur");
        byte[] bytes = File.ReadAllBytes(curPath);
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(curPath, bytes);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CrcMismatch);
    }

    [Fact]
    public void Read_WrongMagic_ReturnsInvalidMagicError()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);
        string curPath = Path.Combine(dir.Path, "influxdb.cur");
        byte[] bytes = File.ReadAllBytes(curPath);
        bytes[0] = 0xFF;
        bytes[1] = 0xFF;
        bytes[2] = 0xFF;
        bytes[3] = 0xFF;
        File.WriteAllBytes(curPath, bytes);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.InvalidMagic);
    }

    [Fact]
    public void Read_WrongVersion_ReturnsInvalidVersionError()
    {
        using var dir = new TempDirectory();
        CursorWriter.Write(dir.Path, _sampleData);
        string curPath = Path.Combine(dir.Path, "influxdb.cur");
        byte[] bytes = File.ReadAllBytes(curPath);
        bytes[4] = 0xFF;
        File.WriteAllBytes(curPath, bytes);

        var result = CursorReader.Read(dir.Path, "influxdb");

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.InvalidVersion);
    }

    [Fact]
    public void Read_NameMismatch_ReturnsCursorNameMismatchError()
    {
        using var dir = new TempDirectory();
        // Write cursor for "influxdb", then copy it to "redis.cur" to simulate a misplaced file
        CursorWriter.Write(dir.Path, _sampleData);
        File.Copy(
            Path.Combine(dir.Path, "influxdb.cur"),
            Path.Combine(dir.Path, "redis.cur"));

        var result = CursorReader.Read(dir.Path, "redis");

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CursorNameMismatch);
    }

    // ── Guard clauses ─────────────────────────────────────────────────────────

    [Fact]
    public void Read_NullDirectory_ThrowsArgumentNullException()
    {
        var act = () => CursorReader.Read(null!, "influxdb");

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Read_NullConsumerName_ThrowsArgumentNullException()
    {
        using var dir = new TempDirectory();

        var act = () => CursorReader.Read(dir.Path, null!);

        act.Should().Throw<ArgumentNullException>();
    }
}
