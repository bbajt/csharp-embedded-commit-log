using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Cursors;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;

namespace ByTech.EmbeddedCommitLog.Tests.Cursors;

public sealed class CursorWriterTests
{
    private static readonly CursorData _sampleData = new(
        ConsumerName: "influxdb",
        SegmentId: 3u,
        Offset: 4096L,
        SeqNo: 500UL);

    // ── File presence ─────────────────────────────────────────────────────────

    [Fact]
    public void Write_CreatesCurFile()
    {
        using var dir = new TempDirectory();

        CursorWriter.Write(dir.Path, _sampleData);

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeTrue();
    }

    [Fact]
    public void Write_NoTempFileAfterSuccess()
    {
        using var dir = new TempDirectory();

        CursorWriter.Write(dir.Path, _sampleData);

        File.Exists(Path.Combine(dir.Path, "influxdb.tmp")).Should().BeFalse();
    }

    [Fact]
    public void Write_FileIsCorrectSize()
    {
        using var dir = new TempDirectory();

        CursorWriter.Write(dir.Path, _sampleData);

        int nameLen = System.Text.Encoding.UTF8.GetByteCount(_sampleData.ConsumerName);
        long expected = CursorData.MinSerializedSize + nameLen;
        new FileInfo(Path.Combine(dir.Path, "influxdb.cur")).Length.Should().Be(expected);
    }

    // ── Overwrite ─────────────────────────────────────────────────────────────

    [Fact]
    public void Write_OverwritesExistingCursor()
    {
        using var dir = new TempDirectory();
        var dataD1 = new CursorData("influxdb", 1u, 100L, 10UL);
        var dataD2 = new CursorData("influxdb", 2u, 200L, 20UL);

        CursorWriter.Write(dir.Path, dataD1);
        CursorWriter.Write(dir.Path, dataD2);

        var result = CursorReader.Read(dir.Path, "influxdb");
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(dataD2);
    }

    // ── Stale temp ────────────────────────────────────────────────────────────

    [Fact]
    public void Write_StaleTempFile_Succeeds()
    {
        using var dir = new TempDirectory();
        File.WriteAllBytes(Path.Combine(dir.Path, "influxdb.tmp"), [0xAB, 0xCD, 0xEF]);

        CursorWriter.Write(dir.Path, _sampleData);

        var result = CursorReader.Read(dir.Path, "influxdb");
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(_sampleData);
    }

    // ── Multiple consumers ────────────────────────────────────────────────────

    [Fact]
    public void Write_MultipleCursors_EachInOwnFile()
    {
        using var dir = new TempDirectory();
        var dataA = new CursorData("influxdb", 1u, 100L, 10UL);
        var dataB = new CursorData("redis", 2u, 200L, 20UL);

        CursorWriter.Write(dir.Path, dataA);
        CursorWriter.Write(dir.Path, dataB);

        File.Exists(Path.Combine(dir.Path, "influxdb.cur")).Should().BeTrue();
        File.Exists(Path.Combine(dir.Path, "redis.cur")).Should().BeTrue();
    }

    // ── Guard clauses ─────────────────────────────────────────────────────────

    [Fact]
    public void Write_NullDirectory_ThrowsArgumentNullException()
    {
        var act = () => CursorWriter.Write(null!, _sampleData);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Write_NullConsumerName_ThrowsArgumentException()
    {
        using var dir = new TempDirectory();
        var data = new CursorData(null!, 0u, 0L, 0UL);

        var act = () => CursorWriter.Write(dir.Path, data);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Write_EmptyConsumerName_ThrowsArgumentException()
    {
        using var dir = new TempDirectory();
        var data = new CursorData("", 0u, 0L, 0UL);

        var act = () => CursorWriter.Write(dir.Path, data);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Write_NameWithPathSeparator_ThrowsArgumentException()
    {
        using var dir = new TempDirectory();
        var data = new CursorData("../../evil", 0u, 0L, 0UL);

        var act = () => CursorWriter.Write(dir.Path, data);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Write_NameExceedsMaxBytes_ThrowsArgumentException()
    {
        using var dir = new TempDirectory();
        string longName = new('a', CursorData.MaxConsumerNameBytes + 1);
        var data = new CursorData(longName, 0u, 0L, 0UL);

        var act = () => CursorWriter.Write(dir.Path, data);

        act.Should().Throw<ArgumentException>();
    }

    // ── Constants ─────────────────────────────────────────────────────────────

    [Fact]
    public void CursorData_MinSerializedSizeIs32()
    {
        CursorData.MinSerializedSize.Should().Be(32);
    }

    [Fact]
    public void CursorData_MaxSerializedSizeIs288()
    {
        CursorData.MaxSerializedSize.Should().Be(288);
    }
}
