using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities.Records;

namespace ByTech.EmbeddedCommitLog.Tests.Records;

public sealed class RecordReaderTests
{
    // ── Happy path ──────────────────────────────────────────────────────────

    [Fact]
    public void Read_ValidRecord_ReturnsSuccess()
    {
        using var stream = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        var result = RecordReader.Read(stream);
        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public void Read_ValidRecord_SeqNoRoundTrips()
    {
        using var stream = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload, seqNo: 99UL);
        var result = RecordReader.Read(stream);
        result.Value.Header.SeqNo.Should().Be(99UL);
    }

    [Fact]
    public void Read_ValidRecord_PayloadRoundTrips()
    {
        using var stream = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        var result = RecordReader.Read(stream);
        result.Value.Payload.ToArray().Should().Equal(RecordTestData.SmallPayload);
    }

    [Fact]
    public void Read_ValidRecord_ContentTypeRoundTrips()
    {
        using var stream = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        var result = RecordReader.Read(stream);
        result.Value.Header.ContentType.Should().Be(RecordTestData.DefaultContentType);
    }

    [Fact]
    public void Read_ValidRecord_FlagsRoundTrips()
    {
        using var stream = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        var result = RecordReader.Read(stream);
        result.Value.Header.Flags.Should().Be(RecordTestData.DefaultFlags);
    }

    [Fact]
    public void Read_EmptyPayload_Succeeds()
    {
        using var stream = RecordTestData.BuildValidRecord(RecordTestData.EmptyPayload);
        var result = RecordReader.Read(stream);
        result.IsSuccess.Should().BeTrue();
        result.Value.Payload.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void Read_LargePayload_Succeeds()
    {
        using var stream = RecordTestData.BuildValidRecord(RecordTestData.LargePayload);
        var result = RecordReader.Read(stream);
        result.IsSuccess.Should().BeTrue();
        result.Value.Payload.ToArray().Should().Equal(RecordTestData.LargePayload);
    }

    // ── CRC failure ─────────────────────────────────────────────────────────

    [Fact]
    public void Read_CorruptCrc_ReturnsCrcMismatchError()
    {
        using var valid = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        byte[] bytes = valid.ToArray();
        bytes[^1] ^= 0xFF; // flip last byte of footer
        using var stream = new MemoryStream(bytes);

        var result = RecordReader.Read(stream);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CrcMismatch);
    }

    // ── Truncation failures ─────────────────────────────────────────────────

    [Fact]
    public void Read_TruncatedAtHeader_ReturnsTruncatedHeaderError()
    {
        using var stream = new MemoryStream([0x01, 0x02, 0x03, 0x04, 0x05]); // 5 bytes, header needs 24
        var result = RecordReader.Read(stream);
        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.TruncatedHeader);
    }

    [Fact]
    public void Read_EmptyStream_ReturnsTruncatedHeaderError()
    {
        using var stream = new MemoryStream();
        var result = RecordReader.Read(stream);
        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.TruncatedHeader);
    }

    [Fact]
    public void Read_TruncatedAtPayload_ReturnsTruncatedPayloadError()
    {
        // SmallPayload is 5 bytes; cut stream to header (24) + 2 payload bytes
        using var full = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        byte[] bytes = full.ToArray();
        using var stream = new MemoryStream(bytes[..26]);

        var result = RecordReader.Read(stream);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.TruncatedPayload);
    }

    [Fact]
    public void Read_TruncatedAtFooter_ReturnsTruncatedFooterError()
    {
        // EmptyPayload record is 28 bytes; cut the last 2 footer bytes
        using var full = RecordTestData.BuildValidRecord(RecordTestData.EmptyPayload);
        byte[] bytes = full.ToArray(); // 28 bytes
        using var stream = new MemoryStream(bytes[..^2]); // 26 bytes

        var result = RecordReader.Read(stream);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.TruncatedFooter);
    }

    // ── Header field validation failures ────────────────────────────────────

    [Fact]
    public void Read_InvalidMagic_ReturnsInvalidMagicError()
    {
        using var full = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        byte[] bytes = full.ToArray();
        bytes[0] = 0xDE; bytes[1] = 0xAD; bytes[2] = 0xBE; bytes[3] = 0xEF;
        using var stream = new MemoryStream(bytes);

        var result = RecordReader.Read(stream);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.InvalidMagic);
    }

    [Fact]
    public void Read_InvalidVersion_ReturnsInvalidVersionError()
    {
        using var full = RecordTestData.BuildValidRecord(RecordTestData.SmallPayload);
        byte[] bytes = full.ToArray();
        bytes[4] = 99; // overwrite version byte
        using var stream = new MemoryStream(bytes);

        var result = RecordReader.Read(stream);

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.InvalidVersion);
    }

    // ── Parameter guards ────────────────────────────────────────────────────

    [Fact]
    public void Read_NullStream_Throws()
    {
        var act = () => RecordReader.Read(null!);
        act.Should().Throw<ArgumentNullException>();
    }
}
