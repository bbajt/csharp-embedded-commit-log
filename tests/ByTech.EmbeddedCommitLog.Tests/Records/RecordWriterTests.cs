using System.Buffers.Binary;
using System.IO.Hashing;
using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.TestUtilities.Records;

namespace ByTech.EmbeddedCommitLog.Tests.Records;

public sealed class RecordWriterTests
{
    // ── Return value ────────────────────────────────────────────────────────

    [Fact]
    public void Write_EmptyPayload_Returns28()
    {
        using var stream = new MemoryStream();
        long written = RecordWriter.Write(stream, [], seqNo: 1);
        written.Should().Be(28);
    }

    [Fact]
    public void Write_EmptyPayload_Produces28Bytes()
    {
        using var stream = new MemoryStream();
        RecordWriter.Write(stream, [], seqNo: 1);
        stream.Length.Should().Be(28);
    }

    [Fact]
    public void Write_KnownPayload_ReturnsCorrectTotalSize()
    {
        using var stream = new MemoryStream();
        long written = RecordWriter.Write(stream, RecordTestData.SmallPayload, seqNo: 1);
        written.Should().Be(28 + RecordTestData.SmallPayload.Length);
    }

    // ── Header field encoding ───────────────────────────────────────────────

    [Fact]
    public void Write_Header_MagicIsPecl()
    {
        using var stream = new MemoryStream();
        RecordWriter.Write(stream, [], seqNo: 1);
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(stream.ToArray());
        magic.Should().Be(RecordHeader.ExpectedMagic);
    }

    [Fact]
    public void Write_Header_VersionIsCurrentVersion()
    {
        using var stream = new MemoryStream();
        RecordWriter.Write(stream, [], seqNo: 1);
        stream.ToArray()[4].Should().Be(RecordHeader.CurrentVersion);
    }

    [Fact]
    public void Write_Header_SeqNoIsStoredLittleEndian()
    {
        const ulong SeqNo = 0x0102030405060708UL;
        using var stream = new MemoryStream();
        RecordWriter.Write(stream, [], seqNo: SeqNo);
        ulong read = BinaryPrimitives.ReadUInt64LittleEndian(stream.ToArray().AsSpan(12));
        read.Should().Be(SeqNo);
    }

    // ── Payload placement ───────────────────────────────────────────────────

    [Fact]
    public void Write_Payload_AppearsBetweenHeaderAndFooter()
    {
        using var stream = new MemoryStream();
        RecordWriter.Write(stream, RecordTestData.SmallPayload, seqNo: 1);
        byte[] bytes = stream.ToArray();
        byte[] payloadSlice = bytes[RecordHeader.SerializedSize..(RecordHeader.SerializedSize + RecordTestData.SmallPayload.Length)];
        payloadSlice.Should().Equal(RecordTestData.SmallPayload);
    }

    // ── CRC integrity ───────────────────────────────────────────────────────

    [Fact]
    public void Write_Footer_CrcCoversHeaderAndPayload()
    {
        using var stream = new MemoryStream();
        RecordWriter.Write(stream, RecordTestData.SmallPayload, seqNo: RecordTestData.DefaultSeqNo);
        byte[] bytes = stream.ToArray();

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(
            bytes.AsSpan(bytes.Length - RecordFooter.SerializedSize));

        var hash = new Crc32();
        hash.Append(bytes.AsSpan(0, RecordHeader.SerializedSize));
        hash.Append(bytes.AsSpan(RecordHeader.SerializedSize, RecordTestData.SmallPayload.Length));
        uint expectedCrc = hash.GetCurrentHashAsUInt32();

        storedCrc.Should().Be(expectedCrc);
    }

    // ── Parameter guards ────────────────────────────────────────────────────

    [Fact]
    public void Write_NullStream_Throws()
    {
        var act = () => RecordWriter.Write(null!, Array.Empty<byte>(), seqNo: 1);
        act.Should().Throw<ArgumentNullException>();
    }
}
