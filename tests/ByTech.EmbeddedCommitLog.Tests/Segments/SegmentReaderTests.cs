using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.Segments;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Records;

namespace ByTech.EmbeddedCommitLog.Tests.Segments;

public sealed class SegmentReaderTests
{
    // ── ReadNext — empty segment ──────────────────────────────────────────────

    [Fact]
    public void ReadNext_EmptySegment_ReturnsTruncatedHeaderWithZeroBytes()
    {
        using var dir = new TempDirectory();
        // Create an empty segment file so the reader can open it.
        File.WriteAllBytes(Path.Combine(dir.Path, "log-000001.seg"), []);

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        var result = reader.ReadNext();

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.TruncatedHeader);
        result.Error.Message.Should().Contain("0");
    }

    // ── ReadNext — happy path ─────────────────────────────────────────────────

    [Fact]
    public void ReadNext_SingleRecord_ReturnsSuccess()
    {
        using var dir = new TempDirectory();
        WriteSegment(dir.Path, 1u, [(RecordTestData.SmallPayload, 1UL)]);

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        var result = reader.ReadNext();

        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public void ReadNext_SeqNoRoundTrips()
    {
        using var dir = new TempDirectory();
        const ulong SeqNo = RecordTestData.DefaultSeqNo;
        WriteSegment(dir.Path, 1u, [(RecordTestData.SmallPayload, SeqNo)]);

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        var result = reader.ReadNext();

        result.Value.Header.SeqNo.Should().Be(SeqNo);
    }

    [Fact]
    public void ReadNext_PayloadRoundTrips()
    {
        using var dir = new TempDirectory();
        WriteSegment(dir.Path, 1u, [(RecordTestData.SmallPayload, 1UL)]);

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        var result = reader.ReadNext();

        result.Value.Payload.ToArray().Should().Equal(RecordTestData.SmallPayload);
    }

    [Fact]
    public void ReadNext_AfterLastRecord_ReturnsTruncatedHeaderWithZeroBytes()
    {
        using var dir = new TempDirectory();
        WriteSegment(dir.Path, 1u, [(RecordTestData.SmallPayload, 1UL)]);

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        reader.ReadNext(); // consume the one record

        var result = reader.ReadNext();

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.TruncatedHeader);
        result.Error.Message.Should().Contain("0");
    }

    // ── ReadNext — CRC corruption ─────────────────────────────────────────────

    [Fact]
    public void ReadNext_CorruptCrc_ReturnsCrcMismatchError()
    {
        using var dir = new TempDirectory();
        string path = Path.Combine(dir.Path, "log-000001.seg");

        using (var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024))
        {
            writer.Append(RecordTestData.SmallPayload, seqNo: 1);
        }

        // Flip the last byte (CRC footer) to corrupt it.
        byte[] bytes = File.ReadAllBytes(path);
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(path, bytes);

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        var result = reader.ReadNext();

        result.IsFailure.Should().BeTrue();
        result.Error.Code.Should().Be(PeclErrorCode.CrcMismatch);
    }

    // ── Position tracking ────────────────────────────────────────────────────

    [Fact]
    public void Position_AfterReadNext_AdvancesByRecordFramingSize()
    {
        using var dir = new TempDirectory();
        WriteSegment(dir.Path, 1u, [([], 1UL)]);

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        long before = reader.Position;
        reader.ReadNext();
        long after = reader.Position;

        (after - before).Should().Be(RecordWriter.FramingOverhead);
    }

    // ── startOffset ──────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_WithStartOffset_ReadsFromCorrectPosition()
    {
        using var dir = new TempDirectory();

        // Write two records; capture offset of the second.
        long secondOffset;
        using (var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 4096))
        {
            writer.Append(RecordTestData.SmallPayload, seqNo: 1);
            secondOffset = writer.Append(RecordTestData.SmallPayload, seqNo: 2);
        }

        using var reader = new SegmentReader(dir.Path, segmentId: 1u, startOffset: secondOffset);
        var result = reader.ReadNext();

        result.IsSuccess.Should().BeTrue();
        result.Value.Header.SeqNo.Should().Be(2UL);
    }

    // ── Guard conditions ─────────────────────────────────────────────────────

    [Fact]
    public void ReadNext_WhenDisposed_ThrowsObjectDisposedException()
    {
        using var dir = new TempDirectory();
        File.WriteAllBytes(Path.Combine(dir.Path, "log-000001.seg"), []);

        var reader = new SegmentReader(dir.Path, segmentId: 1u);
        reader.Dispose();

        var act = () => reader.ReadNext();

        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Constructor_NullDirectory_ThrowsArgumentNullException()
    {
        var act = () => new SegmentReader(null!, segmentId: 1u);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_NegativeStartOffset_ThrowsArgumentOutOfRangeException()
    {
        using var dir = new TempDirectory();
        File.WriteAllBytes(Path.Combine(dir.Path, "log-000001.seg"), []);

        var act = () => new SegmentReader(dir.Path, segmentId: 1u, startOffset: -1);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Constructor_NonExistentFile_ThrowsFileNotFoundException()
    {
        using var dir = new TempDirectory();

        var act = () => new SegmentReader(dir.Path, segmentId: 99u);

        act.Should().Throw<FileNotFoundException>();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Writes one segment containing the given (payload, seqNo) pairs.</summary>
    private static void WriteSegment(string directory, uint segmentId, IEnumerable<(byte[] Payload, ulong SeqNo)> records)
    {
        using var writer = new SegmentWriter(directory, segmentId, maxSegmentSize: long.MaxValue);

        foreach ((byte[] payload, ulong seqNo) in records)
        {
            writer.Append(payload, seqNo);
        }
    }
}
