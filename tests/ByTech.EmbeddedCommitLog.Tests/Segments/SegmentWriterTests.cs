using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.Segments;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Records;

namespace ByTech.EmbeddedCommitLog.Tests.Segments;

public sealed class SegmentWriterTests
{
    // ── Append — offset tracking ─────────────────────────────────────────────

    [Fact]
    public void Append_FirstRecord_ReturnsOffsetZero()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);

        long offset = writer.Append(RecordTestData.SmallPayload, seqNo: 1);

        offset.Should().Be(0);
    }

    [Fact]
    public void Append_SecondRecord_ReturnsOffsetEqualToFirstRecordSize()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);

        long firstOffset = writer.Append(RecordTestData.SmallPayload, seqNo: 1);
        long secondOffset = writer.Append(RecordTestData.SmallPayload, seqNo: 2);

        long expectedSecond = firstOffset + RecordWriter.FramingOverhead + RecordTestData.SmallPayload.Length;
        secondOffset.Should().Be(expectedSecond);
    }

    [Fact]
    public void Append_EmptyPayload_BytesWrittenIs28()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);

        writer.Append([], seqNo: 1);

        writer.BytesWritten.Should().Be(RecordWriter.FramingOverhead);
    }

    [Fact]
    public void Append_KnownPayload_BytesWrittenMatchesFramingPlusPayload()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);

        writer.Append(RecordTestData.SmallPayload, seqNo: 1);

        writer.BytesWritten.Should().Be(RecordWriter.FramingOverhead + RecordTestData.SmallPayload.Length);
    }

    // ── Append — file size ───────────────────────────────────────────────────

    [Fact]
    public void Append_EmptyPayload_CreatesFileOfCorrectSize()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);

        writer.Append([], seqNo: 1);
        writer.Flush();

        string segPath = Path.Combine(dir.Path, "log-000001.seg");
        new FileInfo(segPath).Length.Should().Be(RecordWriter.FramingOverhead);
    }

    // ── IsFull ───────────────────────────────────────────────────────────────

    [Fact]
    public void IsFull_BeforeWriting_IsFalse()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);

        writer.IsFull.Should().BeFalse();
    }

    [Fact]
    public void IsFull_WhenBytesWrittenReachesMaxSegmentSize_IsTrue()
    {
        using var dir = new TempDirectory();
        // maxSegmentSize = exactly one empty record (28 bytes)
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: RecordWriter.FramingOverhead);

        writer.Append([], seqNo: 1);

        writer.IsFull.Should().BeTrue();
    }

    [Fact]
    public void IsFull_WhenBytesWrittenBelowMaxSegmentSize_IsFalse()
    {
        using var dir = new TempDirectory();
        // maxSegmentSize = two empty records
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: RecordWriter.FramingOverhead * 2);

        writer.Append([], seqNo: 1);

        writer.IsFull.Should().BeFalse();
    }

    // ── Append — guard conditions ────────────────────────────────────────────

    [Fact]
    public void Append_WhenFull_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: RecordWriter.FramingOverhead);
        writer.Append([], seqNo: 1); // fills the segment

        var act = () => writer.Append([], seqNo: 2);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Append_WhenSealed_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);
        writer.Seal();

        var act = () => writer.Append([], seqNo: 1);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Append_WhenDisposed_ThrowsObjectDisposedException()
    {
        using var dir = new TempDirectory();
        var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);
        writer.Dispose();

        var act = () => writer.Append([], seqNo: 1);

        act.Should().Throw<ObjectDisposedException>();
    }

    // ── FlushToDisk ──────────────────────────────────────────────────────────

    [Fact]
    public void FlushToDisk_AfterAppend_BytesWrittenIsPositive()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);
        writer.Append(RecordTestData.SmallPayload, seqNo: 1);

        // FlushToDisk must not throw and BytesWritten must reflect the appended record.
        var act = () => writer.FlushToDisk();
        act.Should().NotThrow();
        writer.BytesWritten.Should().BeGreaterThan(0);
    }

    // ── Seal idempotency ─────────────────────────────────────────────────────

    [Fact]
    public void Seal_CalledMultipleTimes_DoesNotThrow()
    {
        using var dir = new TempDirectory();
        using var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);

        var act = () =>
        {
            writer.Seal();
            writer.Seal();
        };

        act.Should().NotThrow();
    }

    // ── Constructor — existing file resumption ────────────────────────────────

    [Fact]
    public void Constructor_ExistingFile_BytesWrittenEqualsExistingFileLength()
    {
        using var dir = new TempDirectory();

        long firstWriteLength;
        using (var writer1 = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024))
        {
            writer1.Append(RecordTestData.SmallPayload, seqNo: 1);
            firstWriteLength = writer1.BytesWritten;
        }

        using var writer2 = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1024);
        writer2.BytesWritten.Should().Be(firstWriteLength);
    }

    [Fact]
    public void Constructor_ExistingFile_AppendsAfterExistingContent()
    {
        using var dir = new TempDirectory();

        using (var writer1 = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 4096))
        {
            writer1.Append(RecordTestData.SmallPayload, seqNo: 1);
        }

        using (var writer2 = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 4096))
        {
            long secondOffset = writer2.Append(RecordTestData.SmallPayload, seqNo: 2);
            secondOffset.Should().Be(RecordWriter.FramingOverhead + RecordTestData.SmallPayload.Length);
        }
    }

    // ── Constructor — parameter guards ───────────────────────────────────────

    [Fact]
    public void Constructor_NullDirectory_ThrowsArgumentNullException()
    {
        var act = () => new SegmentWriter(null!, segmentId: 1u, maxSegmentSize: 1024);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_MaxSizeSmallerThanFramingOverhead_ThrowsArgumentOutOfRangeException()
    {
        using var dir = new TempDirectory();

        var act = () => new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 0);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    // ── SegmentNaming ────────────────────────────────────────────────────────

    [Theory]
    [InlineData(0u, "log-000000.seg")]
    [InlineData(1u, "log-000001.seg")]
    [InlineData(42u, "log-000042.seg")]
    [InlineData(999999u, "log-999999.seg")]
    public void SegmentNaming_GetFileName_FormatsCorrectly(uint id, string expected)
    {
        // SegmentNaming is internal — access via InternalsVisibleTo already wired in csproj
        string actual = ByTech.EmbeddedCommitLog.Segments.SegmentNaming.GetFileName(id);
        actual.Should().Be(expected);
    }

    [Fact]
    public void SegmentNaming_GetFilePath_CombinesDirectoryAndFileName()
    {
        string dir = Path.Combine("some", "dir");
        string expected = Path.Combine(dir, "log-000003.seg");

        string actual = ByTech.EmbeddedCommitLog.Segments.SegmentNaming.GetFilePath(dir, 3u);

        actual.Should().Be(expected);
    }

    // ── Construction — segment ID overflow ──────────────────────────────────────

    [Fact]
    public void SegmentWriter_SegmentIdOverMaxSegmentId_ThrowsInvalidOperationException()
    {
        using var dir = new TempDirectory();

        // 1_000_000 is one past MaxSegmentId (999_999) — filename would require 7 digits.
        Action act = () =>
        {
            using var writer = new SegmentWriter(dir.Path, segmentId: 1_000_000u, maxSegmentSize: 1024);
        };

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*1000000*maximum*");
    }

    [Theory]
    [InlineData("log-000001.seg", true, 1u)]
    [InlineData("log-999999.seg", true, 999999u)]
    [InlineData("log-000000.seg", true, 0u)]
    [InlineData("log-00001.seg", false, 0u)]    // too few digits
    [InlineData("log-0000001.seg", false, 0u)]  // too many digits
    [InlineData("data-000001.seg", false, 0u)]  // wrong prefix
    [InlineData("log-000001.dat", false, 0u)]   // wrong extension
    public void SegmentNaming_TryParseId_ParsesCorrectly(string fileName, bool expectedResult, uint expectedId)
    {
        bool result = ByTech.EmbeddedCommitLog.Segments.SegmentNaming.TryParseId(fileName, out uint id);

        result.Should().Be(expectedResult);
        if (expectedResult)
        {
            id.Should().Be(expectedId);
        }
    }
}
