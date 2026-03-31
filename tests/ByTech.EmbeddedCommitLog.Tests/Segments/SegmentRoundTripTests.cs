using FluentAssertions;
using Xunit;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.Segments;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using ByTech.EmbeddedCommitLog.TestUtilities.Records;

namespace ByTech.EmbeddedCommitLog.Tests.Segments;

/// <summary>
/// End-to-end round-trip tests that exercise both <see cref="SegmentWriter"/> and
/// <see cref="SegmentReader"/> together against real files in a temporary directory.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SegmentRoundTripTests
{
    // ── Basic round-trips ────────────────────────────────────────────────────

    [Fact]
    public void WriteAndRead_MultipleRecords_AllRoundTrip()
    {
        using var dir = new TempDirectory();
        const int RecordCount = 10;
        ulong[] seqNos = Enumerable.Range(1, RecordCount).Select(i => (ulong)i).ToArray();

        using (var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1_048_576))
        {
            foreach (ulong seqNo in seqNos)
            {
                writer.Append(RecordTestData.SmallPayload, seqNo);
            }
        }

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        var readBack = new List<RecordReadResult>();

        while (true)
        {
            var result = reader.ReadNext();

            if (result.IsFailure && result.Error.Code == PeclErrorCode.TruncatedHeader)
            {
                break;
            }

            result.IsSuccess.Should().BeTrue(because: $"record {readBack.Count + 1} should deserialise cleanly");
            readBack.Add(result.Value);
        }

        readBack.Should().HaveCount(RecordCount);

        for (int i = 0; i < RecordCount; i++)
        {
            readBack[i].Header.SeqNo.Should().Be(seqNos[i]);
            readBack[i].Payload.ToArray().Should().Equal(RecordTestData.SmallPayload);
        }
    }

    [Fact]
    public void WriteAndRead_LargePayload_RoundTrips()
    {
        using var dir = new TempDirectory();

        using (var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 4_194_304))
        {
            writer.Append(RecordTestData.LargePayload, seqNo: 1);
        }

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        var result = reader.ReadNext();

        result.IsSuccess.Should().BeTrue();
        result.Value.Payload.ToArray().Should().Equal(RecordTestData.LargePayload);
    }

    [Fact]
    public void WriteAndRead_EmptyPayloads_AllRoundTrip()
    {
        using var dir = new TempDirectory();
        const int RecordCount = 5;

        using (var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1_048_576))
        {
            for (ulong i = 1; i <= RecordCount; i++)
            {
                writer.Append([], seqNo: i);
            }
        }

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        int count = 0;

        while (true)
        {
            var result = reader.ReadNext();

            if (result.IsFailure && result.Error.Code == PeclErrorCode.TruncatedHeader)
            {
                break;
            }

            result.IsSuccess.Should().BeTrue();
            result.Value.Payload.IsEmpty.Should().BeTrue();
            count++;
        }

        count.Should().Be(RecordCount);
    }

    // ── startOffset ──────────────────────────────────────────────────────────

    [Fact]
    public void ReadFrom_NonZeroOffset_StartsAtCorrectRecord()
    {
        using var dir = new TempDirectory();
        long[] offsets = new long[5];

        using (var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1_048_576))
        {
            for (int i = 0; i < 5; i++)
            {
                offsets[i] = writer.Append(RecordTestData.SmallPayload, seqNo: (ulong)(i + 1));
            }
        }

        // Open reader starting at the 3rd record (index 2).
        using var reader = new SegmentReader(dir.Path, segmentId: 1u, startOffset: offsets[2]);
        var result = reader.ReadNext();

        result.IsSuccess.Should().BeTrue();
        result.Value.Header.SeqNo.Should().Be(3UL);
    }

    // ── Seal then read ───────────────────────────────────────────────────────

    [Fact]
    public void WriteAndRead_AfterSeal_AllRecordsReadBack()
    {
        using var dir = new TempDirectory();
        const int RecordCount = 3;

        using (var writer = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: 1_048_576))
        {
            for (ulong i = 1; i <= RecordCount; i++)
            {
                writer.Append(RecordTestData.SmallPayload, seqNo: i);
            }

            writer.Seal();
        }

        using var reader = new SegmentReader(dir.Path, segmentId: 1u);
        int count = 0;

        while (true)
        {
            var result = reader.ReadNext();

            if (result.IsFailure && result.Error.Code == PeclErrorCode.TruncatedHeader)
            {
                break;
            }

            result.IsSuccess.Should().BeTrue();
            count++;
        }

        count.Should().Be(RecordCount);
    }

    // ── Multi-segment rollover ────────────────────────────────────────────────

    [Fact]
    public void Rollover_TwoSegments_BothReadBackInFull()
    {
        using var dir = new TempDirectory();
        // maxSegmentSize = exactly 3 empty records (3 × 28 = 84 bytes)
        const long MaxSize = RecordWriter.FramingOverhead * 3;

        // Write 3 records to segment 1, then 2 records to segment 2.
        using (var writer1 = new SegmentWriter(dir.Path, segmentId: 1u, maxSegmentSize: MaxSize))
        {
            writer1.Append([], seqNo: 1);
            writer1.Append([], seqNo: 2);
            writer1.Append([], seqNo: 3);
            writer1.IsFull.Should().BeTrue();
        }

        using (var writer2 = new SegmentWriter(dir.Path, segmentId: 2u, maxSegmentSize: MaxSize))
        {
            writer2.Append([], seqNo: 4);
            writer2.Append([], seqNo: 5);
        }

        // Read segment 1 — expect seqNos 1, 2, 3.
        var seg1Records = ReadAllRecords(dir.Path, segmentId: 1u);
        seg1Records.Should().HaveCount(3);
        seg1Records.Select(r => r.Header.SeqNo).Should().Equal(1UL, 2UL, 3UL);

        // Read segment 2 — expect seqNos 4, 5.
        var seg2Records = ReadAllRecords(dir.Path, segmentId: 2u);
        seg2Records.Should().HaveCount(2);
        seg2Records.Select(r => r.Header.SeqNo).Should().Equal(4UL, 5UL);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<RecordReadResult> ReadAllRecords(string directory, uint segmentId)
    {
        var records = new List<RecordReadResult>();

        using var reader = new SegmentReader(directory, segmentId);

        while (true)
        {
            var result = reader.ReadNext();

            if (result.IsFailure && result.Error.Code == PeclErrorCode.TruncatedHeader)
            {
                break;
            }

            result.IsSuccess.Should().BeTrue();
            records.Add(result.Value);
        }

        return records;
    }
}
