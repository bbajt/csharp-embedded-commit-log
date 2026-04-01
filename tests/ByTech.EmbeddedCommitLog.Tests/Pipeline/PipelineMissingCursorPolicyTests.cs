using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.TestUtilities.IO;
using FluentAssertions;
using Xunit;
using PeclPipeline = ByTech.EmbeddedCommitLog.Pipeline.Pipeline;

namespace ByTech.EmbeddedCommitLog.Tests.Pipeline;

[Trait("Category", "Integration")]
public sealed class PipelineMissingCursorPolicyTests
{
    private static PipelineConfiguration MakeConfig(
        string rootDirectory,
        MissingCursorPolicy policy = MissingCursorPolicy.FromBeginning) =>
        new() { RootDirectory = rootDirectory, MissingCursorPolicy = policy };

    private static List<ulong> ReadAll(PeclPipeline pipeline, string consumer)
    {
        var seqNos = new List<ulong>();
        while (true)
        {
            var result = pipeline.ReadNext(consumer);
            if (!result.IsSuccess)
            {
                break;
            }

            seqNos.Add(result.Value.Header.SeqNo);
        }

        return seqNos;
    }

    /// <summary>
    /// Default policy (<see cref="MissingCursorPolicy.FromBeginning"/>) with no existing
    /// cursor file: consumer reads all records from the beginning of the log.
    /// </summary>
    [Fact]
    public void MissingCursorPolicy_Default_FromBeginning_ReadsAllRecords()
    {
        using var dir = new TempDirectory();

        // Write 5 records in a first pipeline run (no consumer registered — cursor file never created).
        using (var writer = new PeclPipeline(MakeConfig(dir.Path)))
        {
            writer.Start();
            for (int i = 0; i < 5; i++)
            {
                writer.Append("data"u8);
            }

            writer.Flush();
            writer.Stop();
        }

        // Re-open with a new consumer — no cursor file exists; policy = FromBeginning (default).
        using var reader = new PeclPipeline(MakeConfig(dir.Path, MissingCursorPolicy.FromBeginning));
        reader.RegisterConsumer("c");
        reader.Start();
        List<ulong> seqNos = ReadAll(reader, "c");
        reader.Stop();

        seqNos.Should().HaveCount(5);
        seqNos.Should().ContainInOrder(0UL, 1UL, 2UL, 3UL, 4UL);
    }

    /// <summary>
    /// <see cref="MissingCursorPolicy.FromTail"/> with no existing cursor file:
    /// consumer starts at the log tail and reads no pre-existing records.
    /// </summary>
    [Fact]
    public void MissingCursorPolicy_FromTail_SkipsExistingRecords()
    {
        using var dir = new TempDirectory();

        using (var writer = new PeclPipeline(MakeConfig(dir.Path)))
        {
            writer.Start();
            for (int i = 0; i < 5; i++)
            {
                writer.Append("data"u8);
            }

            writer.Flush();
            writer.Stop();
        }

        // Re-open with FromTail — no cursor file; consumer starts at tail.
        using var reader = new PeclPipeline(MakeConfig(dir.Path, MissingCursorPolicy.FromTail));
        reader.RegisterConsumer("c");
        reader.Start();
        List<ulong> seqNos = ReadAll(reader, "c");
        reader.Stop();

        seqNos.Should().BeEmpty();
    }

    /// <summary>
    /// <see cref="MissingCursorPolicy.FromTail"/> with no existing cursor file:
    /// records appended after <see cref="PeclPipeline.Start"/> are delivered normally.
    /// </summary>
    [Fact]
    public void MissingCursorPolicy_FromTail_ReadsNewRecordsAppendedAfterStart()
    {
        using var dir = new TempDirectory();

        using (var writer = new PeclPipeline(MakeConfig(dir.Path)))
        {
            writer.Start();
            for (int i = 0; i < 5; i++)
            {
                writer.Append("old"u8);
            }

            writer.Flush();
            writer.Stop();
        }

        using var reader = new PeclPipeline(MakeConfig(dir.Path, MissingCursorPolicy.FromTail));
        reader.RegisterConsumer("c");
        reader.Start();

        // Append 3 new records after Start — consumer should see exactly these.
        reader.Append("new"u8);
        reader.Append("new"u8);
        reader.Append("new"u8);
        reader.Flush();

        List<ulong> seqNos = ReadAll(reader, "c");
        reader.Stop();

        seqNos.Should().HaveCount(3);
        seqNos.Should().ContainInOrder(5UL, 6UL, 7UL);
    }

    /// <summary>
    /// When a valid cursor file already exists, <see cref="MissingCursorPolicy"/> is
    /// ignored — the consumer resumes from its recorded position regardless of the policy.
    /// Setup: write 5 records, read the first 2 (cursor advances to seqNo 1), stop.
    /// Second run uses <see cref="MissingCursorPolicy.FromTail"/>: if the policy were
    /// honoured it would skip all 5 records; since the cursor file exists it is ignored
    /// and the consumer resumes from seqNo 2 — reading the remaining 3 records.
    /// </summary>
    [Fact]
    public void MissingCursorPolicy_ExistingCursor_PolicyIgnoredRegardlessOfSetting()
    {
        using var dir = new TempDirectory();

        // First run: write 5 records, read 2 to advance cursor to seqNo 1, stop (flushes cursor).
        using (var first = new PeclPipeline(MakeConfig(dir.Path, MissingCursorPolicy.FromBeginning)))
        {
            first.RegisterConsumer("c");
            first.Start();
            for (int i = 0; i < 5; i++)
            {
                first.Append("data"u8);
            }

            first.Flush();
            first.ReadNext("c"); // seqNo 0
            first.ReadNext("c"); // seqNo 1 — cursor now at seqNo 1
            first.Stop();        // Stop flushes cursors
        }

        // Second run: FromTail would position at seqNo 4 (tail) reading 0 records.
        // Because a cursor file exists, the policy is ignored and the consumer resumes
        // from seqNo 2 — reading the 3 remaining records (seqNos 2, 3, 4).
        using var second = new PeclPipeline(MakeConfig(dir.Path, MissingCursorPolicy.FromTail));
        second.RegisterConsumer("c");
        second.Start();
        List<ulong> seqNos = ReadAll(second, "c");
        second.Stop();

        seqNos.Should().HaveCount(3);
        seqNos.Should().ContainInOrder(2UL, 3UL, 4UL);
    }
}
