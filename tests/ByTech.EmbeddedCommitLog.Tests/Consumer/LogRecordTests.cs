using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Records;
using FluentAssertions;
using Xunit;

namespace ByTech.EmbeddedCommitLog.Tests.Consumer;

public sealed class LogRecordTests
{
    private static RecordHeader MakeHeader(ulong seqNo = 0) =>
        new(RecordHeader.ExpectedMagic, RecordHeader.CurrentVersion,
            RecordFlags.None, ContentType.Unknown, 0, 0, seqNo, 4);

    [Fact]
    public void LogRecord_Constructor_SetsHeaderAndPayload()
    {
        var header = MakeHeader(42);
        var payload = new byte[] { 1, 2, 3, 4 };

        var record = new LogRecord(header, payload);

        record.Header.Should().Be(header);
        record.Payload.Should().BeSameAs(payload);
    }

    [Fact]
    public void LogRecord_WithExpression_CreatesNewInstance()
    {
        var header = MakeHeader(1);
        var payload = new byte[] { 9, 8, 7 };
        var original = new LogRecord(header, payload);

        var updated = original with { Header = MakeHeader(2) };

        updated.Header.SeqNo.Should().Be(2UL);
        updated.Payload.Should().BeSameAs(original.Payload);
        updated.Should().NotBeSameAs(original);
    }

    [Fact]
    public void LogRecord_PayloadEquality_IsReferenceEquality()
    {
        var header = MakeHeader();
        var bytes1 = new byte[] { 1, 2, 3 };
        var bytes2 = new byte[] { 1, 2, 3 }; // same contents, different instance

        var a = new LogRecord(header, bytes1);
        var b = new LogRecord(header, bytes2);

        // C# record struct equality on arrays is reference equality
        a.Should().NotBe(b);
    }
}
