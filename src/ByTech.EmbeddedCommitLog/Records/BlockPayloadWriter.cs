using System.Buffers.Binary;

namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Encodes a list of raw byte payloads into a single block payload suitable for
/// storage as a <see cref="RecordFlags.IsBlock"/> record.
/// </summary>
/// <remarks>
/// Block payload wire format (all multi-byte fields little-endian):
/// <code>
/// [4 bytes: entry count (uint)]
/// for each entry:
///     [4 bytes: payload length (int)]
///     [payload bytes]
/// </code>
/// All entries share the block frame's <c>ContentType</c> and <c>SchemaId</c>.
/// </remarks>
internal static class BlockPayloadWriter
{
    /// <summary>Byte size of the block header (entry count field).</summary>
    internal const int HeaderSize = sizeof(uint);

    /// <summary>Byte size of the per-entry length prefix.</summary>
    internal const int EntryLengthSize = sizeof(int);

    /// <summary>
    /// Computes the total encoded byte length for a block payload containing
    /// <paramref name="entries"/>.
    /// </summary>
    internal static int ComputeSize(IReadOnlyList<ReadOnlyMemory<byte>> entries)
    {
        int total = HeaderSize;
        foreach (ReadOnlyMemory<byte> e in entries)
        {
            total += EntryLengthSize + e.Length;
        }

        return total;
    }

    /// <summary>
    /// Writes the block payload encoding of <paramref name="entries"/> into
    /// <paramref name="destination"/>. The span must be exactly
    /// <see cref="ComputeSize"/> bytes long.
    /// </summary>
    internal static void Write(IReadOnlyList<ReadOnlyMemory<byte>> entries, Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(destination, (uint)entries.Count);
        int pos = HeaderSize;
        foreach (ReadOnlyMemory<byte> e in entries)
        {
            BinaryPrimitives.WriteInt32LittleEndian(destination[pos..], e.Length);
            pos += EntryLengthSize;
            e.Span.CopyTo(destination[pos..]);
            pos += e.Length;
        }
    }
}
