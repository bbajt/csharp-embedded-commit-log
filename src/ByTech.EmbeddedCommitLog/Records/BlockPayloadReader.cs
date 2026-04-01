using System.Buffers.Binary;

namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Decodes a block payload produced by <see cref="BlockPayloadWriter"/> into individual
/// entry payloads. Designed as a <see langword="ref struct"/> for zero-allocation, stack-only
/// sequential decoding.
/// </summary>
/// <remarks>
/// <para>
/// Construct with the raw block payload span (the bytes stored after the record header).
/// Call <see cref="TryReadNext"/> in a loop until it returns <see langword="false"/>.
/// </para>
/// <para>
/// The returned <see cref="ReadOnlySpan{T}"/> slices are valid only as long as the original
/// buffer passed to the constructor is alive.
/// </para>
/// </remarks>
public ref struct BlockPayloadReader
{
    private ReadOnlySpan<byte> _remaining;
    private readonly int _entryCount;
    private int _entriesRead;

    /// <summary>
    /// Initialises a new <see cref="BlockPayloadReader"/> over <paramref name="blockPayload"/>.
    /// </summary>
    /// <param name="blockPayload">
    /// The raw block payload bytes — the 4-byte entry count followed by the encoded entries.
    /// </param>
    public BlockPayloadReader(ReadOnlySpan<byte> blockPayload)
    {
        if (blockPayload.Length < BlockPayloadWriter.HeaderSize)
        {
            _entryCount = 0;
            _remaining = ReadOnlySpan<byte>.Empty;
            _entriesRead = 0;
            return;
        }

        uint rawCount = BinaryPrimitives.ReadUInt32LittleEndian(blockPayload);
        _entryCount = rawCount <= (uint)int.MaxValue ? (int)rawCount : 0;
        _remaining = blockPayload[BlockPayloadWriter.HeaderSize..];
        _entriesRead = 0;
    }

    /// <summary>Total number of entries encoded in the block payload.</summary>
    public int EntryCount => _entryCount;

    /// <summary>
    /// Reads the next entry payload from the block.
    /// </summary>
    /// <param name="payload">
    /// On success, a <see cref="ReadOnlySpan{T}"/> over the entry bytes. The span is a slice
    /// of the original buffer passed to the constructor.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when an entry was successfully read;
    /// <see langword="false"/> when all entries have been consumed or the buffer is malformed.
    /// </returns>
    public bool TryReadNext(out ReadOnlySpan<byte> payload)
    {
        if (_entriesRead >= _entryCount || _remaining.Length < BlockPayloadWriter.EntryLengthSize)
        {
            payload = default;
            return false;
        }

        int len = BinaryPrimitives.ReadInt32LittleEndian(_remaining);
        _remaining = _remaining[BlockPayloadWriter.EntryLengthSize..];

        if (len < 0 || _remaining.Length < len)
        {
            payload = default;
            return false;
        }

        payload = _remaining[..len];
        _remaining = _remaining[len..];
        _entriesRead++;
        return true;
    }
}
