using System.Buffers.Binary;
using System.Text;
using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Cursors;

/// <summary>
/// Persists a <see cref="CursorData"/> snapshot to disk using a crash-safe
/// write-temp + fsync + rename strategy.
/// </summary>
/// <remarks>
/// <para>Write sequence:</para>
/// <list type="number">
///   <item>Validate <see cref="CursorData.ConsumerName"/> (non-empty, valid filename chars,
///     within <see cref="CursorData.MaxConsumerNameBytes"/> UTF-8 bytes).</item>
///   <item>Serialize <see cref="CursorData"/> into a variable-length buffer and write it to
///     <c>{ConsumerName}.tmp</c> in the cursors directory.</item>
///   <item>Call <c>FileStream.Flush(flushToDisk: true)</c> to fsync the temp file.</item>
///   <item>Rename <c>{ConsumerName}.tmp</c> → <c>{ConsumerName}.cur</c> (overwriting any existing cursor).</item>
/// </list>
/// <para>
/// A crash between steps 2 and 4 leaves <c>{ConsumerName}.cur</c> unchanged.
/// A crash during step 2 leaves <c>{ConsumerName}.tmp</c> partial or absent.
/// Neither scenario corrupts the last valid cursor.
/// </para>
/// <para>
/// A stale <c>{ConsumerName}.tmp</c> from a prior crash is silently overwritten.
/// The cursors directory must exist; it is not created automatically.
/// </para>
/// </remarks>
public static class CursorWriter
{
    private const string TempSuffix = ".tmp";
    private const string FinalSuffix = ".cur";

    /// <summary>
    /// Atomically writes <paramref name="data"/> as the new cursor for
    /// <see cref="CursorData.ConsumerName"/> in <paramref name="cursorsDirectory"/>.
    /// </summary>
    /// <param name="cursorsDirectory">
    /// Absolute path to the pipeline's <c>cursors/</c> subdirectory.
    /// The directory must exist; it is not created automatically.
    /// </param>
    /// <param name="data">The cursor state to persist.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="cursorsDirectory"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <see cref="CursorData.ConsumerName"/> is null, empty, contains characters that are
    /// invalid in a filename, or encodes to more than
    /// <see cref="CursorData.MaxConsumerNameBytes"/> UTF-8 bytes.
    /// </exception>
    /// <exception cref="DirectoryNotFoundException">
    /// <paramref name="cursorsDirectory"/> does not exist.
    /// </exception>
    public static void Write(string cursorsDirectory, CursorData data)
    {
        ArgumentNullException.ThrowIfNull(cursorsDirectory);
        ValidateConsumerName(data.ConsumerName);

        byte[] nameBytes = Encoding.UTF8.GetBytes(data.ConsumerName);

        if (nameBytes.Length > CursorData.MaxConsumerNameBytes)
        {
            throw new ArgumentException(
                $"Consumer name '{data.ConsumerName}' encodes to {nameBytes.Length} UTF-8 bytes, " +
                $"which exceeds the maximum of {CursorData.MaxConsumerNameBytes}.",
                nameof(data));
        }

        string tmpPath = Path.Combine(cursorsDirectory, data.ConsumerName + TempSuffix);
        string finalPath = Path.Combine(cursorsDirectory, data.ConsumerName + FinalSuffix);

        int totalSize = CursorData.MinSerializedSize + nameBytes.Length;

        Span<byte> buf = stackalloc byte[CursorData.MaxSerializedSize];
        buf = buf[..totalSize];

        // Header
        BinaryPrimitives.WriteUInt32LittleEndian(buf, CursorData.ExpectedMagic);
        buf[4] = CursorData.CurrentVersion;
        buf[5] = 0; // reserved
        BinaryPrimitives.WriteUInt16LittleEndian(buf[6..], (ushort)nameBytes.Length);

        // Consumer name
        nameBytes.CopyTo(buf[8..]);

        int tail = 8 + nameBytes.Length;

        // Fields
        BinaryPrimitives.WriteUInt32LittleEndian(buf[tail..], data.SegmentId);
        BinaryPrimitives.WriteInt64LittleEndian(buf[(tail + 4)..], data.Offset);
        BinaryPrimitives.WriteUInt64LittleEndian(buf[(tail + 12)..], data.SeqNo);

        // CRC covers all bytes except the trailing 4-byte CRC field
        uint crc = CrcUtility.ComputeCrc32(buf[..(tail + 20)]);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[(tail + 20)..], crc);

        using (var stream = new FileStream(
            tmpPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 512))
        {
            stream.Write(buf);
            stream.Flush(flushToDisk: true);
        }

        File.Move(tmpPath, finalPath, overwrite: true);
    }

    private static void ValidateConsumerName(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException(
                "Consumer name must be non-null and non-empty.", "data");
        }

        if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException(
                $"Consumer name '{name}' contains characters that are invalid in a filename.",
                "data");
        }
    }
}
