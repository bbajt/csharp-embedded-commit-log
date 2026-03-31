namespace ByTech.EmbeddedCommitLog.Segments;

/// <summary>
/// Encapsulates the segment file naming convention: <c>log-NNNNNN.seg</c>
/// where NNNNNN is the zero-padded six-digit segment identifier.
/// </summary>
/// <remarks>
/// <para>
/// Six-digit zero-padding ensures lexicographic sort order matches numeric order
/// for segment IDs 0 through 999,999 — sufficient for all practical log lifetimes.
/// </para>
/// <para>Invariants:</para>
/// <list type="bullet">
///   <item>Filenames are always lower-case ASCII.</item>
///   <item>The format is stable; any change requires a new ADR entry.</item>
/// </list>
/// </remarks>
internal static class SegmentNaming
{
    internal const string Prefix = "log-";
    internal const string Extension = ".seg";

    /// <summary>
    /// The maximum valid segment identifier (999,999). PECL filenames use a 6-digit
    /// zero-padded format (<c>log-NNNNNN.seg</c>), so IDs ≥ 1,000,000 would produce
    /// unparseable 7-digit filenames and must be rejected at write time.
    /// </summary>
    internal const uint MaxSegmentId = 999_999u;

    /// <summary>Returns the filename (no directory) for the given segment identifier.</summary>
    /// <example><c>GetFileName(1u)</c> → <c>"log-000001.seg"</c></example>
    internal static string GetFileName(uint segmentId) =>
        $"{Prefix}{segmentId:D6}{Extension}";

    /// <summary>Returns the full file path for the given segment in <paramref name="segmentsDirectory"/>.</summary>
    internal static string GetFilePath(string segmentsDirectory, uint segmentId) =>
        Path.Combine(segmentsDirectory, GetFileName(segmentId));

    /// <summary>
    /// Attempts to parse the segment identifier from a segment filename.
    /// </summary>
    /// <param name="fileName">The bare filename (no directory), e.g. <c>"log-000042.seg"</c>.</param>
    /// <param name="segmentId">Set to the parsed identifier on success.</param>
    /// <returns><see langword="true"/> if the filename matches the expected pattern.</returns>
    internal static bool TryParseId(string fileName, out uint segmentId)
    {
        segmentId = 0;
        if (fileName.Length != 14)
        {
            return false;
        }

        if (!fileName.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        if (!fileName.EndsWith(Extension, StringComparison.Ordinal))
        {
            return false;
        }

        ReadOnlySpan<char> digits = fileName.AsSpan(Prefix.Length, 6);
        return uint.TryParse(digits, out segmentId);
    }
}
