namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Controls when sealed segments become eligible for garbage collection.
/// </summary>
/// <remarks>
/// <see cref="ConsumerGated"/> is the default and safest policy. <see cref="TimeBased"/>
/// and <see cref="SizeBased"/> delete segments regardless of consumer read positions and
/// should only be used when occasional data loss for lagging consumers is acceptable.
/// </remarks>
public enum RetentionPolicy
{
    /// <summary>
    /// A segment is eligible for deletion only when every registered consumer
    /// has advanced its read position strictly past the segment's end.
    /// This is the default and safest policy: no data reachable by any consumer
    /// is ever deleted.
    /// </summary>
    ConsumerGated = 0,

    /// <summary>
    /// Sealed segments whose last-write timestamp is older than
    /// <see cref="PipelineConfiguration.RetentionMaxAgeMs"/> are eligible for deletion,
    /// regardless of consumer read positions. Suitable for telemetry or log pipelines
    /// where a bounded retention window is required and slow consumers may lose data.
    /// </summary>
    /// <remarks>
    /// Segment age is measured by <c>File.GetLastWriteTimeUtc</c>, which reflects when
    /// the last byte was written to the segment file (effectively its seal time).
    /// The active (writable) segment is never deleted.
    /// </remarks>
    TimeBased = 1,

    /// <summary>
    /// Oldest sealed segments are deleted when the total on-disk size of all segments
    /// (including the active segment) exceeds <see cref="PipelineConfiguration.RetentionMaxBytes"/>.
    /// Suitable for pipelines with strict disk-space budgets. Lagging consumers may lose data.
    /// </summary>
    /// <remarks>
    /// Segments are deleted in ascending order (oldest first) until the total size falls
    /// within the configured limit. The active (writable) segment is never deleted.
    /// </remarks>
    SizeBased = 2,
}
