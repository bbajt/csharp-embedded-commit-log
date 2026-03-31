using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// Immutable read-side envelope for a record delivered to sinks.
/// Owns its payload array; callers must not mutate or share the underlying array across record instances.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Payload"/> equality uses reference equality (the C# record default for arrays).
/// Two <see cref="LogRecord"/> instances with identical byte contents but different array instances
/// are not considered equal by <c>==</c> or <see cref="object.Equals(object?)"/>.
/// </para>
/// </remarks>
/// <param name="Header">The deserialized record header read from the segment file.</param>
/// <param name="Payload">The raw payload bytes owned by this instance. Must not be null.</param>
public sealed record LogRecord(RecordHeader Header, byte[] Payload);
