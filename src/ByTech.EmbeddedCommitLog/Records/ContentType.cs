namespace ByTech.EmbeddedCommitLog.Records;

/// <summary>
/// Advisory hint stored at byte offset 6 of a PECL record header describing
/// how the payload bytes are encoded.
/// </summary>
/// <remarks>
/// PECL does not interpret the payload. This field is purely advisory: consumers
/// use it to select the appropriate deserializer. Writers that do not use schema
/// integration should pass <see cref="Unknown"/> or the most appropriate value.
/// </remarks>
public enum ContentType : byte
{
    /// <summary>Encoding is unspecified or unknown. Consumers must determine encoding out-of-band.</summary>
    Unknown = 0x00,

    /// <summary>UTF-8 encoded JSON.</summary>
    Json = 0x01,

    /// <summary>MessagePack binary encoding.</summary>
    MessagePack = 0x02,

    /// <summary>Protocol Buffers (protobuf) binary encoding.</summary>
    Protobuf = 0x03,

    /// <summary>Application-defined encoding. The meaning is entirely up to the producer and consumer.</summary>
    Custom = 0xFF,
}
