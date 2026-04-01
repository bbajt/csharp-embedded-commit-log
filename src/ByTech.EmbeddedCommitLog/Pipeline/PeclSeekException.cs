using System;

namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// Thrown by <see cref="Pipeline.SeekConsumer"/> when the target sequence number
/// falls within a segment that has been deleted by retention GC.
/// </summary>
/// <remarks>
/// The caller must inspect <see cref="EarliestAvailableSeqNo"/> and either adjust the
/// seek target or call <see cref="Pipeline.ResetConsumer"/> to move to the earliest
/// available position.
/// </remarks>
public sealed class PeclSeekException : Exception
{
    /// <summary>The earliest sequence number currently available in the log.</summary>
    public ulong EarliestAvailableSeqNo { get; }

    /// <summary>
    /// Initialises a new instance of <see cref="PeclSeekException"/>.
    /// </summary>
    /// <param name="earliestAvailableSeqNo">
    /// The earliest sequence number currently available in the log.
    /// </param>
    /// <param name="message">The error message.</param>
    public PeclSeekException(ulong earliestAvailableSeqNo, string message)
        : base(message)
        => EarliestAvailableSeqNo = earliestAvailableSeqNo;

    /// <summary>
    /// Initialises a new instance of <see cref="PeclSeekException"/> with an inner exception.
    /// </summary>
    /// <param name="earliestAvailableSeqNo">
    /// The earliest sequence number currently available in the log.
    /// </param>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">
    /// The exception that caused this exception, or <see langword="null"/>.
    /// </param>
    public PeclSeekException(ulong earliestAvailableSeqNo, string message, Exception? innerException)
        : base(message, innerException)
        => EarliestAvailableSeqNo = earliestAvailableSeqNo;
}
