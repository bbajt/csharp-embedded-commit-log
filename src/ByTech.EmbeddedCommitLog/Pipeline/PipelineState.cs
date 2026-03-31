namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>Lifecycle state of a <see cref="Pipeline"/> instance.</summary>
public enum PipelineState
{
    /// <summary>
    /// The pipeline is performing recovery and preparing internal state.
    /// No I/O operations are accepted in this state.
    /// </summary>
    Starting,

    /// <summary>
    /// The pipeline is fully operational. <c>Append</c>, <c>Flush</c>, and
    /// <c>ReadNext</c> are all valid in this state.
    /// </summary>
    Running,

    /// <summary>
    /// <c>Stop</c> has been called. The pipeline is flushing and sealing the active
    /// segment. No new appends are accepted.
    /// </summary>
    Draining,

    /// <summary>
    /// The pipeline has been cleanly shut down. All data is durable on disk.
    /// </summary>
    Stopped,

    /// <summary>
    /// An unrecoverable error occurred. The pipeline cannot be used further.
    /// </summary>
    /// <remarks>
    /// This is a terminal state. Once a pipeline enters <c>Error</c>,
    /// <see cref="Pipeline.Start"/> will throw <see cref="InvalidOperationException"/>.
    /// To recover, dispose the current instance and construct a new <see cref="Pipeline"/>
    /// with the same configuration; the new instance will run crash recovery on
    /// <see cref="Pipeline.Start"/>.
    /// </remarks>
    Error,
}
