using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using ByTech.EmbeddedCommitLog.Checkpoint;
using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Cursors;
using ByTech.EmbeddedCommitLog.Records;
using ByTech.EmbeddedCommitLog.Segments;
using ByTech.EmbeddedCommitLog.Sinks;

namespace ByTech.EmbeddedCommitLog.Pipeline;

/// <summary>
/// The top-level coordinator that wires together segment writing, checkpoint persistence,
/// and consumer cursor tracking into a single crash-safe append-only log.
/// </summary>
/// <remarks>
/// <para>
/// Lifecycle: <see cref="Start"/> transitions the pipeline from
/// <see cref="PipelineState.Stopped"/> to <see cref="PipelineState.Running"/> after
/// completing crash recovery. <see cref="Stop"/> flushes and seals the active segment
/// then transitions to <see cref="PipelineState.Stopped"/> again. <see cref="Dispose"/>
/// calls <see cref="Stop"/> automatically if the pipeline is still running.
/// </para>
/// <para>
/// Consumers must be registered via <see cref="RegisterConsumer"/> before calling
/// <see cref="Start"/> for their persisted cursor positions to be recovered. Consumers
/// registered after <see cref="Start"/> begin reading from position (segment 0, offset 0).
/// </para>
/// <para>
/// Thread-safety: not thread-safe. All calls must be made from a single thread or
/// externally synchronized.
/// </para>
/// </remarks>
public sealed class Pipeline : IDisposable
{
    private readonly PipelineConfiguration _config;
    private readonly string _segmentsDir;
    private readonly string _cursorsDir;
    private readonly string _spillDir;
    private readonly Dictionary<string, ConsumerState> _consumers = new();

    /// <summary>
    /// Preserves the names of all consumers ever registered via <see cref="RegisterConsumer"/>.
    /// Unlike <see cref="_consumers"/>, this set is never cleared — it survives
    /// <see cref="Stop"/> so that <see cref="SeekConsumer"/> and <see cref="ResetConsumer"/>
    /// can validate consumer names without requiring re-registration after each stop.
    /// </summary>
    private readonly HashSet<string> _registeredConsumerNames = new();

    private readonly IRecordRouter _router;

    private const int ReaderPollIntervalMs = 10;

    private volatile SegmentWriter? _writer;
    private ulong _nextSeqNo;
    private bool _disposed;
    private CancellationTokenSource? _gcCts;
    private Task _gcTask = Task.CompletedTask;
    private CancellationTokenSource? _flushTimerCts;
    private Task _flushTimerTask = Task.CompletedTask;
    private long _recordsSinceLastFlush;

    /// <summary>
    /// Guards observable gauge callbacks and <see cref="_consumers"/>.Clear() in <see cref="Stop"/>
    /// to prevent the callback from iterating a collection being cleared concurrently.
    /// </summary>
    private readonly object _observabilityLock = new();

    /// <summary>
    /// Serialises all calls to <see cref="CheckpointWriter.Write"/> so that the
    /// background Batched-mode flush timer and an explicit <see cref="Flush"/> (or a
    /// rollover <see cref="WriteCheckpointCore"/>) cannot open <c>checkpoint.tmp</c>
    /// concurrently.  On Linux <c>FileShare.None</c> is enforced within the same
    /// process by the BCL's internal file-handle table, causing an
    /// <see cref="IOException"/> on the second opener without this lock.
    /// </summary>
    private readonly object _checkpointLock = new();

    /// <summary>Current lifecycle state of the pipeline.</summary>
    public PipelineState State { get; private set; } = PipelineState.Stopped;

    /// <summary>Live operational counters for this pipeline instance.</summary>
    public PipelineMetrics Metrics { get; }

    /// <summary>
    /// Initialises a new <see cref="Pipeline"/> with the given configuration.
    /// The pipeline starts in the <see cref="PipelineState.Stopped"/> state;
    /// call <see cref="Start"/> to begin operation.
    /// </summary>
    /// <param name="config">Pipeline configuration. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is <see langword="null"/>.</exception>
    public Pipeline(PipelineConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(config);
        _config = config;
        _segmentsDir = Path.Combine(config.RootDirectory, "segments");
        _cursorsDir = Path.Combine(config.RootDirectory, "cursors");
        _spillDir = Path.Combine(config.RootDirectory, "spill");
        Metrics = new PipelineMetrics(config.MeterName);
        _router = new BroadcastRouter(config.BackpressurePolicy, sinkName => Metrics.IncrementSinkDropped(sinkName));
    }

    /// <summary>
    /// Starts the pipeline: creates required directories, runs crash recovery, and
    /// transitions to <see cref="PipelineState.Running"/>.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">The pipeline is not in the <see cref="PipelineState.Stopped"/> state.</exception>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (State != PipelineState.Stopped)
        {
            throw new InvalidOperationException($"Cannot start a pipeline that is in the {State} state.");
        }

        // Validate configuration before any side effects so that a bad config fails fast
        // while State is still Stopped (nothing to clean up, Dispose() is a safe no-op).
        if (_config.DurabilityMode == DurabilityMode.Batched && _config.FsyncIntervalMs <= 0)
        {
            throw new InvalidOperationException(
                $"PipelineConfiguration.FsyncIntervalMs must be > 0 when " +
                $"DurabilityMode is Batched (got {_config.FsyncIntervalMs}).");
        }

        if (_config.RetentionPolicy == RetentionPolicy.TimeBased && _config.RetentionMaxAgeMs <= 0)
        {
            throw new InvalidOperationException(
                $"PipelineConfiguration.RetentionMaxAgeMs must be > 0 when " +
                $"RetentionPolicy is TimeBased (got {_config.RetentionMaxAgeMs}).");
        }

        if (_config.RetentionPolicy == RetentionPolicy.SizeBased && _config.RetentionMaxBytes <= 0)
        {
            throw new InvalidOperationException(
                $"PipelineConfiguration.RetentionMaxBytes must be > 0 when " +
                $"RetentionPolicy is SizeBased (got {_config.RetentionMaxBytes}).");
        }

        State = PipelineState.Starting;

        try
        {
            EnsureDirectories();
            DeleteOrphanedSpillFiles();
            RecoverInternal();
            _recordsSinceLastFlush = 0;

            foreach (ConsumerState cs in _consumers.Values)
            {
                if (!cs.IsPushMode)
                {
                    continue;
                }

                cs.ReaderCts = new CancellationTokenSource();
                CancellationToken ct = cs.ReaderCts.Token;

                foreach (SinkSlot slot in cs.SinkSlots)
                {
                    slot.Task = RunSinkTaskAsync(slot);
                }

                cs.ReaderTask = RunReaderLoopAsync(cs, ct);
            }

            // Initialise segment count metric and launch GC background task.
            Metrics.SetSegmentCount(EnumerateSegmentIds().Count);
            _gcCts = new CancellationTokenSource();
            _gcTask = RunGcAsync(_gcCts.Token);

            if (_config.DurabilityMode == DurabilityMode.Batched)
            {
                _flushTimerCts = new CancellationTokenSource();
                _flushTimerTask = RunFlushTimerAsync(_flushTimerCts.Token);
            }

            // All subsystems (reader tasks, GC task) are live — safe to publish Running state.
            // Setting State here rather than before the reader/GC launch eliminates the window
            // where external callers see State == Running with _gcCts null or _gcTask incomplete (R05-L5).
            State = PipelineState.Running;

            Metrics.StartObservableInstruments(
                () =>
                {
                    lock (_observabilityLock)
                    {
                        if (_consumers.Count == 0)
                        {
                            return [];
                        }

                        return _consumers.Values
                            .Where(cs => cs.IsPushMode ? cs.LastRoutedSeqNo.HasValue : cs.LastReadSeqNo.HasValue)
                            .Select(cs =>
                            {
                                ulong nextSeq = Volatile.Read(ref _nextSeqNo);
                                long tail = nextSeq > 0 ? (long)(nextSeq - 1) : 0L;
                                ulong consumerSeqNo = cs.IsPushMode
                                    ? cs.LastRoutedSeqNo!.Value
                                    : cs.LastReadSeqNo!.Value;
                                long cursor = (long)consumerSeqNo;
                                return new Measurement<long>(
                                    Math.Max(0L, tail - cursor),
                                    new KeyValuePair<string, object?>("consumer", cs.ConsumerName));
                            })
                            .ToArray();
                    }
                },
                () =>
                {
                    lock (_observabilityLock)
                    {
                        if (_consumers.Count == 0)
                        {
                            return [];
                        }

                        return _consumers.Values
                            .SelectMany(cs => cs.SinkSlots.Select(slot =>
                                new Measurement<long>(
                                    (long)slot.Lane.Count,
                                    new KeyValuePair<string, object?>("consumer", cs.ConsumerName),
                                    new KeyValuePair<string, object?>("sink", slot.SinkName))))
                            .ToArray();
                    }
                });
        }
        catch (Exception)
        {
            if (State == PipelineState.Running)
            {
                // Post-Running failure: reader/sink tasks may be live. Stop() handles
                // full cleanup (cancel readers, drain sinks, dispose writer, clear consumers).
                try { Stop(); } catch (Exception) { /* best-effort */ }
            }

            // Ensure State is Stopped so Dispose() can run safely.
            // Stop() may have already set State to Stopped or Error; override Starting/Running.
            if (State == PipelineState.Starting || State == PipelineState.Running)
            {
                State = PipelineState.Stopped;
            }

            throw;
        }
    }

    /// <summary>
    /// Appends a record to the log and returns its globally monotonic sequence number.
    /// If the active segment is full a new segment is opened first.
    /// </summary>
    /// <param name="payload">Raw payload bytes. May be empty.</param>
    /// <param name="contentType">Advisory payload encoding hint. Defaults to <see cref="ContentType.Unknown"/>.</param>
    /// <param name="schemaId">Optional schema identifier. Defaults to 0 (none).</param>
    /// <returns>The sequence number assigned to the appended record.</returns>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">The pipeline is not in the <see cref="PipelineState.Running"/> state.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="payload"/> length plus framing overhead exceeds
    /// <see cref="PipelineConfiguration.MaxSegmentSize"/>. A single record must fit
    /// within one segment.
    /// </exception>
    public ulong Append(
        ReadOnlySpan<byte> payload,
        ContentType contentType = ContentType.Unknown,
        uint schemaId = 0u)
    {
        ThrowIfNotRunning();

        long recordSize = payload.Length + RecordWriter.FramingOverhead;
        if (recordSize > _config.MaxSegmentSize)
        {
            throw new ArgumentException(
                $"Payload length {payload.Length} B plus framing overhead {RecordWriter.FramingOverhead} B " +
                $"({recordSize} B total) exceeds MaxSegmentSize ({_config.MaxSegmentSize} B). " +
                "A single record must fit within one segment.",
                nameof(payload));
        }

        if (_writer!.IsFull)
        {
            // Capture sealed-segment state before disposing the writer.
            uint sealedSegId = _writer.SegmentId;
            long sealedBytes = _writer.BytesWritten;
            ulong sealedSeqNo = _nextSeqNo == 0 ? 0UL : _nextSeqNo - 1;
            uint nextSegId = sealedSegId + 1;

            if (nextSegId > SegmentNaming.MaxSegmentId)
            {
                throw new InvalidOperationException(
                    $"Cannot roll over to segment {nextSegId}: maximum segment ID is {SegmentNaming.MaxSegmentId}. " +
                    "Archive or compact the log to reclaim segment IDs.");
            }

            // Create the new writer FIRST so that _writer always points to a valid open
            // writer — even if this throws (e.g. disk full), the old _writer is untouched.
            // SegmentWriter ctor uses FileMode.OpenOrCreate + seek-to-end, so a retry on
            // the same nextSegId after a prior failure is safe (R04-NLB).
            SegmentWriter nextWriter = new SegmentWriter(_segmentsDir, nextSegId, _config.MaxSegmentSize, _config.CompressionAlgorithm);

            // Seal and dispose the old writer in a finally so _writer = nextWriter always
            // runs — preserving the invariant that _writer points to a valid open writer
            // before WriteCheckpointCore (established by PHASE-03-02).
            try
            {
                _writer.Seal();
            }
            finally
            {
                _writer.Dispose();
                _writer = nextWriter;
            }

            WriteCheckpointCore(sealedSegId, sealedBytes, sealedSeqNo, nextSegId);
            Metrics.IncrementSegmentRollovers();
        }

        ulong seqNo = _nextSeqNo++;
        _recordsSinceLastFlush++;
        _writer.Append(payload, seqNo, contentType, RecordFlags.None, schemaId);

        if (_config.DurabilityMode == DurabilityMode.Strict)
        {
            long tsStart = Stopwatch.GetTimestamp();
            _writer.FlushToDisk();
            double fsyncMs = Stopwatch.GetElapsedTime(tsStart).TotalMilliseconds;
            Metrics.RecordFsyncDuration(fsyncMs);
            Metrics.RecordBatchSize(_recordsSinceLastFlush);
            _recordsSinceLastFlush = 0;
        }

        Metrics.IncrementRecordsAppended();
        Metrics.AddBytesAppended(payload.Length);
        return seqNo;
    }

    /// <summary>
    /// Appends multiple payloads as a single block record and returns the block's globally
    /// monotonic sequence number. The block is stored as one framed record on disk with
    /// <see cref="RecordFlags.IsBlock"/> set; push-mode consumers receive the entries as
    /// individual <see cref="Consumer.LogRecord"/> instances (transparent to
    /// <see cref="Sinks.ISink"/>). Pull-mode consumers receive the raw block record.
    /// </summary>
    /// <param name="entries">
    /// Payloads to encode into the block. Must contain at least one entry.
    /// All entries share the block frame's <paramref name="contentType"/> and
    /// <paramref name="schemaId"/>.
    /// </param>
    /// <param name="contentType">Advisory payload encoding hint. Defaults to <see cref="ContentType.Unknown"/>.</param>
    /// <param name="schemaId">Optional schema identifier. Defaults to 0 (none).</param>
    /// <returns>The sequence number assigned to the block record.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="entries"/> is empty, or the encoded block payload plus framing overhead
    /// exceeds <see cref="PipelineConfiguration.MaxSegmentSize"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">The pipeline is not in the <see cref="PipelineState.Running"/> state.</exception>
    public ulong AppendBlock(
        IReadOnlyList<ReadOnlyMemory<byte>> entries,
        ContentType contentType = ContentType.Unknown,
        uint schemaId = 0u)
    {
        ThrowIfNotRunning();
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            throw new ArgumentException("Block must contain at least one entry.", nameof(entries));
        }

        int blockPayloadSize = BlockPayloadWriter.ComputeSize(entries);
        long recordSize = blockPayloadSize + RecordWriter.FramingOverhead;
        if (recordSize > _config.MaxSegmentSize)
        {
            throw new ArgumentException(
                $"Encoded block payload {blockPayloadSize} B plus framing overhead {RecordWriter.FramingOverhead} B " +
                $"({recordSize} B total) exceeds MaxSegmentSize ({_config.MaxSegmentSize} B). " +
                "The entire block must fit within one segment.",
                nameof(entries));
        }

        if (_writer!.IsFull)
        {
            uint sealedSegId = _writer.SegmentId;
            long sealedBytes = _writer.BytesWritten;
            ulong sealedSeqNo = _nextSeqNo == 0 ? 0UL : _nextSeqNo - 1;
            uint nextSegId = sealedSegId + 1;

            if (nextSegId > SegmentNaming.MaxSegmentId)
            {
                throw new InvalidOperationException(
                    $"Cannot roll over to segment {nextSegId}: maximum segment ID is {SegmentNaming.MaxSegmentId}. " +
                    "Archive or compact the log to reclaim segment IDs.");
            }

            SegmentWriter nextWriter = new SegmentWriter(_segmentsDir, nextSegId, _config.MaxSegmentSize, _config.CompressionAlgorithm);

            try
            {
                _writer.Seal();
            }
            finally
            {
                _writer.Dispose();
                _writer = nextWriter;
            }

            WriteCheckpointCore(sealedSegId, sealedBytes, sealedSeqNo, nextSegId);
            Metrics.IncrementSegmentRollovers();
        }

        ulong seqNo = _nextSeqNo++;
        _recordsSinceLastFlush++;

        byte[] rented = ArrayPool<byte>.Shared.Rent(blockPayloadSize);
        try
        {
            Span<byte> buf = rented.AsSpan(0, blockPayloadSize);
            BlockPayloadWriter.Write(entries, buf);
            _writer.Append(buf, seqNo, contentType, RecordFlags.IsBlock, schemaId);

            if (_config.DurabilityMode == DurabilityMode.Strict)
            {
                long tsStart = Stopwatch.GetTimestamp();
                _writer.FlushToDisk();
                double fsyncMs = Stopwatch.GetElapsedTime(tsStart).TotalMilliseconds;
                Metrics.RecordFsyncDuration(fsyncMs);
                Metrics.RecordBatchSize(_recordsSinceLastFlush);
                _recordsSinceLastFlush = 0;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }

        foreach (ReadOnlyMemory<byte> _ in entries)
        {
            Metrics.IncrementRecordsAppended();
        }

        long totalBytes = 0;
        foreach (ReadOnlyMemory<byte> e in entries)
        {
            totalBytes += e.Length;
        }

        Metrics.AddBytesAppended(totalBytes);
        return seqNo;
    }

    /// <summary>
    /// Flushes the active segment to disk, writes a checkpoint, and flushes all
    /// registered consumer cursors.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">The pipeline is not in the <see cref="PipelineState.Running"/> state.</exception>
    public void Flush()
    {
        ThrowIfNotRunning();
        FlushInternal();
    }

    /// <summary>
    /// Stops the pipeline: flushes, seals the active segment, disposes all consumers,
    /// and transitions to <see cref="PipelineState.Stopped"/>.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="AggregateException">
    /// One or more errors occurred during flush, drain, or GC-task shutdown. All resources
    /// have been released before this exception is thrown. The pipeline transitions to
    /// <see cref="PipelineState.Error"/> — it cannot be restarted. Construct a new
    /// <see cref="Pipeline"/> instance to recover.
    /// </exception>
    public void Stop()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (State == PipelineState.Draining)
        {
            throw new InvalidOperationException(
                "Pipeline.Stop() was called while already in Draining state. " +
                "This indicates a previous Stop() call threw. Call Dispose() to release all resources.");
        }

        if (State != PipelineState.Running)
        {
            return;
        }

        State = PipelineState.Draining;

        List<Exception> errors = new();

        // Stop the Batched-mode flush timer before calling FlushInternal() so the
        // timer cannot call FlushInternal() concurrently on its background thread.
        // ExecuteCleanupSequence() re-cancels (idempotent) and re-waits (returns
        // immediately — task already complete by this point).
        _flushTimerCts?.Cancel();
        try { _flushTimerTask.Wait(5_000); }
        catch (AggregateException) { /* ignore — timer loop does not fault */ }

        try
        {
            // Flush the writer buffer before cancelling reader loops so that any
            // records appended since the last Flush() are on disk and visible to
            // the reader drain pass.
            try
            {
                FlushInternal();
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }

            // ── Push-mode drain ───────────────────────────────────────────────
            // Cancel the GC task before starting the drain so it cannot delete
            // segments that reader loops are about to open. ExecuteCleanupSequence
            // waits for the task and disposes the CTS as usual.
            _gcCts?.Cancel();

            // 1. Cancel reader loops so they drain to tail then exit.
            foreach (ConsumerState cs in _consumers.Values)
            {
                cs.ReaderCts?.Cancel();
            }

            // 2. Await reader loops — they exit via OperationCanceledException.
            Task[] readerTasks = _consumers.Values
                .Select(cs => cs.ReaderTask)
                .Where(t => t != Task.CompletedTask)
                .ToArray();

            if (readerTasks.Length > 0)
            {
                int waitMs = _config.DrainTimeoutMs <= 0 ? Timeout.Infinite : _config.DrainTimeoutMs;
                bool drained;
                try
                {
                    drained = Task.WaitAll(readerTasks, waitMs);
                }
                catch (AggregateException ae)
                    when (ae.InnerExceptions.All(e => e is OperationCanceledException))
                {
                    // Expected exit path for reader loops — ignore.
                    drained = true;
                }
                catch (AggregateException ae)
                {
                    // Non-OCE reader task failure — collect and continue.
                    foreach (Exception inner in ae.InnerExceptions)
                    {
                        errors.Add(inner);
                    }
                    drained = true;
                }

                if (!drained)
                {
                    List<string> timedOut;
                    lock (_observabilityLock)
                    {
                        timedOut = _consumers.Values
                            .Where(cs => cs.ReaderTask != Task.CompletedTask && !cs.ReaderTask.IsCompleted)
                            .Select(cs => cs.ConsumerName)
                            .ToList();
                    }
                    errors.Add(new PeclDrainTimeoutException(_config.DrainTimeoutMs, timedOut));
                }
            }

            // Collect drain-phase errors from reader loops.
            lock (_observabilityLock)
            {
                foreach (ConsumerState cs in _consumers.Values)
                {
                    if (cs.DrainError is { } drainErr)
                    {
                        errors.Add(new PeclDrainException(cs.ConsumerName, drainErr));
                    }
                }
            }

            // 3. Complete sink lanes — signals sink tasks that no more records are coming.
            lock (_observabilityLock)
            {
                foreach (ConsumerState cs in _consumers.Values)
                {
                    foreach (SinkSlot slot in cs.SinkSlots)
                    {
                        slot.Lane.Complete();
                    }
                }
            }

            // 4. Await sink tasks — they drain remaining records from lanes and exit.
            Task[] sinkTasks;
            lock (_observabilityLock)
            {
                sinkTasks = _consumers.Values
                    .SelectMany(cs => cs.SinkSlots.Select(s => s.Task))
                    .Where(t => t != Task.CompletedTask)
                    .ToArray();
            }

            if (sinkTasks.Length > 0)
            {
                try
                {
                    Task.WaitAll(sinkTasks);
                }
                catch (AggregateException ae)
                {
                    // Sink task failures (e.g. ISink.WriteAsync threw) — collect and continue.
                    foreach (Exception inner in ae.InnerExceptions)
                    {
                        errors.Add(inner);
                    }
                }
            }

            // 5. Advance cursor to last routed position so it is persisted on Dispose.
            // Snapshot under lock; Flusher.Advance involves I/O and must run outside.
            ConsumerState[] consumerSnapshot;
            lock (_observabilityLock)
            {
                consumerSnapshot = [.. _consumers.Values];
            }
            foreach (ConsumerState cs in consumerSnapshot)
            {
                if (cs.LastRoutedSeqNo.HasValue)
                {
                    try
                    {
                        cs.Flusher.Advance(
                            cs.CurrentSegmentId,
                            cs.CurrentOffset,
                            cs.LastRoutedSeqNo.Value);
                    }
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                    }
                }
            }

            // 6. Final flush — persists cursors and writes an up-to-date checkpoint.
            try
            {
                FlushInternal();
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        }
        finally
        {
            ExecuteCleanupSequence(errors);
        }

        if (errors.Count > 0)
        {
            throw new AggregateException(
                "Pipeline.Stop() encountered one or more errors during teardown.", errors);
        }
    }

    /// <summary>
    /// Immediately tears down the pipeline without waiting for consumer reader loops
    /// or sinks to drain. Reader cancellation tokens are cancelled and cleanup
    /// (GC task, writer, consumers) proceeds without any drain wait.
    /// </summary>
    /// <remarks>
    /// The primary use case is escaping a <see cref="Stop"/> call that has already timed out
    /// (pipeline is in <see cref="PipelineState.Draining"/> state) or bypassing drain entirely
    /// when fast shutdown is required.
    /// <para>Callable from <see cref="PipelineState.Running"/> or
    /// <see cref="PipelineState.Draining"/>. A no-op if already <see cref="PipelineState.Stopped"/>
    /// or <see cref="PipelineState.Error"/>.</para>
    /// <para>Does not throw <see cref="AggregateException"/> for drain errors (there is no drain).
    /// May throw for I/O errors during cleanup, wrapped in <see cref="AggregateException"/>.</para>
    /// <para>Consumer cursors are persisted at their last flush position — not at the end of
    /// the log, because no drain pass was performed.</para>
    /// <para><strong>Thread safety:</strong> safe to call from a different thread while
    /// <see cref="Stop"/> is blocking in its drain wait (the intended escape-hatch scenario).
    /// All <c>_consumers</c> accesses in both methods are serialized under
    /// <c>_observabilityLock</c>: <see cref="ExecuteCleanupSequence"/> snapshots and clears
    /// the map atomically; the second caller iterates an empty snapshot.</para>
    /// <para>If <see cref="PipelineState"/> is already <see cref="PipelineState.Draining"/>
    /// when <see cref="ForceStop"/> is called, re-assigning <c>Draining</c> is intentional and
    /// a no-op — cleanup proceeds regardless of which call originally set the state.</para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="AggregateException">One or more I/O errors occurred during cleanup.</exception>
    public void ForceStop()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (State != PipelineState.Running && State != PipelineState.Draining)
        {
            return;
        }

        // Re-assigning Draining is a no-op when Stop() already set it — intentional.
        // Cleanup proceeds regardless of which call originally set the state.
        State = PipelineState.Draining;

        List<Exception> errors = new();

        // Cancel all reader loops immediately — no drain wait.
        foreach (ConsumerState cs in _consumers.Values)
        {
            cs.ReaderCts?.Cancel();
        }

        // Proceed directly to cleanup without waiting for readers or sinks.
        ExecuteCleanupSequence(errors);

        if (errors.Count > 0)
        {
            throw new AggregateException(
                "Pipeline.ForceStop() encountered one or more errors during cleanup.", errors);
        }
    }

    /// <summary>
    /// Runs the unconditional pipeline cleanup sequence: cancels and awaits the GC task,
    /// seals and disposes the writer, disposes all consumers, clears the consumer map,
    /// and sets <see cref="State"/> to <see cref="PipelineState.Stopped"/> or
    /// <see cref="PipelineState.Error"/>.
    /// </summary>
    /// <remarks>
    /// Extracted from <see cref="Stop"/>'s <c>finally</c> block so that <see cref="ForceStop"/>
    /// can share the same cleanup path. <c>_consumers</c> is snapshotted and cleared atomically
    /// inside <c>_observabilityLock</c> so that a concurrent <see cref="ForceStop"/> +
    /// <see cref="Stop"/> invocation (the escape-hatch scenario) does not produce a
    /// <see cref="InvalidOperationException"/> from concurrent Dictionary modification.
    /// The second caller that reaches this method iterates an empty snapshot.
    /// </remarks>
    /// <param name="errors">Accumulator for any errors encountered during cleanup.</param>
    private void ExecuteCleanupSequence(List<Exception> errors)
    {
        // Cancel and await the Batched-mode flush timer before disposing the writer.
        _flushTimerCts?.Cancel();
        try { _flushTimerTask.Wait(5_000); }
        catch (AggregateException) { /* ignore — timer loop does not fault */ }
        _flushTimerCts?.Dispose();
        _flushTimerCts = null;
        _flushTimerTask = Task.CompletedTask;

        // Cancel and await GC task before disposing the writer it may read.
        // Use a timeout so a hung I/O operation cannot block cleanup forever (R04-H4).
        // Catch AggregateException so a faulted GC task surfaces through the caller's
        // AggregateException rather than abandoning cleanup (R04-NHA).
        _gcCts?.Cancel();
        int gcTimeout = _config.GcStopTimeoutMs;
        try
        {
            if (!_gcTask.Wait(gcTimeout))
            {
                // GC task did not stop within the timeout (likely blocked on I/O).
                // Proceed with cleanup; the task will terminate when I/O unblocks.
            }
        }
        catch (AggregateException gcEx)
        {
            // GC task faulted. Surface fault(s) through the caller's AggregateException.
            foreach (Exception inner in gcEx.InnerExceptions)
            {
                errors.Add(inner);
            }
        }

        _gcCts?.Dispose();
        _gcCts = null;
        _gcTask = Task.CompletedTask;

        // Seal and dispose the segment writer.
        try { _writer?.Seal(); }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException) { /* best-effort */ }

        try { _writer?.Dispose(); }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException) { /* best-effort */ }
        _writer = null;

        // Snapshot and clear the consumer map atomically so that a concurrent ForceStop()+Stop()
        // call iterating _consumers on the other thread sees an empty map rather than racing
        // on a non-thread-safe Dictionary. Dispose runs on the local snapshot outside the lock
        // (per-consumer resilient — one failure does not abort the rest).
        ConsumerState[] consumersFinal;
        lock (_observabilityLock)
        {
            consumersFinal = [.. _consumers.Values];
            _consumers.Clear();
        }

        foreach (ConsumerState cs in consumersFinal)
        {
            try
            {
                cs.Dispose();
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        }

        State = errors.Count > 0 ? PipelineState.Error : PipelineState.Stopped;
    }

    /// <summary>
    /// Registers a consumer so it can read records via <see cref="ReadNext"/>.
    /// Consumers registered before <see cref="Start"/> have their cursor positions recovered
    /// from disk. Consumers registered after <see cref="Start"/> begin at position (0, 0).
    /// </summary>
    /// <param name="consumerName">The unique name of the consumer. Must not be <see langword="null"/> or empty.</param>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">A consumer with the same name is already registered.</exception>
    public void RegisterConsumer(string consumerName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(consumerName);

        if (State != PipelineState.Stopped)
        {
            throw new InvalidOperationException(
                $"Cannot register consumer '{consumerName}': pipeline is in {State} state. " +
                "Consumers must be registered before Start() is called.");
        }

        if (_consumers.ContainsKey(consumerName))
        {
            throw new InvalidOperationException($"Consumer '{consumerName}' is already registered.");
        }

        var flusher = new CursorFlusher(
            _cursorsDir,
            consumerName,
            _config.CursorFlushRecordThreshold,
            _config.CursorFlushInterval);

        _consumers[consumerName] = new ConsumerState(consumerName, flusher, 0u, 0L);
        _registeredConsumerNames.Add(consumerName);
    }

    /// <summary>
    /// Registers a sink for the named consumer. The pipeline will automatically deliver
    /// records to this sink when <see cref="Start"/> is called.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Must be called before <see cref="Start"/>; the consumer must already be registered
    /// via <see cref="RegisterConsumer"/>. A consumer with at least one sink is in
    /// <em>push mode</em>: records are delivered automatically and <see cref="ReadNext"/>
    /// is unavailable for that consumer.
    /// </para>
    /// </remarks>
    /// <param name="consumerName">The name of the pre-registered consumer.</param>
    /// <param name="sinkName">
    /// A unique name for this sink within the consumer. Used for lane identification and
    /// diagnostics.
    /// </param>
    /// <param name="sink">The sink implementation. Must not be <see langword="null"/>.</param>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The pipeline is not in the <see cref="PipelineState.Stopped"/> state, or
    /// no consumer with <paramref name="consumerName"/> is registered, or a sink named
    /// <paramref name="sinkName"/> is already registered for that consumer.
    /// </exception>
    public void AddSink(string consumerName, string sinkName, ISink sink)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(consumerName);
        ArgumentException.ThrowIfNullOrEmpty(sinkName);
        ArgumentNullException.ThrowIfNull(sink);

        if (State != PipelineState.Stopped)
        {
            throw new InvalidOperationException(
                $"Sinks must be added before Start() (current state: {State}).");
        }

        if (!_consumers.TryGetValue(consumerName, out ConsumerState? cs))
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' is not registered. Call RegisterConsumer first.");
        }

        if (cs.SinkSlots.Any(s => s.SinkName == sinkName))
        {
            throw new InvalidOperationException(
                $"Sink '{sinkName}' is already registered for consumer '{consumerName}'.");
        }

        if (cs.Router is not null)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' already uses a non-broadcast routing strategy. " +
                "Use the same AddSink overload consistently.");
        }

        string spillPath = Path.Combine(_spillDir, $"{consumerName}-{sinkName}.spill");
        var lane = new SinkLane(sinkName, _config.SinkLaneCapacity, spillPath);
        var slot = new SinkSlot(sinkName, sink, lane);
        cs.SinkSlots.Add(slot);
        cs.AddLane(lane);
    }

    /// <summary>
    /// Registers a sink for the named consumer, routing only records whose
    /// <see cref="RecordHeader.ContentType"/> matches <paramref name="contentTypeFilter"/>.
    /// Records that do not match any registered content-type filter are dropped and the
    /// pipeline's drop counter is incremented with sink name <c>"*"</c>.
    /// </summary>
    /// <remarks>
    /// All <c>AddSink</c> calls for a given consumer must use the same overload
    /// (broadcast, content-type, or hash). Mixing raises <see cref="InvalidOperationException"/>.
    /// </remarks>
    /// <param name="consumerName">The name passed to <see cref="RegisterConsumer"/>.</param>
    /// <param name="sinkName">A name unique within this consumer.</param>
    /// <param name="sink">The sink implementation to receive filtered records.</param>
    /// <param name="contentTypeFilter">
    /// The <see cref="ContentType"/> value a record must carry to be forwarded to this sink.
    /// </param>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The pipeline is not in the <see cref="PipelineState.Stopped"/> state,
    /// no consumer with <paramref name="consumerName"/> is registered,
    /// a sink named <paramref name="sinkName"/> is already registered for that consumer,
    /// or the consumer already uses a different routing strategy.
    /// </exception>
    public void AddSink(string consumerName, string sinkName, ISink sink, ContentType contentTypeFilter)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(consumerName);
        ArgumentException.ThrowIfNullOrEmpty(sinkName);
        ArgumentNullException.ThrowIfNull(sink);

        if (State != PipelineState.Stopped)
        {
            throw new InvalidOperationException(
                $"Sinks must be added before Start() (current state: {State}).");
        }

        if (!_consumers.TryGetValue(consumerName, out ConsumerState? cs))
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' is not registered. Call RegisterConsumer first.");
        }

        if (cs.SinkSlots.Any(s => s.SinkName == sinkName))
        {
            throw new InvalidOperationException(
                $"Sink '{sinkName}' is already registered for consumer '{consumerName}'.");
        }

        if (cs.Router is null && cs.SinkSlots.Count > 0)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' already uses a broadcast routing strategy. " +
                "Use the same AddSink overload consistently.");
        }

        if (cs.Router is null)
        {
            cs.Router = new ContentTypeRouter(
                _config.BackpressurePolicy,
                sn => Metrics.IncrementSinkDropped(sn));
        }
        else if (cs.Router is not ContentTypeRouter)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' already uses a different routing strategy. " +
                "Use the same AddSink overload consistently.");
        }

        string spillPath = Path.Combine(_spillDir, $"{consumerName}-{sinkName}.spill");
        var lane = new SinkLane(sinkName, _config.SinkLaneCapacity, spillPath);
        var slot = new SinkSlot(sinkName, sink, lane);
        cs.SinkSlots.Add(slot);
        cs.AddLane(lane);
        ((ContentTypeRouter)cs.Router).RegisterFilter(sinkName, contentTypeFilter);
    }

    /// <summary>
    /// Registers a sink for the named consumer using hash-based routing. Records are
    /// distributed across sinks by <c>abs(keySelector(record)) % sinkCount</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The <paramref name="keySelector"/> is captured from the first call for this consumer;
    /// subsequent calls for the same consumer must use this overload but the
    /// <paramref name="keySelector"/> parameter is ignored — the router retains the original selector.
    /// </para>
    /// <para>
    /// All <c>AddSink</c> calls for a given consumer must use the same overload.
    /// Mixing raises <see cref="InvalidOperationException"/>.
    /// </para>
    /// </remarks>
    /// <param name="consumerName">The name passed to <see cref="RegisterConsumer"/>.</param>
    /// <param name="sinkName">A name unique within this consumer.</param>
    /// <param name="sink">The sink implementation to receive routed records.</param>
    /// <param name="keySelector">
    /// A function that extracts an integer routing key from a record. The key is reduced
    /// modulo the number of registered sinks to select the target lane.
    /// </param>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// The pipeline is not in the <see cref="PipelineState.Stopped"/> state,
    /// no consumer with <paramref name="consumerName"/> is registered,
    /// a sink named <paramref name="sinkName"/> is already registered for that consumer,
    /// or the consumer already uses a different routing strategy.
    /// </exception>
    public void AddSink(string consumerName, string sinkName, ISink sink, Func<LogRecord, int> keySelector)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(consumerName);
        ArgumentException.ThrowIfNullOrEmpty(sinkName);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(keySelector);

        if (State != PipelineState.Stopped)
        {
            throw new InvalidOperationException(
                $"Sinks must be added before Start() (current state: {State}).");
        }

        if (!_consumers.TryGetValue(consumerName, out ConsumerState? cs))
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' is not registered. Call RegisterConsumer first.");
        }

        if (cs.SinkSlots.Any(s => s.SinkName == sinkName))
        {
            throw new InvalidOperationException(
                $"Sink '{sinkName}' is already registered for consumer '{consumerName}'.");
        }

        if (cs.Router is null && cs.SinkSlots.Count > 0)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' already uses a broadcast routing strategy. " +
                "Use the same AddSink overload consistently.");
        }

        if (cs.Router is null)
        {
            cs.Router = new HashRouter(
                keySelector,
                _config.BackpressurePolicy,
                sn => Metrics.IncrementSinkDropped(sn));
        }
        else if (cs.Router is not HashRouter)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' already uses a different routing strategy. " +
                "Use the same AddSink overload consistently.");
        }

        string spillPath = Path.Combine(_spillDir, $"{consumerName}-{sinkName}.spill");
        var lane = new SinkLane(sinkName, _config.SinkLaneCapacity, spillPath);
        var slot = new SinkSlot(sinkName, sink, lane);
        cs.SinkSlots.Add(slot);
        cs.AddLane(lane);
    }

    /// <summary>
    /// Moves the named consumer's cursor to the first record with
    /// <see cref="RecordHeader.SeqNo"/> &gt;= <paramref name="seqNo"/>.
    /// The cursor is persisted to disk immediately.
    /// </summary>
    /// <remarks>
    /// The pipeline must be in the <see cref="PipelineState.Stopped"/> state.
    /// Call <see cref="Stop"/> or <see cref="ForceStop"/> before seeking.
    /// On the next <see cref="Start"/>, the consumer will resume from the seeked position.
    /// </remarks>
    /// <param name="consumerName">The name passed to <see cref="RegisterConsumer"/>.</param>
    /// <param name="seqNo">Target sequence number (inclusive lower bound).</param>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The pipeline is not in the <see cref="PipelineState.Stopped"/> state,
    /// or <paramref name="consumerName"/> is not registered.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="seqNo"/> is beyond the log tail, or the log is empty.
    /// </exception>
    /// <exception cref="PeclSeekException">
    /// <paramref name="seqNo"/> falls within a segment that has been deleted by retention GC.
    /// <see cref="PeclSeekException.EarliestAvailableSeqNo"/> contains the floor value.
    /// </exception>
    public void SeekConsumer(string consumerName, ulong seqNo)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(consumerName);

        if (State != PipelineState.Stopped)
        {
            throw new InvalidOperationException(
                $"SeekConsumer is only valid when the pipeline is Stopped (current state: {State}).");
        }

        if (!_registeredConsumerNames.Contains(consumerName))
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' is not registered.");
        }

        List<uint> segIds = EnumerateSegmentIds();

        if (segIds.Count == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(seqNo), "The log is empty; no records to seek to.");
        }

        // Guard: seqNo must not exceed the last record in the log.
        ulong tailSeqNo = ReadLastSeqNoFromSegment(segIds[segIds.Count - 1]);
        if (seqNo > tailSeqNo)
        {
            throw new ArgumentOutOfRangeException(
                nameof(seqNo),
                seqNo,
                $"SeqNo {seqNo} is beyond the log tail (last written seqNo: {tailSeqNo}).");
        }

        // Find the segment whose first record has the largest SeqNo <= seqNo.
        // That segment contains the first record with SeqNo >= seqNo.
        uint? targetSegId = null;
        foreach (uint segId in segIds)
        {
            ulong firstSeqNo = ReadFirstSeqNoFromSegment(segId);
            if (firstSeqNo <= seqNo)
            {
                targetSegId = segId;
            }
            else
            {
                break;
            }
        }

        if (targetSegId is null)
        {
            // seqNo is below the first record of the oldest remaining segment.
            ulong earliestSeqNo = ReadFirstSeqNoFromSegment(segIds[0]);
            throw new PeclSeekException(
                earliestSeqNo,
                $"SeqNo {seqNo} is below the retention floor; earliest available SeqNo is {earliestSeqNo}.");
        }

        long offset = ScanSegmentForSeqNo(targetSegId.Value, seqNo);
        // cursor.LastReadSeqNo is set to the requested seqNo, not the actual seqNo at the
        // byte offset (which may be >= seqNo if no exact match exists). Lag metrics will be
        // slightly inaccurate until the first ReadNext call updates the cursor to the real seqNo.
        var cursor = new CursorData(consumerName, targetSegId.Value, offset, seqNo);
        CursorWriter.Write(_cursorsDir, cursor);

        // Update in-memory state if the consumer is still live in _consumers
        // (populated when the pipeline is Running; cleared by Stop/ExecuteCleanupSequence).
        // RecoverInternal always re-reads from disk on the next Start(), so this is a
        // best-effort sync — correctness is guaranteed by the disk write above.
        if (_consumers.TryGetValue(consumerName, out ConsumerState? cs))
        {
            cs.CurrentSegmentId = targetSegId.Value;
            cs.CurrentOffset = offset;
        }
    }

    /// <summary>
    /// Resets the named consumer's cursor to the earliest available record in the log.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The pipeline must be in the <see cref="PipelineState.Stopped"/> state.
    /// When the log is empty, a zeroed cursor is written and the consumer will start
    /// from the beginning on the next <see cref="Start"/>.
    /// </para>
    /// <para>
    /// When old segments have been deleted by retention GC, this method moves the cursor
    /// to the first record of the oldest remaining segment (not necessarily SeqNo 0).
    /// </para>
    /// </remarks>
    /// <param name="consumerName">The name passed to <see cref="RegisterConsumer"/>.</param>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The pipeline is not in the <see cref="PipelineState.Stopped"/> state,
    /// or <paramref name="consumerName"/> is not registered.
    /// </exception>
    public void ResetConsumer(string consumerName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(consumerName);

        if (State != PipelineState.Stopped)
        {
            throw new InvalidOperationException(
                $"ResetConsumer is only valid when the pipeline is Stopped (current state: {State}).");
        }

        if (!_registeredConsumerNames.Contains(consumerName))
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' is not registered.");
        }

        List<uint> segIds = EnumerateSegmentIds();

        if (segIds.Count == 0)
        {
            // Empty log — write a zeroed cursor; Start() will begin from the first record.
            CursorWriter.Write(_cursorsDir, new CursorData(consumerName, 0u, 0L, 0UL));
            if (_consumers.TryGetValue(consumerName, out ConsumerState? emptyCs))
            {
                emptyCs.CurrentSegmentId = 0u;
                emptyCs.CurrentOffset = 0L;
            }

            return;
        }

        // Point to the very start of the oldest available segment.
        uint firstSegId = segIds[0];
        ulong firstSeqNo = ReadFirstSeqNoFromSegment(firstSegId);
        CursorWriter.Write(_cursorsDir, new CursorData(consumerName, firstSegId, 0L, firstSeqNo));
        if (_consumers.TryGetValue(consumerName, out ConsumerState? resetCs))
        {
            resetCs.CurrentSegmentId = firstSegId;
            resetCs.CurrentOffset = 0L;
        }
    }

    /// <summary>
    /// Reads the next record for the named consumer.
    /// Returns <see cref="PeclErrorCode.EndOfLog"/> when the consumer has caught up to the tail.
    /// </summary>
    /// <param name="consumerName">The name of the registered consumer.</param>
    /// <returns>
    /// <see cref="Result{T, TError}.Ok"/> with the next <see cref="RecordReadResult"/> on success;
    /// <see cref="Result{T, TError}.Fail"/> with a <see cref="PeclError"/> otherwise.
    /// </returns>
    /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The pipeline is not in the <see cref="PipelineState.Running"/> state, or
    /// no consumer with <paramref name="consumerName"/> is registered.
    /// </exception>
    public Result<RecordReadResult, PeclError> ReadNext(string consumerName)
    {
        ThrowIfNotRunning();

        if (!_consumers.TryGetValue(consumerName, out ConsumerState? cs))
        {
            throw new InvalidOperationException($"Consumer '{consumerName}' is not registered.");
        }

        if (cs.IsPushMode)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' is in push mode (sinks are registered). " +
                "Records are delivered automatically via the registered sinks; " +
                "ReadNext is not available for push-mode consumers.");
        }

        Result<RecordReadResult, PeclError> result = ReadNextCore(cs);

        if (result.IsSuccess)
        {
            cs.Flusher.Advance(cs.CurrentSegmentId, cs.CurrentOffset, result.Value.Header.SeqNo);
            cs.LastReadSeqNo = result.Value.Header.SeqNo;
            Metrics.IncrementConsumerReadRate();
        }

        return result;
    }

    /// <summary>
    /// Blocks until the named consumer's drain error has been recorded by the reader loop,
    /// or until <paramref name="timeoutMs"/> elapses. For use in tests only.
    /// </summary>
    /// <param name="consumerName">The consumer to wait for.</param>
    /// <param name="timeoutMs">Maximum wait in milliseconds.</param>
    /// <param name="ct">Cancellation token (xUnit: <c>TestContext.Current.CancellationToken</c>).</param>
    /// <returns><see langword="true"/> if the drain error was set within the timeout; otherwise <see langword="false"/>.</returns>
    /// <exception cref="InvalidOperationException">
    /// Consumer not found in the active consumer map. Thrown when the consumer was never
    /// registered, or when the pipeline has already been stopped (<see cref="Stop"/> and
    /// <see cref="ForceStop"/> clear the consumer map on completion). Call this method only
    /// while the pipeline is in <see cref="PipelineState.Running"/> state.
    /// </exception>
    internal bool WaitForConsumerDrainError(string consumerName, int timeoutMs, CancellationToken ct)
    {
        if (!_consumers.TryGetValue(consumerName, out ConsumerState? cs))
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerName}' is not registered.");
        }

        return cs.DrainErrorSignal.Wait(timeoutMs, ct);
    }

    /// <summary>
    /// Reads the next record for the given consumer state, advancing the physical read
    /// position (<see cref="ConsumerState.CurrentSegmentId"/> and
    /// <see cref="ConsumerState.CurrentOffset"/>). Does <em>not</em> call
    /// <see cref="CursorFlusher.Advance"/> — caller is responsible for cursor persistence.
    /// </summary>
    private Result<RecordReadResult, PeclError> ReadNextCore(ConsumerState cs)
    {
        if (IsAtTail(cs))
        {
            return Result<RecordReadResult, PeclError>.Fail(PeclError.EndOfLog(cs.ConsumerName));
        }

        EnsureReaderOpen(cs);
        Result<RecordReadResult, PeclError> result = cs.Reader!.ReadNext();

        if (result.IsFailure && result.Error.Code == PeclErrorCode.TruncatedHeader)
        {
            cs.Reader.Dispose();
            cs.Reader = null;

            // TruncatedHeader on the ACTIVE segment means the reader hit the
            // writer's unflushed zone: BytesWritten (updated atomically by Append)
            // advanced ahead of the OS-visible file size. Advancing CurrentSegmentId
            // here would produce a FileNotFoundException on the non-existent next
            // segment. Return EndOfLog instead — the reader will retry on the next
            // poll cycle after the data has been flushed to disk.
            if (cs.CurrentSegmentId == _writer!.SegmentId)
            {
                return Result<RecordReadResult, PeclError>.Fail(PeclError.EndOfLog(cs.ConsumerName));
            }

            // Clean end-of-sealed-segment — advance to the next segment.
            cs.CurrentSegmentId++;
            cs.CurrentOffset = 0;

            if (IsAtTail(cs))
            {
                return Result<RecordReadResult, PeclError>.Fail(PeclError.EndOfLog(cs.ConsumerName));
            }

            EnsureReaderOpen(cs);
            result = cs.Reader!.ReadNext();

            if (result.IsFailure)
            {
                return result;
            }
        }
        else if (result.IsFailure)
        {
            return result;
        }

        cs.CurrentOffset = cs.Reader!.Position;
        return result;
    }

    /// <summary>
    /// Disposes the pipeline. If the pipeline is running it is stopped first.
    /// This method is idempotent.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        if (State == PipelineState.Running || State == PipelineState.Draining)
        {
            try
            {
                Stop();
            }
            catch (IOException) { }
            catch (InvalidOperationException) { }
            catch (AggregateException) { }

            // Belt-and-suspenders: Stop() guarantees cleanup in its finally block,
            // so these are no-ops after a normal Stop(). They only fire if Stop() was
            // entered from Draining state (throws InvalidOperationException caught above),
            // ensuring no file handles are left open in that edge case.

            // If Stop() threw or state was already Draining, the GC task may still be live.
            // Cancel and wait (best-effort) before nulling _writer so the GC task cannot
            // dereference a null _writer (R04-C1, R04-C2).
            _gcCts?.Cancel();
            try { _gcTask.Wait(_config.GcStopTimeoutMs); } catch (Exception) { /* best-effort */ }
            _gcCts?.Dispose();
            _gcCts = null;
            _gcTask = Task.CompletedTask;

            try { _writer?.Dispose(); }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException) { /* best-effort */ }
            _writer = null;

            foreach (ConsumerState cs in _consumers.Values)
            {
                try { cs.Dispose(); }
                catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException) { /* best-effort */ }
            }

            _consumers.Clear();
        }

        _disposed = true;
        Metrics.Dispose();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void EnsureDirectories()
    {
        Directory.CreateDirectory(_segmentsDir);
        Directory.CreateDirectory(_cursorsDir);
        Directory.CreateDirectory(_spillDir);
    }

    /// <summary>
    /// Deletes spill files in <c>{root}/spill/</c> that have no matching registered consumer+sink.
    /// Called at the end of <see cref="Start"/> setup so orphaned files from deregistered
    /// sinks do not accumulate on disk indefinitely.
    /// </summary>
    private void DeleteOrphanedSpillFiles()
    {
        // Build the set of valid {consumer}-{sink}.spill file names from registered consumers.
        var validNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (ConsumerState cs in _consumers.Values)
        {
            foreach (SinkSlot slot in cs.SinkSlots)
            {
                validNames.Add($"{cs.ConsumerName}-{slot.SinkName}.spill");
            }
        }

        foreach (string filePath in Directory.EnumerateFiles(_spillDir, "*.spill"))
        {
            string fileName = Path.GetFileName(filePath);
            if (!validNames.Contains(fileName))
            {
                File.Delete(filePath);
            }
        }
    }

    private void RecoverInternal()
    {
        long recoveryStart = Stopwatch.GetTimestamp();
        uint lastSegId = 0u;
        long lastOffset = 0L;
        ulong lastSeqNo = 0UL;
        uint scanFromSegId = 0u;
        long recoveryTruncatedBytes = 0L;

        Result<CheckpointData, PeclError> chkResult = CheckpointReader.Read(_config.RootDirectory);
        if (chkResult.IsSuccess)
        {
            CheckpointData chk = chkResult.Value;
            scanFromSegId = chk.LastSegmentId;
            lastSeqNo = chk.LastSeqNo;
            lastSegId = chk.LastSegmentId;
            lastOffset = chk.LastOffset;
        }

        List<uint> segIds = EnumerateSegmentIds();
        bool foundAnyRecord = false;

        foreach (uint segId in segIds)
        {
            if (segId < scanFromSegId)
            {
                continue;
            }

            // Scan the segment. We record the truncation position (if any) outside the
            // reader scope so that the reader is disposed before we attempt to write to
            // the file — on Windows a writer cannot open a file held by another handle
            // even when the existing handle is read-only.
            long truncateAt = -1L;

            // For the checkpoint segment, start at LastOffset to skip records already known
            // to be valid. For all earlier segments start at 0 (fully written).
            long scanStart = (segId == scanFromSegId && chkResult.IsSuccess) ? lastOffset : 0L;
            using (var reader = new SegmentReader(_segmentsDir, segId, startOffset: scanStart))
            {
                while (true)
                {
                    long posBeforeRead = reader.Position;
                    Result<RecordReadResult, PeclError> result = reader.ReadNext();

                    if (result.IsFailure)
                    {
                        if (result.Error.Code == PeclErrorCode.TruncatedHeader)
                        {
                            long fileSize = new FileInfo(
                                SegmentNaming.GetFilePath(_segmentsDir, segId)).Length;

                            if (posBeforeRead >= fileSize)
                            {
                                // Normal clean end of segment — nothing to truncate.
                                break;
                            }

                            // Partial header bytes at mid-file — schedule truncation.
                            truncateAt = posBeforeRead;
                            break;
                        }

                        // Any other failure is corruption — schedule truncation.
                        truncateAt = posBeforeRead;
                        break;
                    }

                    lastSegId = segId;
                    lastOffset = reader.Position;
                    lastSeqNo = result.Value.Header.SeqNo;
                    foundAnyRecord = true;
                }
            }

            // Reader is now closed; safe to truncate the file.
            if (truncateAt >= 0L)
            {
                string segPath = SegmentNaming.GetFilePath(_segmentsDir, segId);
                long segFileSize = new FileInfo(segPath).Length;
                recoveryTruncatedBytes = segFileSize - truncateAt;
                TruncateSegment(segId, truncateAt);
                break;
            }
        }

        Metrics.SetRecoveryTruncatedBytes(recoveryTruncatedBytes);
        Metrics.IncrementRecoveryCount();
        Metrics.SetRecoveryDurationMs((long)Stopwatch.GetElapsedTime(recoveryStart).TotalMilliseconds);
        Metrics.SetSegmentBytes(ComputeSegmentBytes(segIds));

        // Determine the starting sequence number for new appends.
        // If the scan found records, use the scanned seqno. If not but a valid
        // checkpoint exists, the checkpoint's LastSeqNo is authoritative — the
        // scan may have started at the checkpoint offset and found nothing new
        // (e.g. clean stop, or crash with a file truncated back to the flush
        // boundary). Only default to 0 when there is neither a checkpoint nor
        // any scanned records.
        _nextSeqNo = foundAnyRecord
            ? lastSeqNo + 1
            : chkResult.IsSuccess ? chkResult.Value.LastSeqNo + 1 : 0UL;

        // Open the segment writer, resuming at the tail position.
        if (segIds.Count == 0)
        {
            _writer = new SegmentWriter(_segmentsDir, 0u, _config.MaxSegmentSize, _config.CompressionAlgorithm);
        }
        else
        {
            _writer = new SegmentWriter(_segmentsDir, lastSegId, _config.MaxSegmentSize, _config.CompressionAlgorithm);
            // SegmentWriter seeks to end on open, which is correct whether truncated or not.
        }

        // Recover consumer cursor positions.
        foreach (ConsumerState cs in _consumers.Values)
        {
            Result<CursorData, PeclError> curResult = CursorReader.Read(_cursorsDir, cs.ConsumerName);
            if (curResult.IsSuccess)
            {
                CursorData cur = curResult.Value;

                // Clamp to tail — cursor must not point past the last written position.
                bool pastTail = cur.SegmentId > lastSegId
                    || (cur.SegmentId == lastSegId && cur.Offset > lastOffset);

                if (pastTail)
                {
                    cs.CurrentSegmentId = lastSegId;
                    cs.CurrentOffset = lastOffset;
                }
                else
                {
                    cs.CurrentSegmentId = cur.SegmentId;
                    cs.CurrentOffset = cur.Offset;
                }
            }
            else if (_config.MissingCursorPolicy == MissingCursorPolicy.FromTail)
            {
                // Position at the tail so the consumer only sees records appended after this Start().
                // On an empty log lastSegId and lastOffset are both 0 — same as FromBeginning.
                cs.CurrentSegmentId = lastSegId;
                cs.CurrentOffset = lastOffset;
            }
            else // MissingCursorPolicy.FromBeginning (default)
            {
                cs.CurrentSegmentId = 0u;
                cs.CurrentOffset = 0L;
            }
        }
    }

    /// <remarks>
    /// During segment rollover, this method is called after the new writer is opened.
    /// <paramref name="lastSegId"/> is the just-sealed segment; <paramref name="activeSegId"/>
    /// is the new segment ID. These values differ at rollover. On recovery,
    /// <c>scanFromSegId = chk.LastSegmentId</c> so the scan starts at the last sealed
    /// segment — correct because no records have been written to the new segment yet.
    ///
    /// During a non-rollover flush, <c>lastSegId == activeSegId == _writer.SegmentId</c>.
    /// </remarks>
    private void WriteCheckpointCore(uint lastSegId, long lastOffset, ulong lastSeqNo, uint activeSegId)
    {
        lock (_checkpointLock)
        {
            CheckpointWriter.Write(
                _config.RootDirectory,
                new CheckpointData(lastSegId, lastOffset, lastSeqNo, activeSegId));
        }
    }

    private void WriteCheckpoint()
    {
        ulong lastSeqNo = _nextSeqNo == 0 ? 0UL : _nextSeqNo - 1;
        WriteCheckpointCore(_writer!.SegmentId, _writer.BytesWritten, lastSeqNo, _writer.SegmentId);
    }

    private async Task RunFlushTimerAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(_config.FsyncIntervalMs, ct);
                if (!ct.IsCancellationRequested)
                {
                    FlushInternal();
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    private void FlushInternal()
    {
        // Measure only the actual disk-sync portion.
        long tsStart = Stopwatch.GetTimestamp();
        _writer!.FlushToDisk();
        double fsyncMs = Stopwatch.GetElapsedTime(tsStart).TotalMilliseconds;

        WriteCheckpoint();

        foreach (ConsumerState cs in _consumers.Values)
        {
            cs.Flusher.Flush();
        }

        Metrics.RecordFsyncDuration(fsyncMs);
        Metrics.RecordBatchSize(_recordsSinceLastFlush);
        _recordsSinceLastFlush = 0;
        Metrics.IncrementFlushCount();
    }

    private bool IsAtTail(ConsumerState cs) =>
        cs.CurrentSegmentId == _writer!.SegmentId
        && cs.CurrentOffset >= _writer.BytesWritten;

    private void EnsureReaderOpen(ConsumerState cs)
    {
        if (cs.Reader is null)
        {
            cs.Reader = new SegmentReader(_segmentsDir, cs.CurrentSegmentId, cs.CurrentOffset);
        }
    }

    private void TruncateSegment(uint segId, long position)
    {
        string path = SegmentNaming.GetFilePath(_segmentsDir, segId);
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
        fs.SetLength(position);
        fs.Flush(flushToDisk: true);     // ensure truncation is durable before recovery completes (R04-H5)
    }

    private List<uint> EnumerateSegmentIds()
    {
        var ids = new List<uint>();

        if (!Directory.Exists(_segmentsDir))
        {
            return ids;
        }

        foreach (string file in Directory.GetFiles(_segmentsDir))
        {
            if (SegmentNaming.TryParseId(Path.GetFileName(file), out uint id))
            {
                ids.Add(id);
            }
        }

        ids.Sort();
        return ids;
    }

    /// <summary>
    /// Opens <paramref name="segId"/> read-only and returns the <c>SeqNo</c> of its first record.
    /// </summary>
    /// <exception cref="InvalidDataException">
    /// The segment is unreadable or contains no valid records.
    /// </exception>
    private ulong ReadFirstSeqNoFromSegment(uint segId)
    {
        string path = SegmentNaming.GetFilePath(_segmentsDir, segId);
        using FileStream stream = File.OpenRead(path);
        Result<RecordReadResult, PeclError> result = RecordReader.Read(stream);
        if (result.IsSuccess)
        {
            return result.Value.Header.SeqNo;
        }

        throw new InvalidDataException(
            $"Segment {segId} at '{path}' is unreadable: {result.Error}");
    }

    /// <summary>
    /// Opens <paramref name="segId"/> read-only and returns the <c>SeqNo</c> of its last valid record.
    /// Returns <c>0</c> if the segment contains no valid records.
    /// </summary>
    private ulong ReadLastSeqNoFromSegment(uint segId)
    {
        string path = SegmentNaming.GetFilePath(_segmentsDir, segId);
        using FileStream stream = File.OpenRead(path);
        // Note: 0UL is returned both for "last valid record is seqNo 0" and "segment has
        // no valid records". Callers using this for a tail guard should be aware that an
        // empty/corrupt segment is indistinguishable from a real seqNo-0 record.
        ulong lastSeqNo = 0UL;
        while (true)
        {
            Result<RecordReadResult, PeclError> result = RecordReader.Read(stream);
            if (result.IsFailure)
            {
                break;
            }

            lastSeqNo = result.Value.Header.SeqNo;
        }

        return lastSeqNo;
    }

    /// <summary>
    /// Scans <paramref name="segId"/> and returns the byte offset of the first record
    /// whose <c>SeqNo</c> is &gt;= <paramref name="targetSeqNo"/>.
    /// Returns the end-of-stream position if no such record exists.
    /// </summary>
    private long ScanSegmentForSeqNo(uint segId, ulong targetSeqNo)
    {
        string path = SegmentNaming.GetFilePath(_segmentsDir, segId);
        using FileStream stream = File.OpenRead(path);
        while (true)
        {
            long offset = stream.Position;
            Result<RecordReadResult, PeclError> result = RecordReader.Read(stream);
            if (result.IsFailure)
            {
                return offset;
            }

            if (result.Value.Header.SeqNo >= targetSeqNo)
            {
                return offset;
            }
        }
    }

    /// <summary>
    /// Routes a single read result to all sink lanes, expanding block records into
    /// individual entries transparently before routing.
    /// </summary>
    private async ValueTask RouteRecordAsync(RecordReadResult readResult, ConsumerState cs, uint segmentIdBeforeRead)
    {
        if (readResult.Header.Flags.HasFlag(RecordFlags.IsBlock))
        {
            // Decode all entries synchronously first (ref struct cannot cross await boundary),
            // then route each materialised entry record.
            var reader = new BlockPayloadReader(readResult.Payload.Span);
            var entries = new List<LogRecord>(reader.EntryCount);
            while (reader.TryReadNext(out ReadOnlySpan<byte> entryPayload))
            {
                RecordHeader entryHeader = readResult.Header with
                {
                    Flags = RecordFlags.None,
                    PayloadLength = (uint)entryPayload.Length,
                };
                entries.Add(new LogRecord(entryHeader, entryPayload.ToArray()));
            }

            foreach (LogRecord entryRecord in entries)
            {
                await (cs.Router ?? _router).RouteAsync(entryRecord, cs.Lanes, CancellationToken.None);
                cs.LastRoutedSeqNo = entryRecord.Header.SeqNo;
            }
        }
        else
        {
            LogRecord record = new(readResult.Header, readResult.Payload.ToArray());
            await (cs.Router ?? _router).RouteAsync(record, cs.Lanes, CancellationToken.None);
            cs.LastRoutedSeqNo = record.Header.SeqNo;
        }

        cs.LastFullyRoutedSegmentId = segmentIdBeforeRead;

        // Drain spill inline after each routed record for faster replay when records flow actively.
        foreach (SinkLane lane in cs.Lanes)
        {
            while (lane.HasSpill && lane.TryDrainOneSpillRecord()) { }
        }
    }

    private async Task RunReaderLoopAsync(ConsumerState cs, CancellationToken ct)
    {
        // Normal polling loop — exits when ct is cancelled.
        while (!ct.IsCancellationRequested)
        {
            // Capture segment ID before ReadNextCore so we know which segment this record
            // came from after routing completes. Used to update the GC watermark safely.
            uint segmentIdBeforeRead = cs.CurrentSegmentId;
            Result<RecordReadResult, PeclError> result = ReadNextCore(cs);

            if (result.IsFailure && result.Error.Code == PeclErrorCode.EndOfLog)
            {
                try
                {
                    await Task.Delay(ReaderPollIntervalMs, ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                // Opportunistic spill drain during idle time.
                foreach (SinkLane lane in cs.Lanes)
                {
                    while (lane.HasSpill && lane.TryDrainOneSpillRecord()) { }
                }

                continue;
            }

            if (result.IsFailure)
            {
                // Non-EndOfLog failure during polling: preserve full PeclError context by routing
                // through the drain-error path, matching drain-phase behaviour (R05-M2).
                // Stop() will surface this as PeclDrainException inside its AggregateException.
                cs.SetDrainError(result.Error);
                break;
            }

            RecordReadResult readResult = result.Value;
            await RouteRecordAsync(readResult, cs, segmentIdBeforeRead);
        }

        // Drain phase: Stop() calls FlushInternal() before cancelling ct, so all
        // appended-but-undelivered records are on disk. Read to tail before exiting.
        while (true)
        {
            Result<RecordReadResult, PeclError> result = ReadNextCore(cs);
            if (result.IsFailure)
            {
                if (result.Error.Code != PeclErrorCode.EndOfLog)
                {
                    // Real data corruption or truncation encountered during drain.
                    // Store on the consumer for surfacing through Stop().
                    cs.SetDrainError(result.Error);
                }

                break;
            }

            uint segmentIdBeforeRead = cs.CurrentSegmentId;
            RecordReadResult readResult = result.Value;

            try
            {
                await RouteRecordAsync(readResult, cs, segmentIdBeforeRead);
            }
            catch
            {
                // Sink already faulted — its error is captured by Task.WaitAll in Stop().
                break;
            }
        }
    }

    private static async Task RunSinkTaskAsync(SinkSlot slot)
    {
        // ReadAllAsync uses CancellationToken.None so the sink task drains all remaining
        // records after lane.Complete() is called — its lifecycle is driven by lane
        // completion, not by ct. CancellationToken.None is used so Stop() drain is
        // never aborted mid-record; all buffered records are delivered before returning.
        await foreach (LogRecord record in slot.Lane.Reader.ReadAllAsync(CancellationToken.None))
        {
            slot.WriteBuffer[0] = record;
            await slot.Sink.WriteAsync(slot.WriteSegment, CancellationToken.None);
        }
    }

    private async Task RunGcAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_config.GcIntervalMs, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            RunGcPass();
        }
    }

    private void RunGcPass()
    {
        uint activeSegmentId = _writer!.SegmentId;
        List<uint> segIds = EnumerateSegmentIds();
        long segmentCount = segIds.Count;

        switch (_config.RetentionPolicy)
        {
            case RetentionPolicy.ConsumerGated:
                {
                    // Acquire _observabilityLock to guard _consumers reads against concurrent
                    // Clear() in Stop() on the GC-task-timeout path (R05-L8). Only the
                    // dictionary read is inside the lock; I/O runs outside.
                    bool noConsumers;
                    uint watermark = 0u;
                    lock (_observabilityLock)
                    {
                        noConsumers = _consumers.Count == 0;
                        if (!noConsumers)
                        {
                            // Pull-mode consumers advance CurrentSegmentId synchronously on the main
                            // thread — no routing race. Push-mode consumers use
                            // LastFullyRoutedSegmentId (updated post-RouteAsync).
                            watermark = _consumers.Values.Min(cs =>
                                cs.IsPushMode ? cs.LastFullyRoutedSegmentId : cs.CurrentSegmentId);
                        }
                    }

                    if (!noConsumers)
                    {
                        foreach (uint segId in segIds)
                        {
                            if (segId >= watermark || segId >= activeSegmentId)
                            {
                                continue;
                            }

                            TryDeleteSegment(segId, ref segmentCount);
                        }
                    }

                    break;
                }

            case RetentionPolicy.TimeBased:
                {
                    DateTime cutoff = DateTime.UtcNow.AddMilliseconds(-_config.RetentionMaxAgeMs);
                    foreach (uint segId in segIds)
                    {
                        if (segId >= activeSegmentId)
                        {
                            continue;
                        }

                        string path = SegmentNaming.GetFilePath(_segmentsDir, segId);
                        try
                        {
                            if (File.GetLastWriteTimeUtc(path) < cutoff)
                            {
                                TryDeleteSegment(segId, ref segmentCount);
                            }
                        }
                        catch (FileNotFoundException)
                        {
                            // Already deleted by a concurrent GC pass or external process — skip.
                        }
                    }

                    break;
                }

            case RetentionPolicy.SizeBased:
                {
                    // Compute total on-disk size across ALL segments (including active) for an
                    // accurate disk-usage picture. Delete oldest sealed segments until within limit.
                    long totalBytes = 0;
                    var sizes = new Dictionary<uint, long>(segIds.Count);
                    foreach (uint segId in segIds)
                    {
                        string path = SegmentNaming.GetFilePath(_segmentsDir, segId);
                        try
                        {
                            long size = new FileInfo(path).Length;
                            sizes[segId] = size;
                            totalBytes += size;
                        }
                        catch (FileNotFoundException)
                        {
                            // Already deleted — exclude from total.
                        }
                    }

                    foreach (uint segId in segIds) // ascending — oldest first
                    {
                        if (segId >= activeSegmentId)
                        {
                            continue;
                        }

                        if (totalBytes <= _config.RetentionMaxBytes)
                        {
                            break;
                        }

                        if (sizes.TryGetValue(segId, out long segSize))
                        {
                            TryDeleteSegment(segId, ref segmentCount);
                            totalBytes -= segSize;
                        }
                    }

                    break;
                }
        }

        Metrics.SetSegmentCount(segmentCount);
        Metrics.SetSegmentBytes(ComputeSegmentBytes(EnumerateSegmentIds())); // post-deletion count
    }

    private void TryDeleteSegment(uint segId, ref long segmentCount)
    {
        string path = SegmentNaming.GetFilePath(_segmentsDir, segId);
        try
        {
            File.Delete(path);
            Metrics.IncrementSegmentsDeleted();
            segmentCount--;
        }
        catch (IOException)
        {
            // Non-fatal: segment remains and will be retried on the next GC pass.
        }
        catch (UnauthorizedAccessException)
        {
            // Non-fatal: same as IOException.
        }
    }

    /// <summary>
    /// Sums the on-disk sizes of all segment files identified by <paramref name="segIds"/>.
    /// Files that have been deleted concurrently are silently skipped.
    /// </summary>
    private long ComputeSegmentBytes(IReadOnlyList<uint> segIds)
    {
        long total = 0;
        foreach (uint id in segIds)
        {
            try
            {
                total += new FileInfo(SegmentNaming.GetFilePath(_segmentsDir, id)).Length;
            }
            catch (FileNotFoundException)
            {
                // Concurrently deleted — exclude from total.
            }
        }

        return total;
    }

    private void ThrowIfNotRunning()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (State != PipelineState.Running)
        {
            throw new InvalidOperationException(
                $"This operation requires the pipeline to be in the Running state (current: {State}).");
        }
    }

    // ── ConsumerState ─────────────────────────────────────────────────────────

    /// <summary>
    /// Holds all mutable per-consumer state for the lifetime of a registered consumer:
    /// cursor position, segment reader, sink slots, reader loop task, and observability
    /// fields. Disposed during <see cref="ExecuteCleanupSequence"/>: in the
    /// <see cref="Pipeline.Stop"/> path after all reader tasks and sink tasks have
    /// completed; in the <see cref="Pipeline.ForceStop"/> path without waiting for
    /// sinks (they may still be running).
    /// </summary>
    private sealed class ConsumerState : IDisposable
    {
        /// <summary>The name of the consumer this state tracks.</summary>
        public string ConsumerName { get; }

        /// <summary>Manages periodic flush of the consumer's cursor to disk.</summary>
        public CursorFlusher Flusher { get; }

        /// <summary>The open reader for the consumer's current segment, or <see langword="null"/> if not yet opened.</summary>
        public SegmentReader? Reader { get; set; }

        /// <summary>Segment the consumer will read from next.</summary>
        /// <remarks>
        /// Volatile: written by the main thread (pull-mode, <see cref="Pipeline.ReadNextCore"/>)
        /// or by the reader loop task (push-mode); read by the GC background task
        /// (<see cref="Pipeline.RunGcPass"/>). On 64-bit runtimes <c>volatile uint</c>
        /// reads and writes are atomic.
        /// </remarks>
        public volatile uint CurrentSegmentId;

        /// <summary>
        /// The segment ID of the segment from which the most recently routed record was read.
        /// Updated after <c>RouteAsync</c> completes for each record. Used as the GC watermark:
        /// the GC task may only delete segments strictly below <c>Min(LastFullyRoutedSegmentId)</c>
        /// across all consumers.
        /// </summary>
        /// <remarks>
        /// Volatile: written exclusively by the reader loop task; read by the GC background task.
        /// On 64-bit runtimes <c>volatile uint</c> reads and writes are atomic. Initial value 0
        /// is safe — segment 0 is not deleted until at least one record has been routed from it.
        /// </remarks>
        public volatile uint LastFullyRoutedSegmentId;

        /// <summary>
        /// Error recorded by the drain phase if a non-<see cref="PeclErrorCode.EndOfLog"/> read
        /// failure occurred. Read by <see cref="Pipeline.Stop"/> after all reader tasks complete.
        /// </summary>
        internal PeclError? DrainError { get; private set; }

        /// <summary>
        /// Signalled when <see cref="SetDrainError"/> records a drain-phase error.
        /// Used for deterministic waiting in tests. Set is idempotent.
        /// </summary>
        internal ManualResetEventSlim DrainErrorSignal { get; } = new ManualResetEventSlim(false);

        /// <summary>Records a drain-phase read error for surfacing through <see cref="Pipeline.Stop"/>.</summary>
        internal void SetDrainError(PeclError error)
        {
            DrainError = error;
            DrainErrorSignal.Set();
        }

        /// <summary>Byte offset within <see cref="CurrentSegmentId"/> from which the next read will start.</summary>
        public long CurrentOffset { get; set; }

        /// <summary>Sink slots registered via <c>AddSink</c>.</summary>
        public List<SinkSlot> SinkSlots { get; } = new();

        /// <summary>
        /// The routing strategy for this consumer. <see langword="null"/> means broadcast
        /// (the pipeline-level <c>_router</c> is used). Set by the content-type and
        /// hash-routing <c>AddSink</c> overloads.
        /// </summary>
        public IRecordRouter? Router { get; set; }

        /// <summary>
        /// Read-only view of the sink lanes, passed to the router's <c>RouteAsync</c>.
        /// Kept in sync with <see cref="SinkSlots"/> by <c>AddSink</c>.
        /// </summary>
        public IReadOnlyList<SinkLane> Lanes => _lanes;

        private readonly List<SinkLane> _lanes = new();

        /// <summary>
        /// True when at least one sink is registered. Push-mode consumers have
        /// records delivered automatically; <see cref="Pipeline.ReadNext"/> is unavailable.
        /// </summary>
        public bool IsPushMode => SinkSlots.Count > 0;

        /// <summary>Cancellation source for the reader loop and all sink tasks.</summary>
        public CancellationTokenSource? ReaderCts { get; set; }

        /// <summary>The background reader loop task; <see cref="Task.CompletedTask"/> until <see cref="Pipeline.Start"/>.</summary>
        public Task ReaderTask { get; set; } = Task.CompletedTask;

        /// <summary>
        /// SeqNo of the last record pushed to all sink lanes by the reader loop.
        /// <see langword="null"/> until at least one record has been routed.
        /// </summary>
        public ulong? LastRoutedSeqNo { get; set; }

        /// <summary>
        /// SeqNo of the last record returned by <see cref="Pipeline.ReadNext"/> for this
        /// pull-mode consumer. <see langword="null"/> until at least one record has been read.
        /// </summary>
        /// <remarks>
        /// Only meaningful for pull-mode consumers (<see cref="IsPushMode"/> == <see langword="false"/>).
        /// Written by the calling thread; read by the observability lag callback.
        /// </remarks>
        public ulong? LastReadSeqNo { get; set; }

        public ConsumerState(string consumerName, CursorFlusher flusher, uint segmentId, long offset)
        {
            ConsumerName = consumerName;
            Flusher = flusher;
            CurrentSegmentId = segmentId;
            CurrentOffset = offset;
        }

        /// <summary>Adds a lane to the lane list in sync with <see cref="SinkSlots"/>.</summary>
        public void AddLane(SinkLane lane) => _lanes.Add(lane);

        public void Dispose()
        {
            List<Exception>? errors = null;

            try { Reader?.Dispose(); }
            catch (Exception ex) { (errors ??= []).Add(ex); }

            foreach (SinkSlot slot in SinkSlots)
            {
                try { slot.Lane.Dispose(); }
                catch (Exception ex) { (errors ??= []).Add(ex); }

                // Return the pooled write buffer. clearArray: false (the default) is used
                // intentionally: WriteSegment wraps WriteBuffer, so clearing would null out
                // WriteBuffer[0] and corrupt any IReadOnlyList<LogRecord> references held by
                // sink implementations after WriteAsync returns. The stale LogRecord reference
                // in the pool is released when the slot is next rented and overwritten.
                //
                // Thread safety: in the Stop() path, ConsumerState.Dispose() runs after
                // Task.WaitAll(sinkTasks), so sinks have already exited and WriteBuffer is
                // no longer referenced. In the ForceStop() path sinks may still be running,
                // but ReaderCts cancellation prevents any new batch from being enqueued, so
                // the returned buffer will not be re-rented during this pipeline's lifetime.
                try { ArrayPool<LogRecord>.Shared.Return(slot.WriteBuffer); }
                catch (Exception ex) { (errors ??= []).Add(ex); }
            }

            try { ReaderCts?.Dispose(); }
            catch (Exception ex) { (errors ??= []).Add(ex); }

            try { Flusher.Dispose(); }
            catch (Exception ex) { (errors ??= []).Add(ex); }

            try { DrainErrorSignal.Dispose(); }
            catch (Exception ex) { (errors ??= []).Add(ex); }

            if (errors is { Count: > 0 })
            {
                throw new AggregateException("ConsumerState.Dispose() encountered errors.", errors);
            }
        }
    }

    // ── SinkSlot ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Pairs a named <see cref="ISink"/> with its dedicated <see cref="SinkLane"/>
    /// and running drain <see cref="Task"/>. Also owns the single-element
    /// <see cref="WriteBuffer"/> rented from <see cref="ArrayPool{T}"/> that is
    /// reused across every <see cref="ISink.WriteAsync"/> call to avoid per-record
    /// allocation.
    /// </summary>
    private sealed class SinkSlot
    {
        /// <summary>Unique name of this sink within the consumer.</summary>
        public string SinkName { get; }

        /// <summary>The sink implementation that receives batches.</summary>
        public ISink Sink { get; }

        /// <summary>Bounded channel decoupling the reader loop from sink I/O.</summary>
        public SinkLane Lane { get; }

        /// <summary>The running sink drain task; <see cref="Task.CompletedTask"/> until <see cref="Pipeline.Start"/>.</summary>
        public Task Task { get; set; } = Task.CompletedTask;

        /// <summary>
        /// Single-element buffer rented from <see cref="ArrayPool{T}"/> once at construction.
        /// Element [0] is overwritten before each <see cref="ISink.WriteAsync"/> call.
        /// Returned to the pool in <see cref="ConsumerState.Dispose"/> after all sink tasks
        /// have completed. Use <see cref="WriteSegment"/> (not this array) when calling
        /// WriteAsync to avoid exposing excess rented capacity.
        /// </summary>
        public readonly LogRecord[] WriteBuffer;

        /// <summary>
        /// An <see cref="ArraySegment{T}"/> view over element [0] of <see cref="WriteBuffer"/>,
        /// stored as <see cref="IReadOnlyList{T}"/> so the boxing occurs once at construction
        /// rather than on every <see cref="ISink.WriteAsync"/> call.
        /// </summary>
        public readonly IReadOnlyList<LogRecord> WriteSegment;

        public SinkSlot(string sinkName, ISink sink, SinkLane lane)
        {
            SinkName = sinkName;
            Sink = sink;
            Lane = lane;
            WriteBuffer = ArrayPool<LogRecord>.Shared.Rent(1);
            WriteSegment = new ArraySegment<LogRecord>(WriteBuffer, 0, 1);
        }
    }
}
