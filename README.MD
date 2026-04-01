# Persistent Embedded Commit Log (PECL)

[![NuGet](https://img.shields.io/nuget/v/ByTech.EmbeddedCommitLog)](https://www.nuget.org/packages/ByTech.EmbeddedCommitLog)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE.MD)
[![.NET 9](https://img.shields.io/badge/.NET-9.0-purple)](https://dotnet.microsoft.com/en-us/download/dotnet/9)
[![Build](https://img.shields.io/github/actions/workflow/status/bbajt/csharp-code-map/ci.yml?branch=main&label=build)](https://github.com/bbajt/csharp-code-map/actions)
[![Tests](https://img.shields.io/github/actions/workflow/status/bbajt/csharp-code-map/ci.yml?branch=main&label=tests)](https://github.com/bbajt/csharp-code-map/actions)
**PECL** — Persisted Embedded Commit Log. A Kafka-inspired, crash-safe, append-only
log that runs entirely in-process. No broker. No network. No ops overhead.

Targets **500K+ records/sec** on NVMe with batched durability on .NET 9.

---

## Why PECL?

| | `Channel<T>` | Kafka / RabbitMQ | **PECL** |
|---|---|---|---|
| Durability | ✗ in-memory only | ✓ | ✓ crash-safe |
| In-process | ✓ | ✗ separate broker | ✓ |
| Independent consumer cursors | ✗ | ✓ | ✓ |
| Zero ops overhead | ✓ | ✗ | ✓ |
| Fan-out to multiple consumers | ✓ | ✓ | ✓ |

Use PECL when you need durable, replayable, fan-out messaging inside a single .NET
process — without standing up a broker.

---

## Installation

```
dotnet add package ByTech.EmbeddedCommitLog
```

Requires **.NET 9.0** or later.

---

## Quick Start

### Write records

```csharp
using ByTech.EmbeddedCommitLog.Pipeline;

var config = new PipelineConfiguration
{
    RootDirectory = "/var/data/myapp-log",
};

using var pipeline = new Pipeline(config);
pipeline.Start();

// Append returns the globally monotonic sequence number assigned to the record.
ulong seqNo = pipeline.Append("hello world"u8);

pipeline.Flush(); // optional — ensures data is durable before proceeding
pipeline.Stop();
```

### Push-mode consumer (sink)

Implement `ISink` — PECL delivers batches to your sink on a background task:

```csharp
using ByTech.EmbeddedCommitLog.Consumer;
using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Sinks;

public sealed class ConsoleSink : ISink
{
    public Task WriteAsync(IReadOnlyList<LogRecord> batch, CancellationToken ct)
    {
        foreach (LogRecord record in batch)
            Console.WriteLine($"[{record.Header.SeqNo}] {record.Payload.Length} bytes");
        return Task.CompletedTask;
    }
}

// Register before Start().
pipeline.RegisterConsumer("console-consumer");
pipeline.AddSink("console-consumer", "console-sink", new ConsoleSink());
pipeline.Start();

// ... append records ...

pipeline.Stop(); // drains all pending records before returning
```

### Block append

Pack multiple small payloads into a single framed record to reduce per-record
header + CRC overhead at high ingestion rates. Push-mode consumers receive the
individual payloads transparently — `ISink` is unaware that a block was written.

```csharp
ulong seqNo = pipeline.AppendBlock(new ReadOnlyMemory<byte>[]
{
    "first payload"u8.ToArray(),
    "second payload"u8.ToArray(),
    "third payload"u8.ToArray(),
});
// All entries share the same seqNo; each arrives as a separate LogRecord at the sink.
```

### Routing strategies

By default all sinks registered for a consumer receive every record (broadcast).
Two additional strategies are available: content-type filtering and hash-based
partitioning. Broadcast and filtered sinks cannot be mixed on the same consumer.

```csharp
// Broadcast (default) — every record goes to every sink.
pipeline.AddSink("my-consumer", "sink-a", sinkA);
pipeline.AddSink("my-consumer", "sink-b", sinkB);

// Content-type routing — each sink receives only records with a matching ContentType.
// Records that match no registered filter are dropped (pecl.sink.dropped_total incremented).
pipeline.AddSink("my-consumer", "json-sink",   jsonSink,   ContentType.Json);
pipeline.AddSink("my-consumer", "binary-sink", binarySink, ContentType.Binary);

// Hash routing — records are distributed across sinks by abs(keySelector(record)) % sinkCount.
// All sinks for the consumer share the same keySelector; only the first one registered is used.
pipeline.AddSink("my-consumer", "partition-0", sink0, record => record.Header.SchemaId.GetHashCode());
pipeline.AddSink("my-consumer", "partition-1", sink1, record => record.Header.SchemaId.GetHashCode());
```

### Pull-mode consumer

Pull records on demand — your loop, your pace:

```csharp
pipeline.RegisterConsumer("audit-consumer");
pipeline.Start();

// ... append records + Flush() ...

while (true)
{
    var result = pipeline.ReadNext("audit-consumer");
    if (!result.IsSuccess) break; // no more records currently available

    RecordReadResult r = result.Value;
    Console.WriteLine($"[{r.Header.SeqNo}] {r.Payload.Length} bytes");
}
```

---

## Configuration

All options are set on `PipelineConfiguration` (a `sealed record` — all properties
are `init`-only). Only `RootDirectory` is required; every other property has a
production-ready default.

### Writer

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RootDirectory` | `string` | *(required)* | Absolute path to the directory that holds `segments/`, `cursors/`, and `checkpoint.dat`. |
| `MaxSegmentSize` | `long` | `67,108,864` (64 MiB) | Maximum bytes per segment file before a new segment is created. |
| `CursorFlushRecordThreshold` | `int` | `1,000` | Records a consumer must advance before its cursor is auto-flushed to disk. |
| `CursorFlushInterval` | `TimeSpan` | `00:00:05` | Maximum time between cursor flushes when the record threshold has not been reached. |

### Durability

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DurabilityMode` | `DurabilityMode` | `Batched` | `None` — no automatic fsync; `Batched` — periodic fsync on a timer; `Strict` — fsync after every `Append`. |
| `FsyncIntervalMs` | `int` | `100` | Milliseconds between automatic fsyncs in `Batched` mode. Ignored for `None` and `Strict`. Must be > 0 when `Batched`. |

### Backpressure

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `SinkLaneCapacity` | `int` | `4,096` | Maximum records buffered per push-mode sink lane before backpressure is applied. |
| `BackpressurePolicy` | `BackpressurePolicy` | `Block` | `Block` — reader loop waits for lane space (no record loss); `Drop` — record is discarded and `pecl.sink.dropped_total` is incremented; `Spill` — overflow records are written to a crash-safe spill file (`{root}/spill/{consumer}-{sink}.spill`) and replayed into the lane when space becomes available. FIFO order is preserved; delivery is at-least-once across restarts. |
| `DrainTimeoutMs` | `int` | `30,000` | Milliseconds `Stop()` waits for push-mode consumers to drain. `0` = unbounded. |

### Retention

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RetentionPolicy` | `RetentionPolicy` | `ConsumerGated` | `ConsumerGated` — delete only when all consumers have passed a segment; `TimeBased` — delete segments older than `RetentionMaxAgeMs`; `SizeBased` — delete oldest segments when total log exceeds `RetentionMaxBytes`. |
| `RetentionMaxAgeMs` | `long` | `604,800,000` (7 days) | Maximum segment age in ms under `TimeBased` retention. Must be > 0 when `TimeBased`. |
| `RetentionMaxBytes` | `long` | `1,073,741,824` (1 GiB) | Maximum total log size in bytes under `SizeBased` retention. Must be > 0 when `SizeBased`. |

### Compression

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `CompressionAlgorithm` | `CompressionAlgorithm` | `None` | Compression applied to record payloads on write. `None` — no compression (default); `Brotli` — Brotli quality 1. Decompression is automatic on read — `LogRecord.Payload` always contains the original bytes. |

Payloads that do not compress (compressed size ≥ original) are stored uncompressed
automatically; no configuration change is needed for mixed workloads.

**Custom algorithm escape hatch (ADR-002):** Pre-compress the payload yourself and set
`CompressionAlgorithm = None`. PECL stores the raw bytes without modification; your
consumer decompresses. This gives full algorithm freedom (LZ4, Zstd, domain-specific
dictionaries) without a PECL release.

### Consumers

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MissingCursorPolicy` | `MissingCursorPolicy` | `FromBeginning` | Starting position for a consumer whose cursor file is absent on `Start()`. `FromBeginning` — replay all retained records (default). `FromTail` — skip history; receive only records appended after `Start()` returns. Ignored when a valid cursor file already exists. |

### GC

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `GcIntervalMs` | `int` | `60,000` | Milliseconds between background GC passes. |
| `GcStopTimeoutMs` | `int` | `5,000` | Milliseconds `Stop()` / `Dispose()` waits for the GC task to finish. |

### Observability

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MeterName` | `string` | `"pecl"` | Name of the `System.Diagnostics.Metrics.Meter` registered by this pipeline. Use a unique value when running multiple pipelines in the same process to avoid instrument name collisions. |

---

## API Overview

### `Pipeline`

The single entry point for all log operations.

```csharp
// Lifecycle
void Start();
void Stop();       // graceful — drains push-mode consumers
void ForceStop();  // immediate — does not wait for consumer drain
void Dispose();

// Write
ulong Append(ReadOnlySpan<byte> payload,
             ContentType contentType = ContentType.Unknown,
             uint schemaId = 0);
ulong AppendBlock(IReadOnlyList<ReadOnlyMemory<byte>> entries,
                  ContentType contentType = ContentType.Unknown,
                  uint schemaId = 0);
void Flush();      // fsync the active segment

// Consumer registration (call before Start)
void RegisterConsumer(string consumerName);

// Sink registration — broadcast (all records to all sinks)
void AddSink(string consumerName, string sinkName, ISink sink);

// Sink registration — content-type routing (only matching records delivered)
void AddSink(string consumerName, string sinkName, ISink sink, ContentType contentTypeFilter);

// Sink registration — hash routing (record distributed by key)
void AddSink(string consumerName, string sinkName, ISink sink, Func<LogRecord, int> keySelector);

// Cursor management (call on a stopped pipeline)
void SeekConsumer(string consumerName, ulong seqNo);   // ArgumentOutOfRangeException if past tail
                                                        // PeclSeekException if below retention floor
void ResetConsumer(string consumerName);                // moves to earliest available record

// Pull-mode read (call after Start)
Result<RecordReadResult, PeclError> ReadNext(string consumerName);

// State
PipelineState State { get; }
PipelineMetrics Metrics { get; }
```

### `ISink` — push-mode consumer

```csharp
public interface ISink
{
    Task WriteAsync(IReadOnlyList<LogRecord> batch, CancellationToken ct);
}
```

PECL calls `WriteAsync` with a batch of one or more records. The method must not return
until the batch is durably committed (throw on failure). Exceptions are captured and
surfaced in the `AggregateException` thrown by `Stop()`.

### `LogRecord`

Immutable read-side envelope delivered to sinks and returned by `TryRead`:

```csharp
public sealed record LogRecord(RecordHeader Header, byte[] Payload);
```

Record metadata is accessed via `Header`:

```csharp
ulong seqNo        = record.Header.SeqNo;        // globally monotonic sequence number
ContentType ct     = record.Header.ContentType;  // advisory encoding hint
RecordFlags flags  = record.Header.Flags;
uint schemaId      = record.Header.SchemaId;
```

### Metrics (`System.Diagnostics.Metrics`)

PECL registers instruments under the meter named by `MeterName` (default `"pecl"`).
Observable via OpenTelemetry, dotnet-monitor, or any `MeterListener`.

| Instrument | Kind | Unit | Description |
|------------|------|------|-------------|
| `pecl.write.records_total` | Counter | records | Total records successfully appended. |
| `pecl.write.bytes_total` | Counter | bytes | Total payload bytes appended. |
| `pecl.flushes` | Counter | flushes | Total explicit flushes (calls to `Flush()`, segment rollovers, `Stop()`). |
| `pecl.segment.rollovers` | Counter | rollovers | Total segment rollovers since last `Start()`. |
| `pecl.recovery.count` | Counter | recoveries | Times crash recovery ran (once per `Start()`). |
| `pecl.segment.deleted` | Counter | segments | Total segments deleted by the GC. |
| `pecl.sink.dropped_total` | Counter | records | Records dropped under `BackpressurePolicy.Drop`. Tagged with `sink` name. |
| `pecl.consumer.read_rate` | Counter | records | Records successfully read via `ReadNext`. |
| `pecl.consumer.lag` | Gauge | records | Records between a push-mode consumer cursor and the log tail. Tagged with `consumer` name. |
| `pecl.sink.lane_depth` | Gauge | records | Approximate records buffered in each sink lane. Tagged with `consumer` and `sink` names. |
| `pecl.segment.count` | Gauge | segments | Current segment file count on disk. |
| `pecl.write.fsync_duration` | Histogram | ms | Wall-clock time of each `FlushToDisk` (fsync) call. |
| `pecl.write.batch_size` | Histogram | records | Records appended since the previous flush (group-commit size). |
| `pecl.recovery.duration` | Gauge | ms | Duration of the most recent recovery scan. |
| `pecl.recovery.truncated_bytes` | Gauge | bytes | Bytes removed from the tail segment during the most recent recovery. Zero when the log was clean. |
| `pecl.segment.bytes` | Gauge | bytes | Total on-disk bytes across all segment files. Updated after each GC pass and at pipeline start. |

---

## Performance

Benchmarks: Intel Core i9-14900K, 64 GB RAM, NVMe 2 TB, Windows 11,
.NET 9.0.14, BenchmarkDotNet v0.14.0, Release build.

| Scenario | Records/sec | Payload | Durability |
|----------|-------------|---------|------------|
| Single-writer append + flush | ~497K | 64 B | Batched |
| Fan-out (3 consumers, no-op sinks) | ~6.3M | 64 B | None |

The 500K/sec target is met under `Batched` durability on NVMe.

---

## On-Disk Layout

```
{RootDirectory}/
  segments/
    log-000000.seg    ← sealed segments
    log-000001.seg
    log-000002.seg    ← active segment (open for writing)
  cursors/
    my-consumer.cur   ← one file per registered consumer
  spill/
    consumer-sink.spill  ← crash-safe overflow files (BackpressurePolicy.Spill only)
  checkpoint.dat      ← crash-recovery snapshot
```

Checkpoint and cursor files use a **write-temp + fsync + rename** strategy,
guaranteeing crash safety even on unclean shutdown.

---

## License

Apache 2.0 © 2026 Branimir Bajt
