# ByTech.EmbeddedCommitLog

[![NuGet](https://img.shields.io/nuget/v/ByTech.EmbeddedCommitLog)](https://www.nuget.org/packages/ByTech.EmbeddedCommitLog)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE.MD)
[![.NET 9](https://img.shields.io/badge/.NET-9.0-purple)](https://dotnet.microsoft.com/en-us/download/dotnet/9)

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
pipeline.RegisterSink("console-consumer", "console-sink", new ConsoleSink());
pipeline.Start();

// ... append records ...

pipeline.Stop(); // drains all pending records before returning
```

### Pull-mode consumer

Pull records on demand — your loop, your pace:

```csharp
pipeline.RegisterConsumer("audit-consumer");
pipeline.Start();

// ... append records + Flush() ...

while (true)
{
    var result = pipeline.TryRead("audit-consumer");
    if (!result.IsSuccess) break; // no more records currently available

    LogRecord record = result.Value;
    Console.WriteLine($"[{record.Header.SeqNo}] {record.Payload.Length} bytes");
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
| `BackpressurePolicy` | `BackpressurePolicy` | `Block` | `Block` — reader loop waits for lane space (no record loss); `Drop` — record is discarded and `pecl.sink.dropped` is incremented. |
| `DrainTimeoutMs` | `int` | `30,000` | Milliseconds `Stop()` waits for push-mode consumers to drain. `0` = unbounded. |

### Retention

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RetentionPolicy` | `RetentionPolicy` | `ConsumerGated` | `ConsumerGated` — delete only when all consumers have passed a segment; `TimeBased` — delete segments older than `RetentionMaxAgeMs`; `SizeBased` — delete oldest segments when total log exceeds `RetentionMaxBytes`. |
| `RetentionMaxAgeMs` | `long` | `604,800,000` (7 days) | Maximum segment age in ms under `TimeBased` retention. Must be > 0 when `TimeBased`. |
| `RetentionMaxBytes` | `long` | `1,073,741,824` (1 GiB) | Maximum total log size in bytes under `SizeBased` retention. Must be > 0 when `SizeBased`. |

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
void Flush();      // fsync the active segment

// Consumer registration (call before Start)
void RegisterConsumer(string consumerName);
void RegisterSink(string consumerName, string sinkName, ISink sink);

// Pull-mode read (call after Start)
Result<LogRecord, PeclError> TryRead(string consumerName);

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
| `pecl.records.appended` | Counter | records | Total records successfully appended. |
| `pecl.bytes.appended` | Counter | bytes | Total payload bytes appended. |
| `pecl.flushes` | Counter | flushes | Total explicit flushes (calls to `Flush()`, segment rollovers, `Stop()`). |
| `pecl.segment.rollovers` | Counter | rollovers | Total segment rollovers since last `Start()`. |
| `pecl.recovery.count` | Counter | recoveries | Times crash recovery ran (once per `Start()`). |
| `pecl.segments.deleted` | Counter | segments | Total segments deleted by the GC. |
| `pecl.sink.dropped` | Counter | records | Records dropped under `BackpressurePolicy.Drop`. Tagged with `sink` name. |
| `pecl.consumer.lag` | Gauge | records | Records between a push-mode consumer cursor and the log tail. Tagged with `consumer` name. |
| `pecl.sink.lane.depth` | Gauge | records | Approximate records buffered in each sink lane. Tagged with `consumer` and `sink` names. |
| `pecl.segments.count` | Gauge | segments | Current segment file count on disk. |

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
  checkpoint.dat      ← crash-recovery snapshot
```

Checkpoint and cursor files use a **write-temp + fsync + rename** strategy,
guaranteeing crash safety even on unclean shutdown.

---

## License

Apache 2.0 © 2026 Branimir Bajt
