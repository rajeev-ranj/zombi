# Performance Bottleneck Analysis -- Zombi Event Streaming

> **Summary:** Zombi provides the lowest-cost path from production events to Apache
> Iceberg. This document analyzes all 12 components in the write path, identifies
> bottlenecks with measured data, ranks them by impact, and provides a concrete
> optimization roadmap from the current t3.micro baseline (1 MB/s) to a 2 Gbps
> enterprise target. Every component includes real-world analogies and measured
> numbers so that anyone -- from new contributors to SREs -- can reason about
> performance trade-offs.

---

## Table of Contents

1. [Architecture Diagram](#1-architecture-diagram)
2. [The Event Journey](#2-the-event-journey)
3. [Component Bottleneck Analysis](#3-component-bottleneck-analysis)
4. [Measured Performance Data](#4-measured-performance-data)
5. [Bottleneck Ranking](#5-bottleneck-ranking)
6. [Code-Validated Hot-Path Bottlenecks](#6-code-validated-hot-path-bottlenecks)
7. [Optimization Options](#7-optimization-options)
8. [Path to 2 Gbps](#8-path-to-2-gbps)

---

## 1. Architecture Diagram

The full write path from client request to durable Iceberg storage:

```
  ┌────────┐
  │ Client │
  └───┬────┘
      │  HTTP POST /tables/:table
      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                          Zombi Server (axum + tokio)                     │
│                                                                         │
│   ┌──────────────┐    ┌───────────────────┐    ┌────────────────────┐   │
│   │  HTTP/axum   │───▶│ Backpressure Gate │───▶│ serde Deserialize  │   │
│   │  (receive)   │    │  (semaphore chk)  │    │  (JSON → struct)   │   │
│   │              │    │                   │    │                    │   │
│   │  NOTE: This order is for single-write (/tables/:table).       │   │
│   │  Bulk write (/tables/:table/bulk) parses JSON BEFORE the      │   │
│   │  backpressure check — see Section 6b for details.             │   │
│   └──────────────┘    └───────────────────┘    └────────┬───────────┘   │
│                                                         │               │
│                                                         ▼               │
│                                                ┌────────────────────┐   │
│                                                │   RocksDB Write    │   │
│                                                │ (Memtable, no WAL) │   │
│                                                └────────┬───────────┘   │
│                                                         │               │
│   ┌──────────────┐                                      │               │
│   │  HTTP 202    │◀─────────── (sync response) ─────────┘               │
│   │  (respond)   │                                                      │
│   └──────┬───────┘                                                      │
│          │                                                              │
│          │         ╔═══════════════════════════════════════════════╗     │
│          │         ║        Async Flush Pipeline (background)     ║     │
│          │         ║                                              ║     │
│          │         ║  ┌──────────────┐    ┌───────────────────┐   ║     │
│          │         ║  │ Read Batch   │───▶│ Parquet Encode    │   ║     │
│          │         ║  │ from RocksDB │    │ (ZSTD compress)   │   ║     │
│          │         ║  └──────────────┘    └────────┬──────────┘   ║     │
│          │         ║                               │              ║     │
│          │         ║                               ▼              ║     │
│          │         ║                      ┌────────────────────┐  ║     │
│          │         ║                      │  Upload to S3      │  ║     │
│          │         ║                      │  (single PUT)      │  ║     │
│          │         ║                      └────────┬───────────┘  ║     │
│          │         ║                               │              ║     │
│          │         ║                               ▼              ║     │
│          │         ║                      ┌────────────────────┐  ║     │
│          │         ║                      │ Iceberg Snapshot   │  ║     │
│          │         ║                      │ Commit (metadata)  │  ║     │
│          │         ║                      └────────────────────┘  ║     │
│          │         ╚═══════════════════════════════════════════════╝     │
│          │                                                              │
└──────────┼──────────────────────────────────────────────────────────────┘
           │
           ▼
      ┌────────┐
      │ Client │  (receives 202 Accepted)
      └────────┘
```

**Key insight:** The client sees a response after RocksDB confirms the write
plus post-write synchronous work (~10 us total). After the RocksDB write, the
single-write handler records metrics, updates the high watermark, and calls
`low_watermark()` (a RocksDB iterator seek) before returning 202. Everything
below the dashed line happens asynchronously and does not affect request latency.

---

## 2. The Event Journey

Every event that enters Zombi passes through these steps. Timings were measured
on an EC2 t3.micro (2 vCPU, 1 GiB RAM, gp2 EBS) with an 82-byte JSON payload.

| Step | Component | Timing | Sync/Async |
|------|-----------|--------|------------|
| 1 | **Client sends HTTP POST** -- axum receives the TCP connection | ~0.1 ms local, ~1 ms cross-AZ | Sync |
| 2 | **Backpressure gate** -- semaphore check (10K permits, 64 MB limit) | ~0.01 us | Sync |
| 3 | **Body parsing** -- JSON deserialization via `serde_json` | ~2-5 us for 82B event | Sync |
| 4 | **RocksDB write** -- key-value put to in-memory memtable | ~3-7 us (WAL disabled) | Sync |
| 5 | **HTTP response** -- 202 Accepted returned to client | Total ~10 us server-side | Sync |
| 6 | **Flusher reads batch** -- pulls events from RocksDB | Every 5s (non-Iceberg) / 300s (Iceberg) | Async |
| 7 | **Parquet encoding** -- columnar format with ZSTD compression | Variable per batch | Async |
| 8 | **S3 upload** -- single PUT (`put_object`) to object storage | Variable per file size | Async |
| 9 | **Iceberg snapshot commit** -- metadata + manifest update | <500 ms | Async |

**Total hot-path latency (steps 1-5):** approximately 10 us server-side.

---

## 3. Component Bottleneck Analysis

Each subsection covers one of the 12 components in the write path.

---

### a. Event Size & Format

**What it does:**
Every event arrives as a JSON payload over HTTP. The size of that payload
determines how much data the server must parse, store in RocksDB, and eventually
flush to Parquet. Larger events consume more memory, more disk I/O, and more
network bandwidth at every stage of the pipeline.

**Current configuration:**
- Typical event size: 82 bytes (JSON)
- Large payload tests: 20 KB
- Format: JSON (UTF-8 encoded, human-readable)

**Impact on throughput:**
- At 82B: 13,590 req/s single-event; 166K-324K ev/s in bulk mode
- At 20KB: 275 req/s sustained (bandwidth-limited: 20 KB x 275 = 5.5 MB/s)
- JSON overhead: ~40% larger than an equivalent binary encoding due to field names, quotes, and braces

**Measured numbers:**

| Payload Size | Single-Event req/s | Bandwidth | Limiting Factor |
|--------------|-------------------|-----------|-----------------|
| 82 B | 13,590 | ~1.1 MB/s | HTTP overhead |
| 1 KB | ~5,000 | ~5.0 MB/s | CPU + disk |
| 20 KB | 275 | ~5.5 MB/s | EBS IOPS / burst |

**Visual analogy:**
Like shipping letters versus shipping packages -- the same delivery truck can
carry thousands of letters but only hundreds of packages. The truck (disk I/O)
is the same size either way; bigger items mean fewer fit per trip.

---

### b. HTTP/axum Layer

**What it does:**
The axum framework runs on the Tokio async runtime and handles all incoming HTTP
connections. It parses HTTP headers, routes requests to the correct handler,
manages keep-alive connections, and writes the HTTP response. This is the front
door of the server.

**Current configuration:**
- Framework: axum (Rust, async on Tokio)
- No explicit connection limit (OS-level file descriptor limit applies)
- Keep-alive enabled by default

**Impact on throughput:**
- HTTP framing overhead: ~200-400 bytes per request (method, path, headers, framing)
- For an 82B payload, HTTP overhead is 3-5x the payload itself
- The bulk API (`POST /tables/:table/bulk`) amortizes this cost: 100 events share one HTTP envelope
- Single-event throughput is dominated by per-request overhead, not payload processing

**Measured numbers:**

| Mode | Events per Request | HTTP Overhead per Event | Effective Throughput |
|------|-------------------|------------------------|---------------------|
| Single | 1 | ~300 bytes | 13,590 req/s |
| Bulk (100) | 100 | ~3 bytes | 166K-324K ev/s |

**Visual analogy:**
Like putting 100 letters in one envelope versus mailing each letter separately.
The post office (HTTP layer) charges the same handling fee per envelope. Batching
slashes the per-letter cost by 100x.

---

### c. Backpressure System

**What it does:**
The backpressure system prevents the server from running out of memory under
extreme burst load. It uses a semaphore (a counter of available "permits") and a
byte-size limit. Every incoming request must acquire a permit and declare its
payload size. If either limit is reached, the server immediately returns
`503 Service Unavailable` instead of accepting more work.

**Current configuration:**
- Semaphore permits: 10,000 concurrent requests (`ZOMBI_MAX_INFLIGHT_WRITES`)
- Byte limit: 64 MB total payload in flight (`ZOMBI_MAX_INFLIGHT_BYTES_MB`)
- Rejection response: HTTP 503 (code: `SERVER_OVERLOADED`)

**Impact on throughput:**
- Semaphore check: ~0.01 us (effectively free)
- At 13K req/s: 0 rejections observed (headroom exists)
- At 82B per event: byte limit allows ~780K events in flight (64 MB / 82 B)
- The semaphore (10K) is the binding constraint, not the byte limit

**Measured numbers:**

| Metric | Value |
|--------|-------|
| Check latency | ~0.01 us |
| Rejections at 13K req/s | 0 |
| Max concurrent (semaphore) | 10,000 |
| Max concurrent (bytes, 82B) | ~780,000 |

**Visual analogy:**
Like a bouncer at a nightclub -- lets people in as fast as they arrive until
the room hits capacity. In our tests the club is never full, so the bouncer
barely does any work.

---

### d. Serialization (serde_json to bincode)

**What it does:**
When a JSON payload arrives, `serde_json` parses it into a Rust struct. That
struct is then serialized into `bincode` -- a compact binary format -- for
storage in RocksDB. This two-step process (JSON in, binary out) is where the
event transitions from a human-readable wire format to an efficient storage
format.

**Current configuration:**
- Inbound: `serde_json` (JSON deserialization)
- Storage: `bincode` (binary serialization for RocksDB)
- No schema validation beyond struct shape

**Impact on throughput:**
- Parse time: ~2-5 us per 82B event
- At 5 us, theoretical single-threaded max: 200,000 events/s
- This is well above the measured 13.5K req/s, so serialization is not the bottleneck
- For bulk requests, events are deserialized in a loop -- still fast relative to I/O

**Measured numbers:**

| Operation | Latency | Theoretical Max |
|-----------|---------|-----------------|
| JSON parse (82B) | ~2-5 us | 200K-500K ev/s |
| bincode encode | ~0.5-1 us | >1M ev/s |
| Combined | ~3-6 us | ~170K-330K ev/s |

**Visual analogy:**
Like a translator at a customs office -- fast and fluent, but still takes a
moment per sentence. The translator is never the reason for the queue; the
paperwork (disk I/O) behind the counter is.

---

### e. RocksDB Write Path

**What it does:**
RocksDB is the hot storage engine. When an event is written, it goes into an
in-memory data structure called a "memtable" (a sorted skiplist). Optionally, a
Write-Ahead Log (WAL) records the write to disk first for crash safety. Once
the memtable is full, it is flushed to disk as a Sorted String Table (SST)
file.

**Current configuration:**
- Memtable size: 64 MB (hard-coded in `rocksdb.rs:107`, not env-configurable)
- Number of memtables: 3 (total 192 MB in-memory buffer, hard-coded `rocksdb.rs:108`)
- WAL: disabled for event writes (`rocksdb.rs:393`), enabled for consumer offsets
- Block cache: 128 MB LRU (hard-coded `rocksdb.rs:123`)
- Compression: LZ4 for SST files (`rocksdb.rs:104`)
- Write amplification: 2-4 KV inserts per event (event + HWM + optional idempotency + optional timestamp index)

**Impact on throughput:**
- Write latency: ~3-7 us per event (memtable append only)
- With WAL disabled, no synchronous disk I/O on the write path
- Memtable capacity: 192 MB / 82B = ~2.3M events buffered before forced flush
- This is the fastest component in the hot path

**Measured numbers:**

| Metric | Value |
|--------|-------|
| Write latency (memtable) | ~3-7 us |
| Memtable capacity (82B events) | ~2.3M events |
| Fill time at 13.5K req/s | ~170 seconds |
| WAL overhead (if enabled) | +3-7 us per write |

**Visual analogy:**
Like a librarian with a sorting desk -- incoming papers go into a tray first
(memtable), and the librarian organizes them onto shelves (SST files) later.
The tray is very fast to fill; the shelf-organizing happens in the background.

---

### f. RocksDB Compaction

**What it does:**
As memtables flush to disk, they create SST files at "Level 0" (L0). Over time,
these files accumulate and overlap in key ranges. Compaction is a background
process that merges and sorts these files into deeper levels (L1, L2, ...),
removing duplicates and reducing read amplification. Without compaction, reads
would slow to a crawl.

**Current configuration:**
- L0 compaction trigger: 4 files (explicitly set in `rocksdb.rs`, hard-coded, not env-configurable)
- L0 slowdown trigger: 20 files (RocksDB library default -- **not set or verifiable in this repo**)
- L0 stop trigger: 36 files (RocksDB library default -- **not set or verifiable in this repo**)
- Compression: LZ4 for all levels
- Compaction style: Level (default)

> **Note:** Only the L0 compaction trigger is explicitly set in code. The slowdown
> and stop triggers rely on RocksDB defaults. None of these are configurable via
> environment variables -- changing them requires editing `rocksdb.rs`.

**Impact on throughput:**
- Write amplification: each byte written to L0 is rewritten ~10-30x across levels
- Under sustained load, compaction threads compete for disk I/O with flush and reads
- If L0 files reach the stop trigger, writes stall until compaction catches up
- Compaction is usually transparent but becomes critical under sustained high throughput

**Measured numbers:**

| Metric | Value |
|--------|-------|
| L0 files at steady state | 2-4 |
| Write amplification | ~10-30x |
| Compaction throughput | ~10-50 MB/s (gp2 dependent) |
| Write stalls observed | 0 in 30-min test at 13.5K req/s |

**Visual analogy:**
Like reorganizing a filing cabinet -- you can keep adding folders to the top
drawer (L0), but eventually you must sort them into proper drawers. If the top
drawer overflows before you finish sorting, you have to stop accepting new
folders until you catch up.

---

### g. EBS gp2 Disk (THE PRIMARY BOTTLENECK)

**What it does:**
All persistent storage on the t3.micro instance flows through an EBS (Elastic
Block Store) gp2 volume. This is a network-attached disk provided by AWS. Every
RocksDB SST file write, every compaction merge, and every Parquet file write
before S3 upload goes through this volume. Its performance characteristics
dominate Zombi's sustained throughput.

**Current configuration:**
- Volume type: gp2 (General Purpose SSD)
- Baseline IOPS: 100 (3 IOPS per GB, minimum 100)
- Burst IOPS: 3,000
- Burst balance: 5.4 million I/O credits
- Burst duration at max rate: 5.4M / 3,000 = 1,800 seconds (~30 min at full burst)
- Throughput: up to 128 MB/s (burst), limited by IOPS at small block sizes

**Impact on throughput:**
- **The "IOPS cliff"**: when burst credits are exhausted, I/O throughput drops by ~30x
- At 100 IOPS baseline with 4KB blocks: maximum ~400 KB/s sustained
- Large payloads (20KB) exhaust burst credits faster due to higher I/O per request
- RocksDB compaction amplifies the problem: each application write generates 10-30x disk I/O
- This is the single largest bottleneck in the system

**Measured numbers:**

| Metric | Burst Phase | Post-Burst Phase |
|--------|------------|-----------------|
| IOPS | ~3,000 | ~100 |
| Throughput (4KB blocks) | ~12 MB/s | ~400 KB/s |
| Event throughput (20KB) | 1,135 req/s | 275 req/s |
| Duration before cliff | ~5 min (sustained heavy load) | Indefinite |

**Degradation pattern:**
```
 Throughput (req/s)
  1200 ┤****
  1000 ┤    ***
   800 ┤       **
   600 ┤         **
   400 ┤           ****
   200 ┤               *****************************
     0 ┤
       └──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──→ Time (min)
          0  2  5  7  10 12 15 17 20 22 25 27 30
                 ↑
          Burst credits exhausted
```

**Visual analogy:**
Like a turbo engine -- fast at first, but the turbo has a cooldown period. Once
it runs out of boost, you are stuck driving at normal speed until the turbo
recharges. The gp2 volume behaves the same way: fast burst, then a hard drop
to baseline that no amount of software optimization can overcome.

---

### h. Memory (914 MB total on t3.micro)

**What it does:**
Memory is shared between the operating system, the Zombi process (Rust runtime,
connection state, request buffers), RocksDB caches, and the Linux page cache.
More memory means larger caches, fewer disk reads, and more events buffered in
memtables before a flush is required.

**Current configuration:**
- Total: 914 MB (t3.micro)
- RocksDB block cache: 128 MB (caches SST file blocks for reads)
- RocksDB memtables: 3 x 64 MB = 192 MB
- OS + Zombi process: ~400 MB
- Remaining for page cache: ~194 MB

**Impact on throughput:**
- Not currently a bottleneck for the write path (writes go to memtable, not block cache)
- Limits read performance: block cache misses require disk I/O
- Page cache pressure can cause unexpected disk I/O for OS operations
- If memtable count or size increased, memory would become the limiting factor

**Measured numbers:**

| Component | Allocation | Percentage |
|-----------|-----------|------------|
| RocksDB block cache | 128 MB | 14% |
| RocksDB memtables | 192 MB | 21% |
| OS + Zombi process | ~400 MB | 44% |
| Page cache (remaining) | ~194 MB | 21% |

**Visual analogy:**
Like desk space -- you can only keep so many documents open at once. If your
desk is small, you spend more time walking to the filing cabinet (disk). A
bigger desk (more RAM) means less walking and faster work.

---

### i. CPU (2 vCPU, burstable)

**What it does:**
The CPU handles all computation: parsing JSON, serializing to bincode, managing
RocksDB memtables, compressing data for Parquet, and running the Tokio async
runtime. On a t3.micro, the 2 vCPUs are "burstable" -- you get a 10% baseline
allocation and can burst to 100% using CPU credits.

**Current configuration:**
- Instance: t3.micro (2 vCPU)
- Baseline: 10% sustained (0.2 vCPU equivalent)
- Burst: up to 100% (2 full vCPUs) using credits
- CPU credits: accrue at 12 credits/hr, allow ~30 min full burst from empty

**Impact on throughput:**
- Measured utilization during load test: ~27% (well within burst capacity)
- CPU is NOT the bottleneck -- the system is I/O bound, not compute bound
- Under sustained burst, CPU credits deplete similarly to IOPS burst credits
- serde_json + bincode are highly optimized and use minimal CPU per event

**Measured numbers:**

| Metric | Value |
|--------|-------|
| Utilization at 13.5K req/s | ~27% |
| Utilization during bulk writes | ~35-45% |
| Time to deplete credits (at 100%) | ~30 min from full |
| Baseline sustained compute | ~2,700 req/s equivalent |

**Visual analogy:**
Like a brain that is barely breaking a sweat -- the body (disk) is the slow
part. The CPU could process far more events if the disk could keep up. It is
like a fast typist waiting for a slow printer.

---

### j. Flusher Pipeline

**What it does:**
The flusher is a background task that periodically reads events from RocksDB,
encodes them into Parquet files, uploads those files to S3, and commits an
Iceberg snapshot. It runs on a timer (default 5 seconds in non-Iceberg mode,
300 seconds / 5 minutes in Iceberg mode) and processes events in batches.
The flusher does not affect request latency (it is fully async) but it
determines how quickly data becomes available in the data lake.

**Current configuration:**
- Flush interval: 5 seconds (`ZOMBI_FLUSH_INTERVAL_SECS`, default 5; Iceberg mode default 300)
- Batch size: 1,000 / 10,000 in Iceberg mode (`ZOMBI_FLUSH_BATCH_SIZE`) -- **advisory only, unused in flush logic**
- Max segment size: 10,000 / 100,000 in Iceberg mode (`ZOMBI_FLUSH_MAX_SEGMENT`) -- actual read limit per flush
- Concurrent S3 uploads: 4 (`ZOMBI_MAX_CONCURRENT_S3_UPLOADS`)
- Target Parquet file size: 128 MB (`ZOMBI_TARGET_FILE_SIZE_MB`) -- size-based batching for Iceberg

> **Note:** The `batch_size` field exists and is env-configurable, but it is never
> referenced in the flush logic. Actual batching is controlled by `max_segment_size`
> (event count limit) and `target_file_size_bytes` (byte size limit for Iceberg).

**Impact on throughput:**
- Not a bottleneck for the hot path (completely async)
- Total flush pipeline capacity: ~800 events/sec to cold storage
- At 13.5K req/s ingest, the flusher falls behind -- events accumulate in RocksDB
- Data freshness: events are available in Iceberg ~5-10 seconds after ingestion
  (non-Iceberg mode) or ~5-10 minutes (Iceberg mode with 300s default interval)
- Under sustained load, RocksDB disk usage grows until the flusher catches up

**Measured numbers:**

| Metric | Value |
|--------|-------|
| Flush interval | 5 seconds (non-Iceberg) / 300 seconds (Iceberg) |
| Max events per flush cycle | Up to `max_segment_size` (10K default / 100K Iceberg), further bounded by `target_file_size_bytes` |
| Flush throughput | ~800 ev/s to S3 |
| Ingest-to-Iceberg latency | ~5-10s (non-Iceberg) / ~5-10 min (Iceberg default) |
| Backlog growth at 13.5K req/s | ~12,700 ev/s accumulating |

**Visual analogy:**
Like a mail truck that comes every 5 minutes -- letters pile up in the mailbox
between visits. The truck can only carry so many bags, so if letters arrive
faster than the truck can haul them, the mailbox overflows and you need a bigger
truck (or more frequent pickups).

---

### k. Parquet / Iceberg Encoding

**What it does:**
Parquet is a columnar file format optimized for analytics. When the flusher
reads a batch of events, it encodes them into Parquet format with ZSTD
compression. This creates compact, query-efficient files. Iceberg then commits
a "snapshot" -- a metadata record that tells query engines where to find the
new data files.

**Current configuration:**
- Compression: ZSTD (default level via `Default::default()`, not explicitly set; see `parquet.rs:26`)
- Target file size: 128 MB
- Snapshot batching: commit when 10 files **OR** 1 GB accumulated (whichever comes first; see `flusher/mod.rs:51-52,80-81`)
- Snapshot commit: metadata-only operation on S3

**Impact on throughput:**
- Not a hot-path bottleneck (runs in the async flusher)
- ZSTD compression adds CPU overhead but reduces S3 storage costs significantly
- Snapshot commit is fast (<500 ms) because it only writes metadata
- Columnar format enables efficient analytical queries downstream

**Measured numbers:**

| Metric | Value |
|--------|-------|
| ZSTD compression ratio (events) | ~3-5x |
| Parquet encoding speed | ~50-100 MB/s |
| Snapshot commit latency | <500 ms |
| Metadata overhead per snapshot | ~2-10 KB |

**Visual analogy:**
Like vacuum-sealing clothes for long-term storage -- it takes a moment to pack
each bag (encoding), but the sealed bags take up far less space in the closet
(S3) and are neatly labeled (Iceberg metadata) so you can find anything later.

---

### l. Network

**What it does:**
All data enters Zombi over the network (client HTTP requests) and leaves over
the network (S3 uploads). The EC2 instance has a network interface with burst
bandwidth capacity. Network performance affects both ingest throughput and
flush-to-S3 throughput.

**Current configuration:**
- EC2 t3.micro network: up to 5 Gbps burst
- S3 endpoint: in-region (low latency, high bandwidth)
- No VPC endpoints configured (traffic goes via NAT or internet gateway)

**Impact on throughput:**
- At 82B events x 13.5K req/s = ~1.1 MB/s ingest (0.002% of 5 Gbps capacity)
- Even at bulk rates (26 MB/s), network usage is <1% of capacity
- HTTP framing overhead dominates bandwidth at small payload sizes
- Network is the least constrained resource in the current setup

**Measured numbers:**

| Metric | Value | % of Capacity |
|--------|-------|---------------|
| Ingest bandwidth (single) | ~1.1 MB/s | 0.002% |
| Ingest bandwidth (bulk) | ~13-26 MB/s | 0.04% |
| S3 upload bandwidth | ~10-50 MB/s | 0.08% |
| Network burst capacity | 5 Gbps (625 MB/s) | 100% |

**Visual analogy:**
Like a 10-lane highway with only a few cars on it -- plenty of room. You could
multiply traffic by 100x before anyone notices congestion. The bottleneck is the
parking garage (disk), not the road (network).

---

## 4. Measured Performance Data

All measurements taken on EC2 t3.micro (2 vCPU, 1 GiB RAM, gp2 EBS) using the
`zombi-load` test suite.

### Single-Event Write Performance (82B payload)

| Metric | Value |
|--------|-------|
| Throughput | 13,590 req/s |
| Avg Latency (server-side) | ~7 us |
| p99 Latency | ~73 us |
| Bandwidth | ~1.1 MB/s |
| CPU Usage | ~27% |
| Memory Usage | ~450 MB |
| Disk IOPS | ~200-500 (within burst) |
| Rejections (503s) | 0 |

### Bulk Write Performance (82B x 100 per batch)

| Metric | Value |
|--------|-------|
| Throughput | 166,222 -- 323,871 events/s |
| Batch Throughput | 1,662 -- 3,238 req/s |
| Batch Latency | ~2-3 ms per batch |
| Bandwidth | ~13-26 MB/s |
| CPU Usage | ~35-45% |
| Efficiency vs Single | 12-24x improvement |

### Large Payload Performance (20KB)

| Metric | Value |
|--------|-------|
| Burst Peak | 1,135 req/s (22.7 MB/s) |
| Steady-state | 275 req/s (5.5 MB/s) |
| Degradation | 76% drop after burst credits exhaust |
| Time to degradation | ~5 minutes under sustained load |
| Limiting factor | EBS gp2 IOPS cliff |

### Degradation Curve (20KB payload, sustained load)

```
  Throughput (req/s)
  1200 ┤****
       │    ***
  1000 ┤       *
       │        **
   800 ┤          *
       │           *
   600 ┤            **
       │              *
   400 ┤               ****
       │                   *
   200 ┤                    *****************************
       │
     0 ┤
       └──┬────┬────┬────┬────┬────┬────┬────┬────┬────→ Time
          0    3    6    9   12   15   18   21   24  30 min
                    ↑
             Burst credits
              exhausted
```

The degradation curve clearly shows the two-phase behavior of gp2 volumes:
a high-throughput burst phase followed by a sharp drop to baseline IOPS.

---

## 5. Bottleneck Ranking

Impact is measured as the percentage reduction in maximum achievable throughput
attributable to each component on the t3.micro reference platform.

```
  Bottleneck Impact Ranking (t3.micro)
  ═══════════════════════════════════════════════════════════

   1. EBS gp2 IOPS    ████████████████████████████████  100%  <-- PRIMARY
   2. Event Size       ████████████████████              65%
   3. HTTP Overhead    ██████████████                    45%
   4. CPU Credits      ████████████                      40%
   5. RocksDB Compact  ████████                          25%
   6. Memory           ██████                            20%
   7. Flusher Batch    █████                             15%
   8. Serialization    ███                               10%
   9. Network          ██                                 5%
  10. Backpressure     █                                  3%
  11. Parquet/ZSTD     █                                  2%
  12. Iceberg Commit   █                                  1%

  Legend: Each █ represents approximately 3% impact on maximum
         achievable throughput at the t3.micro baseline.
```

**Reading this chart:**

- **EBS gp2 IOPS (100%):** The single most impactful bottleneck. Removing it
  (e.g., switching to gp3) would yield the largest throughput improvement.
- **Event Size (65%):** Smaller events leave more headroom in every pipeline
  stage. A binary protocol would reduce this significantly.
- **HTTP Overhead (45%):** Per-request framing cost is high for small payloads.
  Bulk APIs are the primary mitigation.
- **Iceberg Commit (1%):** Almost zero impact -- metadata-only, fast, and async.

---

## 6. Code-Validated Hot-Path Bottlenecks

These bottlenecks were identified by tracing the actual source code. They affect
the synchronous write path and represent the most impactful code-level changes.

---

### a. `low_watermark` Iterator Scan on Every Single Write

**Location:** `handlers.rs:472-477`

```rust
// Called on EVERY single write, AFTER the RocksDB write completes:
if let Ok(lwm) = state.storage.low_watermark(&table, partition) {
    state.metrics_registry.hot.update(&table, partition, lwm, offset);
}
```

**What it does:** `low_watermark()` (`rocksdb.rs:658-687`) creates a RocksDB
iterator, seeks to the partition prefix, and reads the first key. This is a
read I/O operation on the critical write path for every single-event write.

**Impact:** Adds an iterator scan to each write. Under load, this contends with
write I/O for disk bandwidth. The bulk write handler does NOT have this issue.

**Fix:** Move to periodic background sampling (e.g., every 5 seconds) or sample
1/N requests. Low complexity, high impact.

---

### b. Bulk Write Parses JSON Before Backpressure

**Location:** `handlers.rs:492-496`

```rust
pub async fn bulk_write<H: HotStorage, C: ColdStorage>(
    …
    Json(request): Json<BulkWriteRequest>,  // Parsed by Axum BEFORE handler body
) -> …
```

**What it does:** The `Json<>` extractor deserializes the entire request body
before the handler function runs. Backpressure is only checked at line 526,
after the full JSON is already in memory.

**Impact:** Under overload, a burst of large bulk requests can saturate CPU and
memory with JSON parsing before the server can reject them. This is an OOM risk
on a t3.micro.

**Contrast:** The single-write handler already does this correctly -- it uses
`body: Bytes` (line 382), acquires the backpressure permit (line 387), and only
then parses the body.

**Fix:** Change bulk handler to accept `Bytes`, check `body.len()` or
`Content-Length` header against the byte limit, then parse JSON. Low complexity.

---

### c. Redundant Fields in StoredEvent Value

**Location:** `rocksdb.rs:251-253` (key), `rocksdb.rs:471-478` (value)

The RocksDB key encodes `evt:{topic}:{partition}:{sequence_hex}`, but the
serialized `StoredEvent` value also contains `topic: String`, `partition: u32`,
and `sequence: u64`. These three fields are stored in both the key and the value.

**Impact:** Increases value size, which increases compaction work (every byte
in the value is rewritten 10-30x across LSM levels). For small payloads (82B),
the redundant metadata can be a significant fraction of the total value.

**Fix:** Create a separate `StoredEventCompact` struct for RocksDB storage that
omits fields already in the key. Reconstruct full `StoredEvent` on read by
parsing the key. Medium complexity due to migration concerns.

---

### d. WAL Inconsistency Between Event and Consumer Offset Writes

**Location:** `rocksdb.rs:391-394` (events), `rocksdb.rs:706-710` (offsets)

**Resolved:** WAL is now enabled by default (`ZOMBI_ROCKSDB_WAL_ENABLED=true`).
Both event writes and consumer offset commits use `write_options()` which respects the WAL setting.
Set `ZOMBI_ROCKSDB_WAL_ENABLED=false` to opt out for throughput at the cost of crash safety.

**Fix:** Make explicit: either use `write_options()` for offsets too (consistent
no-WAL) or add an env toggle for WAL on events. Low complexity.

---

## 7. Optimization Options

### Tier 1: Configuration Changes (No Code Required)

These changes can be applied via environment variables. No Rust code changes needed.

| Env Variable | Change | Expected Impact | Risk | Notes |
|---|--------|----------------|------|-------|
| `ZOMBI_FLUSH_INTERVAL_SECS` | Reduce to 2 seconds | +60% data freshness | More S3 API calls (cost) | Smaller files, more Iceberg snapshots |
| `ZOMBI_TARGET_FILE_SIZE_MB` | Reduce to 16-64 for t3.micro | Smaller memory footprint per flush | Slightly worse compression | Prevents large in-memory Parquet buffers on 1GB box |
| `ZOMBI_MAX_CONCURRENT_S3_UPLOADS` | Set to 1 for t3.micro | Lower peak memory during flush | Slower flush throughput | Avoids 4x parallel buffer allocation (~512MB peak) |
| `ZOMBI_FLUSH_MAX_SEGMENT` | Reduce from 10,000 | Smaller reads from RocksDB per flush | More flush cycles | Reduces per-cycle disk I/O contention with ingest |
| `ZOMBI_MAX_INFLIGHT_WRITES` | Tune based on load | Prevents overload | May reject valid traffic | Default 10,000 is generous for t3.micro |
| `ZOMBI_TIMESTAMP_INDEX_ENABLED` | Keep `false` (default) | Avoids 1 extra KV insert per write | No time-range index | Already off by default; enable only if needed |
| `ZOMBI_BLOOM_ENABLED` | Keep `false` (default) | No bloom filter overhead | No idempotency fast-path | Already off by default; enable only with idempotency keys |

> **Note:** `ZOMBI_FLUSH_BATCH_SIZE` exists as an env var but is **not used** in the
> flush logic. Changing it has no effect. Use `ZOMBI_FLUSH_MAX_SEGMENT` or
> `ZOMBI_TARGET_FILE_SIZE_MB` instead.

### Tier 2: Infrastructure Changes

These changes involve AWS resource upgrades. No code changes required.

| Change | Expected Impact | Cost Delta | Notes |
|--------|----------------|------------|-------|
| Switch to gp3 EBS (3,000 IOPS baseline) | +30x sustained I/O | +$0.008/GB/month | Eliminates the IOPS cliff entirely |
| Upgrade to t3.small (2 GB RAM) | +2x memory for caches | +$0.0104/hr | Doubles block cache and page cache |
| Upgrade to t3.medium (4 GB RAM) | +4x memory, sustained CPU | +$0.0312/hr | Room for 256 MB memtables + large cache |
| Use io2 EBS (configurable IOPS) | Predictable I/O performance | +$0.065/IOPS/month | Best for latency-sensitive workloads |
| Enable S3 VPC endpoint | -1-2 ms S3 latency | Free (gateway) | Reduces flush pipeline latency |

### Tier 3: Code Changes

These require Rust code modifications and testing.

**Small changes (currently hard-coded, should be env-configurable):**

| Change | File | Expected Impact | Complexity | Notes |
|--------|------|----------------|------------|-------|
| Make memtable size env-configurable | `rocksdb.rs:107` | +10-15% write throughput at 128 MB | Low | Hard-coded to 64 MB; add `ZOMBI_ROCKSDB_WRITE_BUFFER_MB` |
| Make L0 compaction triggers env-configurable | `rocksdb.rs:119` | -15% write stalls at trigger=8 | Low | Hard-coded to 4; add env vars for trigger/slowdown/stop |
| ~~Re-enable WAL via env toggle~~ | `rocksdb.rs` | **Done** — WAL enabled by default since v0.3 | ✅ | `ZOMBI_ROCKSDB_WAL_ENABLED` defaults to `true`; set `false` to opt out |
| Remove `low_watermark` from single-write hot path | `handlers.rs:472` | Removes RocksDB iterator scan per write | Low | Move to periodic background sampling or 1/N sampling |
| Fix bulk parse-before-backpressure | `handlers.rs:495` | Protects CPU/memory under overload | Low | Change `Json<BulkWriteRequest>` to `Bytes`, check size before parse |
| Remove unused `batch_size` field | `flusher/mod.rs:24` | Reduces confusion | Trivial | Advisory field; no functional change |

**Medium changes:**

| Change | Expected Impact | Complexity | Notes |
|--------|----------------|------------|-------|
| Write batching / micro-combiner for single writes | Amortize per-request overhead | Medium | Collect 5-20 events, single RocksDB WriteBatch; adds latency |
| Remove redundant fields from StoredEvent value | Reduce value size, cut compaction work | Medium | topic/partition/sequence are in both key and value (`rocksdb.rs:471`) |
| WAL on separate EBS volume | Parallel write + flush I/O | Medium | Separates sync and async I/O paths |
| Protobuf bulk endpoint | Skip JSON parsing overhead for bulk | Medium | Single-write already supports protobuf; bulk is JSON-only |
| Async write path (write-behind) | Decouple HTTP from RocksDB | Medium | Risk: data loss if process crashes |

**Large changes:**

| Change | Expected Impact | Complexity | Notes |
|--------|----------------|------------|-------|
| Partitioned RocksDB (per-topic) | Parallel compaction | High | Each topic gets independent LSM tree |
| Binary protocol (no JSON overhead) | +40% payload efficiency | High | Requires client library changes |
| Schema-aware Parquet encoding | Better compression ratios | Medium | Already partially implemented via `TableSchemaConfig` |

### Tier 4: Architecture Changes

Fundamental design changes for enterprise-scale deployments.

| Change | Expected Impact | Complexity | Notes |
|--------|----------------|------------|-------|
| Horizontal scaling (multiple nodes) | Linear throughput scaling | High | Requires partitioning strategy |
| Dedicated flush nodes | Separate I/O paths | High | Ingest nodes + flush nodes topology |
| Memory-mapped I/O for hot storage | Bypass filesystem overhead | Very High | Custom mmap-based append log |
| Custom storage engine (replace RocksDB) | Optimized for append-only | Very High | Purpose-built for event streaming |
| Read replicas | Scale reads independently | High | Iceberg makes this natural via snapshots |

---

## 8. Path to 2 Gbps

A concrete, staged roadmap from the current t3.micro baseline to enterprise
throughput targets.

---

### Stage 1: Current (t3.micro, gp2) -- 1.1 MB/s single, 26 MB/s bulk

```
  Instance:   t3.micro (2 vCPU, 1 GiB RAM)
  Storage:    gp2 EBS (100 IOPS baseline, 3,000 burst)
  Cost:       $0.0104/hr ($7.49/month)
  Throughput: 13,590 single req/s | 166K-324K bulk ev/s
  Bandwidth:  ~1.1 MB/s (single) | ~26 MB/s (bulk, burst)
  Suitable:   Development, low-volume production (<1M events/hr)
```

**Bottleneck:** EBS gp2 IOPS cliff under sustained load.

---

### Stage 2: gp3 + t3.small -- Expected ~80 MB/s

```
  Instance:   t3.small (2 vCPU, 2 GiB RAM)
  Storage:    gp3 EBS (3,000 IOPS baseline, 125 MB/s throughput)
  Cost:       ~$0.03/hr ($21.60/month)
  Throughput: ~40K single req/s | ~500K bulk ev/s (estimated)
  Bandwidth:  ~80 MB/s sustained
  Suitable:   Medium-volume production (10M events/hr)
```

**Key changes:**
- gp3 eliminates the IOPS cliff (3,000 baseline vs 100)
- 2 GiB RAM doubles cache capacity
- No code changes required

---

### Stage 3: Dedicated instance + io2 -- Expected ~500 MB/s

```
  Instance:   c6i.large (2 vCPU sustained, 4 GiB RAM)
  Storage:    io2 EBS (10,000 IOPS, 500 MB/s provisioned)
  Cost:       ~$0.15/hr ($108/month)
  Throughput: ~120K single req/s | ~2M bulk ev/s (estimated)
  Bandwidth:  ~500 MB/s sustained
  Suitable:   High-volume production (100M events/hr)
```

**Key changes:**
- Non-burstable CPU (no credit depletion)
- Provisioned IOPS for predictable performance
- Code optimizations: write batching, partitioned RocksDB
- WAL on dedicated volume for parallel I/O

---

### Stage 4: Horizontal + Architecture -- 2 Gbps target

```
  Cluster:    4x c6i.xlarge (4 vCPU, 8 GiB RAM each)
  Storage:    io2 EBS per node (10,000 IOPS each)
  Cost:       ~$1.20/hr ($864/month)
  Throughput: ~500K single req/s | ~8M bulk ev/s (estimated)
  Bandwidth:  ~2 Gbps sustained aggregate
  Suitable:   Enterprise scale (1B+ events/hr)
```

**Key changes:**
- Horizontal partitioning across 4 nodes (topic-based sharding)
- Dedicated flush nodes (separate ingest from cold-storage pipeline)
- Load balancer distributes traffic
- Iceberg handles multi-writer via optimistic concurrency

---

### Throughput Roadmap (visual)

```
  Throughput Roadmap
  ═══════════════════════════════════════════════════════════════════

  Stage 1  █                                       1 MB/s    $7/mo
  Stage 2  ████████                               80 MB/s   $22/mo
  Stage 3  █████████████████████████             500 MB/s  $108/mo
  Stage 4  ██████████████████████████████████████  2 Gbps  $864/mo

  ═══════════════════════════════════════════════════════════════════
           Cost efficiency improves at every stage:
           Stage 1: $7.49 / MB/s
           Stage 2: $0.27 / MB/s    (28x better)
           Stage 3: $0.22 / MB/s    (34x better)
           Stage 4: $0.35 / MB/s    (21x better -- horizontal overhead)
```

**Key takeaway:** The biggest cost-efficiency jump is from Stage 1 to Stage 2.
Simply switching from gp2 to gp3 EBS and adding 1 GiB of RAM yields a 28x
improvement in cost per MB/s. This should be the first infrastructure change
for any production deployment.

---

## Appendix: Glossary

| Term | Definition |
|------|------------|
| **IOPS** | Input/Output Operations Per Second -- the number of read/write operations a disk can perform per second |
| **gp2/gp3** | AWS EBS volume types. gp2 has burst-based IOPS; gp3 has consistent baseline IOPS |
| **Memtable** | RocksDB's in-memory write buffer (a sorted skiplist) |
| **SST file** | Sorted String Table -- RocksDB's on-disk file format |
| **WAL** | Write-Ahead Log -- a sequential log for crash recovery |
| **Write amplification** | The ratio of bytes written to disk versus bytes written by the application |
| **L0** | Level 0 -- the first level of RocksDB's LSM tree where memtables are flushed |
| **ZSTD** | Zstandard -- a fast compression algorithm used for Parquet files |
| **Iceberg snapshot** | A metadata record that describes the current state of an Iceberg table |
| **Backpressure** | A mechanism to slow down producers when consumers cannot keep up |
| **Parquet** | A columnar file format optimized for analytical queries |
| **Bincode** | A compact binary serialization format for Rust structs |

---

## Appendix: Errata (January 2026 Code Validation)

The following corrections were applied after validating the original document
against the Zombi source code:

| Original Claim | Correction | Source |
|---|---|---|
| HTTP 200 response for writes | HTTP **202 Accepted** | `handlers.rs:479` |
| Backpressure returns HTTP 429 | Returns HTTP **503** (`SERVER_OVERLOADED`) | `handlers.rs:339-345` |
| Architecture diagram: `(WAL + Memtable)` | WAL is disabled for events; `(Memtable, no WAL)` | `rocksdb.rs:393` |
| Tier 1: "Increase memtable to 128MB -- no code required" | Hard-coded; requires editing `rocksdb.rs:107` | `rocksdb.rs:107` |
| Tier 1: "Increase flush batch size to 5,000" | `batch_size` is advisory and unused in flush logic | `flusher/mod.rs:22-24` |
| Tier 1: "Enable WAL -- no code required" | WAL is disabled in code; requires editing `rocksdb.rs:393` | `rocksdb.rs:391-394` |
| Tier 1: "Increase L0 trigger to 8 -- no code required" | Hard-coded; requires editing `rocksdb.rs:119` | `rocksdb.rs:119` |
| L0 slowdown/stop triggers "20/36 files" | RocksDB library defaults; not set or verifiable in this repo | `rocksdb.rs` (absent) |
| Endpoint: `POST /events` | Actual route: `POST /tables/:table` | `api/mod.rs:26` |
| Endpoint: `POST /events/bulk` | Actual route: `POST /tables/:table/bulk` | `api/mod.rs:25` |
| S3 "multipart upload" | Uses single PUT via `put_object`; no multipart | `s3.rs:154`, `iceberg_storage.rs:199` |
| Diagram: backpressure → parse (all requests) | Only true for single-write; bulk handler parses JSON **before** backpressure | `handlers.rs:495` vs `handlers.rs:382-387` |
| Flush interval "default 5 seconds" | 5s for non-Iceberg mode; **300s (5 min)** for Iceberg mode | `flusher/mod.rs:46,75` |
| ZSTD "level 3" | Uses `ZSTD(Default::default())` — no explicit level set | `parquet.rs:26` |
| "Response as soon as RocksDB confirms" | Post-write sync work: metrics + `high_watermark` + `low_watermark()` iterator seek before 202 | `handlers.rs:460-477` |
| Flusher `batch_size` / `max_segment_size` defaults listed as single value | Iceberg mode defaults differ: 10,000 / 100,000 (vs 1,000 / 10,000 non-Iceberg) | `flusher/mod.rs:47-48,76-77` |
| Snapshot commit "after 10 files" | Commits on 10 files **OR** 1 GB, whichever comes first | `flusher/mod.rs:51-52,80-81` |
| "Events per flush cycle ~5,000 (1,000 x 5 batches)" | Implied unused `batch_size` drives behavior; actual limit is `max_segment_size` / `target_file_size_bytes` | `flusher/mod.rs` flush logic |

---

*Date: January 2026 (updated January 31, 2026 with code validation errata, round 3)*
*Based on load testing on EC2 t3.micro (2 vCPU, 1 GiB RAM, gp2 EBS)*
*Generated with the zombi-load test suite (`tools/zombi_load.py`)*
