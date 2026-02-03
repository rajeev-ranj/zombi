# Zombi Architecture & Correctness Review — Updated Report

> **Date:** 2026-02-03
> **Scope:** Full codebase review of Zombi — architecture, correctness, product direction, and actionable recommendations.
> **Basis:** Original third-party audit + independent codebase exploration + architectural decisions by maintainer.

---

## 1. What Zombi Is (Updated Identity)

**Zombi is an Iceberg-native event ingestion gateway.** It provides the lowest-cost, lowest-operational-overhead path from production events to queryable Iceberg tables on S3.

### Core Architecture

```
Producers ──HTTP──▶ Zombi ──Parquet+Metadata──▶ Iceberg Table (S3)
                      │                              │
                      │ (hot buffer)                  ▼
                      │                     ┌─────────────────┐
                      │                     │  Query Engines   │
                      │                     │  Spark / Trino / │
                      │                     │  DuckDB / Athena │
                      │                     └────────┬────────┘
                      │                              │
                      └──── Custom Iceberg Plugin ───┘
                           (optional: real-time reads
                            from hot storage)
```

### What Zombi Is NOT

- **Not a Kafka replacement.** It does not provide durable log semantics, consumer group balancing, or multi-node partition ownership.
- **Not an analytics engine.** It does not serve complex queries. Query engines do that via Iceberg.
- **Not a multi-tenant SaaS platform.** Multi-tenancy, auth, quotas are out of scope for v1.

### Architectural Decisions (Confirmed)

| Decision | Rationale |
|----------|-----------|
| **Identity: Iceberg-native ingestion gateway** | Strongest differentiator. Simplest path to value. |
| **Iceberg IS the read interface** | Don't build a custom read API to compete with Spark/Trino/DuckDB. The Iceberg table format is the consumer interface. |
| **Custom Iceberg plugin for real-time reads** | Optional plugin gives engines access to unflushed hot data. Without the plugin, standard Iceberg reads work (cold data only, flush-interval latency). |
| **Hot cache with deletion after flush** | RocksDB is a write buffer, not a permanent store. Delete events after successful Iceberg commit. |
| **Persist coordination state** | Flush watermarks must survive restarts. Store in RocksDB. |
| **Write-only to Iceberg, optimize for reads** | Zombi's value is write quality: optimal Parquet files, correct metadata, fast snapshots. Reads are the engine's job. |

---

## 2. Current Implementation Assessment

### What Works Well

#### Write Path Engineering (Strong)

The write combiner (`src/storage/combiner.rs`) is genuinely well-engineered:

- **Sharded lock-free design:** N independent worker threads (default 4), each with an `mpsc::channel`. Table-hash-based routing eliminates cross-shard contention.
- **Per-table micro-batching:** Configurable time window (default 100μs) and batch size (default 10 events). Flushes on size or deadline, whichever comes first.
- **Atomic byte-level backpressure:** `AtomicU64` tracks inflight bytes across all shards (default limit 256MB). Rejects with `StorageError::Overloaded` when exceeded.
- **Per-event acknowledgment:** Oneshot channels deliver individual write confirmations.
- **Result:** >10,000 single-event writes/sec on t3.micro (2 vCPU, 1GB RAM).

The RocksDB write path (`src/storage/rocksdb.rs`) is correctly designed:

- Atomic `WriteBatch` for event + watermark + idempotency + timestamp index.
- Lock-free sequence generation via `DashMap<_, AtomicSequenceGenerator>`.
- Optional Bloom filters for O(1) idempotency lookups.
- Optional secondary timestamp index for time-range queries.

#### Iceberg Metadata Quality (Production-Grade)

The Iceberg integration (`src/storage/iceberg.rs`, `src/storage/iceberg_storage.rs`) produces **valid Iceberg v2 metadata** that external engines can query:

- **Table metadata:** Correct format-version 2, unique UUID, full schema, partition specs, sort orders, snapshot history.
- **Partition specs:** Two-level identity transforms on `event_date` + `event_hour`.
- **Sort orders:** `timestamp_ms ASC, sequence ASC` — optimizes time-range queries.
- **Manifest files:** Avro-encoded with correct metadata headers, data file entries, and column statistics.
- **Column statistics:** Big-endian encoded bounds per Iceberg spec (min/max for sequence, partition, timestamp, event_date, event_hour).
- **Parquet files:** ZSTD compressed, Arrow-compatible schema, 8 system fields + partition columns.
- **Verified queryable by:** Spark, Trino, DuckDB, Athena, Snowflake (via external tables).

#### Sequence Recovery (Correctly Handled)

`src/storage/sequence.rs` uses `AtomicSequenceGenerator` with optional file persistence. On RocksDB restart, sequences recover from the high watermark (`hwm:topic:partition`). No sequence collisions on restart.

#### Backpressure Design (Sound)

Both the API handlers (`src/api/handlers.rs`) and write combiner apply backpressure before JSON deserialization, preventing memory exhaustion from large payloads. The byte-level tracking is correct and efficient.

---

### What Is Broken (Confirmed Bugs)

#### BUG-1: Flusher Does Not Split Batches by Hour — CRITICAL

**Location:** `src/flusher/mod.rs:211-239` (batch assembly) vs `src/storage/iceberg_storage.rs:337-349` (hour validation)

**The bug:** `BackgroundFlusher` batches events by count/byte-size only. `IcebergStorage::write_segment` validates that all events in a batch share the same `(event_date, event_hour)` and **rejects the entire batch** if they don't.

**Impact:** Under sustained ingest, every hour boundary causes a flush failure for that partition. The flusher retries with the same batch boundaries, causing the partition to get **permanently stuck**. Events accumulate in RocksDB but never reach Iceberg. Eventually, disk fills.

**Severity:** Production-breaking. Will manifest within the first hour of any sustained workload.

**Fix:** The flusher must split batches at hour boundaries before passing to `write_segment`. When a batch spans hours, produce multiple segments (one per hour bucket) and write them separately.

#### BUG-2: Flush Watermarks Not Persisted — CRITICAL

**Location:** `src/flusher/mod.rs:97`

**The bug:** Watermarks stored in `Arc<RwLock<HashMap<(String, u32), u64>>>`. On restart, all watermarks reset to 0. The flusher re-reads events from offset 1 and re-writes to Iceberg, creating **duplicate rows** in the table.

**Impact:** Every restart produces duplicates. For the custom Iceberg plugin, the watermark is the **contract between hot and cold reads** — without it persisted, the plugin cannot determine where cold data ends and hot data begins.

**Severity:** Data integrity violation. Blocks correct plugin implementation.

**Fix:** Store watermarks in RocksDB: `flush:{topic}:{partition} → u64`. On startup, read persisted watermarks. After successful cold commit, update atomically.

#### BUG-3: Hot Storage Never Shrinks — CRITICAL (Operational)

**Location:** `src/storage/rocksdb.rs` — no deletion or TTL logic exists anywhere.

**The bug:** Events remain in RocksDB indefinitely after flush. Under sustained ingest, disk usage grows without bound.

**Impact:** Confirmed by load tests — disk reaches 95% utilization and performance degrades. In production, this leads to disk exhaustion and service failure.

**Severity:** Operationally fatal for any sustained workload.

**Fix:** After successful Iceberg commit, delete RocksDB keys for events up to the committed watermark. Optionally retain a configurable window (e.g., 30 seconds) beyond the watermark for in-flight read requests and plugin transition.

#### BUG-4: Compaction Does Not Update Iceberg Metadata — HIGH

**Location:** `src/storage/compaction.rs`

**The bug:** Compaction rewrites Parquet files (merging small files into larger ones) but does NOT produce a new Iceberg snapshot. After compaction:
- Old (pre-compaction) files are still referenced in Iceberg metadata.
- Compacted files are orphans — no metadata points to them.
- If old files are deleted, query engines get `FileNotFoundException`.
- If old files are kept, compaction provides no read benefit.

**Impact:** Compaction is currently non-functional from an Iceberg perspective. Engines always read the pre-compaction files.

**Severity:** High — compaction is essential for long-running tables (prevents small-file proliferation).

**Fix:** Compaction must produce a new Iceberg snapshot that replaces old data file entries with compacted file entries. This is an Iceberg "rewrite" operation.

#### BUG-5: WAL Disabled by Default — HIGH

**Location:** `src/storage/rocksdb.rs:504` — `opts.disable_wal(!self.wal_enabled)` with default `false`.

**The bug:** With WAL disabled, acknowledged events in RocksDB's memtable are lost on crash. This contradicts documented invariant INV-2 ("No data loss after ACK").

**Impact:** Silent data loss on any non-graceful shutdown (process crash, OOM kill, hardware failure).

**Severity:** High — durability contract is violated.

**Fix:** Enable WAL by default. If WAL-off mode is desired for maximum throughput, document it explicitly as "at-most-once delivery with potential data loss on crash" and require explicit opt-in.

#### BUG-6: Topic/Table Name Validation Missing — HIGH

**Location:** API boundary in `src/api/handlers.rs` — no validation on `{table}` path parameter.

**The bug:** RocksDB keys use `:` as separator (`evt:topic:partition:seq`), S3 paths use `/`. A topic name containing `:` or `/` breaks key parsing and corrupts internal state. A topic containing `..` could potentially traverse S3 directory boundaries.

**Impact:** Data corruption or unintended S3 key layout. Potential path traversal in S3.

**Severity:** High — security and correctness.

**Fix:** Validate topic names at the API boundary. Enforce pattern: `^[a-zA-Z][a-zA-Z0-9_-]{0,127}$` (alphanumeric + underscore/hyphen, 1-128 chars, starts with letter).

#### BUG-7: Snapshot Batching Creates Hidden Freshness Bottleneck — MEDIUM

**Location:** `src/flusher/mod.rs` — snapshot commit thresholds.

**The bug:** Snapshots are committed when `file_count >= 10` OR `total_bytes >= 1GB`. With default 5-second flush interval, a low-throughput table produces ~1 file per cycle. This means **50+ seconds minimum** before a snapshot is committed and data is visible to query engines.

**Impact:** Data freshness is not `flush_interval` (5s) but `flush_interval × snapshot_threshold_files` (50s). This is a surprising and undocumented latency.

**Severity:** Medium — affects data freshness promise.

**Fix:** Make snapshot threshold configurable per table. For latency-sensitive tables, support `snapshot_per_flush` mode (commit immediately after each flush cycle). Document the freshness = flush_interval × snapshot_threshold relationship.

#### BUG-8: Dedup Key in Read API — LOW (under new architecture)

**Location:** `src/api/handlers.rs:767-769`

**The bug:** `dedup_by_key(|e| (e.timestamp_ms, e.sequence))` can drop legitimate events from different partitions that happen to share the same timestamp and sequence number.

**Impact:** Under the new architecture, the custom read endpoint (`GET /tables/{table}`) becomes internal/debug only. Real reads go through Iceberg engines (which have correct dedup via partition-aware file scanning). Severity reduced from HIGH to LOW.

**Fix:** If the read endpoint is retained for debugging, change to `dedup_by_key(|e| (e.topic.clone(), e.partition, e.sequence))`.

#### BUG-9: Payload Encoding Not Binary-Safe — LOW (under new architecture)

**Location:** `src/api/handlers.rs` — response serialization uses `String::from_utf8_lossy`.

**The bug:** Binary payloads (protobuf, Avro, etc.) are corrupted when read back through the JSON API.

**Impact:** Under the new architecture, the primary use case is JSON events (clickstream, application events). The custom Iceberg plugin would read hot data in Arrow/binary format, bypassing JSON encoding. The JSON API corruption only affects direct API reads.

**Fix:** If binary payload support is needed, accept/return base64-encoded payloads in the JSON API. For the plugin's hot data endpoint, serve Arrow IPC or Parquet format (inherently binary-safe).

---

## 3. Architectural Recommendations

### 3.1 The Custom Iceberg Plugin (Key Differentiator)

The most architecturally significant decision is: **Zombi should provide a custom Iceberg Catalog implementation** that gives query engines seamless access to both committed cold data (S3 Parquet) and unflushed hot data (RocksDB).

#### How It Works

```
┌────────────────────────────────────────────────────────────┐
│  Query Engine (Spark / Trino / DuckDB)                     │
│                                                            │
│  spark.sql.catalog.zombi = com.zombi.iceberg.ZombiCatalog  │
│  spark.sql.catalog.zombi.uri = http://zombi:8080           │
│                                                            │
│  SELECT * FROM zombi.events WHERE event_date = '2026-02-03'│
└──────────┬──────────────────────────────────────┬──────────┘
           │                                      │
           ▼                                      ▼
  ┌─────────────────┐                   ┌──────────────────┐
  │  ZombiCatalog    │                   │  Standard Iceberg │
  │  (custom)        │                   │  S3 reads         │
  │                  │                   │  (Parquet files)  │
  │  loadTable() →   │                   │                   │
  │  ZombiTable      │                   │  Committed cold   │
  │                  │                   │  data only        │
  │  planFiles() →   │                   │                   │
  │  S3 data files + │                   │  (flush-interval  │
  │  hot "virtual"   │                   │   latency)        │
  │  files from      │                   │                   │
  │  Zombi API       │                   │                   │
  └─────────┬────────┘                   └──────────────────┘
            │
            ▼
   ┌─────────────────────────────────────────┐
   │  For S3 files: standard Parquet read    │
   │  For hot files: ZombiHotInputFile       │
   │    → HTTP call to Zombi hot data API    │
   │    → returns Arrow batches              │
   │  Merge: engine handles dedup via        │
   │    watermark boundary                   │
   └─────────────────────────────────────────┘
```

#### Key Design Properties

| Property | How It's Achieved |
|----------|-------------------|
| **Backwards compatible** | Without plugin, standard Iceberg reads from S3 work. Plugin is optional. |
| **Graceful degradation** | If Zombi is down, engine falls back to cold-only reads from S3. |
| **No dedup complexity in Zombi** | The watermark defines a clean boundary: sequences < watermark come from S3, sequences >= watermark come from hot. No overlap. |
| **Standard SQL interface** | Consumer writes standard SQL. Only catalog config changes. |
| **Partition pruning works** | Plugin reports hot data partition bounds to engine. Engine prunes normally. |

#### What Zombi Must Expose for the Plugin

1. **Iceberg REST Catalog API** — Standard Iceberg catalog protocol for table discovery and metadata.
2. **Hot data endpoint** — Returns unflushed events for a (table, partition) as Arrow IPC or Parquet bytes (not JSON).
3. **Watermark endpoint** — Returns the last committed flush watermark per (table, partition). This is the boundary between cold and hot.
4. **Watermark in Iceberg metadata** — The committed watermark should also be stored in snapshot summary properties, so the plugin can read it from metadata without a separate API call.

#### Engine Compatibility

| Engine | Plugin Mechanism | Status |
|--------|-----------------|--------|
| **Spark** | Custom `Catalog` + `Table` + `InputFile` (Java/Scala JAR) | Primary target |
| **Trino** | Custom Iceberg connector plugin | Second target |
| **DuckDB** | Custom extension or httpfs-based approach | Future |
| **Athena** | No custom plugin support — cold-only reads | By design |

### 3.2 Flush Pipeline Improvements

The flush pipeline (`src/flusher/mod.rs`) is the most critical component — it's the bridge between hot and cold. Required changes:

1. **Split batches by hour boundary** before passing to `write_segment` (fixes BUG-1).
2. **Persist watermarks** in RocksDB after successful cold commit (fixes BUG-2).
3. **Delete flushed events** from RocksDB after commit, with configurable retention window (fixes BUG-3).
4. **Configurable snapshot thresholds** per table, including `snapshot_per_flush` mode (fixes BUG-7).
5. **Expose committed watermark** in Iceberg snapshot summary properties (enables plugin).

### 3.3 API Surface (Simplified)

Under the new architecture, Zombi's HTTP API surface simplifies to:

| Endpoint | Purpose | Status |
|----------|---------|--------|
| `POST /tables/{table}` | Write single event | Keep (core) |
| `POST /tables/{table}/bulk` | Write batch of events | Keep (core) |
| `GET /v1/config` | Iceberg REST Catalog: config | New |
| `GET /v1/namespaces/{ns}/tables` | Iceberg REST Catalog: list tables | New |
| `GET /v1/namespaces/{ns}/tables/{table}` | Iceberg REST Catalog: load table | New |
| `GET /tables/{table}/hot` | Hot data for plugin (Arrow IPC format) | New |
| `GET /tables/{table}/watermark` | Committed flush watermark | New |
| `POST /tables/{table}/flush` | Admin: force flush | Keep (wire up) |
| `POST /tables/{table}/compact` | Admin: trigger compaction | Keep (wire up) |
| `GET /health` | Health check | Keep |
| `GET /stats` | Operational metrics | Keep |
| `GET /metrics` | Prometheus exporter | Keep |
| `GET /tables/{table}` | Read events (debug/tail only) | Deprecate or keep as debug |

### 3.4 Hot Storage Lifecycle

RocksDB transitions from "unbounded store" to "bounded write buffer":

```
Event arrives → Write to RocksDB
                    │
                    ▼ (flush cycle)
              Flush to S3 Parquet
              Commit Iceberg snapshot
              Update watermark in RocksDB
                    │
                    ▼ (retention window expires)
              Delete from RocksDB
```

**Retention window:** Configurable (default 60 seconds after watermark update). Ensures:
- In-flight hot data API requests complete before deletion.
- Plugin has time to transition between hot and cold boundaries.
- If Iceberg commit fails, events aren't lost (still in RocksDB).

### 3.5 Durability Model (Clarified)

| Mode | WAL | Guarantee | Use Case |
|------|-----|-----------|----------|
| **Durable (default)** | Enabled | No data loss after ACK | Production |
| **Performance** | Disabled | Up to flush-interval data loss on crash | High-throughput, non-critical |

WAL should be **enabled by default**. Performance mode requires explicit opt-in with documented trade-offs.

---

## 4. Product Positioning & Competitive Analysis

### Identity: Iceberg-Native Ingestion Gateway

Zombi occupies a unique position: **the only single-binary system that provides both an ingestion HTTP API and an Iceberg catalog, with optional real-time reads via a custom plugin.**

The traditional pipeline:
```
Producers → Kafka → Kafka Connect / Flink → Iceberg
(3+ systems, significant operational overhead)
```

Zombi's pipeline:
```
Producers → Zombi → Iceberg
(1 system, minimal operational overhead)
```

### Differentiators

| Differentiator | Why It Matters |
|----------------|---------------|
| **Single binary** | No Kafka, no Flink, no Connect. One deployment artifact. |
| **Direct Iceberg writes** | No intermediate format. Events → Parquet + Iceberg metadata. |
| **Built-in Iceberg catalog** | Engines discover tables through Zombi. No external catalog needed. |
| **Custom plugin for real-time** | Sub-second data freshness without sacrificing Iceberg compatibility. |
| **Graceful degradation** | Without plugin, standard Iceberg. Without Zombi, S3 data persists. |

### Competitors

| Competitor | Approach | Zombi's Edge |
|------------|----------|--------------|
| **Redpanda Iceberg Topics** | Kafka API + built-in Iceberg sink | Zombi is simpler (no Kafka protocol overhead). Redpanda requires Kafka clients. |
| **AutoMQ TableTopic** | Kafka-compatible + S3-native storage | Similar direction, but Zombi is lighter weight and Iceberg-first. |
| **Kafka + Connect Iceberg Sink** | Kafka broker + Connect workers + Iceberg sink connector | 3+ systems vs 1. Higher operational overhead. |
| **Flink + Iceberg Sink** | Stream processing + Iceberg writes | Flink is a general-purpose engine. Zombi is purpose-built for ingestion. |
| **Bufstream** | Kafka-compatible, object-storage-native | Focused on Kafka compatibility. Zombi is focused on Iceberg. |

### Where Zombi Should NOT Compete

- **Full stream processing** (Flink, Kafka Streams) — Zombi does ingestion, not transformation.
- **Kafka-compatible protocol** — Don't implement Kafka wire protocol. It's a complexity trap.
- **Multi-tenant SaaS** (in v1) — Focus on single-tenant / self-hosted / managed deployments.
- **Complex query execution** — That's what Spark/Trino/DuckDB are for.

### Viable Business Models

| Model | Fit |
|-------|-----|
| **Open-source core + managed service** | Best fit. Core is OSS, managed service adds operations, monitoring, plugin distribution. |
| **Open-source + commercial plugin** | Plugin (real-time reads) as commercial add-on. Core Iceberg writes are free. |
| **Managed single-tenant deployments** | Per-customer infrastructure. Zombi + monitoring + support. |

---

## 5. Implementation Priority (Ordered)

### P0 — Must Fix Before Any Production Use

These are correctness bugs that will cause data loss or service failure under normal operation.

| ID | Issue | Files | Rationale |
|----|-------|-------|-----------|
| P0-1 | Flusher hour-batching split | `src/flusher/mod.rs` | Partitions get stuck every hour boundary |
| P0-2 | Persist flush watermarks | `src/flusher/mod.rs`, `src/storage/rocksdb.rs` | Restart causes duplicate Iceberg writes |
| P0-3 | Hot deletion after flush | `src/storage/rocksdb.rs`, `src/flusher/mod.rs` | Disk exhaustion under sustained ingest |
| P0-4 | Enable WAL by default | `src/storage/rocksdb.rs` | Silent data loss on crash |
| P0-5 | Topic name validation | `src/api/handlers.rs` | Data corruption / path traversal |

### P1 — Required for Iceberg-Native Architecture

These enable the architectural vision (Iceberg as interface, custom plugin).

| ID | Issue | Files | Rationale |
|----|-------|-------|-----------|
| P1-1 | Iceberg REST Catalog API | `src/api/` (new module) | Engines discover tables through Zombi |
| P1-2 | Hot data endpoint (Arrow IPC) | `src/api/handlers.rs` | Plugin reads unflushed events |
| P1-3 | Watermark endpoint | `src/api/handlers.rs` | Plugin knows hot/cold boundary |
| P1-4 | Watermark in snapshot summary | `src/storage/iceberg_storage.rs` | Plugin reads boundary from metadata |
| P1-5 | Compaction updates Iceberg metadata | `src/storage/compaction.rs`, `src/storage/iceberg_storage.rs` | Compacted files visible to engines |
| P1-6 | Configurable snapshot thresholds | `src/flusher/mod.rs` | Control data freshness per table |

### P2 — Custom Iceberg Plugin

The plugin itself — a separate artifact (Java/Scala JAR for Spark, etc.).

| ID | Issue | Artifact | Rationale |
|----|-------|----------|-----------|
| P2-1 | ZombiCatalog implementation | Java/Scala JAR | Wraps REST Catalog + hot data awareness |
| P2-2 | ZombiTable / ZombiTableScan | Java/Scala JAR | Plans files from both S3 and hot API |
| P2-3 | ZombiHotInputFile | Java/Scala JAR | Reads hot data as Arrow batches |
| P2-4 | Integration tests with Spark | Tests | End-to-end: write → flush → query with plugin |

### P3 — Operational Maturity

| ID | Issue | Files | Rationale |
|----|-------|-------|-----------|
| P3-1 | Wire admin endpoints (flush, compact) | `src/api/handlers.rs`, `src/flusher/mod.rs` | Operational control |
| P3-2 | End-to-end test with external engine | `tests/` | Validates Iceberg metadata works with real engines |
| P3-3 | Deprecate/simplify GET /tables/{table} | `src/api/handlers.rs` | Remove complex hot+cold merge read path |
| P3-4 | Remove consumer offset tracking | `src/storage/rocksdb.rs` | Kafka-ism that doesn't fit ingestion gateway identity |
| P3-5 | Add basic auth (bearer token / mTLS) | `src/api/` | Required for network-exposed deployments |

### P4 — Enhancements

| ID | Issue | Files | Rationale |
|----|-------|-------|-----------|
| P4-1 | Binary payload support (base64 in JSON API) | `src/api/handlers.rs` | Support protobuf/Avro payloads |
| P4-2 | Fix dedup key in read endpoint | `src/api/handlers.rs` | Correctness for retained debug endpoint |
| P4-3 | Idempotency atomicity for non-combiner writes | `src/storage/rocksdb.rs` | Edge case when combiner is disabled |

---

## 6. What the Original Audit Got Right vs Wrong

### Confirmed Findings

| Audit Finding | Verdict | Notes |
|---------------|---------|-------|
| Hot storage never shrinks | ✅ Confirmed | No TTL/deletion in rocksdb.rs |
| Flush watermark in-memory only | ✅ Confirmed | Resets to 0 on restart |
| Flusher hour-batching mismatch | ✅ Confirmed | Most critical production bug |
| Cold reads bypass Iceberg metadata | ✅ Confirmed | But becomes irrelevant under new architecture |
| Dedup key is wrong | ✅ Confirmed | Severity reduced — read endpoint being deprecated |
| Multi-node not implemented | ✅ Confirmed | Expected for v0.x |
| WAL disabled by default | ✅ Confirmed | Durability contract violated |
| Topic name validation missing | ✅ Confirmed | Security + correctness risk |

### Overstated Findings

| Audit Finding | Verdict | Why Overstated |
|---------------|---------|----------------|
| "Not yet production-sound" | ⚠️ Too strong | Issues are implementation gaps, not fundamental design flaws. Architecture is sound. |
| Idempotency not atomic | ⚠️ Partially mitigated | Write combiner serializes per-table writes. Risk only without combiner. |
| Payload UTF-8 lossy | ⚠️ Context-dependent | Primary use case is JSON events. Plugin uses binary format. |
| Cold reads not scalable | ⚠️ Irrelevant | Zombi won't do cold reads under new architecture. |
| "No mature consumer groups" | ⚠️ Wrong framing | Zombi is not a message queue. Consumer groups are out of scope. |

### Missed Findings

| Finding | Severity | Notes |
|---------|----------|-------|
| Compaction doesn't update Iceberg metadata | HIGH | Compaction is non-functional from Iceberg perspective |
| Snapshot batching creates hidden freshness bottleneck | MEDIUM | 50s+ latency vs documented 5s flush interval |
| No end-to-end test with external engines | HIGH | Entire value proposition is untested end-to-end |
| Consumer offset tracking is a misplaced Kafka-ism | LOW | Should be removed or deprecated |
| Write combiner engineering quality | — | Significantly underappreciated |
| Iceberg metadata production quality | — | Significantly underappreciated |
| Sequence recovery correctness | — | Not mentioned but well-handled |

---

## 7. Key Files Reference

| File | Purpose | Key Changes Needed |
|------|---------|-------------------|
| `src/flusher/mod.rs` | Flush pipeline | Hour-split, watermark persistence, hot deletion, snapshot thresholds |
| `src/storage/rocksdb.rs` | Hot storage | Watermark persistence, event deletion, WAL default |
| `src/storage/iceberg_storage.rs` | Iceberg writes | Watermark in snapshot summary, compaction integration |
| `src/storage/compaction.rs` | File compaction | Must produce Iceberg snapshots |
| `src/api/handlers.rs` | HTTP endpoints | Topic validation, hot data endpoint, watermark endpoint |
| `src/api/mod.rs` | Route registration | New catalog + hot data + watermark routes |
| `src/storage/catalog.rs` | Catalog registration | Expand to Iceberg REST Catalog API |
| `src/contracts/storage.rs` | Storage traits | May need watermark persistence trait methods |
| `src/contracts/cold_storage.rs` | Cold storage trait | May need compaction-aware snapshot method |

---

## 8. Verification Strategy

### For P0 (Correctness Fixes)

- **BUG-1 (hour-batching):** Write events spanning an hour boundary. Verify flush succeeds and produces separate Parquet files per hour. Verify engines read both hours correctly.
- **BUG-2 (watermark persistence):** Write events, flush, restart Zombi. Verify flusher resumes from persisted watermark (not 0). Verify no duplicate rows in Iceberg.
- **BUG-3 (hot deletion):** Write events, flush, wait past retention window. Verify RocksDB key count decreased. Verify disk usage bounded.
- **BUG-4 (WAL):** Write events with WAL enabled. Kill process (SIGKILL). Restart. Verify events survive in RocksDB.
- **BUG-5 (topic validation):** Attempt to create topics with `:`, `/`, `..`, empty string, >128 chars. Verify all rejected with 400.

### For P1 (Architecture)

- **Iceberg REST Catalog:** Point Spark at Zombi's catalog endpoint. Verify `SHOW TABLES` lists Zombi-managed tables. Verify `SELECT *` returns data.
- **Hot data + watermark:** Write events (some flushed, some not). Verify watermark endpoint returns correct offset. Verify hot data endpoint returns only unflushed events in Arrow format.
- **Compaction:** Write many small files. Trigger compaction. Verify Iceberg metadata references compacted files (not originals). Verify engine reads succeed.

### For P2 (Plugin)

- **End-to-end with plugin:** Write events → some flushed to Iceberg, some still in hot. Query via Spark with ZombiCatalog. Verify results include BOTH flushed and unflushed events. Verify no duplicates.
- **Degradation:** Stop Zombi. Query via Spark with ZombiCatalog. Verify it falls back to cold-only reads (no error, just stale data).

---

## 9. Summary

Zombi's core engineering — the write combiner, RocksDB storage, and Iceberg metadata generation — is strong. The critical gaps are operational (hour-batching, watermark persistence, hot deletion, WAL defaults) rather than architectural.

The most impactful strategic decision is positioning Zombi as an **Iceberg-native ingestion gateway** with a **custom Iceberg catalog plugin** for real-time reads. This creates a unique product that:

1. Is simpler than any Kafka-based pipeline for the ingestion use case.
2. Is fully compatible with the Iceberg ecosystem (Spark, Trino, DuckDB, Athena).
3. Offers optional sub-second freshness via the plugin, with graceful degradation to standard Iceberg reads.
4. Has a clear open-source + commercial model (core OSS, plugin/managed service as commercial).

The implementation path is: fix P0 correctness bugs → build P1 Iceberg-native architecture → build P2 custom plugin → mature P3 operations. Each phase is independently valuable and shippable.
