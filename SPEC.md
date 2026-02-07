# Zombi Specification v2.0

> Iceberg-native event ingestion gateway — the lowest-cost path from events to queryable Iceberg tables.

---

## Vision

**Primary identity:** Zombi is an Iceberg-native event ingestion gateway. It accepts events and produces correct, efficient Iceberg tables.

**Problem:** Getting data into Iceberg today requires expensive, complex infrastructure:
```
Producer → Kafka ($$$) → Flink/Spark ($$$) → Iceberg
           Complex        Complex             Finally queryable
```

**Zombi's Solution:** Direct ingestion to Iceberg with minimal infrastructure:
```
Producer → Zombi → Iceberg
           Simple   Queryable by Spark/Trino/DuckDB
```

**Optional:** Real-time visibility of unflushed data via an Iceberg-compatible plugin (not a separate read API).

---

## Architecture

### Single Node (Current)
```
Producer → HTTP API → RocksDB (hot buffer) → Flusher → Iceberg (S3)
                                                        ↓
                                              Spark/Trino/DuckDB
```

Iceberg is the read interface. An optional Iceberg catalog plugin can merge hot + cold data for real-time reads without bypassing Iceberg semantics.

### Detailed Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                            HTTP Layer (Axum)                                  │
│                                                                               │
│  POST /tables/{table}       POST /tables/{table}/bulk    GET /tables/{table}  │
│  (JSON or Protobuf)         (JSON; Protobuf planned)     (JSON or Arrow IPC)  │
│         │                           │                          ▲              │
│         ▼                           ▼                          │              │
│  ┌─────────────┐           ┌─────────────────┐          ┌─────┴──────┐       │
│  │ Backpressure │           │  Backpressure    │          │ Content    │       │
│  │ (Semaphore + │           │  (Semaphore +    │          │ Negotiation│       │
│  │  AtomicU64   │           │   AtomicU64      │          │            │       │
│  │  byte limit) │           │   byte limit)    │          │ JSON(impl) │       │
│  └──────┬──────┘           └────────┬────────┘          │ Arrow(P1)  │       │
│         │                           │                    └─────┬──────┘       │
│         ▼                           │                          │              │
│  ┌──────────────────┐               │                          │              │
│  │  WriteCombiner    │               │                          │              │
│  │  (optional)       │               │                          │              │
│  │                   │               │                          │              │
│  │  N shards (def 4) │               │                          │              │
│  │  mpsc channels    │               │                          │              │
│  │  micro-batch by   │               │                          │              │
│  │  size or 100μs    │               │                          │              │
│  │  window           │               │                          │              │
│  └──────┬───────────┘               │                          │              │
│         │                           │                          │              │
│         ▼                           ▼                          │              │
│  ┌──────────────────────────────────────────────────────────────┐             │
│  │           RocksDbStorage                                     │             │
│  │                                                              │             │
│  │  Write path:                        Read path:               │             │
│  │  1. Idempotency check (Bloom+get)   1. Range scan by prefix  │             │
│  │  2. Sequence gen (DashMap+AtomicU64) 2. bincode → StoredEvent │             │
│  │  3. WriteBatch:                      3. Optional ts filter    │             │
│  │     evt:{topic}:{part}:{seq} → bin   4. Column projection     │             │
│  │     idem:{topic}:{part}:{key} → seq                          │             │
│  │     hwm:{topic}:{part} → seq                                 │             │
│  │  4. Atomic write (WAL configurable)                          │             │
│  └──────────────────────┬───────────────────────────────────────┘             │
│                         │                                                     │
└─────────────────────────┼─────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
    ┌──────────┐   ┌───────────┐   ┌──────────────┐
    │  Metrics  │   │ RocksDB   │   │ Background   │
    │ Registry  │   │ (disk)    │   │ Flusher      │
    │           │   │           │   │              │
    │ counters  │   │ WAL +     │   │ Timer loop   │
    │ histograms│   │ LSM tree  │   │ (def 5min)   │
    │ Prometheus│   │ memtable  │   │              │
    └──────────┘   └───────────┘   └──────┬───────┘
                                          │
                              ┌───────────┼──────────────┐
                              │    Per-partition flush    │
                              │                          │
                              │  1. Read from RocksDB    │
                              │     (watermark → HWM)    │
                              │  2. Size-based batching   │
                              │     (128MB target)       │
                              │  3. Split by hour        │
                              │     boundary (planned)   │
                              └────────────┬─────────────┘
                                           │
                              ┌─────────────▼──────────────┐
                              │    IcebergStorage           │
                              │                             │
                              │  write_segment():           │
                              │    1. Validate same hour    │
                              │    2. Sort by sequence      │
                              │    3. Write Parquet (Zstd)  │
                              │    4. Upload to S3          │
                              │    5. Track pending files   │
                              │                             │
                              │  commit_snapshot():         │
                              │    1. Build manifest (Avro) │
                              │    2. Build manifest list   │
                              │    3. Write metadata.json   │
                              │    4. Upload all to S3      │
                              │    5. Register with catalog │
                              └─────────────┬──────────────┘
                                            │
                                            ▼
                              ┌──────────────────────────┐
                              │  S3 (Iceberg Table)       │
                              │                           │
                              │  metadata/                │
                              │    v{N}.metadata.json     │
                              │    snap-{id}-manifest.avro│
                              │    manifest-{uuid}.avro   │
                              │  data/                    │
                              │    event_date=YYYY-MM-DD/ │
                              │      event_hour=HH/       │
                              │        partition=N/       │
                              │          data-{uuid}.par  │
                              └──────────┬───────────────┘
                                         │
                  ┌──────────────────────┼──────────────────────┐
                  │                      │                      │
                  ▼                      ▼                      ▼
           ┌────────────┐        ┌─────────────┐        ┌──────────┐
           │ Spark      │        │ Trino       │        │ DuckDB   │
           │            │        │             │        │          │
           │ Direct     │        │ Direct      │        │ Direct   │
           │ Iceberg    │        │ Iceberg     │        │ Iceberg  │
           │ read       │        │ read        │        │ read     │
           └────────────┘        └─────────────┘        └──────────┘
                  │                      │                      │
                  └──────────┬───────────┘                      │
                             ▼                                  │
                  ┌─────────────────────┐                       │
                  │ ZombiCatalog (P2)   │◄──────────────────────┘
                  │ (optional plugin)   │
                  │                     │
                  │ 1. Load Iceberg     │    ┌─────────────────────┐
                  │    metadata (S3)    │───▶│ Zombi HTTP endpoint │
                  │ 2. Get watermark    │    │ GET /tables/{table} │
                  │ 3. Plan: cold files │◄───│ Accept: arrow.stream│
                  │    + hot "virtual   │    └─────────────────────┘
                  │    files"           │
                  │ 4. Merge cold       │
                  │    Parquet + hot    │
                  │    Arrow IPC        │
                  └─────────────────────┘
```

#### Key Data Structures

| Struct | Location | Purpose |
|--------|----------|---------|
| `StoredEvent` | `contracts/storage.rs` | Event after write (has sequence) |
| `BulkWriteEvent` | `contracts/storage.rs` | Event in bulk request (pre-sequence) |
| `AppState<H,C>` | `api/handlers.rs` | Server state: storage, metrics, backpressure |
| `RocksDbStorage` | `storage/rocksdb.rs` | Hot storage: DB handle, DashMap sequences, bloom filters |
| `WriteCombiner` | `storage/combiner.rs` | Sharded micro-batcher with per-event oneshot ack |
| `IcebergStorage` | `storage/iceberg_storage.rs` | Cold storage: S3 client, metadata cache, pending files |
| `BackgroundFlusher` | `flusher/mod.rs` | Timer loop, concurrent S3 uploads, snapshot batching |
| `TableMetadata` | `storage/iceberg.rs` | Iceberg v2 metadata: schemas, snapshots, partitions |

#### RocksDB Key Formats

```
evt:{topic}:{partition}:{sequence:016x}     → bincode(StoredEvent)
idem:{topic}:{partition}:{idempotency_key}  → sequence (8 bytes BE)
hwm:{topic}:{partition}                     → sequence (8 bytes BE)
consumer:{group}:{topic}:{partition}        → offset (8 bytes BE)  [deprecated]
ts:{topic}:{partition}:{ts_hex}:{seq_hex}   → sequence (optional timestamp index)
```

All integer values are stored big-endian for correct lexicographic ordering in RocksDB iterators.

---

## Storage Tiers

| Tier | Technology | Purpose | Latency | Retention |
|------|------------|---------|---------|-----------|
| **L1 Hot** | RocksDB | Write buffer + optional low-latency reads | <1ms | Bounded (delete after flush; configurable window) |
| **L2 Cold** | Iceberg (Parquet) | Analytics queries | 50-500ms | Forever |

### Design Principles

1. **Iceberg is the source of truth** - Writes must produce correct Iceberg metadata; reads should be Iceberg-native
2. **Hot data is a bounded cache** - RocksDB is a write buffer; delete after successful Iceberg commit
3. **Persist the hot/cold boundary** - Flush watermarks are durable and exposed
4. **Optional real-time plugin** - Low-latency reads must be Iceberg-compatible via a catalog/plugin

---

## Data Flow

### Write Path
```
1. Producer sends event via HTTP (table name validated)
2. Zombi writes to RocksDB (WAL configurable; recommended enabled for durability)
3. Returns offset to producer
4. Background flusher splits batches by (event_date, event_hour) and target file size
5. Writes Parquet files to S3
6. Commits Iceberg metadata (atomic)
7. Persists flush watermark and records boundary in snapshot summary (`zombi.watermark.{partition}`, `zombi.high_watermark.{partition}`)
8. Deletes hot data up to watermark (configurable retention window)
9. Optional: Register/update with external REST catalog
```

### Read Path - Primary (Iceberg)
```
Spark/Trino: SELECT * FROM iceberg.zombi.events WHERE timestamp > X

1. Query engine reads Iceberg metadata
2. Prunes partitions based on query
3. Reads Parquet files directly from S3
4. Zombi not involved (stateless)
```

### Read Path - Optional Real-Time Plugin (Planned)
```
Spark/Trino/DuckDB → ZombiCatalog plugin

1. Load Iceberg metadata (cold data)
2. Fetch hot data boundary (watermark)
3. Plan cold files + hot “virtual files”
4. Engine reads cold Parquet + hot Arrow/Parquet bytes
```

### Read Path - HTTP (Hot Buffer Only)
```
GET /tables/{table}?partition=0&offset=1000&limit=100
Accept: application/json                         → JSON response (default)
Accept: application/vnd.apache.arrow.stream      → Arrow IPC bytes (planned)

Operational/tail reads from RocksDB hot buffer. Not Iceberg-consistent.
See API section for full parameter reference.
```

---

## Iceberg Integration

### Table Format
- **Format Version:** Iceberg v2 (v3 when stable)
- **File Format:** Parquet with Zstd compression
- **Partitioning:** By event date/hour (events in a segment must share the same hour)

---

## Operational Notes

- `ZOMBI_FLUSH_BATCH_SIZE` is advisory only and currently unused in flush logic.
  Use `ZOMBI_FLUSH_MAX_SEGMENT` or `ZOMBI_TARGET_FILE_SIZE_MB` to control batching.
- `ZOMBI_ROCKSDB_WAL_ENABLED` controls WAL usage for event writes and consumer offset commits.
- WAL is enabled by default for durability. Disabling WAL (`ZOMBI_ROCKSDB_WAL_ENABLED=false`) is a
  performance mode with potential data loss on crash.
- Flush watermarks are persisted in RocksDB per (topic, partition) to prevent duplicate Iceberg writes on restart.
- Hot data is deleted after successful Iceberg commit (or direct flush in non-Iceberg mode), with an optional retention window
  (`ZOMBI_HOT_RETENTION_SECS`, default 0) for in-flight reads. Cleanup is idempotent and retried on next cycle if it fails.
  Note: the retention window is best-effort during normal runtime only; on startup and shutdown, retention
  is bypassed since there are no in-flight reads.
- RocksDB tuning env vars (defaults in parentheses): `ZOMBI_ROCKSDB_WRITE_BUFFER_MB` (64),
  `ZOMBI_ROCKSDB_MAX_WRITE_BUFFERS` (3), `ZOMBI_ROCKSDB_L0_COMPACTION_TRIGGER` (4),
  `ZOMBI_ROCKSDB_TARGET_FILE_SIZE_MB` (64), `ZOMBI_ROCKSDB_BLOCK_CACHE_MB` (128),
  `ZOMBI_ROCKSDB_BLOCK_SIZE_KB` (16).
- Write combiner (single-write batching) env vars: `ZOMBI_WRITE_COMBINER_ENABLED` (false),
  `ZOMBI_WRITE_COMBINER_SIZE` (10), `ZOMBI_WRITE_COMBINER_WINDOW_US` (100),
  `ZOMBI_WRITE_COMBINER_QUEUE_CAPACITY` (10000), `ZOMBI_WRITE_COMBINER_SHARDS` (4),
  `ZOMBI_WRITE_COMBINER_QUEUE_BYTES_MB` (256).

### Schema
```
┌─────────────────┬──────────┬──────────┬──────────┐
│ Field           │ Type     │ Field ID │ Required │
├─────────────────┼──────────┼──────────┼──────────┤
│ sequence        │ long     │ 1        │ yes      │
│ topic           │ string   │ 2        │ yes      │
│ partition       │ int      │ 3        │ yes      │
│ payload         │ binary   │ 4        │ yes      │
│ timestamp_ms    │ long     │ 5        │ yes      │
│ idempotency_key │ string   │ 6        │ no       │
│ event_date      │ date     │ 7        │ yes      │  ← partition column
│ event_hour      │ int      │ 8        │ yes      │  ← partition column
└─────────────────┴──────────┴──────────┴──────────┘
```

### Sort Order
Data is sorted within each Parquet file by `timestamp_ms ASC, sequence ASC`. This:
- Improves data locality for time-range queries
- Produces tighter min/max column statistics per file
- Enables better compression ratios for time-series data

### Column Statistics
Each DataFile in Iceberg manifests includes `lower_bounds` and `upper_bounds` for indexed columns:
- `sequence` - enables offset-based file pruning
- `partition` - enables partition-aware queries
- `timestamp_ms` - enables time-range file pruning
- `event_date`, `event_hour` - enables partition pruning

Query engines (Spark, Trino, DuckDB) use these statistics to skip files that cannot contain matching rows, significantly improving query performance for selective time-range queries.

### Directory Layout
```
s3://bucket/tables/{topic}/
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json
│   └── snap-{id}.avro
└── data/
    └── event_date=2024-01-15/
        └── event_hour=14/
            └── partition=0/
                ├── {uuid}-00001.parquet
                └── {uuid}-00002.parquet
```

### Catalog Support
| Catalog | Status | Use Case |
|---------|--------|----------|
| Filesystem | ✅ Implemented | Development, simple deployments |
| REST Catalog (client registration) | ✅ Implemented | Auto-register tables in external catalogs |
| REST Catalog API (server, read-only) | ✅ Implemented | Engine discovery via Zombi (`/v1/config`, namespaces, tables, load-table) |
| Hive Metastore | Planned | Legacy Hadoop environments |

### Query Engine Compatibility
| Engine | Support | Notes |
|--------|---------|-------|
| Apache Spark | ✅ | Via Iceberg connector |
| Trino/Presto | ✅ | Via Iceberg connector |
| DuckDB | ✅ | Via iceberg extension |
| Athena | ✅ | Native Iceberg support |
| Snowflake | ✅ | External Iceberg tables |
| Databricks | ✅ | Unity Catalog or external |

---

## API Specification

OpenAPI: `docs/openapi.yaml`

### Write Events

**Single Event:**
```http
POST /tables/{table}
Content-Type: application/json

{
  "payload": "string (UTF-8)",
  "partition": 0,
  "timestamp_ms": 1704067200000,
  "idempotency_key": "unique-key"
}

Response 202:
{
  "offset": 12345,
  "partition": 0,
  "table": "events"
}
```

**Bulk Write:**
```http
POST /tables/{table}/bulk
Content-Type: application/json

{
  "records": [
    {"partition": 0, "payload": "event1"},
    {"partition": 0, "payload": "event2"},
    {"partition": 1, "payload": "event3"}
  ]
}

Response 202:
{
  "offsets": [12345, 12346, 12347],
  "count": 3
}
```

Bulk write accepts both JSON and Protobuf payloads:

- **application/json** — `BulkWriteRequest` with `records` entries. `partition` defaults to 0. `timestamp_ms` is optional (server time if omitted). `idempotency_key` is optional.
- **application/x-protobuf** — `BulkWriteRequest` defined in `proto/event.proto`:
  - `repeated BulkWriteRecord records`
  - `BulkWriteRecord` fields: `bytes payload`, `uint32 partition`, `int64 timestamp_ms`, `string idempotency_key`
  - Defaults: `partition=0`, `timestamp_ms=0` (server time used when 0), empty `idempotency_key` treated as unset.

### Write Events (Protobuf)

Single writes also accept `Content-Type: application/x-protobuf`. Partition is specified via the `X-Partition` header (default 0).

```http
POST /tables/{table}
Content-Type: application/x-protobuf
X-Partition: 0

<binary protobuf bytes>

Response 202:
{
  "offset": 12345,
  "partition": 0,
  "table": "events"
}
```

**Proto schema** (`proto/event.proto`):
```protobuf
syntax = "proto3";
package zombi;

message Event {
  bytes payload = 1;
  int64 timestamp_ms = 2;
  string idempotency_key = 3;
  map<string, string> headers = 4;
}

message BulkWriteRecord {
  bytes payload = 1;
  uint32 partition = 2;
  int64 timestamp_ms = 3;
  string idempotency_key = 4;
}

message BulkWriteRequest {
  repeated BulkWriteRecord records = 1;
}
```

### Table Name Validation

Table names must be safe for internal keys and S3 paths. The following regex is enforced at the API boundary:

```
^[a-zA-Z][a-zA-Z0-9_-]{0,127}$
```

Invalid names return `400 Bad Request`.

### Payload Encoding

The JSON API treats `payload` as UTF-8 text. Binary-safe JSON payloads should use base64 encoding (planned),
and real-time reads should use Arrow IPC via content negotiation (`Accept: application/vnd.apache.arrow.stream`).

### Read Events - Primary (Iceberg)

Iceberg is the read interface. Query engines (Spark/Trino/DuckDB/Athena) read tables directly.

### Read Events - Operational/Tail (Hot Buffer Only)

Lightweight HTTP read for monitoring, debugging, and tailing recent events. Reads from the
RocksDB hot buffer only — does **not** merge with cold storage or provide Iceberg-consistent semantics.

```http
GET /tables/{table}?partition=0&offset=1000&limit=100

Response 200:
{
  "records": [...],
  "count": 100,
  "has_more": true
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `partition` | all | Partition to read from |
| `offset` | 0 | Starting sequence number |
| `limit` | 100 | Max records to return |
| `since` | - | Optional: filter by timestamp (ms) |
| `fields` | all | Optional: column projection (e.g., `payload,timestamp_ms`) |

**Content negotiation:**

| Accept Header | Format | Status |
|---------------|--------|--------|
| `application/json` (default) | JSON object with `records`, `count`, `has_more` | Implemented |
| `application/vnd.apache.arrow.stream` | Arrow IPC stream bytes | Planned (P1) |

The Arrow IPC format is designed for the ZombiCatalog plugin, which presents hot buffer data
as Iceberg-compatible "virtual files" to query engines.

For complete, consistent reads use Iceberg query engines. For real-time merged reads (hot + cold),
use the ZombiCatalog plugin (planned).

The legacy `/tables/{table}/stream` endpoint is deprecated and not part of the v1 architecture.

### Consumer Offsets (Deprecated)

```http
POST /consumers/{group}/commit
{"topic": "events", "partition": 0, "offset": 2000}

GET /consumers/{group}/offset?topic=events&partition=0
{"offset": 2000}
```

### Planned Endpoints (Not Yet Implemented)

- **Arrow IPC response format** — `Accept: application/vnd.apache.arrow.stream` on `GET /tables/{table}` for plugin reads
- **Watermark boundary** — per-partition committed flush watermark

### Iceberg REST Catalog API (Read-Only)

For query-engine discovery, Zombi exposes a read-only subset of the Iceberg REST Catalog API:

```http
GET  /v1/config
GET  /v1/namespaces
GET  /v1/namespaces/{namespace}
GET  /v1/namespaces/{namespace}/tables
GET  /v1/namespaces/{namespace}/tables/{table}
HEAD /v1/namespaces/{namespace}/tables/{table}
```

These routes are isolated from `/tables/*` ingestion/read routes and return Iceberg-compatible
catalog responses, including the current `s3://.../metadata/v{N}.metadata.json` location on load-table.

### Admin APIs

```http
GET /health                    # Basic health check (JSON)
GET /health/live               # Kubernetes liveness probe
GET /health/ready              # Kubernetes readiness probe
GET /stats                     # Server statistics (JSON)
GET /metrics                   # Prometheus metrics format
GET /tables/{table}/metadata   # Iceberg metadata location
POST /tables/{table}/compact   # Trigger compaction (currently not wired)
POST /tables/{table}/flush     # Force flush to Iceberg (currently not wired)
```

#### Health Endpoints

**Liveness Probe** (`GET /health/live`):
```http
GET /health/live

Response 200:
{
  "status": "ok"
}
```

**Readiness Probe** (`GET /health/ready`):
```http
GET /health/ready

Response 200 (ready):
{
  "status": "ready",
  "hot_storage": {"status": "ok"},
  "cold_storage": {"status": "ok"},
  "backpressure": {
    "status": "ok",
    "inflight_writes": 10000,
    "inflight_bytes": 1048576,
    "max_inflight_bytes": 67108864
  }
}

Response 503 (not ready):
{
  "status": "not_ready",
  "hot_storage": {"status": "error", "error": "RocksDB unavailable"},
  "cold_storage": {"status": "ok"},
  "backpressure": {"status": "warning", ...}
}
```

#### Prometheus Metrics

**Metrics Endpoint** (`GET /metrics`):
```http
GET /metrics

Response 200 (text/plain):
# HELP zombi_uptime_secs Server uptime in seconds
# TYPE zombi_uptime_secs gauge
zombi_uptime_secs 3600.5

# HELP zombi_writes_total Total write requests
# TYPE zombi_writes_total counter
zombi_writes_total 150000

# HELP zombi_writes_bytes_total Total bytes written
# TYPE zombi_writes_bytes_total counter
zombi_writes_bytes_total 75000000

# HELP zombi_writes_rate_per_sec Current write rate
# TYPE zombi_writes_rate_per_sec gauge
zombi_writes_rate_per_sec 41.67

# HELP zombi_writes_avg_latency_us Average write latency
# TYPE zombi_writes_avg_latency_us gauge
zombi_writes_avg_latency_us 85.30

# HELP zombi_reads_total Total read requests
# TYPE zombi_reads_total counter
zombi_reads_total 50000

# HELP zombi_reads_records_total Total records read
# TYPE zombi_reads_records_total counter
zombi_reads_records_total 500000

# HELP zombi_reads_rate_per_sec Current read rate
# TYPE zombi_reads_rate_per_sec gauge
zombi_reads_rate_per_sec 13.89

# HELP zombi_reads_avg_latency_us Average read latency
# TYPE zombi_reads_avg_latency_us gauge
zombi_reads_avg_latency_us 120.50

# HELP zombi_errors_total Total errors
# TYPE zombi_errors_total counter
zombi_errors_total 5

# HELP zombi_inflight_bytes Current inflight write bytes
# TYPE zombi_inflight_bytes gauge
zombi_inflight_bytes 1048576

# HELP zombi_inflight_writes_available Available write permits
# TYPE zombi_inflight_writes_available gauge
zombi_inflight_writes_available 9500
```

---

## Scalability

### Single Node Limits
- Writes: ~50,000 events/sec
- Storage: Limited by local disk
- Suitable for: Small-medium workloads, development

### Multi-Node (Deferred)

Multi-node coordination (Redis, partition ownership, distributed idempotency) is future scope
and **not** required for v1 of the ingestion-gateway identity.

### Partition Strategy
- Producers specify partition (hash-based or explicit)
- Each partition has independent offset sequence
- Partitions are local to a node in the current single-node architecture

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| **Core** |
| `ZOMBI_DATA_DIR` | `./data` | RocksDB directory |
| `ZOMBI_HOST` | `0.0.0.0` | HTTP bind address |
| `ZOMBI_PORT` | `8080` | HTTP port |
| **Storage** |
| `ZOMBI_S3_BUCKET` | - | S3 bucket (required for Iceberg) |
| `ZOMBI_S3_ENDPOINT` | AWS | Custom endpoint (MinIO) |
| `ZOMBI_S3_REGION` | `us-east-1` | AWS region |
| `ZOMBI_STORAGE_PATH` | `tables` | Base path in bucket |
| `ZOMBI_S3_MAX_RETRIES` | `5` | Maximum S3 retry attempts |
| `ZOMBI_S3_RETRY_INITIAL_MS` | `100` | Initial backoff delay (ms) |
| `ZOMBI_S3_RETRY_MAX_MS` | `10000` | Maximum backoff delay (ms) |
| **Iceberg** |
| `ZOMBI_ICEBERG_ENABLED` | `true` | Enable Iceberg output |
| `ZOMBI_TARGET_FILE_SIZE_MB` | `128` | Target Parquet file size (flush) |
| `ZOMBI_COMPACTED_FILE_SIZE_MB` | `512` | Target file size after compaction |
| `ZOMBI_SNAPSHOT_THRESHOLD_FILES` | `10` | Min files before snapshot commit |
| `ZOMBI_SNAPSHOT_THRESHOLD_GB` | `1` | Min GB before snapshot commit |
| `ZOMBI_SNAPSHOT_MAX_AGE_SECS` | `1800` | Max seconds before forcing snapshot commit (ensures low-throughput tables commit). Per-table overrides not yet supported |
| `ZOMBI_MAX_CONCURRENT_S3_UPLOADS` | `4` | Max concurrent S3 uploads |
| `ZOMBI_FLUSH_INTERVAL_SECS` | `300` | Flush interval (Iceberg mode) |
| `ZOMBI_FLUSH_MAX_SEGMENT` | `10000` | Max events per flush segment |
| `ZOMBI_ROCKSDB_WAL_ENABLED` | `true` | Enable WAL for durability (set `false` for max throughput) |
| **Catalog** |
| `ZOMBI_CATALOG_URL` | - | REST catalog URL |
| `ZOMBI_CATALOG_NAMESPACE` | `zombi` | Iceberg namespace |
| `ZOMBI_CATALOG_TOKEN` | - | REST catalog auth token |
| **Backpressure** |
| `ZOMBI_MAX_INFLIGHT_WRITES` | `10000` | Max concurrent writes |
| `ZOMBI_MAX_INFLIGHT_BYTES_MB` | `64` | Max inflight bytes (MB) |
| **Scaling (Deferred)** |
| `ZOMBI_REDIS_URL` | - | Reserved for future multi-node coordination |
| `ZOMBI_NODE_ID` | `auto` | Reserved for future multi-node coordination |
| **Optimization** |
| `ZOMBI_TIMESTAMP_INDEX_ENABLED` | `false` | Enable timestamp secondary index |
| **Observability** |
| `RUST_LOG` | `zombi=info` | Log level |

---

## Tuning Guide

### File Size Thresholds

Zombi uses two file size settings that affect performance:

| Setting | Env Variable | Default | Purpose |
|---------|--------------|---------|---------|
| Flush file size | `ZOMBI_TARGET_FILE_SIZE_MB` | 128 | Target size for initial Parquet files |
| Compacted file size | `ZOMBI_COMPACTED_FILE_SIZE_MB` | 512 | Target size after compaction merges |

**Recommended settings by scale:**

| Workload | Events/day | Flush Size | Compacted Size | Notes |
|----------|------------|------------|----------------|-------|
| Development | <100K | 16 MB | 64 MB | Faster iteration |
| Small | 100K-1M | 64 MB | 256 MB | Balanced |
| Medium | 1M-10M | 128 MB | 512 MB | Default settings |
| Large | 10M-100M | 256 MB | 1024 MB | Fewer files, larger batches |

**Tuning principles:**
- Larger files = fewer S3 PUTs = lower cost and less metadata
- Smaller files = more frequent flushes = lower data loss window
- Compacted files should be 2-4x the flush size for efficient merging

**Example configuration for high-throughput:**
```bash
export ZOMBI_TARGET_FILE_SIZE_MB=256
export ZOMBI_COMPACTED_FILE_SIZE_MB=1024
export ZOMBI_FLUSH_INTERVAL_SECS=60
```

---

## Invariants

| ID | Invariant | Scope |
|----|-----------|-------|
| INV-1 | Sequences are monotonically increasing | Per partition |
| INV-2 | No data loss after ACK (requires WAL enabled) | Always |
| INV-3 | Order preserved within partition | Per partition |
| INV-4 | Idempotent writes | Per partition |
| INV-5 | Iceberg commits are atomic | Always |
| INV-6 | Compaction preserves all data | Always |
| INV-7 | Hot storage bounded (delete after flush + retention window) | Per partition |
| INV-8 | Flush watermarks survive restart (persisted in RocksDB) | Always |
| INV-9 | Topic names validated at API boundary | Always |

---

## Non-Goals

These are explicitly **not** in scope:

- **Kafka replacement semantics** - No durable log semantics or consumer group balancing
- **Exactly-once delivery** - At-least-once with idempotency keys
- **Complex stream processing** - Use Flink/Spark for that
- **Schema enforcement** - Payload is opaque bytes
- **Multi-tenancy** - Single tenant per deployment
- **Multi-node coordination (v1)** - Redis coordination/partition ownership are future scope
- **Transaction support** - Append-only model
- **Message replay by time** - Use Iceberg time travel instead

---

## Positioning

Zombi is an ingestion gateway to Iceberg, **not** a Kafka replacement.

**Use Zombi when:**
- Primary goal is analytics-ready Iceberg tables
- You want the lowest-cost ingestion path (single binary + S3)
- Reads are via Iceberg engines, with optional real-time plugin

**Use Kafka when:**
- You need durable log semantics and consumer groups
- You run complex streaming topologies
- You depend on the Kafka Connect ecosystem

---

## Version History

| Version | Status | Key Features |
|---------|--------|--------------|
| v0.1 | ✅ Done | RocksDB + S3 JSON |
| v0.2 | ✅ Done | Iceberg/Parquet output |
| v0.3 | 🚧 Next | Correctness hardening (watermarks, bounded hot buffer) |
| v0.4 | Planned | Iceberg catalog API + real-time plugin (optional) |
| v1.0 | Planned | Production ready |
