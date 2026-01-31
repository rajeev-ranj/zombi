# Zombi Specification v2.0

> The lowest-cost path from events to Iceberg, with optional streaming support.

---

## Vision

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

**Secondary Goal:** Replace Kafka for simple streaming use cases where consumers need real-time access to recent events.

---

## Architecture

### Single Node (Current)
```
Producer → HTTP API → RocksDB (hot) → Flusher → Iceberg (S3)
                           │                        │
                           ↓                        ↓
                    Streaming Consumer      Analytics (Spark/Trino)
```

### Multi-Node (Target)
```
                    ┌─────────────────────────────────────┐
                    │         Zombi Cluster               │
Producer → LB ────→ │  ┌──────────┐  ┌──────────┐        │
                    │  │ Proxy 1  │  │ Proxy 2  │        │
                    │  │ RocksDB  │  │ RocksDB  │        │
                    │  └────┬─────┘  └────┬─────┘        │
                    └───────┼─────────────┼──────────────┘
                            │             │
                            └──────┬──────┘
                                   ↓
                             ┌──────────┐
                             │  Redis   │ (offset coordination)
                             └────┬─────┘
                                  ↓
                          ┌──────────────┐
                          │   Iceberg    │ ←── Spark/Trino/DuckDB
                          │  (S3/GCS)    │
                          └──────────────┘
```

---

## Storage Tiers

| Tier | Technology | Purpose | Latency | Retention |
|------|------------|---------|---------|-----------|
| **L1 Hot** | RocksDB | Streaming consumers | <1ms | Hours |
| **L2 Cold** | Iceberg (Parquet) | Analytics queries | 50-500ms | Forever |

### Design Principles

1. **Iceberg is the source of truth** - All data eventually lands in Iceberg
2. **RocksDB is ephemeral** - A write buffer for streaming, not permanent storage
3. **Stateless reads for analytics** - Query engines read Iceberg directly
4. **Simple streaming** - Offset-based reads for real-time consumers

---

## Data Flow

### Write Path
```
1. Producer sends event via HTTP
2. Zombi writes to RocksDB (sub-ms, durable via WAL)
3. Returns offset to producer
4. Background flusher batches events
5. Writes Parquet file to S3
6. Commits Iceberg metadata (atomic)
7. Optional: Register with REST catalog
```

### Read Path - Streaming
```
Consumer: GET /tables/{table}/stream?partition=0&offset=1000

1. Read from RocksDB by offset
2. Return events in sequence order
3. Consumer tracks offset, commits periodically
```

### Read Path - Analytics
```
Spark/Trino: SELECT * FROM iceberg.zombi.events WHERE timestamp > X

1. Query engine reads Iceberg metadata
2. Prunes partitions based on query
3. Reads Parquet files directly from S3
4. Zombi not involved (stateless)
```

---

## Iceberg Integration

### Table Format
- **Format Version:** Iceberg v2 (v3 when stable)
- **File Format:** Parquet with Zstd compression
- **Partitioning:** By event date/hour (configurable)

---

## Operational Notes

- `ZOMBI_FLUSH_BATCH_SIZE` is advisory only and currently unused in flush logic.
  Use `ZOMBI_FLUSH_MAX_SEGMENT` or `ZOMBI_TARGET_FILE_SIZE_MB` to control batching.
- `ZOMBI_ROCKSDB_WAL_ENABLED` controls WAL usage for event writes and consumer offset commits.
  Default is `false` (WAL disabled) for throughput.
- RocksDB tuning env vars (defaults in parentheses): `ZOMBI_ROCKSDB_WRITE_BUFFER_MB` (64),
  `ZOMBI_ROCKSDB_MAX_WRITE_BUFFERS` (3), `ZOMBI_ROCKSDB_L0_COMPACTION_TRIGGER` (4),
  `ZOMBI_ROCKSDB_TARGET_FILE_SIZE_MB` (64), `ZOMBI_ROCKSDB_BLOCK_CACHE_MB` (128),
  `ZOMBI_ROCKSDB_BLOCK_SIZE_KB` (16).
- Write combiner (single-write batching) env vars: `ZOMBI_WRITE_COMBINER_ENABLED` (false),
  `ZOMBI_WRITE_COMBINER_SIZE` (10), `ZOMBI_WRITE_COMBINER_WINDOW_US` (100).

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
| REST Catalog | ✅ Implemented | Production (AWS Glue, Tabular, etc.) |
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
  "payload": "base64-or-string",
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
  "events": [
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

### Read Events - Streaming

**By Offset (for streaming consumers):**
```http
GET /tables/{table}/stream?partition=0&offset=1000&limit=100

Response 200:
{
  "events": [
    {"sequence": 1000, "payload": "...", "timestamp_ms": ...},
    {"sequence": 1001, "payload": "...", "timestamp_ms": ...}
  ],
  "next_offset": 1100,
  "has_more": true
}
```

**Long Polling (optional):**
```http
GET /tables/{table}/stream?partition=0&offset=1000&wait_ms=5000

Waits up to 5 seconds for new events if none available.
```

### Read Events - Simple Query

**For light queries (not analytics):**
```http
GET /tables/{table}?since=1704067200000&limit=100

Response 200:
{
  "records": [...],
  "count": 100,
  "has_more": true
}
```

### Consumer Offsets

```http
POST /consumers/{group}/commit
{"topic": "events", "partition": 0, "offset": 2000}

GET /consumers/{group}/offset?topic=events&partition=0
{"offset": 2000}
```

### Admin APIs

```http
GET /health                    # Basic health check (JSON)
GET /health/live               # Kubernetes liveness probe
GET /health/ready              # Kubernetes readiness probe
GET /stats                     # Server statistics (JSON)
GET /metrics                   # Prometheus metrics format
GET /tables                    # List all tables
GET /tables/{table}/metadata   # Iceberg metadata location
POST /tables/{table}/compact   # Trigger compaction
POST /tables/{table}/flush     # Force flush to Iceberg
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

### Multi-Node Architecture

**Coordination via Redis:**
```
Redis stores:
  offset:{topic}:{partition} → next offset (atomic increment)
  consumer:{group}:{topic}:{partition} → committed offset
  flush:{topic}:{partition} → flush watermark
```

**Scaling Strategy:**
1. **Writes:** Any proxy can accept writes (Redis coordinates offsets)
2. **Streaming reads:** Route to proxy that has data in RocksDB, or read from Iceberg
3. **Analytics:** Direct to Iceberg (Zombi not involved)

**Consistency Model:**
- Writes: Strongly consistent (Redis atomic increment)
- Streaming reads: Eventually consistent (local RocksDB or Iceberg)
- Analytics: Read committed (Iceberg snapshots)

### Partition Strategy
- Producers specify partition (hash-based or explicit)
- Each partition has independent offset sequence
- Partitions can be distributed across proxies

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
| `ZOMBI_MAX_CONCURRENT_S3_UPLOADS` | `4` | Max concurrent S3 uploads |
| `ZOMBI_FLUSH_INTERVAL_SECS` | `300` | Flush interval (Iceberg mode) |
| `ZOMBI_FLUSH_MIN_EVENTS` | `10000` | Min events before flush |
| **Catalog** |
| `ZOMBI_CATALOG_TYPE` | `filesystem` | `filesystem` or `rest` |
| `ZOMBI_CATALOG_URL` | - | REST catalog URL |
| `ZOMBI_CATALOG_NAMESPACE` | `zombi` | Iceberg namespace |
| **Backpressure** |
| `ZOMBI_MAX_INFLIGHT_WRITES` | `10000` | Max concurrent writes |
| `ZOMBI_MAX_INFLIGHT_BYTES_MB` | `64` | Max inflight bytes (MB) |
| **Scaling** |
| `ZOMBI_REDIS_URL` | - | Redis URL (enables multi-node) |
| `ZOMBI_NODE_ID` | `auto` | Unique node identifier |
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
| INV-2 | No data loss after ACK | Always |
| INV-3 | Order preserved within partition | Per partition |
| INV-4 | Idempotent writes | Per partition |
| INV-5 | Iceberg commits are atomic | Always |
| INV-6 | Compaction preserves all data | Always |

---

## Non-Goals

These are explicitly **not** in scope:

- **Exactly-once delivery** - At-least-once with idempotency keys
- **Complex stream processing** - Use Flink/Spark for that
- **Schema enforcement** - Payload is opaque bytes
- **Multi-tenancy** - Single tenant per deployment
- **Transaction support** - Append-only model
- **Message replay by time** - Use Iceberg time travel instead

---

## Comparison

| Feature | Kafka | Zombi |
|---------|-------|-------|
| Streaming | Full-featured | Simple (offset-based) |
| Analytics | Requires ETL | Native Iceberg |
| Infrastructure | Brokers + ZK/KRaft | Single binary + S3 |
| Cost | $$$ | $ |
| Latency (write) | ~5ms | <1ms |
| Latency (analytics) | N/A | 50-500ms (Iceberg) |
| Retention | Expensive | Cheap (S3) |
| Query engines | Limited | Spark/Trino/DuckDB/etc |

**Use Zombi when:**
- Primary goal is analytics on event data
- Simple streaming needs (few consumers)
- Cost is a concern
- Want to query with standard SQL engines

**Use Kafka when:**
- Complex streaming topologies
- Many consumer groups with rebalancing
- Exactly-once semantics required
- Kafka Connect ecosystem needed

---

## Version History

| Version | Status | Key Features |
|---------|--------|--------------|
| v0.1 | ✅ Done | RocksDB + S3 JSON |
| v0.2 | ✅ Done | Iceberg/Parquet output |
| v0.3 | 🚧 Next | Partitioning, catalog registration |
| v0.4 | Planned | Redis coordination (multi-node) |
| v1.0 | Planned | Production ready |
