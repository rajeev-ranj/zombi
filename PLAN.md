# Zombi Implementation Plan

> From current state to production-ready Iceberg ingestion system.

---

## Current State (v0.2)

### Completed
- [x] RocksDB hot storage with optimized read/write paths
- [x] Parquet writer with Zstd compression
- [x] Iceberg metadata generation (v2 format)
- [x] IcebergStorage implementation
- [x] Basic compaction (merge small files)
- [x] REST catalog client
- [x] Bulk write API
- [x] Performance optimizations (2000+ events/sec, <10μs latency)

### Gaps
- [ ] Time-based partitioning (event_date, event_hour)
- [ ] Proper Iceberg manifest files (currently JSON, should be Avro)
- [ ] Catalog registration on flush
- [ ] Streaming read endpoint (`/stream`)
- [ ] Multi-node coordination (Redis)
- [ ] Production hardening

---

## Phase 1: Iceberg Production Ready (v0.3)

**Goal:** Iceberg output that works with Spark/Trino out of the box.

**Timeline:** 2 weeks

### 1.1 Time-Based Partitioning

Add partition columns derived from timestamp:

```rust
// In parquet.rs - add partition columns to schema
pub fn event_schema() -> Schema {
    Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::UInt32, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("idempotency_key", DataType::Utf8, true),
        // NEW: Partition columns
        Field::new("event_date", DataType::Date32, false),
        Field::new("event_hour", DataType::Int32, false),
    ])
}

// Derive from timestamp_ms
fn derive_partitions(timestamp_ms: i64) -> (i32, i32) {
    let datetime = chrono::DateTime::from_timestamp_millis(timestamp_ms);
    let date = datetime.num_days_from_ce(); // days since epoch
    let hour = datetime.hour() as i32;
    (date, hour)
}
```

**Directory structure after:**
```
s3://bucket/tables/events/
└── data/
    └── event_date=2024-01-15/
        └── event_hour=14/
            └── {uuid}.parquet
```

### 1.2 Proper Iceberg Manifests

Replace JSON manifest lists with Avro format:

```rust
// Use apache-avro crate for manifest files
// Manifest list schema (Iceberg spec)
let manifest_list_schema = r#"{
  "type": "record",
  "name": "manifest_list",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_files_count", "type": "int"},
    {"name": "added_rows_count", "type": "long"}
  ]
}"#;
```

### 1.3 Catalog Auto-Registration

Register tables with catalog on first write:

```rust
// In IcebergStorage::write_segment
async fn write_segment(&self, topic: &str, ...) -> Result<...> {
    // ... write parquet ...

    // Auto-register if catalog configured
    if let Some(ref catalog) = self.catalog {
        catalog.ensure_table_registered(topic, &self.metadata).await?;
    }
}
```

### 1.4 Streaming Read Endpoint

Simple offset-based reads for streaming consumers:

```rust
// GET /tables/{table}/stream?partition=0&offset=1000&limit=100
pub async fn stream_read<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
    Query(params): Query<StreamParams>,
) -> Result<Json<StreamResponse>, ApiError> {
    let events = state.storage.read(
        &table,
        params.partition,
        params.offset,
        params.limit,
    )?;

    let next_offset = events.last()
        .map(|e| e.sequence + 1)
        .unwrap_or(params.offset);

    Ok(Json(StreamResponse {
        events,
        next_offset,
        has_more: events.len() == params.limit,
    }))
}
```

### 1.5 Verification Tests

```bash
# Test with Spark
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0 \
  -e "SELECT * FROM iceberg.\`s3://bucket/tables/events\` LIMIT 10"

# Test with DuckDB
duckdb -c "
  INSTALL iceberg;
  LOAD iceberg;
  SELECT * FROM iceberg_scan('s3://bucket/tables/events');
"

# Test with Trino
trino --execute "
  SELECT count(*) FROM iceberg.zombi.events
  WHERE event_date = DATE '2024-01-15'
"
```

### Deliverables
- [ ] Time-based partitioning in Parquet output
- [ ] Avro manifest files (Iceberg compliant)
- [ ] Partition spec in table metadata
- [ ] Auto catalog registration
- [ ] `/tables/{table}/stream` endpoint
- [ ] Spark/Trino/DuckDB verification tests

---

## Phase 2: Scalability Foundation (v0.4)

**Goal:** Multi-node deployment with Redis coordination.

**Timeline:** 3 weeks

### 2.1 Redis Client Integration

```toml
# Cargo.toml
redis = { version = "0.24", features = ["tokio-comp", "connection-manager"] }
```

```rust
// src/coordination/redis.rs
pub struct RedisCoordinator {
    client: redis::Client,
    node_id: String,
}

impl RedisCoordinator {
    /// Atomically get next offset for a partition
    pub async fn next_offset(&self, topic: &str, partition: u32) -> Result<u64, Error> {
        let key = format!("offset:{}:{}", topic, partition);
        let offset: u64 = self.client.incr(&key, 1).await?;
        Ok(offset)
    }

    /// Get/set consumer group offset
    pub async fn commit_offset(&self, group: &str, topic: &str, partition: u32, offset: u64);
    pub async fn get_offset(&self, group: &str, topic: &str, partition: u32) -> Option<u64>;

    /// Coordinate flush across nodes
    pub async fn claim_flush(&self, topic: &str, partition: u32) -> bool;
    pub async fn release_flush(&self, topic: &str, partition: u32);
}
```

### 2.2 Offset Generation Modes

```rust
// Single node: local atomic counter (current)
// Multi-node: Redis INCR

pub trait OffsetGenerator: Send + Sync {
    fn next(&self, topic: &str, partition: u32) -> Result<u64, Error>;
}

// Feature flag selection
#[cfg(feature = "redis")]
type Coordinator = RedisCoordinator;

#[cfg(not(feature = "redis"))]
type Coordinator = LocalCoordinator;
```

### 2.3 Flush Coordination

Prevent duplicate flushes when multiple nodes run:

```rust
// Only one node flushes a partition at a time
async fn flush_partition(&self, topic: &str, partition: u32) -> Result<()> {
    // Try to acquire flush lock
    if !self.coordinator.claim_flush(topic, partition).await {
        return Ok(()); // Another node is flushing
    }

    // Do the flush
    let result = self.do_flush(topic, partition).await;

    // Release lock
    self.coordinator.release_flush(topic, partition).await;

    result
}
```

### 2.4 Streaming Read Routing

For unflushed data, route to the node that has it:

```
Option A: Sticky routing (partition → node affinity)
  - Simpler, but unbalanced load

Option B: Read from Iceberg if not in local RocksDB
  - Higher latency for recent data
  - Simpler architecture (current choice)

Option C: Gossip protocol for data location
  - Complex, not needed initially
```

**Recommendation:** Option B - accept slightly higher latency for recent data in multi-node mode. Most consumers can tolerate T+30 seconds.

### Deliverables
- [ ] Redis client integration
- [ ] `RedisCoordinator` for offset generation
- [ ] Consumer offset storage in Redis
- [ ] Flush coordination (distributed lock)
- [ ] Feature flag for single/multi-node mode
- [ ] Docker Compose for multi-node testing

---

## Phase 3: Production Hardening (v1.0)

**Goal:** Production-ready with observability and reliability.

**Timeline:** 2 weeks

### 3.1 Prometheus Metrics

```rust
// Metrics to expose
zombi_writes_total{topic, partition}
zombi_write_latency_seconds{quantile="0.5|0.9|0.99"}
zombi_write_bytes_total{topic}

zombi_flushes_total{topic, partition}
zombi_flush_latency_seconds
zombi_flush_bytes_total
zombi_flush_lag_seconds{topic, partition}  // time since last flush

zombi_iceberg_commits_total{topic}
zombi_iceberg_files_total{topic}
zombi_iceberg_rows_total{topic}

zombi_compactions_total{topic}
zombi_compaction_bytes_saved

zombi_rocksdb_size_bytes
zombi_rocksdb_memtable_size_bytes
```

### 3.2 Health Checks

```rust
// Liveness: process is running
GET /health/live → 200

// Readiness: can accept traffic
GET /health/ready → 200 (checks RocksDB, S3 connectivity)

// Detailed status
GET /health/status → {
  "rocksdb": "healthy",
  "s3": "healthy",
  "redis": "healthy|disconnected",
  "last_flush": "2024-01-15T14:30:00Z",
  "flush_lag_seconds": 25
}
```

### 3.3 Graceful Shutdown

```rust
async fn shutdown_signal() {
    // 1. Stop accepting new writes
    // 2. Flush all pending data to Iceberg
    // 3. Commit final metadata
    // 4. Close connections
}
```

### 3.4 Error Recovery

```rust
// Retry policy for S3 operations
let retry_config = RetryConfig::default()
    .with_max_retries(3)
    .with_backoff(ExponentialBackoff::default());

// Dead letter handling for failed writes
// (log to local file, retry on restart)
```

### 3.5 Documentation

- [ ] Architecture diagram
- [ ] Deployment guide (single node, multi-node)
- [ ] Configuration reference
- [ ] Query engine setup guides (Spark, Trino, DuckDB)
- [ ] Monitoring/alerting recommendations
- [ ] Troubleshooting guide

### Deliverables
- [ ] Prometheus metrics endpoint
- [ ] Grafana dashboard JSON
- [ ] Health check endpoints
- [ ] Graceful shutdown
- [ ] Retry/recovery logic
- [ ] Production documentation

---

## Implementation Priority

### Must Have (v0.3)
1. Time-based partitioning
2. Avro manifests (Iceberg compliant)
3. Catalog registration
4. Streaming endpoint

### Should Have (v0.4)
5. Redis coordination
6. Multi-node flush coordination
7. Prometheus metrics

### Nice to Have (v1.0)
8. Long polling for streaming
9. Grafana dashboard
10. Helm chart

---

## Testing Strategy

### Unit Tests
- Parquet schema with partition columns
- Avro manifest serialization
- Redis coordinator logic

### Integration Tests
- End-to-end write → flush → query with Spark
- Multi-node write coordination
- Catalog registration flow

### Performance Tests
- Sustained throughput (target: 50K events/sec)
- Latency percentiles (p99 < 10ms writes)
- Flush latency under load

### Compatibility Tests
- Spark 3.4, 3.5
- Trino 4xx
- DuckDB 0.10+
- Iceberg 1.4+

---

## Resource Estimates

| Phase | Duration | Effort |
|-------|----------|--------|
| Phase 1 (v0.3) | 2 weeks | Iceberg compliance |
| Phase 2 (v0.4) | 3 weeks | Multi-node |
| Phase 3 (v1.0) | 2 weeks | Production hardening |
| **Total** | **7 weeks** | |

---

## Next Steps

1. **Immediate:** Add time-based partitioning to Parquet output
2. **This week:** Implement Avro manifest files
3. **Next week:** Streaming endpoint + Spark verification
4. **After v0.3:** Start Redis integration

Start with Phase 1.1 (partitioning) as it's required for efficient analytics queries.
