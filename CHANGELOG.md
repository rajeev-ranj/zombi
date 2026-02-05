# Changelog

All notable changes to Zombi are documented here.

## [Unreleased]

### Added
- **Comprehensive Observability** (Issue #9, #10)
  - `GET /metrics` - Prometheus metrics endpoint
  - `GET /health/live` - Kubernetes liveness probe
  - `GET /health/ready` - Kubernetes readiness probe
  - Flush pipeline metrics (`zombi_flush_total`, `zombi_flush_events_total`, `zombi_flush_bytes_total`, `zombi_flush_duration_us`)
  - Iceberg/cold storage metrics (`zombi_parquet_files_written_total`, `zombi_iceberg_snapshots_committed_total`, `zombi_s3_errors_total`)
  - Consumer lag metrics (`zombi_consumer_lag`, `zombi_high_watermark`, `zombi_committed_offset`)
  - Hot storage metrics (`zombi_hot_storage_events`)
  - Latency histograms (`zombi_write_latency_us`, `zombi_read_latency_us`)
  - Per-topic breakdowns (`zombi_writes_by_topic_total`, `zombi_reads_by_topic_total`)
  - Backpressure tracking (`zombi_backpressure_rejections_total`)
  - Grafana dashboard with pre-configured panels

- **Write Combiner** (Issue #86)
  - Batches concurrent single-event writes into RocksDB `WriteBatch` operations
  - Configurable via `ZOMBI_WRITE_COMBINER_ENABLED` env var
  - A/B tested on t3.micro: +4.3% throughput, −15.9% p99 latency at c=500
  - No regression on bulk write path (bypasses combiner)

- **Bulk Protobuf Support** (Issue #85)
  - `POST /tables/{table}/bulk` accepts `application/x-protobuf`

- **Monitoring Infrastructure**
  - `docker-compose.monitoring.yml` - Full observability stack (Zombi + MinIO + Prometheus + Grafana)
  - Prometheus scrape configuration
  - Grafana provisioning with data source and dashboard

- **Sorted Writes** - Events sorted by `timestamp_ms ASC, sequence ASC` before writing to Parquet
  - Improves data locality for time-range queries
  - Produces tighter column statistics per file
  - Enables better compression ratios
  - Sort order metadata included in Iceberg table metadata

- **Column Statistics** - Iceberg `lower_bounds`/`upper_bounds` populated in DataFile manifests
  - Enables query engines (Spark, Trino, DuckDB) to skip files during planning
  - Statistics for: `sequence`, `partition`, `timestamp_ms`, `event_date`, `event_hour`

- **Table Name Validation** (Issue #101) — API rejects invalid table names at boundary (must match `^[a-zA-Z][a-zA-Z0-9_-]{0,127}$`)

### Changed
- **Hot-only HTTP reads** — `GET /tables/{table}` now reads from RocksDB hot buffer only; cold/historical reads go through Iceberg engines
- RocksDB event value encoding is now compact (payload + timestamp + idempotency key only).
  - New binaries can read legacy values.
  - Older binaries cannot read newly written values.

### Fixed
- Iceberg metadata files now created after flush (snapshots + version metadata)

### Planned
- **P0 Correctness Hardening**
  - Hour-boundary flush splitting
  - Persist flush watermarks (restart-safe)
  - Delete hot data after Iceberg commit (bounded hot buffer)
  - Enable WAL by default (explicit performance opt-out)
- **P1 Iceberg-Native Interfaces**
  - Iceberg REST Catalog API (server-side)
  - Arrow IPC content negotiation on read endpoint (`Accept` header)
  - Watermark boundary endpoint
  - Watermark in snapshot summary
  - Compaction snapshot rewrite integration
  - Configurable snapshot thresholds
- **P2 Optional Real-Time Plugin**
  - ZombiCatalog + ZombiTable + hot InputFile
  - End-to-end engine integration tests
- **P3 Operational Maturity**
  - Wire `/flush` and `/compact` admin endpoints
  - Deprecate/remove consumer offsets
  - Optional auth (token/mTLS)
- **P4 Enhancements**
  - JSON base64 payload support
  - Debug read dedup fix
  - Idempotency atomicity when combiner disabled

---

## [0.2.0] - 2026-01-18

### Added
- **Iceberg Integration**
  - Parquet output with Zstd compression
  - Iceberg v2 table metadata generation
  - Time-based partitioning (`event_date`, `event_hour`)
  - `IcebergStorage` backend for cold storage
  - `ColdStorageBackend` enum for unified S3/Iceberg access

- **Compaction**
  - `Compactor` for merging small Parquet files
  - Configurable target file size (default 128MB)

- **REST Catalog**
  - `CatalogClient` for Iceberg REST catalog registration
  - Configurable namespace and credentials

- **Admin Endpoints**
  - `GET /tables/{table}/metadata` - Table storage info
  - `POST /tables/{table}/flush` - Force flush (placeholder)
  - `POST /tables/{table}/compact` - Trigger compaction (placeholder)

- **Flusher Improvements**
  - Size-based batching when Iceberg enabled
  - `target_file_size_bytes` configuration
  - `FlusherConfig::iceberg_defaults()` preset

### Changed
- Cold storage now supports both JSON (S3) and Parquet (Iceberg) formats
- Flusher uses size-based limits for Iceberg mode

### Fixed
- Metadata endpoint now returns actual storage backend info
- Iceberg metadata files now created after flush (snapshots + version metadata)

---

## [0.1.0] - 2026-01-15

### Added
- **Core Storage**
  - RocksDB hot storage with optimized read/write paths
  - S3 cold storage with JSON segments
  - Atomic sequence generator with persistence
  - Background flusher with watermark tracking

- **HTTP API**
  - `POST /tables/{table}` - Write single event (JSON + Protobuf)
  - `POST /tables/{table}/bulk` - Bulk write events
  - `GET /tables/{table}` - Read events (unified hot + cold)
  - `POST /consumers/{group}/commit` - Commit consumer offset
  - `GET /consumers/{group}/offset` - Get consumer offset
  - `GET /health` - Health check
  - `GET /stats` - Server metrics

- **Features**
  - Idempotent writes via `idempotency_key`
  - Multi-partition support
  - Unified reads across hot and cold storage

- **Testing**
  - Property tests for invariants (monotonic, no-loss, order, idempotent, isolation)
  - Concurrency tests
  - Crash recovery tests
  - Integration tests

### Technical Details
- WAL disabled for write throughput optimization
- Data durability via RocksDB memtable flushes and S3 persistence
- Sub-millisecond write latency

---

## Version Numbering

Zombi follows [Semantic Versioning](https://semver.org/):
- **0.x.y**: Pre-production development
- **1.0.0**: Production-ready release (target)

### Roadmap to v1.0

| Version | Focus | Status |
|---------|-------|--------|
| v0.1 | Core storage + API | Done |
| v0.2 | Iceberg integration | Done |
| v0.3 | Correctness hardening | Next |
| v0.4 | Iceberg-native interfaces + optional plugin | Planned |
| v1.0 | Production hardening | Planned |
