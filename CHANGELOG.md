# Changelog

All notable changes to Zombi are documented here.

## [Unreleased]

### Planned
- Streaming endpoint (`GET /tables/{table}/stream`)
- Prometheus metrics endpoint (`/metrics`)
- Redis coordination for multi-node deployment
- Avro manifest files (full Iceberg compliance)

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
| v0.3 | Streaming + observability | Next |
| v0.4 | Multi-node (Redis) | Planned |
| v1.0 | Production hardening | Planned |
