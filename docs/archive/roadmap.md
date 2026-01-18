# ⚠️ ARCHIVED - DO NOT USE

> **This document is outdated and kept for historical reference only.**
>
> **The version numbering in this file conflicts with the actual implementation.**
> - This doc says v0.2 = Consumer Groups, v1.0 = Iceberg
> - Reality: v0.2 = Iceberg (implemented), Consumer Groups not yet done
>
> **For current information, see:**
> - [SPEC.md](../../SPEC.md) - Authoritative specification
> - [CHANGELOG.md](../../CHANGELOG.md) - Accurate version history
>
> **Archived:** January 2026

---

# Zombi Development Roadmap (HISTORICAL)

> Phased plan from simple prototype (v0.1) to production system (v1.0) matching HLD vision.
>
> ⚠️ **This plan was superseded. Iceberg was prioritized over consumer groups.**

---

## Version Summary

| Version | Architecture | Timeline | Goal |
|---------|-------------|----------|------|
| **v0.1** | RocksDB + S3 | 4 weeks | Working prototype |
| **v0.2** | + Consumer groups | 2 weeks | Multi-consumer support |
| **v0.3** | + Metrics & observability | 2 weeks | Production visibility |
| **v0.4** | + Redis coordination | 3 weeks | Horizontal scaling |
| **v0.5** | + S3 Express | 2 weeks | Low-latency hot tier |
| **v1.0** | + Iceberg compaction | 3 weeks | Full HLD implementation |

**Total: ~16 weeks to v1.0**

---

## v0.1: Working Prototype (4 weeks)

### Architecture
```
Producer → HTTP API → RocksDB → S3 Standard → Consumer
                         ↑
                    (embedded, single node)
```

### Components
- Single Rust binary
- Embedded RocksDB for hot storage
- S3 Standard for cold storage
- Offset storage in RocksDB
- No external dependencies (except S3)

### Deliverables
| Week | Focus | Tasks |
|------|-------|-------|
| 1 | Storage | Event type, SequenceGenerator, RocksDB write/read |
| 2 | Persistence | S3 client, background flusher, watermark tracking |
| 3 | API | HTTP endpoints (write, read), error handling |
| 4 | Consumer | Offset commit/fetch, basic Dockerfile, README |

### Tests (8 total)
- 3 property tests (monotonic, no-loss, order)
- 4 integration tests (RocksDB, S3, HTTP write, HTTP read)
- 1 crash recovery test

### Success Criteria
```bash
# Start server
./zombi --data-dir ./data --s3-bucket test-bucket

# Write event
curl -X POST localhost:8080/events/test \
  -H "Content-Type: application/x-protobuf" \
  -d @event.pb
# Returns: {"offset": 1}

# Read events
curl "localhost:8080/events/test?offset=0&limit=10"
# Returns events

# Kill and restart - data persists
```

### GitHub Milestones
- [ ] #1 Project setup (Cargo, CI)
- [ ] #2 Event type + protobuf
- [ ] #3 RocksDB storage
- [ ] #4 S3 flusher
- [ ] #5 HTTP write endpoint
- [ ] #6 HTTP read endpoint
- [ ] #7 Consumer offsets
- [ ] #8 Docker + docs

---

## v0.2: Consumer Groups (2 weeks)

### New Features
- Consumer group abstraction
- Multiple consumers per group
- Partition assignment (static, not dynamic rebalancing)
- Consumer lag tracking

### Architecture Change
```
v0.1:  Consumer → GET /events → (any partition)
v0.2:  Consumer → GET /events?group=X → (assigned partitions only)
```

### API Additions
```
POST /consumers/join
{
  "group_id": "analytics",
  "client_id": "consumer-1",
  "topics": ["user-events"]
}
→ {"assigned_partitions": [0, 1, 2, 3]}

POST /consumers/leave
{
  "group_id": "analytics",
  "client_id": "consumer-1"
}

GET /consumers/lag?group_id=analytics
```

### Storage
- Consumer group state in RocksDB
- Key: `group:{group_id}:member:{client_id}`
- Value: `{partitions: [0,1,2,3], last_heartbeat: timestamp}`

### Tests
- [ ] Consumer registration/deregistration
- [ ] Partition assignment correctness
- [ ] Lag calculation

---

## v0.3: Observability (2 weeks)

### New Features
- Prometheus metrics endpoint
- Structured logging (JSON)
- Health check endpoints
- Basic Grafana dashboard

### Metrics
```rust
// Write path
zombi_events_written_total{topic, partition}
zombi_write_latency_seconds{quantile}

// Read path
zombi_events_read_total{topic, partition, source}
zombi_read_latency_seconds{source, quantile}

// Storage
zombi_rocksdb_size_bytes
zombi_flush_watermark{topic, partition}
zombi_consumer_lag{group, topic, partition}

// System
zombi_active_connections
zombi_http_requests_total{method, path, status}
```

### Endpoints
```
GET /health/live    → 200 if process running
GET /health/ready   → 200 if can accept traffic
GET /metrics        → Prometheus format
```

### Deliverables
- [ ] Prometheus metrics crate integration
- [ ] `/metrics` endpoint
- [ ] Health check endpoints
- [ ] Grafana dashboard JSON
- [ ] Alerting rules YAML

---

## v0.4: Redis Coordination (3 weeks)

### Why Redis Now?
- v0.1-v0.3 is single-node only
- To scale horizontally, need shared state
- Redis provides atomic offset generation across proxies

### Architecture Change
```
v0.3 (single node):
  Producer → Proxy → RocksDB → S3

v0.4 (multi-node):
  Producer → LB → Proxy 1 ─┐
                  Proxy 2 ─┼→ Redis (offsets) → S3
                  Proxy N ─┘
                     ↓
                  (Ring buffer per proxy, ephemeral)
```

### New Components
| Component | Purpose |
|-----------|---------|
| Redis client | Offset generation, consumer groups |
| Ring buffer | In-memory buffer per proxy |
| Shared flusher | Coordinates S3 writes across proxies |

### Migration Path
1. Add Redis as optional dependency
2. Feature flag: `--coordination=rocksdb` (default) or `--coordination=redis`
3. Migrate offset generation to Redis
4. Migrate consumer group state to Redis
5. Add ring buffer for local caching

### Redis Schema (from HLD)
```
offset:{topic}:{partition} → int64
consumer:{group}:{topic}:{partition} → int64
watermark:{topic}:{partition} → int64
topic:{topic} → {partitions, retention, ...}
```

### Tests
- [ ] Multi-proxy write correctness
- [ ] Offset uniqueness across proxies
- [ ] Consumer group coordination
- [ ] Redis failover handling

---

## v0.5: S3 Express (2 weeks)

### Why S3 Express?
- S3 Standard: 100-200ms latency
- S3 Express One Zone: 5-10ms latency
- Worth it for hot reads

### Architecture Change
```
v0.4:
  Ring Buffer → S3 Standard → Consumer

v0.5:
  Ring Buffer → S3 Express (24h) → S3 Standard → Consumer
                     ↑                    ↑
                   (hot)               (cold)
```

### Implementation
1. Add S3 Express client configuration
2. Implement tiered write: buffer → S3 Express
3. Implement tiered read: buffer → S3 Express → S3 Standard
4. Add lifecycle policy: S3 Express → S3 Standard after 24h

### Configuration
```yaml
storage:
  hot:
    type: s3-express
    bucket: zombi-hot
    region: us-east-1
    retention_hours: 24
  cold:
    type: s3-standard
    bucket: zombi-cold
    region: us-east-1
```

### Tests
- [ ] S3 Express write/read
- [ ] Tiered read fallback
- [ ] Lifecycle transition

---

## v1.0: Iceberg Compaction (3 weeks) ✅ COMPLETED

### Why Iceberg?
- Analytics queries on cold data
- Schema evolution
- Time travel
- Integration with Spark/Trino

### Architecture (matches HLD)
```
Ring Buffer → S3 Express → Iceberg (S3 Standard)
    L1           L2              L3
  (<1ms)      (5-10ms)       (100-500ms)
```

### Implemented Components
| Component | Status | Location |
|-----------|--------|----------|
| Parquet writer | ✅ | `src/storage/parquet.rs` |
| Iceberg metadata | ✅ | `src/storage/iceberg.rs` |
| IcebergStorage | ✅ | `src/storage/iceberg_storage.rs` |
| Compaction worker | ✅ | `src/storage/compaction.rs` |
| REST Catalog client | ✅ | `src/storage/catalog.rs` |

### Usage
```rust
// Enable Iceberg mode
use zombi::storage::{IcebergStorage, Compactor, CompactionConfig};

// Create Iceberg-compatible cold storage
let storage = IcebergStorage::new("bucket", "tables").await?;

// Write segments (Parquet format)
storage.write_segment("topic", 0, &events).await?;

// Commit snapshot
storage.commit_snapshot("topic").await?;

// Run compaction
let compactor = Compactor::new(client, "bucket", "tables", CompactionConfig::default());
compactor.compact_topic("topic").await?;
```

### Configuration
```bash
ZOMBI_ICEBERG_ENABLED=true
ZOMBI_TARGET_FILE_SIZE_MB=128
```

### Iceberg Schema
```
sequence: long (required)
topic: string (required)
partition: int (required)
payload: binary (required)
timestamp_ms: long (required)
idempotency_key: string (optional)
```

### Tests
- [x] Parquet write/read roundtrip (7 tests)
- [x] Iceberg metadata serialization
- [x] Large batch handling (1000 events)
- [x] Binary payload preservation
- [x] Compaction path extraction
- [x] REST catalog client

---

## Post-v1.0 Roadmap

| Version | Feature | Estimate |
|---------|---------|----------|
| v1.1 | Schema Registry integration | 2 weeks |
| v1.2 | Dynamic consumer rebalancing | 3 weeks |
| v1.3 | gRPC API | 2 weeks |
| v1.4 | Multi-region replication | 4 weeks |
| v2.0 | Raft consensus (replace Redis) | 8 weeks |

---

## Repository Milestones

### GitHub Milestones
```
v0.1.0 - Working Prototype
v0.2.0 - Consumer Groups
v0.3.0 - Observability
v0.4.0 - Horizontal Scaling
v0.5.0 - Low-Latency Hot Tier
v1.0.0 - Production Ready
```

### Branch Strategy
```
main           ← stable, tagged releases
develop        ← integration branch
feature/xyz    ← feature branches
release/v0.x   ← release prep
```

### Release Process
1. Create release branch from develop
2. Version bump in Cargo.toml
3. Update CHANGELOG.md
4. PR to main
5. Tag after merge
6. GitHub Actions builds binaries + Docker

---

## Architecture Evolution

```
v0.1 (Simple)
┌─────────────────┐
│     Proxy       │
│   ┌─────────┐   │
│   │ RocksDB │   │  (embedded)
│   └────┬────┘   │
│        ↓        │
│      S3         │
└─────────────────┘

v0.4 (Scalable)
┌─────────────────────────────────────┐
│        Proxy Cluster                  │
│  ┌──────┐  ┌──────┐  ┌──────┐       │
│  │Ring  │  │Ring  │  │Ring  │       │
│  │Buffer│  │Buffer│  │Buffer│       │
│  └──┬───┘  └──┬───┘  └──┬───┘       │
└─────┼─────────┼─────────┼───────────┘
      │         │         │
      └────────┬┴─────────┘
               ↓
         ┌─────────┐
         │  Redis  │
         └────┬────┘
              ↓
           ┌──────┐
           │  S3  │
           └──────┘

v1.0 (Full HLD)
┌─────────────────────────────────────┐
│        Proxy Cluster                  │
│  ┌──────┐  ┌──────┐  ┌──────┐       │
│  │Ring  │  │Ring  │  │Ring  │       │
│  │Buffer│  │Buffer│  │Buffer│       │
│  └──┬───┘  └──┬───┘  └──┬───┘       │
└─────┼─────────┼─────────┼───────────┘
      │         │         │
      └────────┬┴─────────┘
               ↓
         ┌─────────┐
         │  Redis  │
         └────┬────┘
              ↓
       ┌──────────────┐
       │ S3 Express   │ (L2 hot)
       └──────┬───────┘
              ↓ compaction
       ┌──────────────┐
       │   Iceberg    │ (L3 cold)
       │ (S3 Standard)│
       └──────────────┘
```

---

## Getting Started

### Week 1 Checklist

```bash
# 1. Create repo
gh repo create zombi-project/zombi --public

# 2. Initialize Rust project
cargo init
cargo add tokio axum rocksdb prost serde tracing

# 3. Copy spec files
cp zombi-docs/SPEC.md .
cp zombi-docs/CLAUDE.md .

# 4. Create basic structure
mkdir -p src/{storage,api,proto} tests proto

# 5. Set up CI
mkdir -p .github/workflows
# Add ci.yml

# 6. First commit
git add .
git commit -m "feat: initial project structure"
git push
```

### Parallel Work Streams
```
Developer 1: Storage layer (RocksDB, S3)
Developer 2: API layer (HTTP, protobuf)
AI (Claude): Unit tests, documentation
```
