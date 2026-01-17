# Zombi Specification

> A Kafka-replacement streaming proxy with RocksDB hot storage and S3 cold storage.

---

## Overview

Zombi is a lightweight event streaming system designed for:
- **Simplicity**: Single binary, no ZooKeeper, no broker cluster
- **Low latency**: Sub-millisecond writes to RocksDB
- **Cost efficiency**: Cold storage on S3, 10x cheaper than Kafka
- **Unified API**: Iceberg-style table interface abstracts storage tiers

---

## Architecture

```
Producers → HTTP API → RocksDB (hot) → BackgroundFlusher → S3 (cold)
                              ↑                              ↑
                              └────── Unified Read ──────────┘
```

### Storage Tiers

| Tier | Technology | Latency | Retention |
|------|------------|---------|-----------|
| L1 Hot | RocksDB | <1ms | Hours |
| L2 Cold | S3 JSON Segments | 10-50ms | Forever |

---

## Core Components

### 1. Event
The fundamental data unit.

```protobuf
message Event {
  bytes payload = 1;           // Arbitrary bytes
  int64 timestamp_ms = 2;      // Producer timestamp
  string idempotency_key = 3;  // For deduplication
  map<string, string> headers = 4;
}
```

### 2. Sequence Generator
Assigns monotonically increasing sequence numbers per partition.

**Invariants:**
- `seq[n+1] > seq[n]` always
- Survives process restart
- Lock-free in hot path

### 3. Hot Storage (RocksDB)
Embedded key-value store for recent events.

**Key format:** `evt:{table}:{partition}:{sequence:016x}`
**Value:** Serialized StoredEvent (JSON)

**Operations:**
- `write(table, partition, event) → sequence`
- `read_all_partitions(table, since, limit) → Vec<Event>`
- `list_topics() → Vec<String>`
- `list_partitions(table) → Vec<u32>`

### 4. Cold Storage (S3)
Long-term storage for archived events.

**Segment format:** `segments/{table}/{partition}/{start:016x}-{end:016x}.json`

**Operations:**
- `write_segment(table, partition, events) → segment_id`
- `read_events(table, partition, offset, limit) → Vec<Event>`
- `list_segments(table, partition) → Vec<SegmentInfo>`

### 5. Background Flusher
Automatically moves data from hot to cold storage.

**Behavior:**
- Discovers topics/partitions from RocksDB
- Triggers every N seconds (configurable)
- Writes JSON segments to S3
- Tracks flush watermarks per partition

### 6. HTTP API

#### Write Records
```
POST /tables/{table}
Content-Type: application/json

{"payload": "hello world", "partition": 0}

Response 202:
{
  "offset": 12345,
  "partition": 0,
  "table": "events"
}
```

Or with protobuf:
```
POST /tables/{table}
Content-Type: application/x-protobuf
X-Partition: 0

<protobuf Event>
```

#### Read Records
Reads from all partitions, merged by timestamp. Partitions are abstracted away.

```
GET /tables/{table}
GET /tables/{table}?limit=100
GET /tables/{table}?since=1704067200000

Response 200:
{
  "records": [
    {"payload": "hello", "timestamp_ms": 1704067200000},
    {"payload": "world", "timestamp_ms": 1704067201000}
  ],
  "count": 2,
  "has_more": false
}
```

#### Consumer Offsets
```
POST /consumers/{group}/commit
{
  "topic": "events",
  "partition": 0,
  "offset": 200
}

GET /consumers/{group}/offset?topic=events&partition=0
{
  "group": "my-group",
  "topic": "events",
  "partition": 0,
  "offset": 200
}
```

#### Health Check
```
GET /health

{"status": "healthy"}
```

#### Server Metrics
```
GET /stats

Response 200:
{
  "uptime_secs": 123.45,
  "writes": {
    "total": 1000,
    "bytes_total": 50000,
    "rate_per_sec": 8.1,
    "avg_latency_us": 45.2
  },
  "reads": {
    "total": 200,
    "records_total": 5000,
    "rate_per_sec": 1.6,
    "avg_latency_us": 120.5
  },
  "errors_total": 0
}
```

**Metrics tracked:**
- `writes_total`: Total write requests
- `writes_bytes`: Total bytes written
- `write_latency_sum_us`: Cumulative write latency (microseconds)
- `reads_total`: Total read requests
- `read_records_total`: Total records returned
- `read_latency_sum_us`: Cumulative read latency (microseconds)
- `errors_total`: Total error responses

### 7. Unified Read Path
Transparently merges data from hot and cold storage:

1. Read from RocksDB (all partitions)
2. Read from S3 (all partitions, if configured)
3. Merge by timestamp
4. Deduplicate
5. Apply limit

---

## Invariants

These MUST always hold:

| ID | Invariant | Description |
|----|-----------|-------------|
| INV-1 | Monotonic | Sequence numbers never go backwards |
| INV-2 | No Loss | Every ACKed write is readable |
| INV-3 | Order | Events read in timestamp order |
| INV-4 | Idem | Same idempotency key = same offset |
| INV-5 | Isolation | Partition data stays in partition |
| INV-6 | Recovery | Data survives crash after ACK |

---

## Error Handling

| Error | HTTP Code | Behavior |
|-------|-----------|----------|
| Invalid JSON/protobuf | 400 | Reject, no retry |
| Table not found | 200 | Return empty records |
| S3 unavailable | 200 | Return hot storage only |
| RocksDB error | 500 | Return error |

---

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ZOMBI_DATA_DIR` | `./data` | RocksDB data directory |
| `ZOMBI_HOST` | `0.0.0.0` | HTTP server host |
| `ZOMBI_PORT` | `8080` | HTTP server port |
| `ZOMBI_S3_BUCKET` | - | S3 bucket (enables cold storage) |
| `ZOMBI_S3_ENDPOINT` | AWS | Custom S3 endpoint (for MinIO) |
| `ZOMBI_S3_REGION` | `us-east-1` | AWS region |
| `ZOMBI_FLUSH_INTERVAL_SECS` | `5` | Flush interval in seconds |
| `ZOMBI_FLUSH_BATCH_SIZE` | `1000` | Min events before flush |
| `ZOMBI_FLUSH_MAX_SEGMENT` | `10000` | Max events per segment |
| `RUST_LOG` | `zombi=info` | Log level |

---

## Non-Goals (v0.1)

- Replication / HA
- Consumer groups with rebalancing
- Iceberg/Parquet format
- Exactly-once semantics
- Multi-tenancy
- Authentication

---

## Future (v0.2+)

- Consumer group rebalancing
- Iceberg/Parquet cold storage
- gRPC API
- Raft replication
- Schema registry integration
- Prometheus metrics
