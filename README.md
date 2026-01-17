# Zombi

A lightweight Kafka-replacement event streaming system with RocksDB hot storage and S3 cold storage.

## Features

- **Simple**: Single binary, no ZooKeeper, no broker cluster
- **Low latency**: Sub-millisecond writes to RocksDB
- **Cost efficient**: Cold storage on S3
- **Unified API**: Iceberg-style table interface abstracts storage tiers
- **Dual format**: Supports both JSON and Protobuf writes

## Quick Start

### Build from source

```bash
# Requires protobuf compiler
brew install protobuf  # macOS
# or: apt-get install protobuf-compiler  # Linux

# Build
cargo build --release

# Run (hot storage only)
./target/release/zombi
```

### With S3/MinIO (hot + cold storage)

```bash
# Start MinIO
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create bucket
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/zombi-events

# Run Zombi with S3
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
ZOMBI_S3_BUCKET=zombi-events \
ZOMBI_S3_ENDPOINT=http://localhost:9000 \
cargo run
```

### Docker

```bash
docker build -t zombi .
docker run -p 8080:8080 -v zombi-data:/var/lib/zombi zombi
```

## API

The API uses a **unified table interface** similar to Iceberg, providing consistent access to both hot (RocksDB) and cold (S3) storage.

### Write Records

**JSON format:**
```bash
curl -X POST http://localhost:8080/tables/events \
  -H "Content-Type: application/json" \
  -d '{"payload": "hello world"}'
```

**Protobuf format:**
```bash
curl -X POST http://localhost:8080/tables/events \
  -H "Content-Type: application/x-protobuf" \
  --data-binary <protobuf-event>
```

Response:
```json
{"offset": 1, "partition": 0, "table": "events"}
```

### Read Records

Reads from all partitions, merged by timestamp. Partitions are abstracted away.

```bash
# Read all records
curl "http://localhost:8080/tables/events"

# With limit
curl "http://localhost:8080/tables/events?limit=100"

# Filter by timestamp
curl "http://localhost:8080/tables/events?since=1704067200000"
```

Response:
```json
{
  "records": [
    {"payload": "hello world", "timestamp_ms": 1234567890}
  ],
  "count": 1,
  "has_more": false
}
```

### Consumer Offsets

Commit offset:
```bash
curl -X POST http://localhost:8080/consumers/my-group/commit \
  -H "Content-Type: application/json" \
  -d '{"topic": "events", "partition": 0, "offset": 100}'
```

Get offset:
```bash
curl "http://localhost:8080/consumers/my-group/offset?topic=events&partition=0"
```

### Health Check

```bash
curl http://localhost:8080/health
```

### Server Metrics

Get real-time performance statistics:

```bash
curl http://localhost:8080/stats
```

Response:
```json
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
| `RUST_LOG` | `zombi=info` | Log level |

## Architecture

```
Write → RocksDB (hot) → BackgroundFlusher → S3 (cold)
              ↑                              ↑
              └────── Unified Read ──────────┘
```

- **Hot storage**: RocksDB for recent data (<1ms latency)
- **Cold storage**: S3 for archived data (optional)
- **Background flusher**: Automatically moves data from hot to cold
- **Unified reads**: Transparently merges data from both tiers

## Development

```bash
# Run tests
cargo test

# Run with verbose logging
RUST_LOG=zombi=debug cargo run

# Format code
cargo fmt

# Lint
cargo clippy -- -D warnings
```

## Testing with MinIO

```bash
# Check segments in S3
mc ls local/zombi-events/segments/ --recursive

# View segment contents
mc cat local/zombi-events/segments/events/0/0000000000000001-0000000000000005.json | jq .

# MinIO console
open http://localhost:9001  # minioadmin/minioadmin
```

## Load Testing with Producer Tool

A Python producer tool is included for load testing with variable burst patterns.

### Setup

```bash
cd tools/producer

# Install dependencies
pip install -r requirements.txt

# Generate protobuf code (requires protoc)
protoc --python_out=. events.proto
```

### Usage

```bash
# Bursty traffic (default) - random bursts of 50-200 events
python producer.py --profile bursty --duration 60

# Steady traffic - constant 10 events/sec
python producer.py --profile steady --duration 60

# Spike traffic - normal traffic with 10x spikes
python producer.py --profile spike --duration 60

# Stress test - maximum throughput with 10 threads
python producer.py --profile stress --duration 60
```

### Monitoring Performance

While the producer is running, monitor Zombi metrics:

```bash
# Watch metrics in real-time
watch -n 1 'curl -s http://localhost:8080/stats | jq .'

# Or continuously with python
while true; do curl -s http://localhost:8080/stats | jq '.writes.rate_per_sec, .reads.avg_latency_us'; sleep 1; done
```

The `/stats` endpoint provides:
- Write rate (events/sec)
- Read latency (microseconds)
- Total bytes written
- Error counts

See [SPEC.md](SPEC.md) for full specification.
