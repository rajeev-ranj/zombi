<p align="center">
  <img src="assets/logo.png" alt="Zombi Logo" width="200">
</p>

# Zombi

[![Build](https://github.com/rajeev-ranj/zombi/actions/workflows/ci.yml/badge.svg)](https://github.com/rajeev-ranj/zombi/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Iceberg-native event ingestion gateway — the lowest-cost path from events to queryable Iceberg tables.

## Features

- **Simple** — Single binary, no ZooKeeper, no broker cluster
- **Iceberg-native** — Events land directly in Iceberg tables, queryable by Spark/Trino/DuckDB
- **Cost efficient** — Data stored on S3 with Iceberg metadata
- **Fast writes** — Buffered in RocksDB, durable in Iceberg
- **Dual format** — Supports both JSON and Protobuf writes
- **Optional real-time reads** — Planned Iceberg catalog plugin for hot+cold reads
- **Bounded hot buffer** — Hot data evicted after successful Iceberg commit (planned)
- **Compaction tooling** — Small-file rewrite; Iceberg snapshot integration planned

## Architecture

```
Producers → Zombi → Iceberg (S3) ──▶ Spark/Trino/DuckDB/Athena
              │
              ▼
           RocksDB (hot buffer)
```

- **RocksDB** — Write buffer for fast ingestion (bounded, ephemeral)
- **Iceberg** — Source of truth and primary read interface
- **Background flusher** — Batches events into Parquet files with Iceberg metadata

Optional: an Iceberg catalog plugin can merge hot + cold data for real-time reads without bypassing Iceberg semantics.

## Prerequisites

Before you begin, make sure you have:

- **Rust** (1.70+) — [Install via rustup](https://rustup.rs/)
- **Protobuf compiler** — `brew install protobuf` (macOS) or `apt-get install protobuf-compiler` (Linux)
- **Docker** — For running MinIO (local S3) — [Install Docker](https://docs.docker.com/get-docker/)
- **MinIO Client (mc)** — `brew install minio/stable/mc` (macOS) or [see docs](https://min.io/docs/minio/linux/reference/minio-mc.html)

## Quick Start

### Getting Started

```bash
# 1. Clone the repository
git clone https://github.com/rajeev-ranj/zombi.git
cd zombi

# 2. Build
cargo build --release

# 3. Start MinIO (local S3)
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# 4. Create bucket
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/zombi-events

# 5. Start Zombi
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
ZOMBI_S3_BUCKET=zombi-events \
ZOMBI_S3_ENDPOINT=http://localhost:9000 \
ZOMBI_ICEBERG_ENABLED=true \
ZOMBI_FLUSH_INTERVAL_SECS=5 \
./target/release/zombi

# 6. Test it works (in a new terminal)
curl -X POST http://localhost:8080/tables/events \
  -H "Content-Type: application/json" \
  -d '{"payload": "hello world"}'

# Read from hot buffer (not Iceberg semantics — use Iceberg engines for production reads)
curl "http://localhost:8080/tables/events?limit=10"

# Query via Iceberg (preferred; requires DuckDB with the iceberg extension)
duckdb -c "SELECT * FROM iceberg_scan('s3://zombi-events/tables/events') LIMIT 10"
```

### Local Only (No Persistence)

For quick testing without S3 storage (data is lost on restart):

```bash
git clone https://github.com/rajeev-ranj/zombi.git
cd zombi
cargo build --release
./target/release/zombi
```

### Production (AWS)

For production deployments, use real S3 instead of MinIO:

```bash
AWS_ACCESS_KEY_ID=AKIA... \
AWS_SECRET_ACCESS_KEY=... \
AWS_REGION=us-east-1 \
ZOMBI_S3_BUCKET=your-bucket-name \
ZOMBI_ICEBERG_ENABLED=true \
./target/release/zombi
```

See [AWS Deployment docs](docs/aws/IAM_S3_SETUP.md) for IAM permissions, instance profiles, and Terraform automation.

### Docker

```bash
docker build -t zombi .
docker run -p 8080:8080 -v zombi-data:/var/lib/zombi zombi
```

## Usage

### Write events

```bash
curl -X POST http://localhost:8080/tables/events \
  -H "Content-Type: application/json" \
  -d '{"payload": "hello world"}'
# Response: {"offset": 1, "partition": 0, "table": "events"}
```

### Read events (hot buffer)

```bash
curl "http://localhost:8080/tables/events?partition=0&offset=0&limit=100"
```

Reads from the RocksDB hot buffer only. Does **not** provide Iceberg-consistent semantics.
For production analytics, query Iceberg tables directly via Spark/Trino/DuckDB.

### Health check

```bash
curl http://localhost:8080/health
```

### View stats

```bash
curl http://localhost:8080/stats
```

### Verify Iceberg data

```bash
# List files in MinIO
mc ls local/zombi-events/tables/ --recursive

# Query with DuckDB
duckdb -c "SELECT * FROM iceberg_scan('s3://zombi-events/tables/events')"
```

## Load Testing

```bash
# Quick validation
python tools/zombi_load.py run --profile quick --url http://localhost:8080

# Full test suite
python tools/zombi_load.py run --profile full
```

See [tools/README.md](tools/README.md) for more options.

## Configuration

| Variable | Dev Default | Iceberg Default | Description |
|----------|-------------|-----------------|-------------|
| `ZOMBI_DATA_DIR` | `./data` | `./data` | RocksDB data directory |
| `ZOMBI_HOST` | `0.0.0.0` | `0.0.0.0` | HTTP server host |
| `ZOMBI_PORT` | `8080` | `8080` | HTTP server port |
| `ZOMBI_S3_BUCKET` | — | (required) | S3 bucket for Iceberg storage |
| `ZOMBI_S3_ENDPOINT` | AWS | AWS | Custom S3 endpoint (for MinIO) |
| `ZOMBI_S3_REGION` | `us-east-1` | `us-east-1` | AWS region |
| `ZOMBI_ICEBERG_ENABLED` | `false` | `true` | Enable Iceberg format output |
| `ZOMBI_FLUSH_INTERVAL_SECS` | `5` | `300` | Flush interval in seconds |
| `ZOMBI_FLUSH_BATCH_SIZE` | `1000` | `10000` | Min events before flushing |
| `ZOMBI_TARGET_FILE_SIZE_MB` | `128` | `128` | Target Parquet file size |
| `ZOMBI_ROCKSDB_WAL_ENABLED` | `true` | `true` | Enable WAL for durability (set `false` for max throughput at risk of data loss on crash) |
| `ZOMBI_HOT_RETENTION_SECS` | `0` | `0` | Seconds to keep flushed events in hot storage before deletion (0 = delete immediately) |
| `RUST_LOG` | `zombi=info` | `zombi=info` | Log level |

## Documentation

- [SPEC.md](SPEC.md) — Full API reference and specification
- [CONTRIBUTING.md](CONTRIBUTING.md) — How to contribute
- [CHANGELOG.md](CHANGELOG.md) — Version history
- [tools/README.md](tools/README.md) — Load testing and performance tools

### AWS Deployment

- [docs/aws/IAM_S3_SETUP.md](docs/aws/IAM_S3_SETUP.md) — IAM permissions and S3 bucket setup
- [docs/aws/ICEBERG_VERIFICATION.md](docs/aws/ICEBERG_VERIFICATION.md) — Verifying Iceberg tables in AWS
- [docs/aws/SCALING_ALB_REDIS.md](docs/aws/SCALING_ALB_REDIS.md) — Scaling with ALB and Redis
- [infra/terraform/](infra/terraform/) — Terraform modules for automated AWS deployment

## Development

```bash
cargo test          # Run tests
cargo fmt           # Format code
cargo clippy        # Lint
```

## License

[MIT](LICENSE)
