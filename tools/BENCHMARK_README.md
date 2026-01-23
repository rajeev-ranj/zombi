# Proto vs JSON Performance Benchmark

Comprehensive performance comparison of Protobuf vs JSON encoding for Zombi.

## Quick Start

### Prerequisites

```bash
# Install Python dependencies
pip install requests protobuf

# Ensure the protobuf module is generated (run from tools/ directory)
cd tools/
protoc --python_out=. ../proto/event.proto
# Or use the existing event_pb2.py
```

### Run Locally

```bash
# Start Zombi server locally first
cargo run --release

# Quick test (30 seconds)
cd tools/
python proto_json_benchmark.py --url http://localhost:8080 --duration 30 --workers 5

# Full benchmark (5 minutes)
python proto_json_benchmark.py --url http://localhost:8080 --duration 300 --workers 10
```

### Run on EC2

```bash
# Get the EC2 IP from Terraform output
cd infra/terraform
terraform output instance_public_ip

# Run benchmark against EC2
cd tools/
python proto_json_benchmark.py \
    --url http://<EC2_IP>:8080 \
    --duration 300 \
    --workers 20 \
    --s3-bucket <your-bucket-name>
```

## Test Scenarios

### 1. Write Performance Test

Measures throughput and latency for write operations.

```bash
# Write-only test
python proto_json_benchmark.py --url http://localhost:8080 --skip-read --skip-storage
```

### 2. Read Performance Test

Measures read latency after writes complete.

```bash
# Include reads (default)
python proto_json_benchmark.py --url http://localhost:8080 --read-requests 2000
```

### 3. Iceberg/S3 Storage Analysis

Compares storage efficiency after flush to cold storage.

```bash
# Full test with storage analysis
python proto_json_benchmark.py \
    --url http://localhost:8080 \
    --s3-bucket zombi-events-test1 \
    --flush-wait 30
```

## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--url` | `http://localhost:8080` | Zombi server URL |
| `--duration` | `60` | Write test duration (seconds) |
| `--workers` | `10` | Concurrent worker threads |
| `--payload-size` | `200` | Target payload size (bytes) |
| `--read-requests` | `1000` | Number of read requests |
| `--json-table` | `bench_json` | Table for JSON tests |
| `--proto-table` | `bench_proto` | Table for Protobuf tests |
| `--s3-bucket` | `zombi-events-test1` | S3 bucket for storage analysis |
| `--skip-read` | `false` | Skip read benchmark |
| `--skip-storage` | `false` | Skip storage analysis |
| `--flush-wait` | `15` | Seconds to wait for S3 flush |

## Expected Results

### Wire Format Size

| Format | Envelope Overhead | Total Request Size |
|--------|-------------------|-------------------|
| JSON | ~50 bytes | ~250 bytes |
| Protobuf | ~15 bytes | ~215 bytes |
| **Savings** | **~70%** | **~15%** |

### Throughput (typical)

| Environment | JSON | Protobuf | Difference |
|-------------|------|----------|------------|
| Local (Mac) | ~3,000/s | ~3,500/s | +15-20% |
| EC2 (t3.medium) | ~8,000/s | ~9,500/s | +15-20% |
| EC2 (c5.xlarge) | ~25,000/s | ~30,000/s | +15-20% |

### Latency (P50)

| Environment | JSON | Protobuf |
|-------------|------|----------|
| Local | ~2-5ms | ~2-4ms |
| EC2 | ~1-3ms | ~1-2ms |

## Other Test Scripts

### Individual Tests

```bash
# JSON-only load test
python load_test.py --url http://localhost:8080 --profile max --duration 60

# Protobuf-only load test (requires events_pb2.py)
python load_test_proto.py --url http://localhost:8080 --duration 60

# Original comparison script
python compare_encodings.py --url http://localhost:8080 --duration 30
```

### Payload Size Testing

```bash
# Test different payload sizes
python test_payload_sizes.py --url http://localhost:8080
```

## Interpreting Results

### Key Metrics

1. **Throughput (events/sec)**: Higher is better. Protobuf should be 15-20% faster due to smaller wire format.

2. **P50 Latency**: Lower is better. Represents typical request latency.

3. **P99 Latency**: Lower is better. Represents tail latency under load.

4. **Wire Size**: Protobuf overhead is ~15 bytes vs JSON ~50 bytes for the envelope.

### What Affects Results

| Factor | Impact |
|--------|--------|
| Network latency | Higher latency reduces throughput difference |
| Payload size | Larger payloads reduce relative format overhead |
| CPU | Faster CPU benefits both equally |
| Worker count | More workers = higher throughput (up to saturation) |

## Troubleshooting

### "No module named 'event_pb2'"

```bash
cd tools/
protoc --python_out=. ../proto/event.proto
```

### Connection refused

Ensure Zombi is running:
```bash
cargo run --release
# or check Docker
docker ps
```

### S3 storage analysis fails

Ensure AWS credentials are configured:
```bash
aws configure
aws s3 ls s3://your-bucket/
```

### EC2 connection timeout

Check security group allows port 8080:
```bash
aws ec2 describe-security-groups --group-names zombi-sg
```
