# Zombi Benchmark Tools

Comprehensive benchmark suite for testing Zombi performance.

## Quick Start

### Local Testing

```bash
# Start Zombi locally first
cargo run
# or
docker-compose up -d

# Run quick sanity check (~2 min)
./benchmark_local.sh quick

# Run full benchmark suite (~10 min)
./benchmark_local.sh full
```

### EC2 Testing

```bash
# Deploy to AWS, run benchmarks, cleanup automatically
./benchmark_ec2.sh full

# Keep instance running for manual testing
./benchmark_ec2.sh full no
```

## Tools

| Tool | Description |
|------|-------------|
| `benchmark.py` | Unified benchmark tool with comprehensive test suites |
| `benchmark_local.sh` | Wrapper for local testing |
| `benchmark_ec2.sh` | Wrapper for EC2 deploy/test/cleanup |
| `load_test.py` | Legacy load test tool (kept for backwards compatibility) |

## Benchmark Suites

### Quick Suite (~2 min)
- Write throughput test (30s)
- Read throughput test
- Basic sanity check

### Full Suite (~10 min)
- Proto vs JSON encoding comparison
- Write throughput at max rate
- Read throughput with various batch sizes
- Write-to-read lag measurement
- Payload size impact (100B, 1KB, 4KB, 32KB)
- Iceberg/S3 data verification

### Stress Suite (~30 min)
- Extended write throughput (120s, 20 workers)
- Extended proto vs JSON (60s)
- Extended payload sizes

## Individual Tests

```bash
# Run specific test
python benchmark.py --url http://localhost:8080 --test proto-vs-json
python benchmark.py --url http://localhost:8080 --test write-throughput
python benchmark.py --url http://localhost:8080 --test read-throughput
python benchmark.py --url http://localhost:8080 --test write-read-lag
python benchmark.py --url http://localhost:8080 --test payload-sizes
python benchmark.py --url http://localhost:8080 --test iceberg --s3-bucket zombi-events-test1
```

## Output

Results are saved to:
- `benchmark_results.json` - Latest run
- `results/ec2_YYYYMMDD_HHMMSS.json` - Timestamped EC2 results

### Sample Output

```
========================================
ZOMBI BENCHMARK REPORT
Target: http://localhost:8080
Date: 2026-01-24T10:00:00
========================================

PROTO vs JSON COMPARISON
  JSON:     15,234 ev/s, P95: 12.3ms
  Protobuf: 17,891 ev/s, P95: 10.1ms
  Proto improvement: +17.4%

WRITE THROUGHPUT
  Throughput: 17,335 ev/s
  P50: 8.2ms, P95: 25.1ms, P99: 45.2ms
  Errors: 0

READ THROUGHPUT
  Records/sec: 45,000
  P50: 8.5ms, P95: 15.2ms
```

## Expected Performance

### t3.micro (2 vCPU, 1GB RAM)

| Metric | Expected Value |
|--------|----------------|
| Max write throughput | 15,000-18,000 events/sec |
| Max read throughput | 50,000-100,000 records/sec |
| Proto vs JSON improvement | 10-20% |
| P95 write latency @ max | 30-50 ms |

### Local (M1/M2 Mac)

| Metric | Expected Value |
|--------|----------------|
| Max write throughput | 50,000-100,000 events/sec |
| Max read throughput | 200,000+ records/sec |

## Terraform Quick Reference

```bash
cd infra/terraform

# Deploy
terraform apply -auto-approve

# Get IP
terraform output -raw instance_public_ip

# SSH
$(terraform output -raw ssh_command)

# Destroy (now <60s with force_destroy)
terraform destroy -auto-approve
```

## Archived Tools

Legacy test scripts are archived in `archive/` for reference.
The unified `benchmark.py` consolidates all functionality.
