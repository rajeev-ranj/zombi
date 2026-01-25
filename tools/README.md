# Zombi Load Testing Tools

Comprehensive load testing framework for Zombi performance validation.

## zombi-load: Unified CLI (Recommended)

The **`zombi-load`** CLI provides a single entry point for all load testing scenarios with consistent configuration and standardized output.

### Quick Start

```bash
# Install dependencies
pip install click pyyaml requests

# Quick sanity check (~3 min)
python tools/zombi_load.py run --profile quick

# Full test suite (~30 min)
python tools/zombi_load.py run --profile full --url http://ec2:8080

# Specific scenario with overrides
python tools/zombi_load.py run --scenario single-write --workers 100 --duration 120

# Peak performance testing (requires hey)
python tools/zombi_load.py run --profile peak

# Compare results for regression detection
python tools/zombi_load.py compare results/run1/summary.json results/run2/summary.json

# List available scenarios and profiles
python tools/zombi_load.py list scenarios
python tools/zombi_load.py list profiles

# Generate config file
python tools/zombi_load.py init
```

### AWS/Remote Testing

When benchmarking a Zombi server on AWS, **run the load generator on EC2** to avoid network bottlenecks:

```
┌─────────────────────────────────────────────────┐
│                    AWS VPC                      │
│                                                 │
│   ┌─────────────┐        ┌─────────────┐       │
│   │   EC2 #1    │        │   EC2 #2    │       │
│   │ zombi-load  │  ───►  │   Zombi     │       │
│   │  (client)   │        │  (server)   │       │
│   └─────────────┘        └─────────────┘       │
│         │                                       │
│         └───── same AZ, <1ms latency ──────────│
└─────────────────────────────────────────────────┘
```

**Why this matters:**
- Local → EC2 testing measures your internet connection, not Zombi performance
- Network latency adds 20-100ms+ per request, masking actual server latency
- Bandwidth limits throttle throughput before the server is saturated

**Deployment options:**

| Setup | Pros | Cons |
|-------|------|------|
| Same instance | Simple, no network overhead | Client competes for CPU/RAM |
| Separate instance (same AZ) | Realistic, isolated resources | Slight network overhead (<1ms) |
| Multiple client instances | Can saturate large servers | More complex coordination |

**Quick setup:**
```bash
# SSH to your EC2 instance
ssh -i key.pem ubuntu@<ec2-ip>

# Install dependencies
pip install click pyyaml requests

# Clone tools or copy zombi_load.py
scp -i key.pem tools/zombi_load.py ubuntu@<ec2-ip>:~/

# Run against Zombi (localhost if same instance, or private IP)
python zombi_load.py run --profile full --url http://localhost:8080
```

For automated EC2 deployment, see `benchmark_ec2.sh` in Legacy Tools below.

### Profiles

| Profile | Duration | Scenarios |
|---------|----------|-----------|
| `quick` | ~3 min | single-write, read-throughput, consistency |
| `full` | ~30 min | All scenarios |
| `stress` | ~2 hours | Extended write, mixed, backpressure |
| `peak` | ~10 min | peak-single, peak-bulk (requires hey/wrk) |
| `iceberg` | ~5 min | cold-storage, iceberg-read (requires S3) |

### Scenarios

| Scenario | Description | Metrics |
|----------|-------------|---------|
| `single-write` | Single event write throughput | events/s, MB/s, P50/P95/P99 |
| `bulk-write` | Bulk API write throughput | events/s, MB/s, batch_req/s |
| `read-throughput` | Read throughput from hot storage | records/s, MB/s, latency |
| `write-read-lag` | Write to read visibility latency | P50/P95/P99 lag |
| `mixed-workload` | Concurrent read/write (70/30) | R/W throughput, MB/s |
| `backpressure` | Overload testing with 503 verification | 503 rate, recovery time |
| `cold-storage` | Iceberg flush verification [S3] | files created, rows |
| `peak-single` | Peak single-event throughput | max req/s, optimal concurrency |
| `peak-bulk` | Peak bulk-event throughput | max events/s, optimal concurrency |
| `iceberg-read` | Cold storage read performance [S3] | query latency, MB/s scanned |
| `consistency` | INV-2/INV-3 invariant verification | violations |

### Configuration

Create `zombi-load.yaml` in your project root:

```yaml
target:
  url: http://localhost:8080
  health_timeout: 30

cold_storage:
  enabled: true
  s3_bucket: zombi-events
  s3_endpoint: http://localhost:9000

settings:
  encoding: proto
  default_duration: 60
  num_workers: 10

output:
  directory: ./results
  format: json  # or markdown, both
```

Environment variables override config: `ZOMBI_URL`, `ZOMBI_S3_BUCKET`, etc.

### Output

Results are saved to `./results/{run_id}_{profile}/`:
- `summary.json` - Complete results
- `summary.md` - Human-readable report (if format=both)
- `scenarios/*.json` - Per-scenario details

---

## Legacy Tools

The following tools are deprecated but kept for backwards compatibility:

### Local Testing (Shell Scripts)

```bash
# Start Zombi locally first
cargo run
# or
docker-compose up -d

# Run all benchmarks (Python + scenario + hey)
./benchmark_local.sh

# Run Python benchmark only (complex tests)
./benchmark_local.sh python

# Run scenario tests only (comprehensive load testing)
./benchmark_local.sh scenario full

# Run hey stress test only (high-throughput)
./benchmark_local.sh hey

# Run Python with specific suite
./benchmark_local.sh python full
```

### Local Testing with MinIO (Cold Storage)

```bash
# Start MinIO for S3-compatible storage
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://zombi-events

# Run scenario tests with S3 verification
ZOMBI_S3_BUCKET=zombi-events ZOMBI_S3_ENDPOINT=http://localhost:9000 \
  ./benchmark_local.sh scenario full
```

### EC2 Testing

```bash
# Deploy to AWS, run benchmarks (Python + hey), cleanup automatically
./benchmark_ec2.sh benchmark full

# Deploy and run scenario tests
./benchmark_ec2.sh scenario full

# Run everything (benchmark + scenario + hey)
./benchmark_ec2.sh all full

# Keep instance running for manual testing
./benchmark_ec2.sh scenario full no
```

## Benchmark Tools

This suite provides three complementary tools for different testing needs:

### Python Benchmark (`benchmark.py`)

Complex, feature-rich testing:
- Proto vs JSON encoding comparison
- Write-to-read lag measurement
- Read/write throughput with latency percentiles
- Iceberg/S3 data verification
- Multiple payload sizes

Best for: **Detailed performance analysis, regression testing, feature validation**

### Scenario Test (`scenario_test.py`)

Comprehensive load testing with realistic scenarios:
- **Multi-Producer**: Multiple producers writing to multiple topics/partitions
- **Consumer**: Consumer offset tracking and order verification
- **Mixed Workload**: Concurrent 70% writes / 30% reads
- **Backpressure**: Overload testing with 503 response verification
- **Cold Storage**: Iceberg/S3 Parquet verification
- **Consistency**: INV-2 (no data loss) and INV-3 (order preserved) verification

Best for: **Production readiness testing, invariant validation, system behavior under load**

```bash
# Run single scenario
python scenario_test.py --url http://localhost:8080 --scenario multi-producer

# Run scenario suite
python scenario_test.py --url http://localhost:8080 --suite full

# With cold storage verification
python scenario_test.py --url http://localhost:8080 --suite full \
  --s3-bucket zombi-events --s3-endpoint http://localhost:9000
```

### Hey Stress Test

Simple, high-throughput stress testing:
- Maximum requests/second measurement
- CPU saturation testing
- Simple pass/fail for throughput targets

Best for: **Quick stress tests, capacity planning, CPU bottleneck detection**

Install hey:
```bash
# macOS
brew install hey

# Ubuntu (already in EC2 user_data.sh)
apt-get install -y hey
```

## Tools Reference

| Tool | Description |
|------|-------------|
| `benchmark.py` | Python benchmark with comprehensive test suites |
| `scenario_test.py` | Scenario-based load testing with invariant verification |
| `benchmark_local.sh` | Local testing wrapper (Python + scenario + hey) |
| `benchmark_ec2.sh` | EC2 deploy/test/cleanup (benchmark + scenario) |
| `load_test.py` | Legacy load test tool (kept for backwards compatibility) |

### Scenario Test File Structure

```
tools/
  scenario_test.py          # Main orchestrator
  scenarios/                # Scenario implementations
    __init__.py
    base.py                 # Base class (imports from benchmark.py)
    producer.py             # Multi-topic/partition simulation
    consumer.py             # Consumer offset tracking
    mixed.py                # Mixed read/write workload
    backpressure.py         # Overload testing
    cold_storage.py         # Iceberg/S3 Parquet verification
    consistency.py          # Data consistency validation
  lib/                      # Utility modules
    __init__.py
    s3_verifier.py          # Parquet reading (boto3 + pyarrow)
```

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

## Scenario Test Suites

### Quick Suite (~3 min)
- Multi-producer scenario
- Consistency verification

### Full Suite (~30 min)
- Multi-producer (10 producers, 3 topics)
- Consumer (offset tracking, order verification)
- Mixed workload (70% writes / 30% reads)
- Backpressure (200 workers burst, recovery)
- Cold storage (Parquet verification, requires S3)
- Consistency (INV-2, INV-3)

### Stress Suite (~2 hours)
- Extended multi-producer
- Extended mixed workload
- Extended backpressure
- Extended consistency

## Scenario Test Scenarios

| Scenario | Purpose | Duration |
|----------|---------|----------|
| `multi-producer` | N producers writing to M topics/partitions | 5-10 min |
| `consumer` | Read with offset commits, verify ordering | 5 min |
| `mixed` | Concurrent 70% writes / 30% reads | 10 min |
| `backpressure` | Exceed limits, verify 503 responses | 2 min |
| `cold-storage` | Write, wait for flush, verify Parquet | 5 min |
| `consistency` | Verify INV-2 and INV-3 invariants | 3 min |

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

## Scenario Test Success Criteria

| Scenario | Pass Criteria |
|----------|---------------|
| Multi-producer | >90% of target events written, no errors |
| Consumer | All offsets verified, no ordering errors |
| Mixed | No deadlocks, <1% read errors |
| Backpressure | 503 responses during burst, full recovery |
| Cold-storage | All events found in Parquet files |
| Consistency | INV-2 (no data loss) and INV-3 (order preserved) pass |

## Dependencies

### Required
```bash
pip install requests
```

### Optional (for cold storage verification)
```bash
pip install boto3 pyarrow pyyaml
```

### System Tools
```bash
# hey (stress testing)
brew install hey  # macOS
apt-get install hey  # Ubuntu

# AWS CLI (for S3)
brew install awscli
```

## Archived Tools

Legacy test scripts are archived in `archive/` for reference.
The unified `benchmark.py` consolidates all functionality.
