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

For automated EC2 deployment, use `aws_peak_performance.sh` (auto-deploys infra, runs tests, starts local Grafana).

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

## Tools Reference

| Tool | Purpose |
|------|---------|
| `zombi_load.py` | **Local CLI** — profiles: quick, full, stress, peak, baseline |
| `aws_peak_performance.sh` | **AWS CLI** — deploy, test, local Grafana, compare, cleanup |
| `config.py` | Configuration loader (profiles, scenarios) |
| `output.py` | Result formatting and comparison |
| `peak_performance.py` | Single-write throughput (used by AWS scripts on EC2) |
| `peak_performance_bulk.py` | Bulk-write throughput (used by AWS scripts on EC2) |
| `benchmark.py` | Shared library + read/lag/encoding tests |
| `sustained_test.py` | Long-running sustained load |
| `scenario_test.py` | Scenario orchestrator (mixed, consistency) |
| `bandwidth_test.py` | Bandwidth maximization |

## AWS Testing

```bash
# Full baseline: deploy infra, run all tests, local Grafana, cleanup (~75 min)
./aws_peak_performance.sh baseline yes t3.micro

# Keep infra running after tests (Grafana stays at localhost:3000)
./aws_peak_performance.sh baseline no t3.micro

# Other modes
./aws_peak_performance.sh quick yes t3.micro       # ~10 min
./aws_peak_performance.sh sustained yes t3.micro    # ~12 min
./aws_peak_performance.sh comprehensive yes t3.micro # ~40 min
```

**What happens:** Terraform deploys EC2 (server + loadgen), starts local Grafana at `http://localhost:3000` with SSH-tunneled Prometheus, runs tests, saves results to `tools/results/<branch>/<timestamp>/`, prints cross-branch comparison, cleans up.

**Prerequisites:** Terraform, AWS CLI, Docker, SSH key at `~/.ssh/id_ed25519`

## Individual Tests

```bash
# Run specific test against a running Zombi instance
python benchmark.py --url http://localhost:8080 --test proto-vs-json
python benchmark.py --url http://localhost:8080 --test write-throughput
python benchmark.py --url http://localhost:8080 --test read-throughput
python benchmark.py --url http://localhost:8080 --test write-read-lag
python benchmark.py --url http://localhost:8080 --test payload-sizes
python benchmark.py --url http://localhost:8080 --test iceberg --s3-bucket zombi-events-test1
```

## Results Structure

Results are organized by branch and timestamp for cross-branch comparison:

```
tools/results/
  <branch>/                     # / replaced with ~ in branch names
    <YYYYMMDD_HHMMSS>/
      baseline_report.json      # Main report with PERF checks + git metadata
      phase1_peak_single.json
      phase1_peak_bulk.json
      phase1_clickstream.json
      phase2_sustained.json
      phase3_iceberg.json
      run_metadata.json         # Git branch, commit, instance type
  comparison.md                 # Auto-generated cross-branch comparison
```

## Dependencies

### Required
```bash
pip install requests click pyyaml
```

### Optional (for cold storage verification)
```bash
pip install boto3 pyarrow
```

### System Tools
```bash
# hey (stress testing)
brew install hey  # macOS

# AWS CLI + Terraform (for AWS testing)
brew install awscli terraform
```
