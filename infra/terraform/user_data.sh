#!/usr/bin/env bash
set -euo pipefail

# Log everything
exec > >(tee /var/log/user-data.log) 2>&1
echo "Starting user-data script at $(date)"

# Install dependencies
apt-get update
apt-get install -y docker.io python3-pip protobuf-compiler git jq hey
systemctl enable docker
systemctl start docker

# Clone repo for producer tools
git clone https://github.com/rajeev-ranj/zombi.git /opt/zombi
pip3 install requests protobuf

# Generate protobuf code
cd /opt/zombi/tools/producer
protoc --python_out=. events.proto

# Run Zombi container
echo "Starting Zombi container..."
# Use --network host to access EC2 instance metadata for IAM role credentials
/usr/bin/docker run -d --name zombi \
  --restart always \
  --network host \
  -v /data/zombi:/var/lib/zombi \
  -e ZOMBI_S3_BUCKET=${s3_bucket} \
  -e ZOMBI_S3_REGION=${region} \
  -e ZOMBI_ICEBERG_ENABLED=true \
  -e ZOMBI_FLUSH_INTERVAL_SECS=5 \
  -e ZOMBI_FLUSH_BATCH_SIZE=100 \
  -e ZOMBI_FLUSH_MAX_SEGMENT=1000 \
  -e RUST_LOG=zombi=info \
  ${zombi_image}

# Wait for Zombi to be healthy
echo "Waiting for Zombi to be healthy..."
for i in $(seq 1 60); do
  if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
    echo "Zombi is healthy!"
    break
  fi
  sleep 5
done

# Create benchmark script
cat > /opt/run_benchmark.sh << 'BENCHMARK'
#!/bin/bash
set -e
PROFILE=$${1:-steady}
DURATION=$${2:-60}
WORKERS=$${3:-5}

echo "Running benchmark: profile=$$PROFILE duration=$$DURATION workers=$$WORKERS"
cd /opt/zombi/tools
python3 load_test.py --url http://localhost:8080 --profile "$$PROFILE" --duration "$$DURATION" --workers "$$WORKERS"
BENCHMARK
chmod +x /opt/run_benchmark.sh

# Create stats collection script
cat > /opt/collect_stats.sh << 'STATS'
#!/bin/bash
echo "=== Zombi Stats ==="
curl -s http://localhost:8080/stats | jq .

echo ""
echo "=== Docker Stats ==="
docker stats zombi --no-stream

echo ""
echo "=== Recent Logs ==="
docker logs zombi --tail 20
STATS
chmod +x /opt/collect_stats.sh

# Create results directory with ubuntu ownership
mkdir -p /opt/results
chown ubuntu:ubuntu /opt/results

# Peak test script (runs FROM EC2, not local)
cat > /opt/run_peak_test.sh << 'PEAKTEST'
#!/bin/bash
set -e
CONCURRENCY="$${1:-50,100,200}"
DURATION="$${2:-30}"
OUTPUT_DIR="$${3:-/opt/results}"

mkdir -p "$${OUTPUT_DIR}"
cd /opt/zombi/tools

echo "=== Peak Performance Test (Single Write API) ==="
python3 peak_performance.py \
    --url http://localhost:8080 \
    --concurrency "$${CONCURRENCY}" \
    --duration "$${DURATION}" \
    --output "$${OUTPUT_DIR}/peak_single.json"

echo "=== Peak Performance Test (Bulk Write API) ==="
python3 peak_performance_bulk.py \
    --url http://localhost:8080 \
    --concurrency "$${CONCURRENCY}" \
    --duration "$${DURATION}" \
    --batch-size 100 \
    --output "$${OUTPUT_DIR}/peak_bulk.json"

echo "=== Read Throughput Test ==="
python3 benchmark.py \
    --url http://localhost:8080 \
    --test read-throughput
cp benchmark_results.json "$${OUTPUT_DIR}/read_results.json" 2>/dev/null || true

echo "=== Waiting for Iceberg Flush (120s) ==="
sleep 120

echo "=== Iceberg Verification ==="
/opt/verify_iceberg.sh "$${OUTPUT_DIR}"

echo "=== Final Server Stats ==="
curl -s http://localhost:8080/stats > "$${OUTPUT_DIR}/server_stats.json"

echo "Results saved to: $${OUTPUT_DIR}"
PEAKTEST
chmod +x /opt/run_peak_test.sh

# Iceberg verification script
cat > /opt/verify_iceberg.sh << 'VERIFY'
#!/bin/bash
OUTPUT_DIR="$${1:-.}"
BUCKET="$${ZOMBI_S3_BUCKET:-zombi-events-test1}"

echo "=== Iceberg Verification: s3://$${BUCKET} ==="

METADATA=$(aws s3 ls "s3://$${BUCKET}/tables/" --recursive | grep -c "metadata.json" || echo 0)
PARQUET=$(aws s3 ls "s3://$${BUCKET}/tables/" --recursive | grep -c ".parquet" || echo 0)
TOTAL_SIZE=$(aws s3 ls "s3://$${BUCKET}/tables/" --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $3}' || echo 0)

echo "  Metadata files: $${METADATA}"
echo "  Parquet files: $${PARQUET}"
echo "  Total size: $${TOTAL_SIZE} bytes"

# List actual files
echo ""
echo "S3 Contents:"
aws s3 ls "s3://$${BUCKET}/tables/" --recursive | head -20

cat > "$${OUTPUT_DIR}/iceberg_verification.json" << EOF
{
  "bucket": "$${BUCKET}",
  "metadata_files": $${METADATA},
  "parquet_files": $${PARQUET},
  "total_bytes": $${TOTAL_SIZE},
  "verified_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
VERIFY
chmod +x /opt/verify_iceberg.sh

# Comprehensive test script - runs all 9 phases
cat > /opt/run_comprehensive_test.sh << 'COMPREHENSIVE'
#!/bin/bash
set -e
OUTPUT_DIR="$${1:-/opt/results}"
mkdir -p "$${OUTPUT_DIR}"
cd /opt/zombi/tools

echo "========================================"
echo "ZOMBI COMPREHENSIVE PERFORMANCE TEST"
echo "========================================"
echo "Started: $(date)"
echo "Output directory: $${OUTPUT_DIR}"
echo ""

# Phase 1: Peak Single Write
echo "=== Phase 1/9: Peak Single Write ==="
python3 peak_performance.py \
    --url http://localhost:8080 \
    --concurrency "50,100,200" \
    --duration 30 \
    --output "$${OUTPUT_DIR}/peak_single.json"

# Phase 2: Peak Bulk Write
echo ""
echo "=== Phase 2/9: Peak Bulk Write ==="
python3 peak_performance_bulk.py \
    --url http://localhost:8080 \
    --concurrency "50,100" \
    --duration 30 \
    --batch-size 100 \
    --output "$${OUTPUT_DIR}/peak_bulk.json"

# Phase 3: Read Throughput
echo ""
echo "=== Phase 3/9: Read Throughput ==="
python3 benchmark.py \
    --url http://localhost:8080 \
    --test read-throughput \
    --output "$${OUTPUT_DIR}/read_throughput.json"

# Phase 4: Write-to-Read Lag
echo ""
echo "=== Phase 4/9: Write-to-Read Lag ==="
python3 benchmark.py \
    --url http://localhost:8080 \
    --test write-read-lag \
    --output "$${OUTPUT_DIR}/write_read_lag.json"

# Phase 5: Proto vs JSON
echo ""
echo "=== Phase 5/9: Proto vs JSON Encoding ==="
python3 benchmark.py \
    --url http://localhost:8080 \
    --test proto-vs-json \
    --duration 30 \
    --output "$${OUTPUT_DIR}/proto_vs_json.json"

# Phase 6: Payload Sizes
echo ""
echo "=== Phase 6/9: Payload Size Impact ==="
python3 benchmark.py \
    --url http://localhost:8080 \
    --test payload-sizes \
    --duration 30 \
    --output "$${OUTPUT_DIR}/payload_sizes.json"

# Phase 7: Mixed Workload
echo ""
echo "=== Phase 7/9: Mixed Workload ==="
python3 scenario_test.py \
    --url http://localhost:8080 \
    --scenario mixed \
    --duration 60 \
    --output "$${OUTPUT_DIR}/mixed_workload.json" \
    --no-save || echo "Mixed workload test skipped or failed"

# Phase 8: Consistency Check
echo ""
echo "=== Phase 8/9: Consistency Verification ==="
python3 scenario_test.py \
    --url http://localhost:8080 \
    --scenario consistency \
    --output "$${OUTPUT_DIR}/consistency.json" \
    --no-save || echo "Consistency test skipped or failed"

# Phase 9: Wait for Iceberg flush then verify
echo ""
echo "=== Phase 9/9: Iceberg Verification ==="
echo "Waiting 120s for flush..."
sleep 120
/opt/verify_iceberg.sh "$${OUTPUT_DIR}"

# Collect final server stats
echo ""
echo "=== Collecting Final Server Stats ==="
curl -s http://localhost:8080/stats > "$${OUTPUT_DIR}/server_stats.json"

# Generate unified summary
echo ""
echo "=== Generating Comprehensive Summary ==="
python3 << SUMMARY
import json
import os
from datetime import datetime

output_dir = "$${OUTPUT_DIR}"
summary = {
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "instance_type": "t3.micro",
    "phases": {}
}

# Load each phase result
phase_files = {
    "peak_single": "peak_single.json",
    "peak_bulk": "peak_bulk.json",
    "read_throughput": "read_throughput.json",
    "write_read_lag": "write_read_lag.json",
    "proto_vs_json": "proto_vs_json.json",
    "payload_sizes": "payload_sizes.json",
    "mixed_workload": "mixed_workload.json",
    "consistency": "consistency.json",
    "iceberg": "iceberg_verification.json",
}

for name, filename in phase_files.items():
    path = os.path.join(output_dir, filename)
    if os.path.exists(path):
        try:
            with open(path) as f:
                summary["phases"][name] = json.load(f)
        except:
            pass

# Generate human-readable summary
summary_text = []
summary_text.append("COMPREHENSIVE TEST SUMMARY")
summary_text.append("=" * 50)

# Peak single
if "peak_single" in summary["phases"]:
    ps = summary["phases"]["peak_single"]
    summary_text.append(f"Peak Write: {ps.get('peak_throughput', 0):,.0f} req/s, {ps.get('peak_mb_per_sec', 0):.1f} MB/s")

# Peak bulk
if "peak_bulk" in summary["phases"]:
    pb = summary["phases"]["peak_bulk"]
    summary_text.append(f"Peak Bulk: {pb.get('peak_events_per_sec', 0):,.0f} events/s, {pb.get('peak_mb_per_sec', 0):.1f} MB/s")

# Read throughput
if "read_throughput" in summary["phases"]:
    rt = summary["phases"]["read_throughput"].get("results", {}).get("read_throughput", {})
    summary_text.append(f"Read: {rt.get('records_per_sec', 0):,.0f} records/s")

# Write-read lag
if "write_read_lag" in summary["phases"]:
    wrl = summary["phases"]["write_read_lag"].get("results", {}).get("write_read_lag", {})
    summary_text.append(f"Write-Read Lag: P95={wrl.get('p95_ms', 0):.1f}ms")

# Proto vs JSON
if "proto_vs_json" in summary["phases"]:
    pvj = summary["phases"]["proto_vs_json"].get("results", {}).get("proto_vs_json", {})
    improvement = pvj.get("improvement_pct", 0)
    summary_text.append(f"Proto vs JSON: +{improvement:.1f}% improvement")

# Consistency
if "consistency" in summary["phases"]:
    cons = summary["phases"]["consistency"]
    scenarios = cons.get("scenarios", [{}])
    if scenarios:
        s = scenarios[0]
        status = "PASS" if s.get("success", False) else "FAIL"
        summary_text.append(f"Consistency: {status}")

# Iceberg
if "iceberg" in summary["phases"]:
    ice = summary["phases"]["iceberg"]
    pq = ice.get("parquet_files", 0)
    tb = ice.get("total_bytes", 0)
    mb = tb / (1024 * 1024) if isinstance(tb, (int, float)) else 0
    summary_text.append(f"Iceberg: {pq} parquet files, {mb:.1f} MB")

summary["summary_text"] = summary_text

# Write unified summary
with open(os.path.join(output_dir, "comprehensive_results.json"), "w") as f:
    json.dump(summary, f, indent=2)

# Print summary
print()
for line in summary_text:
    print(line)
print()
print(f"Full results saved to: {output_dir}/comprehensive_results.json")
SUMMARY

echo ""
echo "========================================"
echo "COMPREHENSIVE TEST COMPLETE"
echo "========================================"
echo "Finished: $(date)"
echo "Results: $${OUTPUT_DIR}"
ls -la "$${OUTPUT_DIR}"
COMPREHENSIVE
chmod +x /opt/run_comprehensive_test.sh

echo "User-data script completed at $(date)"
