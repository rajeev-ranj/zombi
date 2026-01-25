#!/bin/bash
# AWS Peak Performance Test - Runs load generation FROM EC2 instance (not local)
#
# This script orchestrates peak performance testing by:
# 1. Deploying Zombi infrastructure via Terraform
# 2. SSH-ing to EC2 and running tests locally on the instance (no network latency)
# 3. Copying results back to local machine
# 4. Optionally cleaning up infrastructure
#
# Usage:
#   ./aws_peak_performance.sh [MODE] [CLEANUP] [CONCURRENCY] [DURATION]
#
# Modes:
#   quick        - Peak single/bulk + read throughput + iceberg verification (~10 min)
#   comprehensive - All 9 phases: peak, read, lag, encoding, payload, mixed, consistency, iceberg (~40 min)
#
# Examples:
#   ./aws_peak_performance.sh                           # Default: quick mode, cleanup
#   ./aws_peak_performance.sh quick yes                 # Quick test with cleanup
#   ./aws_peak_performance.sh comprehensive no          # Full test, keep instance for inspection
#   ./aws_peak_performance.sh quick yes "50,100" 30     # Quick with custom concurrency/duration

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/../infra/terraform"
MODE="${1:-quick}"
CLEANUP="${2:-yes}"
CONCURRENCY="${3:-50,100,200}"
DURATION="${4:-30}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$SCRIPT_DIR/results/aws_${MODE}_$TIMESTAMP"

echo "=========================================="
echo "Zombi AWS Performance Test"
echo "=========================================="
echo "Mode: $MODE"
echo "Instance type: t3.micro (2 vCPU, 1GB RAM)"
if [ "$MODE" = "quick" ]; then
    echo "Concurrency levels: $CONCURRENCY"
    echo "Duration: ${DURATION}s per level"
fi
echo "Cleanup: $CLEANUP"
echo ""

mkdir -p "$RESULTS_DIR"

# Deploy infrastructure
echo "Deploying infrastructure..."
cd "$TF_DIR"
terraform init -upgrade > /dev/null 2>&1
terraform apply -auto-approve

# Get outputs
IP=$(terraform output -raw instance_public_ip)
S3_BUCKET=$(terraform output -raw s3_bucket)
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/id_ed25519}"

echo ""
echo "EC2 Instance: $IP"
echo "S3 Bucket: $S3_BUCKET"
echo "SSH Key: $SSH_KEY"

# Check SSH key exists
if [ ! -f "$SSH_KEY" ]; then
    echo "ERROR: SSH key not found at $SSH_KEY"
    echo "Set SSH_KEY_PATH environment variable to point to your SSH private key"
    exit 1
fi

# Wait for Zombi health
echo ""
echo "Waiting for Zombi to be healthy..."
HEALTHY=false
for i in $(seq 1 60); do
    if curl -sf "http://$IP:8080/health" > /dev/null 2>&1; then
        HEALTHY=true
        echo "Zombi is healthy!"
        break
    fi
    echo -n "."
    sleep 5
done

if [ "$HEALTHY" != "true" ]; then
    echo ""
    echo "ERROR: Zombi did not become healthy after 5 minutes"
    echo "Check EC2 logs: ssh -i $SSH_KEY ubuntu@$IP 'sudo journalctl -u docker'"
    exit 1
fi

# Get initial stats
echo ""
echo "Initial server stats:"
curl -s "http://$IP:8080/stats" | python3 -m json.tool 2>/dev/null || echo "(stats unavailable)"

# Run tests ON EC2 (not from local - avoids network latency)
echo ""
echo "=========================================="
echo "Running $MODE tests FROM EC2 instance..."
echo "=========================================="
echo "(All load generation happens on EC2, no local -> EC2 network latency)"
echo ""

if [ "$MODE" = "comprehensive" ]; then
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running COMPREHENSIVE test suite on EC2..."
echo "This will take approximately 40 minutes."
echo ""
/opt/run_comprehensive_test.sh /opt/results
REMOTE
else
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running QUICK tests on EC2..."
echo "Concurrency: $CONCURRENCY"
echo "Duration: ${DURATION}s per level"
echo ""
/opt/run_peak_test.sh "$CONCURRENCY" "$DURATION" /opt/results
REMOTE
fi

# Copy results back
echo ""
echo "Copying results from EC2..."
scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -r ubuntu@"$IP":/opt/results/* "$RESULTS_DIR/" 2>/dev/null || true

# Display summary
echo ""
echo "=========================================="
echo "Results Summary"
echo "=========================================="
echo ""

if [ "$MODE" = "comprehensive" ] && [ -f "$RESULTS_DIR/comprehensive_results.json" ]; then
    # Display comprehensive results
    python3 -c "
import json
with open('$RESULTS_DIR/comprehensive_results.json') as f:
    d = json.load(f)

# Print stored summary text if available
for line in d.get('summary_text', []):
    print(line)

print()
print('DETAILED RESULTS')
print('=' * 50)

# Peak single
if 'peak_single' in d.get('phases', {}):
    ps = d['phases']['peak_single']
    print(f'Peak Write: {ps.get(\"peak_throughput\", 0):,.0f} req/s, {ps.get(\"peak_mb_per_sec\", 0):.1f} MB/s')
    print(f'  Optimal concurrency: {ps.get(\"optimal_concurrency\", \"N/A\")}')
    for r in ps.get('results', []):
        cpu = r.get('peak_cpu_percent', 0)
        cpu_str = f'{cpu:.0f}%' if cpu else 'N/A'
        print(f'    c={r.get(\"concurrency\", 0):>3}: {r.get(\"requests_per_sec\", 0):>8,.0f} req/s, {r.get(\"mb_per_sec\", 0):>6.1f} MB/s, P99: {r.get(\"p99_ms\", 0):>6.1f}ms, CPU: {cpu_str}')
    print()

# Peak bulk
if 'peak_bulk' in d.get('phases', {}):
    pb = d['phases']['peak_bulk']
    print(f'Peak Bulk: {pb.get(\"peak_events_per_sec\", 0):,.0f} events/s, {pb.get(\"peak_mb_per_sec\", 0):.1f} MB/s')
    print(f'  Batch size: {pb.get(\"batch_size\", \"N/A\")}')
    for r in pb.get('results', []):
        cpu = r.get('peak_cpu_percent', 0)
        cpu_str = f'{cpu:.0f}%' if cpu else 'N/A'
        print(f'    c={r.get(\"concurrency\", 0):>3}: {r.get(\"events_per_sec\", 0):>10,.0f} ev/s, {r.get(\"mb_per_sec\", 0):>6.1f} MB/s, P99: {r.get(\"p99_ms\", 0):>6.1f}ms, CPU: {cpu_str}')
    print()

# Read throughput
if 'read_throughput' in d.get('phases', {}):
    rt = d['phases']['read_throughput'].get('results', {}).get('read_throughput', {})
    print(f'Read Throughput: {rt.get(\"records_per_sec\", 0):,.0f} records/s')
    print(f'  P50: {rt.get(\"p50_ms\", 0):.1f}ms, P95: {rt.get(\"p95_ms\", 0):.1f}ms')
    print()

# Write-read lag
if 'write_read_lag' in d.get('phases', {}):
    wrl = d['phases']['write_read_lag'].get('results', {}).get('write_read_lag', {})
    print(f'Write-Read Lag: P50={wrl.get(\"p50_ms\", 0):.1f}ms, P95={wrl.get(\"p95_ms\", 0):.1f}ms, P99={wrl.get(\"p99_ms\", 0):.1f}ms')
    print(f'  Max: {wrl.get(\"max_ms\", 0):.1f}ms')
    print()

# Proto vs JSON
if 'proto_vs_json' in d.get('phases', {}):
    pvj = d['phases']['proto_vs_json'].get('results', {}).get('proto_vs_json', {})
    improvement = pvj.get('improvement_pct', 0)
    json_tp = pvj.get('json', {}).get('throughput', 0)
    proto_tp = pvj.get('proto', {}).get('throughput', 0)
    print(f'Proto vs JSON: +{improvement:.1f}% improvement')
    print(f'  JSON: {json_tp:,.0f} ev/s, Proto: {proto_tp:,.0f} ev/s')
    print()

# Payload sizes
if 'payload_sizes' in d.get('phases', {}):
    ps = d['phases']['payload_sizes'].get('results', {}).get('payload_sizes', {})
    print('Payload Size Impact:')
    for size, data in sorted(ps.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0):
        tp = data.get('throughput', 0)
        mbps = data.get('mbps', 0)
        print(f'  {size}B: {tp:,.0f} ev/s, {mbps:.1f} Mbps')
    print()

# Iceberg
if 'iceberg' in d.get('phases', {}):
    ice = d['phases']['iceberg']
    pq = ice.get('parquet_files', 0)
    tb = ice.get('total_bytes', 0)
    mb = tb / (1024 * 1024) if isinstance(tb, (int, float)) else 0
    print(f'Iceberg: {pq} parquet files, {mb:.1f} MB total')
    print(f'  Bucket: {ice.get(\"bucket\", \"N/A\")}')
" 2>/dev/null || echo "  (comprehensive results not found)"

else
    # Display quick test results
    echo "--- Single API (POST /tables/{table}) ---"
    if [ -f "$RESULTS_DIR/peak_single.json" ]; then
        python3 -c "
import json
with open('$RESULTS_DIR/peak_single.json') as f:
    d = json.load(f)
print(f'  Peak throughput: {d.get(\"peak_throughput\", 0):,.0f} req/s')
print(f'  Peak bandwidth: {d.get(\"peak_mb_per_sec\", 0):.1f} MB/s')
print(f'  Optimal concurrency: {d.get(\"optimal_concurrency\", \"N/A\")}')
for r in d.get('results', []):
    cpu = r.get('peak_cpu_percent', 0)
    cpu_str = f'{cpu:.0f}%' if cpu else 'N/A'
    print(f'    c={r[\"concurrency\"]:>3}: {r[\"requests_per_sec\"]:>8,.0f} req/s, {r.get(\"mb_per_sec\", 0):>6.1f} MB/s, P99: {r[\"p99_ms\"]:.1f}ms, CPU: {cpu_str}')
" 2>/dev/null || echo "  (results file not found)"
    else
        echo "  (results file not found)"
    fi

    echo ""
    echo "--- Bulk API (POST /tables/{table}/bulk) ---"
    if [ -f "$RESULTS_DIR/peak_bulk.json" ]; then
        python3 -c "
import json
with open('$RESULTS_DIR/peak_bulk.json') as f:
    d = json.load(f)
print(f'  Peak throughput: {d.get(\"peak_throughput\", 0):,.0f} req/s')
print(f'  Peak events/s: {d.get(\"peak_events_per_sec\", 0):,.0f}')
print(f'  Peak bandwidth: {d.get(\"peak_mb_per_sec\", 0):.1f} MB/s')
print(f'  Batch size: {d.get(\"batch_size\", \"N/A\")} records/request')
print(f'  Optimal concurrency: {d.get(\"optimal_concurrency\", \"N/A\")}')
for r in d.get('results', []):
    cpu = r.get('peak_cpu_percent', 0)
    cpu_str = f'{cpu:.0f}%' if cpu else 'N/A'
    print(f'    c={r[\"concurrency\"]:>3}: {r[\"requests_per_sec\"]:>8,.0f} req/s, {r[\"events_per_sec\"]:>10,.0f} ev/s, {r.get(\"mb_per_sec\", 0):>6.1f} MB/s, P99: {r[\"p99_ms\"]:.1f}ms, CPU: {cpu_str}')
" 2>/dev/null || echo "  (results file not found)"
    else
        echo "  (results file not found)"
    fi

    echo ""
    echo "--- Iceberg Verification ---"
    if [ -f "$RESULTS_DIR/iceberg_verification.json" ]; then
        python3 -c "
import json
with open('$RESULTS_DIR/iceberg_verification.json') as f:
    d = json.load(f)
print(f'  Bucket: {d.get(\"bucket\", \"N/A\")}')
print(f'  Metadata files: {d.get(\"metadata_files\", 0)}')
print(f'  Parquet files: {d.get(\"parquet_files\", 0)}')
bytes_val = d.get('total_bytes', 0)
if isinstance(bytes_val, (int, float)) and bytes_val > 0:
    print(f'  Total size: {bytes_val / 1024 / 1024:.2f} MB')
else:
    print(f'  Total size: {bytes_val}')
" 2>/dev/null || echo "  (verification file not found)"
    else
        echo "  (verification file not found)"
    fi
fi

echo ""
echo "--- Final Server Stats ---"
if [ -f "$RESULTS_DIR/server_stats.json" ]; then
    python3 -c "
import json
with open('$RESULTS_DIR/server_stats.json') as f:
    d = json.load(f)
writes = d.get('writes', {})
print(f'  Total writes: {writes.get(\"total\", 0):,}')
print(f'  Total bytes: {writes.get(\"bytes_total\", 0) / 1024 / 1024:.1f} MB')
print(f'  Avg write latency: {writes.get(\"avg_latency_us\", 0):.1f} us')
print(f'  Uptime: {d.get(\"uptime_secs\", 0):.1f}s')
" 2>/dev/null || echo "  (stats file not found)"
else
    echo "  (stats file not found)"
fi

# Cleanup
if [ "$CLEANUP" = "yes" ]; then
    echo ""
    echo "=========================================="
    echo "Cleaning up infrastructure..."
    echo "=========================================="
    cd "$TF_DIR" && terraform destroy -auto-approve
    echo "Infrastructure destroyed."
else
    echo ""
    echo "=========================================="
    echo "Infrastructure left running"
    echo "=========================================="
    echo "SSH: ssh -i $SSH_KEY ubuntu@$IP"
    echo "Health: curl http://$IP:8080/health"
    echo "Stats: curl http://$IP:8080/stats"
    echo ""
    echo "To destroy manually:"
    echo "  cd $TF_DIR && terraform destroy"
fi

echo ""
echo "=========================================="
echo "Results saved to: $RESULTS_DIR"
echo "=========================================="
ls -la "$RESULTS_DIR"
