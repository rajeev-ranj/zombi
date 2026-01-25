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
#   ./aws_peak_performance.sh [CONCURRENCY] [DURATION] [CLEANUP]
#
# Examples:
#   ./aws_peak_performance.sh                    # Default: 50,100,200 concurrency, 30s, cleanup
#   ./aws_peak_performance.sh "50,100" 30 yes   # Quick test
#   ./aws_peak_performance.sh "50,100,200" 60 no # Keep instance for inspection

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/../infra/terraform"
CONCURRENCY="${1:-50,100,200}"
DURATION="${2:-30}"
CLEANUP="${3:-yes}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$SCRIPT_DIR/results/aws_peak_$TIMESTAMP"

echo "=========================================="
echo "Zombi AWS Peak Performance Test"
echo "=========================================="
echo "Instance type: t3.micro (2 vCPU, 1GB RAM)"
echo "Concurrency levels: $CONCURRENCY"
echo "Duration: ${DURATION}s per level"
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
echo "Running peak tests FROM EC2 instance..."
echo "=========================================="
echo "(All load generation happens on EC2, no local -> EC2 network latency)"
echo ""

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running tests on EC2..."
echo "Concurrency: $CONCURRENCY"
echo "Duration: ${DURATION}s per level"
echo ""
/opt/run_peak_test.sh "$CONCURRENCY" "$DURATION" /opt/results
REMOTE

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

echo "--- Single API (POST /tables/{table}) ---"
if [ -f "$RESULTS_DIR/peak_single.json" ]; then
    python3 -c "
import json
with open('$RESULTS_DIR/peak_single.json') as f:
    d = json.load(f)
print(f'  Peak throughput: {d.get(\"peak_throughput\", 0):,.0f} req/s')
print(f'  Optimal concurrency: {d.get(\"optimal_concurrency\", \"N/A\")}')
for r in d.get('results', []):
    print(f'    c={r[\"concurrency\"]:>3}: {r[\"requests_per_sec\"]:>8,.0f} req/s, P99: {r[\"p99_ms\"]:.1f}ms')
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
print(f'  Batch size: {d.get(\"batch_size\", \"N/A\")} records/request')
print(f'  Optimal concurrency: {d.get(\"optimal_concurrency\", \"N/A\")}')
for r in d.get('results', []):
    print(f'    c={r[\"concurrency\"]:>3}: {r[\"requests_per_sec\"]:>8,.0f} req/s, {r[\"events_per_sec\"]:>10,.0f} ev/s, P99: {r[\"p99_ms\"]:.1f}ms')
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

echo ""
echo "--- Final Server Stats ---"
if [ -f "$RESULTS_DIR/server_stats.json" ]; then
    python3 -c "
import json
with open('$RESULTS_DIR/server_stats.json') as f:
    d = json.load(f)
writes = d.get('writes', {})
print(f'  Total writes: {writes.get(\"total\", 0):,}')
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
