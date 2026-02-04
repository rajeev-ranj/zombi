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
#   ./aws_peak_performance.sh [MODE] [CLEANUP] [INSTANCE_TYPE]
#
# Modes:
#   quick        - Peak single/bulk + read throughput + iceberg verification (~10 min)
#   comprehensive - All 9 phases: peak, read, lag, encoding, payload, mixed, consistency, iceberg (~40 min)
#   bandwidth    - Maximum throughput test with large payloads (~15 min)
#   bandwidth-sweep - Full bandwidth sweep across configurations (~45 min)
#   sustained    - 10-minute sustained peak test with resource monitoring (~12 min)
#   sustained-20 - 20-minute sustained peak test with resource monitoring (~22 min)
#   baseline     - Full baseline: peak + sustained + scenarios from separate load gen (~75 min)
#
# Instance Types (for bandwidth testing, use larger instances):
#   t3.micro   - 2 vCPU, 1GB RAM, up to 5 Gbps (default, free tier)
#   t3.medium  - 2 vCPU, 4GB RAM, up to 5 Gbps
#   t3.large   - 2 vCPU, 8GB RAM, up to 5 Gbps
#   t3.xlarge  - 4 vCPU, 16GB RAM, up to 5 Gbps (recommended for bandwidth tests)
#   c5.xlarge  - 4 vCPU, 8GB RAM, up to 10 Gbps (best for bandwidth)
#   c5.2xlarge - 8 vCPU, 16GB RAM, up to 10 Gbps
#
# Examples:
#   ./aws_peak_performance.sh                           # Default: quick mode, t3.micro, cleanup
#   ./aws_peak_performance.sh quick yes                 # Quick test with cleanup
#   ./aws_peak_performance.sh bandwidth no t3.xlarge    # Bandwidth test on larger instance
#   ./aws_peak_performance.sh bandwidth-sweep no c5.xlarge  # Full bandwidth sweep

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/../infra/terraform"
MODE="${1:-quick}"
CLEANUP="${2:-yes}"
INSTANCE_TYPE="${3:-t3.micro}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BRANCH=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null | tr '/' '~')
COMMIT=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_DIRTY=$(git -C "$REPO_ROOT" diff --quiet 2>/dev/null && echo "false" || echo "true")
RESULTS_DIR="$SCRIPT_DIR/results/${BRANCH}/${TIMESTAMP}"

# Get instance specs for display
get_instance_spec() {
    case "$1" in
        t3.micro)   echo "2 vCPU, 1GB RAM, up to 5 Gbps" ;;
        t3.medium)  echo "2 vCPU, 4GB RAM, up to 5 Gbps" ;;
        t3.large)   echo "2 vCPU, 8GB RAM, up to 5 Gbps" ;;
        t3.xlarge)  echo "4 vCPU, 16GB RAM, up to 5 Gbps" ;;
        c5.xlarge)  echo "4 vCPU, 8GB RAM, up to 10 Gbps" ;;
        c5.2xlarge) echo "8 vCPU, 16GB RAM, up to 10 Gbps" ;;
        *)          echo "unknown" ;;
    esac
}

SPEC=$(get_instance_spec "$INSTANCE_TYPE")
TUNNEL_PID_FILE="/tmp/zombi-prometheus-tunnel.pid"

# Start local Grafana with SSH-tunneled Prometheus from EC2
start_local_grafana() {
    local server_ip="$1"
    local ssh_key="$2"

    echo ""
    echo "=========================================="
    echo "Starting local Grafana..."
    echo "=========================================="

    # Kill any existing tunnel/grafana from previous runs
    stop_local_grafana 2>/dev/null || true

    # Open SSH tunnel for Prometheus (background, no shell)
    echo "Opening SSH tunnel for Prometheus (localhost:9090 -> EC2:9090)..."
    ssh -i "$ssh_key" -o StrictHostKeyChecking=no \
        -L 9090:localhost:9090 -f -N ubuntu@"$server_ip"
    # Find the tunnel PID
    pgrep -f "ssh.*-L 9090:localhost:9090.*$server_ip" > "$TUNNEL_PID_FILE" 2>/dev/null

    # Start local Grafana container with AWS datasource + dashboards
    echo "Starting Grafana container..."
    docker run -d --name zombi-grafana -p 3000:3000 \
        -e GF_AUTH_ANONYMOUS_ENABLED=true \
        -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
        -v "$REPO_ROOT/infra/monitoring/grafana/provisioning/datasources/aws.yml:/etc/grafana/provisioning/datasources/datasource.yml:ro" \
        -v "$REPO_ROOT/infra/monitoring/grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards:ro" \
        grafana/grafana:10.2.0

    # Wait for Grafana health
    echo "Waiting for Grafana..."
    for i in $(seq 1 30); do
        if curl -sf http://localhost:3000/api/health > /dev/null 2>&1; then
            echo "Grafana is ready at http://localhost:3000"
            break
        fi
        sleep 2
    done
}

# Stop local Grafana and SSH tunnel
stop_local_grafana() {
    echo "Stopping local Grafana..."
    docker rm -f zombi-grafana 2>/dev/null || true

    if [ -f "$TUNNEL_PID_FILE" ]; then
        local pid
        pid=$(cat "$TUNNEL_PID_FILE")
        kill "$pid" 2>/dev/null || true
        rm -f "$TUNNEL_PID_FILE"
    fi
    # Also kill any orphaned tunnels
    pkill -f "ssh.*-L 9090:localhost:9090" 2>/dev/null || true
}

echo "=========================================="
echo "Zombi AWS Performance Test"
echo "=========================================="
echo "Mode: $MODE"
echo "Instance type: $INSTANCE_TYPE ($SPEC)"
echo "Branch: $BRANCH (${COMMIT}${GIT_DIRTY:+, dirty})"
echo "Cleanup: $CLEANUP"
echo "Results: $RESULTS_DIR"
echo ""

# Warn about cost for larger instances
case "$INSTANCE_TYPE" in
    c5*|*xlarge*)
        echo "NOTE: Using larger instance type - this will incur higher AWS costs."
        echo ""
        ;;
esac

mkdir -p "$RESULTS_DIR"

# Deploy infrastructure
echo "Deploying infrastructure..."
cd "$TF_DIR"
terraform init -upgrade > /dev/null 2>&1

if [ "$MODE" = "baseline" ]; then
    terraform apply -auto-approve \
      -var="instance_type=$INSTANCE_TYPE" \
      -var="enable_loadgen=true" \
      -var="loadgen_instance_type=t3.micro"
else
    terraform apply -auto-approve -var="instance_type=$INSTANCE_TYPE"
fi

# Get outputs
IP=$(terraform output -raw instance_public_ip)
S3_BUCKET=$(terraform output -raw s3_bucket)
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/id_ed25519}"

if [ "$MODE" = "baseline" ]; then
    LOADGEN_IP=$(terraform output -raw loadgen_public_ip)
    SERVER_PRIV_IP=$(terraform output -raw server_private_ip)
    echo ""
    echo "EC2 Server: $IP"
    echo "Load Generator: $LOADGEN_IP"
    echo "Server Private IP: $SERVER_PRIV_IP"
else
    echo ""
    echo "EC2 Instance: $IP"
fi
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

# Start local Grafana with live metrics
start_local_grafana "$IP" "$SSH_KEY"

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

case "$MODE" in
    comprehensive)
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running COMPREHENSIVE test suite on EC2..."
echo "This will take approximately 40 minutes."
echo ""
/opt/run_comprehensive_test.sh /opt/results
REMOTE
        ;;
    bandwidth)
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running BANDWIDTH test on EC2..."
echo "Testing with large payloads to maximize throughput."
echo ""
/opt/run_bandwidth_test.sh /opt/results quick
REMOTE
        ;;
    bandwidth-sweep)
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running BANDWIDTH SWEEP test on EC2..."
echo "This will test multiple configurations to find maximum throughput."
echo "This will take approximately 45 minutes."
echo ""
/opt/run_bandwidth_test.sh /opt/results sweep
REMOTE
        ;;
    sustained)
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running SUSTAINED PEAK TEST on EC2..."
echo "10-minute sustained load with resource monitoring."
echo ""
/opt/run_sustained_test.sh /opt/results 600 bulk
REMOTE
        ;;
    sustained-20)
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running SUSTAINED PEAK TEST on EC2..."
echo "20-minute sustained load with resource monitoring."
echo ""
/opt/run_sustained_test.sh /opt/results 1200 bulk
REMOTE
        ;;
    baseline)
        # Wait for load generator to be ready
        echo ""
        echo "Waiting for load generator to be ready..."
        for i in $(seq 1 60); do
            if ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no \
                -o ConnectTimeout=5 ubuntu@"$LOADGEN_IP" \
                "test -f /opt/run_baseline.sh" 2>/dev/null; then
                echo "Load generator is ready!"
                break
            fi
            echo -n "."
            sleep 5
        done

        # Run baseline from load generator
        echo ""
        echo "Running BASELINE test from load generator ($LOADGEN_IP)..."
        echo "This will take approximately 75 minutes."
        echo ""
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 \
            ubuntu@"$LOADGEN_IP" \
            "source /opt/zombi_env.sh && export ZOMBI_S3_BUCKET='$S3_BUCKET' && /opt/run_baseline.sh"
        ;;
    *)
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ubuntu@"$IP" << REMOTE
set -e
export ZOMBI_S3_BUCKET="$S3_BUCKET"
echo "Running QUICK tests on EC2..."
echo ""
/opt/run_peak_test.sh "50,100,200" "30" /opt/results
REMOTE
        ;;
esac

# Copy results back
echo ""
echo "Copying results from EC2..."
if [ "$MODE" = "baseline" ]; then
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -r ubuntu@"$LOADGEN_IP":/opt/results/* "$RESULTS_DIR/" 2>/dev/null || true
else
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -r ubuntu@"$IP":/opt/results/* "$RESULTS_DIR/" 2>/dev/null || true
fi

# Display summary
echo ""
echo "=========================================="
echo "Results Summary"
echo "=========================================="
echo ""

if [ "$MODE" = "baseline" ] && [ -f "$RESULTS_DIR/baseline_report.json" ]; then
    # Display baseline results
    python3 -c "
import json
with open('$RESULTS_DIR/baseline_report.json') as f:
    d = json.load(f)

print('BASELINE PERFORMANCE REPORT')
print('=' * 50)

# PERF checks
for check_id in sorted(d.get('perf_checks', {}).keys()):
    check = d['perf_checks'][check_id]
    status = 'PASS' if check.get('pass') else 'FAIL'
    print(f'  {check_id}: {status} - {check[\"metric\"]}: {check[\"actual\"]} {check[\"unit\"]} (baseline: {check[\"baseline\"]} {check[\"unit\"]})')

# Burst-to-steady
bts = d.get('burst_to_steady', {})
if bts:
    print()
    print(f'Burst-to-Steady Degradation: {bts.get(\"degradation_pct\", 0):.1f}%')
    print(f'  Burst avg: {bts.get(\"burst_phase_avg_req_per_sec\", 0):,.0f} req/s')
    print(f'  Steady avg: {bts.get(\"steady_phase_avg_req_per_sec\", 0):,.0f} req/s')

# Summary
summary = d.get('summary', {})
print()
print(f'Overall: {summary.get(\"checks_passed\", 0)}/{summary.get(\"checks_total\", 0)} checks passed')

# Phase results
phases = d.get('phases', {})
if 'phase1_peak_single' in phases:
    ps = phases['phase1_peak_single']
    print()
    print(f'Peak Single Write: {ps.get(\"peak_throughput\", 0):,.0f} req/s, {ps.get(\"peak_mb_per_sec\", 0):.1f} MB/s')
if 'phase1_peak_bulk' in phases:
    pb = phases['phase1_peak_bulk']
    print(f'Peak Bulk Write: {pb.get(\"peak_events_per_sec\", 0):,.0f} ev/s, {pb.get(\"peak_mb_per_sec\", 0):.1f} MB/s')
if 'phase2_sustained' in phases:
    sus = phases['phase2_sustained']
    perf = sus.get('performance', {})
    print(f'Sustained: {perf.get(\"avg_events_per_sec\", 0):,.0f} ev/s avg over {sus.get(\"config\", {}).get(\"duration_secs\", 0)/60:.0f} min')
" 2>/dev/null || echo "  (baseline results not found)"

elif [ "$MODE" = "comprehensive" ] && [ -f "$RESULTS_DIR/comprehensive_results.json" ]; then
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

# Display bandwidth results if available
if [[ "$MODE" == bandwidth* ]]; then
    echo ""
    echo "--- Bandwidth Test Results ---"

    # Check for combined results first
    if [ -f "$RESULTS_DIR/bandwidth_combined.json" ]; then
        python3 -c "
import json
with open('$RESULTS_DIR/bandwidth_combined.json') as f:
    d = json.load(f)

print(f'PEAK: {d.get(\"peak_mb_per_sec\", 0):.1f} MB/s ({d.get(\"peak_gbps\", 0):.3f} Gbps)')
print()

# Print all test results
tests = d.get('tests', [])
if tests:
    print(f'{\"Payload\":>10} {\"Batch\":>8} {\"Conc\":>6} {\"MB/s\":>10} {\"Gbps\":>8} {\"Events/s\":>12} {\"P99\":>10} {\"CPU\":>8}')
    print('-' * 80)
    for r in sorted(tests, key=lambda x: x.get('mb_per_sec', 0), reverse=True):
        payload_kb = r.get('payload_size', 0) / 1024
        cpu = r.get('peak_cpu_percent', 0)
        cpu_str = f'{cpu:.0f}%' if cpu else 'N/A'
        print(f'{payload_kb:>9.0f}K {r.get(\"batch_size\", 0):>8} {r.get(\"concurrency\", 0):>6} {r.get(\"mb_per_sec\", 0):>9.1f} {r.get(\"gbps\", 0):>7.3f} {r.get(\"events_per_sec\", 0):>11,.0f} {r.get(\"p99_ms\", 0):>9.1f}ms {cpu_str:>8}')
" 2>/dev/null || echo "  (results not found)"
    elif [ -f "$RESULTS_DIR/bandwidth_sweep.json" ]; then
        python3 -c "
import json
with open('$RESULTS_DIR/bandwidth_sweep.json') as f:
    d = json.load(f)

peak = d.get('peak', {})
print(f'PEAK: {peak.get(\"mb_per_sec\", 0):.1f} MB/s ({peak.get(\"gbps\", 0):.3f} Gbps)')
print()

results = d.get('results', [])
if results:
    print(f'{\"Payload\":>10} {\"Batch\":>8} {\"Conc\":>6} {\"MB/s\":>10} {\"Gbps\":>8} {\"Events/s\":>12} {\"P99\":>10} {\"CPU\":>8}')
    print('-' * 80)
    for r in sorted(results, key=lambda x: x.get('mb_per_sec', 0), reverse=True)[:10]:
        payload_kb = r.get('payload_size', 0) / 1024
        cpu = r.get('peak_cpu_percent', 0)
        cpu_str = f'{cpu:.0f}%' if cpu else 'N/A'
        print(f'{payload_kb:>9.0f}K {r.get(\"batch_size\", 0):>8} {r.get(\"concurrency\", 0):>6} {r.get(\"mb_per_sec\", 0):>9.1f} {r.get(\"gbps\", 0):>7.3f} {r.get(\"events_per_sec\", 0):>11,.0f} {r.get(\"p99_ms\", 0):>9.1f}ms {cpu_str:>8}')
" 2>/dev/null || echo "  (results not found)"
    else
        echo "  (no bandwidth results found)"
    fi
fi

# Display sustained test results if available
if [[ "$MODE" == sustained* ]] && [ -f "$RESULTS_DIR/sustained_test.json" ]; then
    echo ""
    echo "--- Sustained Test Results ---"
    python3 -c "
import json
with open('$RESULTS_DIR/sustained_test.json') as f:
    d = json.load(f)

config = d.get('config', {})
perf = d.get('performance', {})
res = d.get('resources', {})

print(f'Duration: {config.get(\"duration_secs\", 0)/60:.1f} minutes')
print(f'Mode: {config.get(\"mode\", \"N/A\")}')
print()

print('PERFORMANCE:')
print(f'  Total events: {perf.get(\"total_events\", 0):,}')
print(f'  Total data: {perf.get(\"total_bytes\", 0) / 1024 / 1024:.1f} MB')
print(f'  Avg throughput: {perf.get(\"avg_requests_per_sec\", 0):,.0f} req/s')
print(f'  Avg events/s: {perf.get(\"avg_events_per_sec\", 0):,.0f}')
print(f'  Avg bandwidth: {perf.get(\"avg_mb_per_sec\", 0):.1f} MB/s ({perf.get(\"avg_gbps\", 0):.3f} Gbps)')
print()

print('RESOURCE CONSUMPTION:')
print(f'  CPU: {res.get(\"avg_cpu_percent\", 0):.1f}% avg, {res.get(\"peak_cpu_percent\", 0):.1f}% peak')
print(f'  Memory: {res.get(\"avg_memory_mb\", 0):.0f} MB avg, {res.get(\"peak_memory_mb\", 0):.0f} MB peak')
print(f'  Disk write: {res.get(\"total_disk_write_mb\", 0):.1f} MB total')
print(f'  Network TX: {res.get(\"total_net_tx_mb\", 0):.1f} MB total')
" 2>/dev/null || echo "  (results not found)"
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

# Save git metadata to results
python3 -c "
import json, os
meta = {
    'git': {
        'branch': '$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)',
        'commit': '$COMMIT',
        'dirty': $GIT_DIRTY
    },
    'instance_type': '$INSTANCE_TYPE',
    'mode': '$MODE',
    'timestamp': '$TIMESTAMP'
}
meta_path = os.path.join('$RESULTS_DIR', 'run_metadata.json')
with open(meta_path, 'w') as f:
    json.dump(meta, f, indent=2)
" 2>/dev/null || true

# Inject git metadata into baseline_report.json if it exists
if [ -f "$RESULTS_DIR/baseline_report.json" ]; then
    python3 -c "
import json
with open('$RESULTS_DIR/baseline_report.json') as f:
    d = json.load(f)
d['git'] = {
    'branch': '$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)',
    'commit': '$COMMIT',
    'dirty': $GIT_DIRTY
}
d['instance_type'] = '$INSTANCE_TYPE'
with open('$RESULTS_DIR/baseline_report.json', 'w') as f:
    json.dump(d, f, indent=2)
" 2>/dev/null || true
fi

# Compare results across branches
compare_results() {
    echo ""
    echo "=========================================="
    echo "Branch Comparison"
    echo "=========================================="
    python3 << 'COMPARE'
import json, os, glob

results_root = os.path.join(os.environ.get('SCRIPT_DIR', '.'), 'results')
branches = {}

# Find latest baseline_report.json per branch
for branch_dir in sorted(glob.glob(os.path.join(results_root, '*'))):
    if not os.path.isdir(branch_dir):
        continue
    branch_name = os.path.basename(branch_dir)
    # Find latest timestamp directory with a baseline_report.json
    for ts_dir in sorted(glob.glob(os.path.join(branch_dir, '*')), reverse=True):
        report = os.path.join(ts_dir, 'baseline_report.json')
        if os.path.isfile(report):
            try:
                with open(report) as f:
                    branches[branch_name] = json.load(f)
                break
            except:
                pass

if len(branches) < 1:
    print("  No baseline results found for comparison.")
    exit(0)

# Extract metrics
metrics = []
for name, data in branches.items():
    checks = data.get('perf_checks', {})
    phases = data.get('phases', {})
    sus = phases.get('phase2_sustained', {}).get('performance', {})
    m = {'branch': name}
    for cid, c in checks.items():
        m[cid] = c.get('actual', 'N/A')
    m['sustained_evs'] = sus.get('avg_events_per_sec', 'N/A')
    m['sustained_mbs'] = sus.get('avg_mb_per_sec', 'N/A')
    metrics.append(m)

# Print table
branch_names = [m['branch'] for m in metrics]
col_width = max(20, max(len(b) for b in branch_names) + 2)
header = f"{'Metric':<30}" + "".join(f"{b:>{col_width}}" for b in branch_names)
print(header)
print("=" * len(header))

rows = [
    ('PERF-1 Single (req/s)', 'PERF-1'),
    ('PERF-2 Bulk (ev/s)', 'PERF-2'),
    ('PERF-3 Latency (us)', 'PERF-3'),
    ('PERF-5 Click (ev/s)', 'PERF-5'),
    ('Sustained avg (ev/s)', 'sustained_evs'),
    ('Sustained MB/s', 'sustained_mbs'),
]

for label, key in rows:
    vals = []
    for m in metrics:
        v = m.get(key, 'N/A')
        if isinstance(v, (int, float)):
            if 'Latency' in label or 'MB/s' in label:
                vals.append(f"{v:.1f}")
            else:
                vals.append(f"{v:,.0f}")
        else:
            vals.append(str(v))
    print(f"{label:<30}" + "".join(f"{v:>{col_width}}" for v in vals))

# Save comparison to markdown
md_path = os.path.join(results_root, 'comparison.md')
with open(md_path, 'w') as f:
    f.write("# Branch Performance Comparison\n\n")
    f.write(f"| Metric | " + " | ".join(branch_names) + " |\n")
    f.write("|--------|" + "|".join(["--------" for _ in branch_names]) + "|\n")
    for label, key in rows:
        vals = []
        for m in metrics:
            v = m.get(key, 'N/A')
            if isinstance(v, (int, float)):
                if 'Latency' in label or 'MB/s' in label:
                    vals.append(f"{v:.1f}")
                else:
                    vals.append(f"{v:,.0f}")
            else:
                vals.append(str(v))
        f.write(f"| {label} | " + " | ".join(vals) + " |\n")
    f.write(f"\n_Generated at: {os.popen('date -u').read().strip()}_\n")

print(f"\nComparison saved to: {md_path}")
COMPARE
}
export SCRIPT_DIR="$SCRIPT_DIR"
compare_results

# Cleanup
if [ "$CLEANUP" = "yes" ]; then
    echo ""
    echo "=========================================="
    echo "Cleaning up..."
    echo "=========================================="
    stop_local_grafana
    cd "$TF_DIR"
    if [ "$MODE" = "baseline" ]; then
        terraform destroy -auto-approve \
          -var="enable_loadgen=true" \
          -var="loadgen_instance_type=t3.micro"
    else
        terraform destroy -auto-approve
    fi
    echo "Infrastructure destroyed."
else
    echo ""
    echo "=========================================="
    echo "Infrastructure left running"
    echo "=========================================="
    echo "Grafana: http://localhost:3000"
    echo "SSH (server): ssh -i $SSH_KEY ubuntu@$IP"
    echo "Health: curl http://$IP:8080/health"
    echo "Stats: curl http://$IP:8080/stats"
    if [ "$MODE" = "baseline" ]; then
        echo "SSH (loadgen): ssh -i $SSH_KEY ubuntu@$LOADGEN_IP"
    fi
    echo ""
    echo "To destroy manually:"
    echo "  stop_local_grafana && cd $TF_DIR && terraform destroy"
fi

echo ""
echo "=========================================="
echo "Results saved to: $RESULTS_DIR"
echo "=========================================="
ls -la "$RESULTS_DIR"
