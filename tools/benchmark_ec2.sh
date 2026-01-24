#!/bin/bash
# Zombi EC2 Benchmark Script
# Deploys to AWS, runs benchmarks, and cleans up
#
# Usage:
#   ./benchmark_ec2.sh [mode] [suite] [cleanup]
#
# Modes:
#   benchmark - Run traditional benchmark only (default)
#   scenario  - Run scenario tests only
#   all       - Run both benchmark and scenario tests
#
# Suites: quick, full, stress (for scenario mode)
# Cleanup: yes (default), no

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/../infra/terraform"
MODE="${1:-benchmark}"
SUITE="${2:-full}"
CLEANUP="${3:-yes}"  # Set to "no" to skip cleanup

echo "=========================================="
echo "Zombi EC2 Benchmark"
echo "=========================================="
echo "Mode: $MODE"
echo "Suite: $SUITE"
echo "Cleanup: $CLEANUP"
echo ""

# Check prerequisites
command -v terraform >/dev/null 2>&1 || { echo "ERROR: terraform not found"; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "ERROR: aws cli not found"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found"; exit 1; }

# Deploy infrastructure
echo "Deploying EC2 instance..."
cd "$TF_DIR"
terraform init -upgrade
terraform apply -auto-approve

# Get instance IP
IP=$(terraform output -raw instance_public_ip)
S3_BUCKET=$(terraform output -raw s3_bucket)
echo ""
echo "EC2 IP: $IP"
echo "S3 Bucket: $S3_BUCKET"

# Wait for Zombi to be healthy
echo ""
echo "Waiting for Zombi to be healthy..."
MAX_WAIT=180  # 3 minutes
WAITED=0
while ! curl -sf "http://$IP:8080/health" > /dev/null 2>&1; do
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "ERROR: Zombi not healthy after ${MAX_WAIT}s"
        echo "Check instance logs: ssh ubuntu@$IP 'docker logs zombi'"
        exit 1
    fi
    echo -n "."
    sleep 5
    WAITED=$((WAITED + 5))
done
echo ""
echo "Zombi is healthy! (waited ${WAITED}s)"

# Run benchmarks based on mode
cd "$SCRIPT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "$SCRIPT_DIR/results"

run_benchmark() {
    echo ""
    echo "=========================================="
    echo "Python Benchmark (Suite: $SUITE)"
    echo "=========================================="
    python3 benchmark.py --url "http://$IP:8080" --suite "$SUITE" --s3-bucket "$S3_BUCKET"

    # Copy results with timestamp
    cp benchmark_results.json "$SCRIPT_DIR/results/ec2_benchmark_${TIMESTAMP}.json"
    echo ""
    echo "Results saved to: tools/results/ec2_benchmark_${TIMESTAMP}.json"
}

run_scenario() {
    echo ""
    echo "=========================================="
    echo "Scenario Tests (Suite: $SUITE)"
    echo "=========================================="
    python3 scenario_test.py --url "http://$IP:8080" --suite "$SUITE" \
        --s3-bucket "$S3_BUCKET" --output-dir "$SCRIPT_DIR/results"
    echo ""
    echo "Results saved to: tools/results/scenario_results_*.json"
}

run_hey() {
    echo ""
    echo "=========================================="
    echo "Hey Stress Test (60s, 100 concurrent)"
    echo "=========================================="
    PAYLOAD='{"payload":"{\"test\":1}","partition":0,"timestamp_ms":0}'
    echo "$PAYLOAD" > /tmp/zombi_payload.json

    hey -z 60s -c 100 -m POST \
        -D /tmp/zombi_payload.json \
        -H "Content-Type: application/json" \
        "http://$IP:8080/tables/benchmark"

    rm -f /tmp/zombi_payload.json
}

case "$MODE" in
    benchmark)
        run_benchmark
        run_hey
        ;;
    scenario)
        run_scenario
        ;;
    all)
        run_benchmark
        run_scenario
        run_hey
        ;;
    *)
        echo "ERROR: Unknown mode '$MODE'"
        echo "Usage: $0 [benchmark|scenario|all] [suite] [cleanup]"
        echo "  Modes: benchmark (default), scenario, all"
        echo "  Suites: quick, full, stress"
        echo "  Cleanup: yes (default), no"
        exit 1
        ;;
esac

# Get final server stats
echo ""
echo "Final server stats:"
curl -s "http://$IP:8080/stats" | python3 -m json.tool

# Cleanup
if [ "$CLEANUP" = "yes" ]; then
    echo ""
    echo "Cleaning up infrastructure..."
    cd "$TF_DIR"
    terraform destroy -auto-approve
    echo "Cleanup complete!"
else
    echo ""
    echo "Skipping cleanup. Instance running at: $IP"
    echo "To clean up later: cd $TF_DIR && terraform destroy -auto-approve"
fi

echo ""
echo "=========================================="
echo "EC2 Benchmark complete!"
echo "=========================================="
