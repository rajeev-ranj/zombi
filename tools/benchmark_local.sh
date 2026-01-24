#!/bin/bash
# Zombi Local Benchmark Script
# Runs benchmarks against a local Zombi instance
# Supports Python benchmark (complex tests), scenario tests, and hey (stress testing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZOMBI_URL="${ZOMBI_URL:-http://localhost:8080}"
MODE="${1:-all}"  # all, python, hey, scenario
SUITE="${2:-quick}"  # quick, full, stress (for python/scenario mode)

# S3/MinIO settings for cold storage tests (optional)
S3_BUCKET="${ZOMBI_S3_BUCKET:-}"
S3_ENDPOINT="${ZOMBI_S3_ENDPOINT:-}"
S3_REGION="${ZOMBI_S3_REGION:-us-east-1}"

echo "=========================================="
echo "Zombi Local Benchmark"
echo "=========================================="
echo "URL: $ZOMBI_URL"
echo "Mode: $MODE"
if [ "$MODE" = "python" ] || [ "$MODE" = "scenario" ] || [ "$MODE" = "all" ]; then
    echo "Suite: $SUITE"
fi
if [ -n "$S3_BUCKET" ]; then
    echo "S3 Bucket: $S3_BUCKET"
    echo "S3 Endpoint: ${S3_ENDPOINT:-AWS}"
fi
echo ""

# Check if Zombi is running
if ! curl -sf "$ZOMBI_URL/health" > /dev/null 2>&1; then
    echo "ERROR: Zombi not running at $ZOMBI_URL"
    echo ""
    echo "Start Zombi first:"
    echo "  cargo run"
    echo "  # or"
    echo "  docker-compose up -d"
    exit 1
fi

echo "Zombi is healthy!"
echo ""

# Python benchmark (complex tests: proto vs JSON, lag, read/write, Iceberg)
run_python() {
    echo "=========================================="
    echo "Python Benchmark (Suite: $SUITE)"
    echo "=========================================="
    cd "$SCRIPT_DIR"
    python3 benchmark.py --url "$ZOMBI_URL" --suite "$SUITE"
    echo ""
    echo "Results saved to: benchmark_results.json"
}

# Scenario tests (comprehensive load testing)
run_scenario() {
    echo "=========================================="
    echo "Scenario Test (Suite: $SUITE)"
    echo "=========================================="
    cd "$SCRIPT_DIR"

    # Build S3 args if configured
    S3_ARGS=""
    if [ -n "$S3_BUCKET" ]; then
        S3_ARGS="--s3-bucket $S3_BUCKET"
        if [ -n "$S3_ENDPOINT" ]; then
            S3_ARGS="$S3_ARGS --s3-endpoint $S3_ENDPOINT"
        fi
        if [ -n "$S3_REGION" ]; then
            S3_ARGS="$S3_ARGS --s3-region $S3_REGION"
        fi
    fi

    python3 scenario_test.py --url "$ZOMBI_URL" --suite "$SUITE" $S3_ARGS
    echo ""
    echo "Results saved to: results/scenario_results_*.json"
}

# Hey stress test (simple high-throughput, CPU saturation)
run_hey() {
    if ! command -v hey >/dev/null 2>&1; then
        echo "ERROR: hey not found. Install with: brew install hey"
        exit 1
    fi

    echo "=========================================="
    echo "Hey Stress Test (30s, 100 concurrent)"
    echo "=========================================="

    # Create payload file
    PAYLOAD='{"payload":"{\"test\":1}","partition":0,"timestamp_ms":0}'
    echo "$PAYLOAD" > /tmp/zombi_payload.json

    hey -z 30s -c 100 -m POST \
        -D /tmp/zombi_payload.json \
        -H "Content-Type: application/json" \
        "$ZOMBI_URL/tables/benchmark"

    rm -f /tmp/zombi_payload.json
}

# Run based on mode
case "$MODE" in
    python)
        run_python
        ;;
    scenario)
        run_scenario
        ;;
    hey)
        run_hey
        ;;
    all)
        run_python
        echo ""
        run_scenario
        echo ""
        run_hey
        ;;
    *)
        echo "ERROR: Unknown mode '$MODE'"
        echo "Usage: $0 [all|python|hey|scenario] [suite]"
        echo "  Modes:"
        echo "    all      - Run Python, scenario, and hey benchmarks (default)"
        echo "    python   - Run Python benchmark only (complex tests)"
        echo "    scenario - Run scenario tests only (comprehensive load testing)"
        echo "    hey      - Run hey stress test only (high-throughput)"
        echo "  Suites (for python/scenario mode): quick, full, stress"
        echo ""
        echo "  Environment variables for S3/MinIO cold storage tests:"
        echo "    ZOMBI_S3_BUCKET   - S3 bucket name (required for cold-storage scenario)"
        echo "    ZOMBI_S3_ENDPOINT - S3 endpoint URL (for MinIO, e.g., http://localhost:9000)"
        echo "    ZOMBI_S3_REGION   - S3 region (default: us-east-1)"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Benchmark complete!"
echo "=========================================="
