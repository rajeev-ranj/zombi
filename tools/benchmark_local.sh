#!/bin/bash
# Zombi Local Benchmark Script
# Runs benchmarks against a local Zombi instance

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZOMBI_URL="${ZOMBI_URL:-http://localhost:8080}"
SUITE="${1:-quick}"

echo "=========================================="
echo "Zombi Local Benchmark"
echo "=========================================="
echo "URL: $ZOMBI_URL"
echo "Suite: $SUITE"
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

# Run benchmark
cd "$SCRIPT_DIR"
python3 benchmark.py --url "$ZOMBI_URL" --suite "$SUITE"

echo ""
echo "=========================================="
echo "Benchmark complete!"
echo "Results saved to: benchmark_results.json"
echo "=========================================="
