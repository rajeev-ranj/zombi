#!/bin/bash
#
# Quick test runner for Zombi load testing
#
# Usage:
#   ./quick_test.sh                    # Run quick sanity check
#   ./quick_test.sh max                # Find max throughput
#   ./quick_test.sh stress             # 10 minute stress test
#

URL="${ZOMBI_URL:-http://18.143.135.197:8080}"
TEST="${1:-quick}"
DURATION="${2:-60}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_TEST="$SCRIPT_DIR/load_test.py"

echo "======================================================================"
echo "Zombi Load Test"
echo "======================================================================"
echo "URL: $URL"
echo "Test: $TEST"
echo "Duration: $DURATION seconds"
echo "======================================================================"
echo ""

case $TEST in
    quick)
        echo "Running quick sanity check..."
        python3 "$LOAD_TEST" \
            --url "$URL" \
            --profile steady \
            --rate 100 \
            --duration "$DURATION"
        ;;
    steady)
        echo "Running steady load test..."
        python3 "$LOAD_TEST" \
            --url "$URL" \
            --profile steady \
            --rate 500 \
            --duration "$DURATION"
        ;;
    ramp)
        echo "Running ramp test..."
        python3 "$LOAD_TEST" \
            --url "$URL" \
            --profile ramp \
            --min-rate 100 \
            --max-rate 2000 \
            --duration "$DURATION"
        ;;
    max)
        echo "Finding maximum sustainable throughput..."
        python3 "$LOAD_TEST" \
            --url "$URL" \
            --profile max \
            --duration "$DURATION"
        ;;
    stress)
        echo "Running stress test..."
        python3 "$LOAD_TEST" \
            --url "$URL" \
            --profile max \
            --duration "$DURATION"
        ;;
    spike)
        echo "Running spike test..."
        python3 "$LOAD_TEST" \
            --url "$URL" \
            --profile spike \
            --min-rate 100 \
            --max-rate 1000 \
            --duration "$DURATION"
        ;;
    *)
        echo "Unknown test: $TEST"
        echo ""
        echo "Available tests:"
        echo "  quick    - Quick sanity check (100 events/sec)"
        echo "  steady   - Steady load (500 events/sec)"
        echo "  ramp     - Ramp from 100 to 2000 events/sec"
        echo "  max      - Find maximum throughput (adaptive)"
        echo "  stress   - Stress test (adaptive)"
        echo "  spike    - Spike test with 10x spikes"
        echo ""
        echo "Usage: $0 [test] [duration_seconds]"
        echo "Example: $0 max 300"
        exit 1
        ;;
esac

echo ""
echo "======================================================================"
echo "Test complete!"
echo "======================================================================"
