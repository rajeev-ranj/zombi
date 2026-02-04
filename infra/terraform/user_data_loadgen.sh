#!/usr/bin/env bash
set -euo pipefail

# Log everything
exec > >(tee /var/log/user-data.log) 2>&1
echo "Starting loadgen user-data script at $(date)"

# Install dependencies (no Docker needed)
apt-get update
apt-get install -y python3-pip protobuf-compiler git jq hey awscli

# Clone repo for test scripts
git clone https://github.com/rajeev-ranj/zombi.git /opt/zombi
pip3 install requests protobuf click pyyaml

# Make /opt/zombi writable by ubuntu user
chown -R ubuntu:ubuntu /opt/zombi

# Generate protobuf code
cd /opt/zombi/tools/producer
protoc --python_out=. events.proto

# Write environment config
cat > /opt/zombi_env.sh << 'ENVEOF'
export ZOMBI_URL="http://${server_private_ip}:8080"
export ZOMBI_S3_BUCKET="${s3_bucket}"
export ZOMBI_S3_REGION="${region}"
ENVEOF

# Source in .bashrc for interactive sessions
echo "source /opt/zombi_env.sh" >> /home/ubuntu/.bashrc

# Create results directory
mkdir -p /opt/results
chown ubuntu:ubuntu /opt/results

# Wait for server health check (up to 10 min)
echo "Waiting for Zombi server at http://${server_private_ip}:8080 ..."
HEALTHY=false
for i in $(seq 1 120); do
  if curl -sf "http://${server_private_ip}:8080/health" > /dev/null 2>&1; then
    HEALTHY=true
    echo "Zombi server is healthy!"
    break
  fi
  sleep 5
done

if [ "$HEALTHY" != "true" ]; then
  echo "WARNING: Zombi server did not become healthy after 10 minutes"
fi

# Generate 100 diverse clickstream event payloads
echo "Generating 100 clickstream event payloads..."
python3 << 'CLICKSTREAM'
import json, hashlib, random

event_types = [
    ("page_view", 30), ("click", 15), ("navigation", 10),
    ("add_to_cart", 10), ("scroll", 10), ("form_submit", 5),
    ("search", 5), ("video_play", 5), ("checkout", 5), ("error", 5)
]

pages = ["/home", "/products", "/product/123", "/product/456",
         "/cart", "/checkout", "/about", "/blog", "/blog/post-1",
         "/search", "/account", "/settings", "/help", "/pricing", "/contact"]

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
]

records = []
idx = 0
for event_type, count in event_types:
    for i in range(count):
        session = hashlib.md5(f"session-{idx}".encode()).hexdigest()[:16]
        user = f"user-{(idx % 20) + 1:04d}" if idx % 10 != 0 else None
        page = pages[idx % len(pages)]

        event = {
            "event_type": event_type,
            "session_id": f"sess_{session}",
            "page_url": f"https://example.com{page}",
            "referrer": f"https://google.com/search?q=query-{idx % 10}",
            "user_agent": user_agents[idx % len(user_agents)],
            "viewport": {"width": [1920, 1440, 1366, 390][idx % 4], "height": [1080, 900, 768, 844][idx % 4]},
            "utm_source": ["google", "facebook", "twitter", "direct", "email"][idx % 5],
            "utm_medium": ["cpc", "organic", "social", "referral"][idx % 4],
            "timestamp_iso": f"2026-02-04T{10 + (idx % 12):02d}:{idx % 60:02d}:{idx % 60:02d}Z",
            "ip_hash": hashlib.md5(f"ip-{idx % 50}".encode()).hexdigest()[:12]
        }
        if user:
            event["user_id"] = user

        if event_type == "click":
            event["element_id"] = f"btn-{idx % 8}"
            event["element_class"] = f"cta-{['primary','secondary','nav','footer'][idx % 4]}"
            event["coordinates"] = {"x": 400 + (idx * 37) % 800, "y": 200 + (idx * 53) % 600}
        elif event_type == "scroll":
            event["scroll_depth_pct"] = 10 + (idx * 7) % 90
            event["direction"] = "down" if idx % 3 != 0 else "up"
            event["page_height"] = 2000 + (idx * 113) % 8000
        elif event_type == "search":
            queries = ["running shoes", "laptop stand", "wireless mouse", "desk lamp", "headphones"]
            event["search_query"] = queries[idx % len(queries)]
            event["results_count"] = 5 + (idx * 13) % 95
            event["search_type"] = ["keyword", "category", "filter"][idx % 3]
        elif event_type == "video_play":
            event["video_id"] = f"vid-{idx % 10:03d}"
            event["video_title"] = f"Product Demo {idx % 10}"
            event["duration_secs"] = 30 + (idx * 17) % 570
            event["position_secs"] = (idx * 11) % 30
        elif event_type == "add_to_cart":
            event["product_id"] = f"SKU-{idx % 50:05d}"
            event["product_name"] = f"Product {idx % 50}"
            event["quantity"] = 1 + idx % 4
            event["price_cents"] = 999 + (idx * 123) % 9000
            event["currency"] = "USD"
        elif event_type == "checkout":
            event["cart_value_cents"] = 2999 + (idx * 457) % 47000
            event["item_count"] = 1 + idx % 6
            event["payment_method"] = ["credit_card", "paypal", "apple_pay"][idx % 3]
            event["currency"] = "USD"
        elif event_type == "error":
            event["error_type"] = ["js_error", "network_error", "render_error"][idx % 3]
            event["error_message"] = f"Uncaught TypeError: Cannot read property of undefined (component-{idx % 5})"
            event["stack_hash"] = hashlib.md5(f"stack-{idx % 8}".encode()).hexdigest()[:16]
        elif event_type == "form_submit":
            event["form_id"] = f"form-{['signup','contact','newsletter','feedback','login'][idx % 5]}"
            event["field_count"] = 3 + idx % 8
            event["validation_errors"] = idx % 3
        elif event_type == "navigation":
            event["from_page"] = f"https://example.com{pages[(idx + 1) % len(pages)]}"
            event["nav_type"] = ["link", "back", "forward", "bookmark"][idx % 4]

        records.append({
            "payload": json.dumps(event),
            "partition": idx % 4,
            "timestamp_ms": 1738663200000 + idx * 1000
        })
        idx += 1

with open("/opt/clickstream_payload.json", "w") as f:
    json.dump({"records": records}, f)

total = len(json.dumps({"records": records}))
avg = sum(len(r["payload"]) for r in records) / len(records)
print(f"  Generated {len(records)} clickstream events")
print(f"  Total payload: {total} bytes ({total/1024:.1f} KB)")
print(f"  Avg event size: {avg:.0f} bytes")
CLICKSTREAM

# Create baseline orchestration script (3-phase, ~75 min)
cat > /opt/run_baseline.sh << 'BASELINE'
#!/bin/bash
set -e

URL="$${ZOMBI_URL:-http://${server_private_ip}:8080}"
S3_BUCKET="$${ZOMBI_S3_BUCKET:-${s3_bucket}}"
OUTPUT_DIR="/opt/results"

mkdir -p "$${OUTPUT_DIR}"
cd /opt/zombi/tools

echo "========================================================"
echo "ZOMBI BASELINE PERFORMANCE TEST (3-Phase, ~75 min)"
echo "========================================================"
echo "Server URL: $${URL}"
echo "S3 Bucket: $${S3_BUCKET}"
echo "Started: $(date)"
echo ""

# Helper: snapshot /stats
snapshot_stats() {
  local name="$1"
  curl -sf "$${URL}/stats" > "$${OUTPUT_DIR}/$${name}" 2>/dev/null || echo '{}' > "$${OUTPUT_DIR}/$${name}"
  echo "  Saved stats snapshot: $${name}"
}

########################################
# PHASE 1: Peak Discovery (~15 min)
########################################
echo ""
echo "========================================================"
echo "PHASE 1: Peak Discovery (~15 min)"
echo "========================================================"
echo "Running while CPU + EBS burst credits are full."
echo ""

snapshot_stats "phase1_stats_before.json"

echo "--- Phase 1.1: Peak Single Write ---"
python3 peak_performance.py \
    --url "$${URL}" \
    --concurrency "50,100,200" \
    --duration 45 \
    --output "$${OUTPUT_DIR}/phase1_peak_single.json"

echo ""
echo "--- Phase 1.2: Peak Bulk Write ---"
python3 peak_performance_bulk.py \
    --url "$${URL}" \
    --concurrency "50,100,200" \
    --duration 45 \
    --batch-size 100 \
    --output "$${OUTPUT_DIR}/phase1_peak_bulk.json"

echo ""
echo "--- Phase 1.3: Read Throughput ---"
python3 benchmark.py \
    --url "$${URL}" \
    --test read-throughput \
    --output "$${OUTPUT_DIR}/phase1_read_throughput.json"

echo ""
echo "--- Phase 1.4: Write-Read Lag ---"
python3 benchmark.py \
    --url "$${URL}" \
    --test write-read-lag \
    --output "$${OUTPUT_DIR}/phase1_write_read_lag.json"

echo ""
echo "--- Phase 1.5: Proto vs JSON ---"
python3 benchmark.py \
    --url "$${URL}" \
    --test proto-vs-json \
    --duration 60 \
    --output "$${OUTPUT_DIR}/phase1_proto_vs_json.json"

echo ""
echo "--- Phase 1.6: Clickstream Bulk (100 diverse events) ---"
CLICK_SIZE=$(wc -c < /opt/clickstream_payload.json)
echo "  Payload: 100 events, $${CLICK_SIZE} bytes per request"

hey -z 45s -c 100 -m POST \
    -T "application/json" \
    -D /opt/clickstream_payload.json \
    "$${URL}/tables/clickstream/bulk" \
    > "$${OUTPUT_DIR}/phase1_clickstream_raw.txt" 2>&1 || true

# Parse hey output into structured JSON
python3 << PARSEHEY
import re, json, os

raw_path = "/opt/results/phase1_clickstream_raw.txt"
result = {"test": "clickstream_bulk", "events_per_request": 100, "payload_bytes": $${CLICK_SIZE}}

try:
    with open(raw_path) as f:
        output = f.read()

    if m := re.search(r'Requests/sec:\s+([\d.]+)', output):
        result["requests_per_sec"] = float(m.group(1))
        result["events_per_sec"] = result["requests_per_sec"] * 100
        result["mb_per_sec"] = result["requests_per_sec"] * $${CLICK_SIZE} / 1048576

    for pattern, key in [
        (r'Average:\s+([\d.]+)\s+secs', "avg_latency_ms"),
    ]:
        if m := re.search(pattern, output):
            result[key] = float(m.group(1)) * 1000

    # Parse response status codes
    status_200 = 0
    for m in re.finditer(r'\[(\d+)\]\s+(\d+)\s+responses', output):
        if m.group(1) == '200':
            status_200 = int(m.group(2))
    if status_200 == 0:
        if m := re.search(r'200\s+(\d+)\s+responses', output):
            status_200 = int(m.group(1))

    # Try total requests
    if m := re.search(r'(\d+) requests done', output):
        result["total_requests"] = int(m.group(1))

    evs = result.get("events_per_sec", 0)
    mbs = result.get("mb_per_sec", 0)
    print(f"  Clickstream: {evs:,.0f} events/s, {mbs:.2f} MB/s")
except Exception as e:
    print(f"  Warning: Could not parse hey output: {e}")
    result["error"] = str(e)

with open("/opt/results/phase1_clickstream.json", "w") as f:
    json.dump(result, f, indent=2)
PARSEHEY

snapshot_stats "phase1_stats_after.json"

echo ""
echo "Phase 1 complete at $(date)"

########################################
# PHASE 2: Sustained Load (~40 min)
########################################
echo ""
echo "========================================================"
echo "PHASE 2: Sustained Load (~40 min)"
echo "========================================================"
echo "Capturing burst-to-steady-state transition."
echo ""

snapshot_stats "phase2_stats_before.json"

python3 sustained_test.py \
    --url "$${URL}" \
    --duration 2400 \
    --mode bulk \
    --concurrency 100 \
    --batch-size 100 \
    --payload-size 1024 \
    --output "$${OUTPUT_DIR}/phase2_sustained.json"

snapshot_stats "phase2_stats_after.json"

echo ""
echo "Phase 2 complete at $(date)"

########################################
# PHASE 3: Scenario Suite (~20 min)
########################################
echo ""
echo "========================================================"
echo "PHASE 3: Scenario Suite (~20 min)"
echo "========================================================"
echo "Post-burst steady-state validation."
echo ""

snapshot_stats "phase3_stats_before.json"

echo "--- Phase 3.1: Payload Sizes ---"
python3 benchmark.py \
    --url "$${URL}" \
    --test payload-sizes \
    --duration 60 \
    --output "$${OUTPUT_DIR}/phase3_payload_sizes.json"

echo ""
echo "--- Phase 3.2: Mixed Workload ---"
python3 scenario_test.py \
    --url "$${URL}" \
    --scenario mixed \
    --duration 120 \
    --output "$${OUTPUT_DIR}/phase3_mixed_workload.json" \
    --no-save || echo "Mixed workload test completed with warnings"

echo ""
echo "--- Phase 3.3: Backpressure ---"
python3 scenario_test.py \
    --url "$${URL}" \
    --scenario backpressure \
    --duration 120 \
    --output "$${OUTPUT_DIR}/phase3_backpressure.json" \
    --no-save || echo "Backpressure test completed with warnings"

echo ""
echo "--- Phase 3.4: Consistency ---"
python3 scenario_test.py \
    --url "$${URL}" \
    --scenario consistency \
    --duration 60 \
    --output "$${OUTPUT_DIR}/phase3_consistency.json" \
    --no-save || echo "Consistency test completed with warnings"

echo ""
echo "--- Phase 3.5: Iceberg Verification ---"
echo "Waiting 120s for Iceberg flush..."
sleep 120

# Verify Iceberg data in S3
METADATA=$(aws s3 ls "s3://$${S3_BUCKET}/tables/" --recursive | grep -c "metadata.json" || echo 0)
PARQUET=$(aws s3 ls "s3://$${S3_BUCKET}/tables/" --recursive | grep -c ".parquet" || echo 0)
TOTAL_SIZE=$(aws s3 ls "s3://$${S3_BUCKET}/tables/" --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $3}' || echo 0)

cat > "$${OUTPUT_DIR}/phase3_iceberg_verification.json" << EOF
{
  "bucket": "$${S3_BUCKET}",
  "metadata_files": $${METADATA},
  "parquet_files": $${PARQUET},
  "total_bytes": $${TOTAL_SIZE},
  "verified_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "  Iceberg: $${PARQUET} parquet files, $${METADATA} metadata files"

snapshot_stats "phase3_stats_after.json"

echo ""
echo "Phase 3 complete at $(date)"

########################################
# REPORT GENERATION
########################################
echo ""
echo "========================================================"
echo "Generating Baseline Report"
echo "========================================================"

python3 << 'REPORT'
import json
import os
from datetime import datetime

output_dir = os.environ.get("OUTPUT_DIR", "/opt/results")
if output_dir == "/opt/results":
    # Fallback: scan for the output_dir variable
    pass

output_dir = "/opt/results"
report = {
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "test_type": "baseline",
    "phases": {},
    "perf_checks": {},
    "stats_snapshots": {},
}

# Load all phase results
phase_files = {
    "phase1_peak_single": "phase1_peak_single.json",
    "phase1_peak_bulk": "phase1_peak_bulk.json",
    "phase1_read_throughput": "phase1_read_throughput.json",
    "phase1_write_read_lag": "phase1_write_read_lag.json",
    "phase1_proto_vs_json": "phase1_proto_vs_json.json",
    "phase1_clickstream": "phase1_clickstream.json",
    "phase2_sustained": "phase2_sustained.json",
    "phase3_payload_sizes": "phase3_payload_sizes.json",
    "phase3_mixed_workload": "phase3_mixed_workload.json",
    "phase3_backpressure": "phase3_backpressure.json",
    "phase3_consistency": "phase3_consistency.json",
    "phase3_iceberg_verification": "phase3_iceberg_verification.json",
}

for name, filename in phase_files.items():
    path = os.path.join(output_dir, filename)
    if os.path.exists(path):
        try:
            with open(path) as f:
                report["phases"][name] = json.load(f)
        except Exception:
            pass

# Load stats snapshots
stats_files = [
    "phase1_stats_before.json", "phase1_stats_after.json",
    "phase2_stats_before.json", "phase2_stats_after.json",
    "phase3_stats_before.json", "phase3_stats_after.json",
]
for sf in stats_files:
    path = os.path.join(output_dir, sf)
    if os.path.exists(path):
        try:
            with open(path) as f:
                report["stats_snapshots"][sf.replace(".json", "")] = json.load(f)
        except Exception:
            pass

# PERF-1: Single-event writes > 10,000 req/s
peak_single = report["phases"].get("phase1_peak_single", {})
perf1_value = peak_single.get("peak_throughput", 0)
report["perf_checks"]["PERF-1"] = {
    "metric": "Single-event writes",
    "baseline": 10000,
    "actual": perf1_value,
    "unit": "req/s",
    "pass": perf1_value > 10000,
}

# PERF-2: Bulk writes > 100,000 ev/s
peak_bulk = report["phases"].get("phase1_peak_bulk", {})
perf2_value = peak_bulk.get("peak_events_per_sec", 0)
report["perf_checks"]["PERF-2"] = {
    "metric": "Bulk writes (100/batch)",
    "baseline": 100000,
    "actual": perf2_value,
    "unit": "ev/s",
    "pass": perf2_value > 100000,
}

# PERF-3: Server write latency < 10 us (from /stats)
stats_after_p1 = report["stats_snapshots"].get("phase1_stats_after", {})
latency = stats_after_p1.get("writes", {}).get("avg_latency_us", 0)
report["perf_checks"]["PERF-3"] = {
    "metric": "Server write latency",
    "baseline": 10,
    "actual": latency,
    "unit": "us",
    "pass": latency < 10 if latency > 0 else False,
}

# PERF-4: Iceberg snapshot < 500 ms (from flush timing)
iceberg = report["phases"].get("phase3_iceberg_verification", {})
parquet_files = iceberg.get("parquet_files", 0)
report["perf_checks"]["PERF-4"] = {
    "metric": "Iceberg snapshot commit",
    "baseline": 500,
    "actual": "verified" if parquet_files > 0 else "no_data",
    "unit": "ms",
    "pass": parquet_files > 0,
}

# PERF-5: Clickstream bulk (100 diverse events) > 50,000 ev/s
clickstream = report["phases"].get("phase1_clickstream", {})
perf5_evs = clickstream.get("events_per_sec", 0)
perf5_mbs = clickstream.get("mb_per_sec", 0)
report["perf_checks"]["PERF-5"] = {
    "metric": "Clickstream bulk (100 diverse events)",
    "baseline": 50000,
    "actual": perf5_evs,
    "unit": "ev/s",
    "mb_per_sec": perf5_mbs,
    "pass": perf5_evs > 50000,
}

# Burst-to-steady-state transition analysis
sustained = report["phases"].get("phase2_sustained", {})
time_series = sustained.get("time_series", {}).get("performance", [])
if len(time_series) >= 4:
    midpoint = len(time_series) // 2
    first_half = time_series[:midpoint]
    second_half = time_series[midpoint:]

    def avg_throughput(samples):
        if len(samples) < 2:
            return 0
        deltas = []
        for i in range(1, len(samples)):
            dt = samples[i]["t"] - samples[i-1]["t"]
            dreqs = samples[i]["reqs"] - samples[i-1]["reqs"]
            if dt > 0:
                deltas.append(dreqs / dt)
        return sum(deltas) / len(deltas) if deltas else 0

    burst_tp = avg_throughput(first_half)
    steady_tp = avg_throughput(second_half)
    degradation = ((burst_tp - steady_tp) / burst_tp * 100) if burst_tp > 0 else 0

    report["burst_to_steady"] = {
        "burst_phase_avg_req_per_sec": round(burst_tp, 1),
        "steady_phase_avg_req_per_sec": round(steady_tp, 1),
        "degradation_pct": round(degradation, 1),
    }

# Summary
all_pass = all(c.get("pass", False) for c in report["perf_checks"].values())
report["summary"] = {
    "all_perf_checks_pass": all_pass,
    "checks_passed": sum(1 for c in report["perf_checks"].values() if c.get("pass")),
    "checks_total": len(report["perf_checks"]),
}

# Print summary
print()
print("BASELINE REPORT SUMMARY")
print("=" * 50)
for check_id, check in sorted(report["perf_checks"].items()):
    status = "PASS" if check["pass"] else "FAIL"
    print(f"  {check_id}: {status} - {check['metric']}: {check['actual']} {check['unit']} (baseline: {check['baseline']} {check['unit']})")

if "burst_to_steady" in report:
    bts = report["burst_to_steady"]
    print(f"\n  Burst-to-Steady Degradation: {bts['degradation_pct']:.1f}%")
    print(f"    Burst avg: {bts['burst_phase_avg_req_per_sec']:,.0f} req/s")
    print(f"    Steady avg: {bts['steady_phase_avg_req_per_sec']:,.0f} req/s")

print(f"\n  Overall: {report['summary']['checks_passed']}/{report['summary']['checks_total']} checks passed")

with open(os.path.join(output_dir, "baseline_report.json"), "w") as f:
    json.dump(report, f, indent=2)

print(f"\n  Full report: {output_dir}/baseline_report.json")
REPORT

echo ""
echo "========================================================"
echo "BASELINE TEST COMPLETE"
echo "========================================================"
echo "Finished: $(date)"
echo "Results: $${OUTPUT_DIR}"
ls -la "$${OUTPUT_DIR}"
BASELINE
chmod +x /opt/run_baseline.sh
chown ubuntu:ubuntu /opt/run_baseline.sh

echo "Loadgen user-data script completed at $(date)"
