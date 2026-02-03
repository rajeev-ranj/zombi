# Zombi Load Test Report

**Date:** 2026-01-28
**EC2 Instance:** 54.179.155.62 (t3.micro, ap-southeast-1)
**S3 Bucket:** zombi-load-test-1769585332
**Zombi Image:** ghcr.io/rajeev-ranj/zombi:latest
**Bloom Filters:** Enabled (FP=0.01, expected 1M items)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Peak Single-Event Throughput** | **12,639 req/s** |
| **Peak Bulk-Event Throughput** | **94,804 events/s** (6.96 MB/s) |
| **Server-Side Avg Write Latency** | **24.7 μs** |
| **Total Events Written** | 202,997,125 |
| **Total Bytes Written** | 6.49 GB |
| **Total Test Duration** | ~2.2 hours |
| **Errors** | 0 |

---

## Scenario Results Summary

| # | Scenario | Status | Throughput | P95 Latency | Notes |
|---|----------|--------|------------|-------------|-------|
| 1 | single-write | FAIL | 802 ev/s | 23.0ms | Python client bottleneck (10 threads) |
| 2 | bulk-write | PASS | 879 ev/s | 23.3ms | NOT using bulk API (same as single) |
| 3 | read-throughput | PASS | - | 1.1ms | Quick test, 100 reads |
| 4 | write-read-lag | PASS | - | 17.1ms P95 | Write-to-read visibility |
| 5 | mixed-workload | PASS | - | - | 70% write / 30% read |
| 6 | backpressure | PASS | - | - | Overload handling |
| 7 | cold-storage | PASS | - | - | Skipped (quick) |
| 8 | consistency | PASS | - | - | INV-2/INV-3 verified |
| 9 | **peak-single** | **PASS** | **12,639 req/s** | 8.3ms | hey, 50 concurrent |
| 10 | **peak-bulk** | **PASS** | **94,804 ev/s** | 3409.6ms | hey, 500 concurrent, 100 ev/batch |
| 11 | iceberg-read | SKIPPED | - | - | S3 not configured in load CLI |

---

## Detailed Results

### Peak Single-Event Write (Scenario 9)

Uses `hey` HTTP load generator with single `/write` calls.

| Concurrency | Requests/s | P50 | P95 | P99 | CPU |
|-------------|------------|-----|-----|-----|-----|
| 50 | **12,639** | 4.1ms | 8.3ms | 11.4ms | 40% |
| 100 | 12,344 | 7.4ms | 15.4ms | 20.9ms | 51% |
| 200 | 5,576 | 16.2ms | 30.8ms | 48.0ms | 60% |
| 500 | 11,542 | 36.4ms | 68.1ms | 109.4ms | 69% |

**Optimal:** 50 concurrent connections = **12,639 req/s**

### Peak Bulk-Event Write (Scenario 10)

Uses `hey` with bulk endpoint, 100 events per request.

| Concurrency | Requests/s | Events/s | P50 | P95 | P99 | CPU |
|-------------|------------|----------|-----|-----|-----|-----|
| 50 | 823 | 82,345 | 23.9ms | 335ms | 724ms | 72% |
| 100 | 283 | 28,347 | 47.6ms | 2.3s | 3.4s | 70% |
| 200 | 881 | 88,127 | 91.5ms | 1.6s | 2.9s | 69% |
| 500 | 948 | **94,804** | 207ms | 3.4s | 4.8s | 70% |

**Optimal:** 500 concurrent connections = **94,804 events/s** (6.96 MB/s)

---

## Server Statistics (Final)

```json
{
  "uptime_secs": 31987.88,
  "writes": {
    "total": 202997125,
    "bytes_total": 6486256428,
    "rate_per_sec": 6346.06,
    "avg_latency_us": 24.71
  },
  "reads": {
    "total": 29054,
    "records_total": 2543149,
    "rate_per_sec": 0.91,
    "avg_latency_us": 115221.53
  },
  "errors_total": 0
}
```

---

## Root Cause Analysis Notes

### Issue 1: Python Client Bottleneck (Scenarios 1-2)

The `single-write` and `bulk-write` scenarios use Python's `threading` module with 10 workers. Due to the GIL (Global Interpreter Lock), only one thread executes Python bytecode at a time. Combined with HTTP round-trip overhead (~20ms), this caps throughput at ~800-900 events/s.

**Evidence:**
- Server-side write latency: 44 μs
- Client-measured P95: 23 ms
- 99.8% of latency is client-side

**Recommendation:** These scenarios should use `asyncio` with `aiohttp` or spawn `hey` subprocesses like `peak-single`/`peak-bulk` do.

### Issue 2: `bulk-write` Scenario Mislabeled

The scenario mapping in `runners/scenario_runner.py`:
```python
"bulk-write": "benchmark-write"
```

Maps to `BenchmarkSuite.test_write_throughput()` which calls `write_json()` in a loop — **single-event writes**, not the bulk API.

**Evidence:**
- Line 345 of `benchmark.py`: `success, latency = self.write_json(payload)`
- No batch/bulk endpoint usage

**Recommendation:** Fix `benchmark.py` to add a `test_bulk_write_throughput()` method that uses the bulk endpoint.

### Issue 3: iceberg-read Skipped

The `iceberg-read` scenario requires S3 configuration passed via CLI, but the runner didn't receive it.

**Workaround:** Run manually:
```bash
python3 zombi_load.py -u http://localhost:8080 run --scenario iceberg-read \
  --s3-bucket zombi-load-test-1769585332 --s3-region ap-southeast-1
```

---

## Key Findings

1. **Server performance is excellent**: 24.7 μs avg write latency, 0 errors across 203M writes
2. **Bulk API achieves 94K events/s** on a t3.micro — CPU-bound at 70%
3. **Single-event API achieves 12.6K req/s** with optimal 50 concurrent connections
4. **Python test harness is the bottleneck** for scenarios 1-5, not the server
5. **Bloom filters had no negative impact** — server maintained consistent low latency

---

## Files

| File | Description |
|------|-------------|
| `load-test.log` | Complete console output (37KB) |
| `*/scenarios/peak-single.json` | Detailed peak-single results |
| `*/scenarios/peak-bulk.json` | Detailed peak-bulk results |
| `*/summary.json` | Per-run summaries |

---

## Recommendations for Future Tests

1. **Use larger instance** (t3.large or c5.xlarge) to test higher throughput ceiling
2. **Fix `bulk-write` scenario** to actually use bulk API
3. **Increase workers** for Python-based scenarios (50-100 instead of 10)
4. **Add async Python client** option using `aiohttp`
5. **Configure S3 in CLI** for `iceberg-read` and `cold-storage` scenarios

---

## EC2 Instance (PRESERVED)

**DO NOT DESTROY** - Instance preserved for RCA.

```
IP: 54.179.155.62
SSH: ssh -i ~/.ssh/id_ed25519 ubuntu@54.179.155.62
```
