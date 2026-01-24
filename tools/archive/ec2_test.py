#!/usr/bin/env python3
"""
Direct EC2 Load Test (JSON only)
Measures real throughput and latency
"""

import time
import json
import random
from concurrent.futures import ThreadPoolExecutor
import requests
import statistics


def test_write(url, table, duration, workers):
    sent = 0
    errors = 0
    latencies = []

    session = requests.Session()
    end_time = time.time() + duration

    while time.time() < end_time:
        start = time.time()
        try:
            payload = {"type": "test", "data": "x" * 100}
            event = {
                "payload": json.dumps(payload),
                "timestamp_ms": int(time.time() * 1000),
                "partition": random.randint(0, 3),
            }
            response = session.post(
                f"{url}/tables/{table}",
                json=event,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            latency_ms = (time.time() - start) * 1000

            if response.status_code in (200, 202):
                sent += 1
                latencies.append(latency_ms)
            else:
                errors += 1
                latencies.append(999)  # 999ms for errors

        except Exception as e:
            errors += 1
            latencies.append(999)

    p50 = statistics.quantiles(latencies, n=100)[49] if latencies else 0
    p95 = statistics.quantiles(latencies, n=100)[94] if len(latencies) >= 95 else 0
    p99 = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 99 else 0

    return sent, errors, latencies, p50, p95, p99


def main():
    url = "http://localhost:8080"
    duration = 30
    workers = 5
    table = "test_local"

    print("=" * 70)
    print("ZOMBI EC2 LOAD TEST")
    print("=" * 70)
    print(f"URL: {url}")
    print(f"Duration: {duration}s")
    print(f"Workers: {workers}")
    print(f"Table: {table}")
    print("=" * 70)
    print()

    # Test
    print(f"[1/1] Running test ({duration}s, {workers} workers)...")
    sent, errors, latencies, p50, p95, p99 = test_write(url, table, duration, workers)

    # Wait for flush
    print(f"\n[2/2] Waiting 10s for S3 flush...")
    time.sleep(10)

    # Summary
    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Total: {sent + errors:,} requests")
    print(f"Successful: {sent:,}")
    print(f"Errors: {errors:,}")
    print(f"Throughput: {sent / duration:.1f} events/sec")
    print()

    if latencies:
        percentiles = statistics.quantiles(latencies, n=100)
        print("Latency (ms):")
        print(f"  P50: {percentiles[49]:.1f}")
        print(f"  P95: {percentiles[94]:.1f}")
        print(f"  P99: {percentiles[98]:.1f}")
        print(f"  Max: {max(latencies):.1f}")
        print()

    print("=" * 70)
