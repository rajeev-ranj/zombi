#!/usr/bin/env python3
"""
Zombi Simple Load Test
Tests write throughput and latency
"""

import time
import json
import random
from concurrent.futures import ThreadPoolExecutor
import requests
import statistics


def generate_event():
    """Generate a test event."""
    return {
        "payload": json.dumps(
            {
                "type": "test",
                "data": "x" * 100,  # ~100 bytes
            }
        ),
        "timestamp_ms": int(time.time() * 1000),
        "partition": random.randint(0, 3),
    }


def test_payload(url, table, duration):
    """Test payload size and return stats."""
    sent = 0
    errors = 0
    latencies = []
    total_bytes = 0

    session = requests.Session()
    end = time.time() + duration

    while time.time() < end:
        start = time.time()
        try:
            event = generate_event()
            response = session.post(f"{url}/tables/{table}", json=event, timeout=10)
            latency_ms = (time.time() - start) * 1000
            bytes_sent = len(json.dumps(event).encode())

            if response.status_code in (200, 202):
                sent += 1
                latencies.append(latency_ms)
                total_bytes += bytes_sent
            else:
                errors += 1
                latencies.append(latency_ms)
        except Exception:
            errors += 1
            latencies.append(999)

    return {
        "sent": sent,
        "errors": errors,
        "latencies": latencies,
        "total_bytes": total_bytes,
        "avg_bytes": total_bytes / sent if sent > 0 else 0,
    }


def main():
    url = "http://18.143.135.197:8080"
    duration = 30  # seconds

    print("=" * 70)
    print("ZOMBI SIMPLE LOAD TEST")
    print("=" * 70)
    print(f"URL: {url}")
    print(f"Duration: {duration}s")
    print("=" * 70)
    print()

    workers = 5
    tables = ["a", "b", "c", "d", "e"]

    start = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(test_payload, url, table, duration) for table in tables
        ]

        # Monitor progress
        last_sent = {table: 0 for table in tables}
        while not all(f.done() for f in futures):
            time.sleep(1)

            for table in tables:
                stats = futures[tables.index(table)].result()
                rate = (stats["sent"] - last_sent[table]) / 1.0
                last_sent[table] = stats["sent"]
                print(
                    f"  {table:>4} | Rate: {rate:7.1f} | Sent: {last_sent[table]:>7,} | Errors: {stats['errors']:>5}"
                )

    elapsed = time.time() - start
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    total_sent = sum(f.result()["sent"] for f in futures)
    total_errors = sum(f.result()["errors"] for f in futures)
    all_latencies = []
    for f in futures:
        all_latencies.extend(f.result()["latencies"])

    print(f"Total Requests: {total_sent + total_errors:,}")
    print(f"  Successful: {total_sent:,}")
    print(f" Errors: {total_errors:,}")

    rate = total_sent / elapsed if elapsed > 0 else 0
    print(f"Throughput: {rate:.1f} events/sec")

    if all_latencies:
        p = statistics.quantiles(all_latencies, n=100)
        print(f"\nLatency (ms):")
        print(f"  P50: {p[49]:.1f}")
        print(f"  P90: {p[89]:.1f}")
        print(f"  P95: {p[94]:.1f}")
        print(f"  P99: {p[98]:.1f}")
        print(f"  Max: {max(all_latencies):.1f}")
        print()

    print("=" * 70)


if __name__ == "__main__":
    main()
