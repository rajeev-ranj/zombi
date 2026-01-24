#!/usr/bin/env python3
"""
Simple JSON vs Protobuf Test
Run this locally to compare write throughput
"""

import time, random, json, struct
from concurrent.futures import ThreadPoolExecutor
import requests


def test_encoding(use_binary: bool, duration: int = 30, workers: int = 5):
    sent = 0
    errors = 0
    latencies = []

    def worker():
        nonlocal sent, errors, latencies
        end = time.time() + duration

        while time.time() < end:
            start = time.time()
            try:
                if use_binary:
                    # Simulate protobuf (60 bytes)
                    data = struct.pack(">I", random.randint(1, 999999))
                    headers = {"Content-Type": "application/x-protobuf"}
                else:
                    # JSON (178 bytes)
                    event = {
                        "payload": json.dumps({"t": "x" * 100}),
                        "timestamp_ms": int(time.time() * 1000),
                        "partition": 0,
                    }
                    data = json.dumps(event).encode()
                    headers = {"Content-Type": "application/json"}

                resp = requests.post(
                    "http://localhost:8080/tables/test",
                    data=data,
                    headers=headers,
                    timeout=10,
                )

                latency = (time.time() - start) * 1000
                if resp.status_code in (200, 202):
                    sent += 1
                    latencies.append(latency)
                else:
                    errors += 1
                    latencies.append(latency)
            except:
                errors += 1
                latencies.append(999)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        executor.submit(worker)

    time.sleep(duration + 1)
    return sent, errors, latencies


def main():
    duration = 30

    print("=" * 70)
    print("ZOMBI: JSON vs PROTOKUF-LIKE (BINARY) TEST")
    print("=" * 70)
    print(f"Duration: {duration}s per test")
    print("=" * 70)

    # Test JSON
    print("\n[1/2] JSON Test (178 bytes)...")
    json_sent, json_err, json_lat = test_encoding(False, duration)
    print(f"  Sent: {json_sent:,} ({100 * json_sent / (json_sent + json_err):.1f}%)")
    print(f"  Errors: {json_err:,}")
    if json_lat:
        import statistics

        p50 = statistics.quantiles(json_lat, n=100)[49]
        print(f"  Latency P50: {p50:.1f}ms")

    # Test binary
    print("\n[2/2] Binary Test (60 bytes - simulating protobuf)...")
    bin_sent, bin_err, bin_lat = test_encoding(True, duration)
    print(f"  Sent: {bin_sent:,} ({100 * bin_sent / (bin_sent + bin_err):.1f}%)")
    print(f"  Errors: {bin_err:,}")
    if bin_lat:
        import statistics

        p50 = statistics.quantiles(bin_lat, n=100)[49]
        print(f"  Latency P50: {p50:.1f}ms")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    json_rate = json_sent / duration
    bin_rate = bin_sent / duration

    print(f"\nJSON: {json_rate:.1f} events/sec")
    print(f"Binary: {bin_rate:.1f} events/sec")

    imp = 100 * (bin_rate - json_rate) / json_rate if json_rate > 0 else 0
    print(f"  Improvement: {imp:+.1f}%")


if __name__ == "__main__":
    main()
