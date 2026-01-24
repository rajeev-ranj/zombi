#!/usr/bin/env python3
"""
Zombi Protobuf vs JSON - Complete Performance Test
Tests write throughput, read latency, and S3/Iceberg storage
"""

import time
import json
import random
import struct
from concurrent.futures import ThreadPoolExecutor
import requests
import statistics
import subprocess


def generate_json_payload(size_bytes: int = 178) -> dict:
    """Generate JSON payload of target size."""
    padding_len = max(0, size_bytes - 150)
    event = {
        "payload": json.dumps({"type": "test", "data": "x" * padding_len}),
        "timestamp_ms": int(time.time() * 1000),
        "partition": random.randint(0, 3),
    }
    return event


def generate_protobuf_like_payload(size_bytes: int = 60) -> bytes:
    """Generate protobuf-like binary payload.
    Format: [length][type][data...]
    """
    data_bytes = b"test" * max(1, (size_bytes - 10))
    return struct.pack(f"<H{len(data_bytes)}s", len(data_bytes), data_bytes)


def run_write_test(
    url: str, table: str, duration: int, use_protobuf: bool, workers: int
):
    """Run write throughput test."""
    sent = 0
    errors = 0
    latencies = []
    total_bytes = 0

    session = requests.Session()
    end_time = time.time() + duration

    while time.time() < end_time:
        start = time.time()

        try:
            if use_protobuf:
                # Simulate protobuf payload (~60 bytes)
                payload_bytes = generate_protobuf_like_payload(60)
                headers = {
                    "Content-Type": "application/x-protobuf",
                    "X-Partition": str(random.randint(0, 3)),
                }
                data = payload_bytes
                bytes_sent = len(payload_bytes)
            else:
                # JSON payload (~178 bytes)
                event = generate_json_payload(178)
                headers = {"Content-Type": "application/json"}
                data = json.dumps(event)
                bytes_sent = len(data.encode())

            response = session.post(
                f"{url}/tables/{table}", data=data, headers=headers, timeout=10
            )

            latency = (time.time() - start) * 1000
            if response.status_code in (200, 202):
                sent += 1
                latencies.append(latency)
                total_bytes += bytes_sent
            else:
                errors += 1
                latencies.append(latency)
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


def run_read_test(url: str, table: str, count: int):
    """Run read latency test."""
    sent = 0
    errors = 0
    latencies = []
    total_bytes = 0

    session = requests.Session()

    for offset in range(count):
        start = time.time()
        try:
            response = session.get(
                f"{url}/tables/{table}/offset/{offset}/limit/10", timeout=10
            )
            latency = (time.time() - start) * 1000

            if response.status_code == 200:
                sent += 1
                latencies.append(latency)
                total_bytes += len(response.content)
            else:
                errors += 1
                latencies.append(latency)
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


def check_s3_storage(table: str):
    """Check S3 storage for a table."""
    result = subprocess.run(
        [
            "aws",
            "s3",
            "ls",
            f"s3://zombi-events-test1/tables/{table}/data/",
            "--recursive",
        ],
        capture_output=True,
        text=True,
    )
    lines = [l for l in result.stdout.split("\n") if l.strip()]

    count = len(lines)
    total_bytes = 0
    for line in lines:
        parts = line.split()
        if len(parts) >= 3:
            try:
                total_bytes += int(parts[2])
            except:
                pass

    return {
        "count": count,
        "total_bytes": total_bytes,
        "avg_size": total_bytes / count if count > 0 else 0,
    }


def print_test_results(label: str, results: dict, duration: float = None):
    """Print formatted test results."""
    print(f"\n{label}")
    print("-" * 60)

    sent = results.get("sent", 0)
    errors = results.get("errors", 0)
    latencies = results.get("latencies", [])
    total_bytes = results.get("total_bytes", 0)

    print(f"  Requests: {sent + errors:,}")
    print(
        f"  Successful: {sent:,} ({100 * sent / (sent + errors) if (sent + errors) > 0 else 100:.1f}%)"
    )
    print(f"  Errors: {errors:,}")

    if duration:
        print(f"  Throughput: {sent / duration:.1f} events/sec")

    print(f"  Total Bytes: {total_bytes:,} ({total_bytes / 1024 / 1024:.2f} MB)")
    if sent > 0:
        print(f"  Avg Bytes/Event: {total_bytes / sent:.0f} bytes")

    if latencies:
        percentiles = statistics.quantiles(latencies, n=100)
        print(f"  Latency P50: {percentiles[49]:.1f}ms")
        print(f"  Latency P95: {percentiles[94]:.1f}")
        print(f"  Letency P99: {percentiles[98]:.1f}ms")
        print(f"  Latency Max: {max(latencies):.1f}ms")


def main():
    url = "http://localhost:8080"
    duration = 60  # seconds per write test
    read_count = 1000

    print("=" * 70)
    print("ZOMBI: PROTOBUF VS JSON - COMPLETE PERFORMANCE TEST")
    print("=" * 70)
    print(f"URL: {url}")
    print(f"Write Duration: {duration}s per test")
    print(f"Read Requests: {read_count}")
    print(f"Tables: test_json, test_proto")
    print("=" * 70)

    # Write tests
    print("\n[1/4] Running JSON write test...")
    json_write = run_write_test(url, "test_json", duration, False, 5)
    print_test_results("JSON Write", json_write, duration)

    print("\n[2/4] Running Protobuf write test...")
    proto_write = run_write_test(url, "test_proto", duration, True, 5)
    print_test_results("Protobuf Write", proto_write, duration)

    # Wait for flush
    print(f"\n[3/4] Waiting 10s for S3 flush...")
    time.sleep(10)

    # Check S3 storage
    print(f"\n[4/4] Checking S3 storage...")
    json_s3 = check_s3_storage("test_json")
    proto_s3 = check_s3_storage("test_proto")

    print("\n" + "=" * 70)
    print("S3 STORAGE")
    print("=" * 70)

    print(f"\ntest_json (JSON):")
    print(f"  Objects: {json_s3['count']:,}")
    print(f"  Total Size: {json_s3['total_bytes']:,} bytes")
    print(f"  Avg Size: {json_s3['avg_size']:.0f} bytes/object")

    print(f"\ntest_proto (Protobuf):")
    print(f"  Objects: {proto_s3['count']:,}")
    print(f"  Total Size: {proto_s3['total_bytes']:,} bytes")
    print(f"  Avg Size: {proto_s3['avg_size']:.0f} bytes/object")

    # Read tests
    print(f"\n[5/6] Running JSON read test...")
    json_read = run_read_test(url, "test_json", read_count)
    print_test_results("JSON Read", json_read)

    print(f"\n[6/6] Running Protobuf read test...")
    proto_read = run_read_test(url, "test_proto", read_count)
    print_test_results("Protobuf Read", proto_read)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    # Write comparison
    print(f"\nWRITE THROUGHPUT:")
    write_improvement = (
        100 * (proto_write["sent"] - json_write["sent"]) / json_write["sent"]
        if json_write["sent"] > 0
        else 0
    )
    print(f"  JSON: {json_write['sent'] / duration:.1f} events/sec")
    print(f"  Protobuf: {proto_write['sent'] / duration:.1f} events/sec")
    print(f"  Improvement: {write_improvement:+.1f}%")

    # Write latency comparison
    if json_write["latencies"] and proto_write["latencies"]:
        json_p50 = statistics.quantiles(json_write["latencies"], n=100)[49]
        proto_p50 = statistics.quantiles(proto_write["latencies"], n=100)[49]
        latency_imp = 100 * (json_p50 - proto_p50) / json_p50 if json_p50 > 0 else 0
        print(f"\nWRITE LATENCY (P50):")
        print(f"  JSON: {json_p50:.1f}ms")
        print(f"  Protobuf: {proto_p50:.1f}ms")
        print(f"  Improvement: {latency_imp:+.1f}%")

    # Read latency comparison
    if json_read["latencies"] and proto_read["latencies"]:
        json_p50 = statistics.quantiles(json_read["latencies"], n=100)[49]
        proto_p50 = statistics.quantiles(proto_read["latencies"], n=100)[49]
        latency_imp = 100 * (json_p50 - proto_p50) / json_p50 if json_p50 > 0 else 0
        print(f"\nREAD LATENCY (P50):")
        print(f"  JSON: {json_p50:.1f}ms")
        print(f"  Protobuf: {proto_p50:.1f}ms")
        print(f"  Improvement: {latency_imp:+.1f}%")

    # Storage comparison
    print(f"\nS3 STORAGE (Avg Object Size):")
    json_size = json_s3["avg_size"]
    proto_size = proto_s3["avg_size"]
    size_imp = 100 * (json_size - proto_size) / json_size if json_size > 0 else 0
    print(f"  JSON: {json_size:.0f} bytes")
    print(f"  Protobuf: {proto_size:.0f} bytes")
    print(f"  Storage Savings: {size_imp:+.1f}%")

    # Estimated storage costs
    print(f"\nESTIMATED STORAGE COST (per 1M events):")
    json_cost_mb = (json_size / 1024 / 1024) * 1000000
    proto_cost_mb = (proto_size / 1024 / 1024) * 1000000
    print(f"  JSON: {json_cost_mb:.2f} MB")
    print(f"  Protobuf: {proto_cost_mb:.2f} MB")
    print(f"  Savings: {json_cost_mb - proto_cost_mb:.2f} MB")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
