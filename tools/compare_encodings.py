#!/usr/bin/env python3
"""
Zombi Protobuf vs JSON Comparison Test

Tests both encoding formats for:
- Write throughput
- Read latency
- S3/Iceberg file sizes
- Network overhead
"""

import argparse
import json
import random
import statistics
import sys
import time
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class TestStats:
    """Thread-safe statistics collector."""

    total_sent: int = 0
    total_errors: int = 0
    total_bytes: int = 0
    latencies_ms: List[float] = None
    lock: threading.Lock = None

    def __post_init__(self):
        self.latencies_ms = []
        self.lock = threading.Lock()

    def record(self, success: bool, latency_ms: float, bytes_sent: int = 0):
        with self.lock:
            if success:
                self.total_sent += 1
                self.total_bytes += bytes_sent
                self.latencies_ms.append(latency_ms)
            else:
                self.total_errors += 1


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def generate_json_payload(size_bytes: int = 178) -> str:
    """Generate a JSON payload of target size."""
    # Create a realistic event that serializes to ~178 bytes
    event = {
        "event_type": random.choice(["user", "order", "click", "metric"]),
        "user_id": str(random.randint(1, 100000)),
        "timestamp": int(time.time()),
        "data": "x" * max(0, size_bytes - 80),  # Adjust to hit target size
    }
    payload = json.dumps(event)
    # Trim/pad to hit target size
    if len(payload) > size_bytes:
        payload = payload[:size_bytes]
    elif len(payload) < size_bytes:
        payload = payload + " " * (size_bytes - len(payload))
    return payload


def run_write_test(
    url: str, table: str, duration: int, use_protobuf: bool, workers: int
) -> TestStats:
    """Run write throughput test."""
    stats = TestStats()
    session = create_session()

    def worker():
        end = time.time() + duration
        while time.time() < end:
            # Generate payload
            if use_protobuf:
                # Use the proto file from tools/producer
                # For now, simulate with binary
                import struct

                payload = struct.pack(
                    f"<I{len('test') + 4}s", len("test"), b"test_payload_data_here" * 10
                )
                content_type = "application/x-protobuf"
                body = payload
            else:
                payload = generate_json_payload()
                content_type = "application/json"
                body = json.dumps(
                    {
                        "payload": payload,
                        "timestamp_ms": int(time.time() * 1000),
                        "partition": random.randint(0, 3),
                    }
                )

            start = time.time()
            try:
                response = session.post(
                    f"{url}/tables/{table}",
                    data=body,
                    headers={
                        "Content-Type": content_type,
                        "X-Partition": str(random.randint(0, 3)),
                    },
                    timeout=10,
                )
                latency_ms = (time.time() - start) * 1000

                if response.status_code in (200, 202):
                    bytes_sent = (
                        len(body) if isinstance(body, bytes) else len(body.encode())
                    )
                    stats.record(True, latency_ms, bytes_sent)
                else:
                    stats.record(False, latency_ms)
            except Exception:
                latency_ms = (time.time() - start) * 1000
                stats.record(False, latency_ms)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker) for _ in range(workers)]
        for f in futures:
            f.result()

    return stats


def run_read_test(
    url: str, table: str, offset: int, limit: int, use_protobuf: bool
) -> TestStats:
    """Run read latency test."""
    stats = TestStats()
    session = create_session()

    for _ in range(limit):
        start = time.time()
        try:
            response = session.get(
                f"{url}/tables/{table}/offset/{offset}",
                headers={
                    "Accept": "application/x-protobuf"
                    if use_protobuf
                    else "application/json"
                },
                timeout=10,
            )
            latency_ms = (time.time() - start) * 1000
            bytes_received = len(response.content) if response.status_code == 200 else 0

            if response.status_code == 200:
                stats.record(True, latency_ms, bytes_received)
            else:
                stats.record(False, latency_ms)
        except Exception:
            latency_ms = (time.time() - start) * 1000
            stats.record(False, latency_ms)

    return stats


def print_stats(label: str, stats: TestStats, duration: float):
    """Print test statistics."""
    print(f"\n{label}:")
    print(f"  Total: {stats.total_sent + stats.total_errors:,} requests")
    print(
        f"  Success: {stats.total_sent:,} ({100 * stats.total_sent / (stats.total_sent + stats.total_errors or 1):.1f}%)"
    )
    print(f"  Errors: {stats.total_errors:,}")
    print(f"  Throughput: {stats.total_sent / duration:.1f} events/sec")
    print(
        f"  Total Bytes: {stats.total_bytes:,} ({stats.total_bytes / 1024 / 1024:.2f} MB)"
    )

    if stats.latencies_ms:
        p = statistics.quantiles(stats.latencies_ms, n=100)
        print(f"  Latency P50: {p[49]:.1f}ms")
        print(f"  Latency P95: {p[94]:.1f}ms")
        print(f"  Latency P99: {p[98]:.1f}ms")
        print(f"  Latency Max: {max(stats.latencies_ms):.1f}ms")
        print(f"  Avg Bytes/Req: {stats.total_bytes / stats.total_sent:.0f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--json-table", default="test_json")
    parser.add_argument("--proto-table", default="test_proto")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--read-requests", type=int, default=1000)
    args = parser.parse_args()

    print("=" * 70)
    print("ZOMBI PROTOBUF VS JSON COMPARISON TEST")
    print("=" * 70)
    print(f"URL: {args.url}")
    print(f"Duration: {args.duration}s")
    print(f"Workers: {args.workers}")
    print("=" * 70)

    # Write tests
    print("\n" + "=" * 70)
    print("WRITE TESTS")
    print("=" * 70)

    print(f"\n[1/2] JSON Write Test...")
    json_stats = run_write_test(
        args.url, args.json_table, args.duration, False, args.workers
    )
    print_stats("JSON Write", json_stats, args.duration)

    print(f"\n[2/2] Protobuf Write Test...")
    proto_stats = run_write_test(
        args.url, args.proto_table, args.duration, True, args.workers
    )
    print_stats("Protobuf Write", proto_stats, args.duration)

    # Wait for flush
    print(f"\nWaiting 10s for S3 flush...")
    time.sleep(10)

    # Check S3
    print("\n" + "=" * 70)
    print("S3 STORAGE COMPARISON")
    print("=" * 70)

    for table, label in [(args.json_table, "JSON"), (args.proto_table, "Protobuf")]:
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
        total_size = sum(int(l.split()[2]) for l in lines if len(l.split()) >= 3)
        avg_size = total_size / count if count > 0 else 0

        print(f"\n{label} ({table}):")
        print(f"  Objects: {count}")
        print(f"  Total Size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
        print(f"  Avg Object Size: {avg_size:.0f} bytes")

    # Read tests
    print("\n" + "=" * 70)
    print("READ TESTS")
    print("=" * 70)

    print(f"\n[1/2] JSON Read Test...")
    json_read = run_read_test(args.url, args.json_table, 0, args.read_requests, False)
    print_stats("JSON Read", json_read, args.read_requests)

    print(f"\n[2/2] Protobuf Read Test...")
    proto_read = run_read_test(args.url, args.proto_table, 0, args.read_requests, True)
    print_stats("Protobuf Read", proto_read, args.read_requests)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\nWrite Throughput:")
    write_improvement = (
        100 * (proto_stats.total_sent - json_stats.total_sent) / json_stats.total_sent
    )
    print(f"  JSON: {json_stats.total_sent / args.duration:.1f} events/sec")
    print(f"  Protobuf: {proto_stats.total_sent / args.duration:.1f} events/sec")
    print(f"  Improvement: {write_improvement:+.1f}%")

    print(f"\nWrite Latency (P50):")
    print(f"  JSON: {statistics.quantiles(json_stats.latencies_ms, n=100)[49]:.1f}ms")
    print(
        f"  Protobuf: {statistics.quantiles(proto_stats.latencies_ms, n=100)[49]:.1f}ms"
    )

    print(f"\nRead Latency (P50):")
    print(f"  JSON: {statistics.quantiles(json_read.latencies_ms, n=100)[49]:.1f}ms")
    print(
        f"  Protobuf: {statistics.quantiles(proto_read.latencies_ms, n=100)[49]:.1f}ms"
    )

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
