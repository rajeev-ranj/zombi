#!/usr/bin/env python3
"""
Zombi Proto vs JSON Performance Benchmark

Comprehensive comparison of Protobuf vs JSON encoding for:
- Write throughput and latency
- Read throughput and latency
- Iceberg/S3 storage efficiency

Usage:
    # Local testing
    python proto_json_benchmark.py --url http://localhost:8080

    # EC2 testing
    python proto_json_benchmark.py --url http://<EC2-IP>:8080 --duration 300

    # Quick test
    python proto_json_benchmark.py --url http://localhost:8080 --duration 30 --workers 5
"""

import argparse
import json
import random
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import the generated protobuf module
try:
    import event_pb2 as pb
except ImportError:
    from tools import event_pb2 as pb


@dataclass
class BenchmarkStats:
    """Thread-safe statistics collector for benchmarks."""

    total_sent: int = 0
    total_errors: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    latencies_ms: List[float] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record_write(self, success: bool, latency_ms: float, bytes_sent: int = 0):
        with self.lock:
            if success:
                self.total_sent += 1
                self.total_bytes_sent += bytes_sent
                self.latencies_ms.append(latency_ms)
            else:
                self.total_errors += 1

    def record_read(self, success: bool, latency_ms: float, bytes_received: int = 0):
        with self.lock:
            if success:
                self.total_sent += 1
                self.total_bytes_received += bytes_received
                self.latencies_ms.append(latency_ms)
            else:
                self.total_errors += 1

    def get_percentiles(self) -> dict:
        with self.lock:
            if len(self.latencies_ms) < 100:
                return {"p50": 0, "p90": 0, "p95": 0, "p99": 0, "max": 0, "min": 0}
            percentiles = statistics.quantiles(self.latencies_ms, n=100)
            return {
                "p50": percentiles[49],
                "p90": percentiles[89],
                "p95": percentiles[94],
                "p99": percentiles[98],
                "max": max(self.latencies_ms),
                "min": min(self.latencies_ms),
            }

    def throughput(self, duration: float) -> float:
        return self.total_sent / duration if duration > 0 else 0


def create_session() -> requests.Session:
    """Create HTTP session with connection pooling and retry logic."""
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def generate_payload_data(size_bytes: int = 200) -> dict:
    """Generate consistent payload data for fair comparison."""
    event_types = ["user_action", "order_created", "page_view", "metric", "system_event"]
    event_type = random.choice(event_types)

    base_data = {
        "event_type": event_type,
        "user_id": f"user_{random.randint(1, 100000)}",
        "session_id": f"sess_{random.randint(1, 1000000)}",
        "timestamp": int(time.time()),
        "region": random.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]),
        "source": random.choice(["web", "mobile", "api", "worker"]),
    }

    # Pad to target size
    current_size = len(json.dumps(base_data))
    if current_size < size_bytes:
        base_data["padding"] = "x" * (size_bytes - current_size - 15)

    return base_data


def create_json_request(payload_data: dict, partition: int) -> Tuple[bytes, dict]:
    """Create JSON request body and headers."""
    request = {
        "payload": json.dumps(payload_data),
        "timestamp_ms": int(time.time() * 1000),
        "partition": partition,
    }
    body = json.dumps(request).encode()
    headers = {"Content-Type": "application/json"}
    return body, headers


def create_protobuf_request(payload_data: dict) -> Tuple[bytes, dict]:
    """Create Protobuf request body and headers."""
    event = pb.Event()
    event.payload = json.dumps(payload_data).encode()
    event.timestamp_ms = int(time.time() * 1000)
    event.idempotency_key = f"bench_{random.randint(1, 1000000000)}"
    event.headers["source"] = "benchmark"
    event.headers["encoding"] = "protobuf"

    body = event.SerializeToString()
    headers = {"Content-Type": "application/x-protobuf"}
    return body, headers


# =============================================================================
# WRITE BENCHMARK
# =============================================================================

def write_worker(
    url: str,
    table: str,
    duration: float,
    stats: BenchmarkStats,
    use_protobuf: bool,
    payload_size: int,
):
    """Worker thread for write benchmark."""
    session = create_session()
    end_time = time.time() + duration

    while time.time() < end_time:
        payload_data = generate_payload_data(payload_size)
        partition = random.randint(0, 3)

        if use_protobuf:
            body, headers = create_protobuf_request(payload_data)
        else:
            body, headers = create_json_request(payload_data, partition)

        headers["X-Partition"] = str(partition)

        start = time.time()
        try:
            response = session.post(
                f"{url}/tables/{table}",
                data=body,
                headers=headers,
                timeout=10,
            )
            latency_ms = (time.time() - start) * 1000

            if response.status_code in (200, 202):
                stats.record_write(True, latency_ms, len(body))
            else:
                stats.record_write(False, latency_ms)
        except Exception:
            latency_ms = (time.time() - start) * 1000
            stats.record_write(False, latency_ms)


def run_write_benchmark(
    url: str,
    table: str,
    duration: float,
    workers: int,
    use_protobuf: bool,
    payload_size: int,
) -> BenchmarkStats:
    """Run write throughput benchmark."""
    stats = BenchmarkStats()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                write_worker, url, table, duration, stats, use_protobuf, payload_size
            )
            for _ in range(workers)
        ]

        # Progress monitoring
        start_time = time.time()
        last_sent = 0
        while any(not f.done() for f in futures):
            time.sleep(5)
            elapsed = time.time() - start_time
            rate = (stats.total_sent - last_sent) / 5
            encoding = "Proto" if use_protobuf else "JSON"
            print(f"    [{elapsed:.0f}s] {encoding}: {rate:.0f}/s | Total: {stats.total_sent:,}")
            last_sent = stats.total_sent

        for f in futures:
            f.result()

    return stats


# =============================================================================
# READ BENCHMARK
# =============================================================================

def read_worker(
    url: str,
    table: str,
    iterations: int,
    stats: BenchmarkStats,
    limit: int = 100,
):
    """Worker thread for read benchmark."""
    session = create_session()

    for _ in range(iterations):
        since = int((time.time() - 300) * 1000)  # Last 5 minutes

        start = time.time()
        try:
            response = session.get(
                f"{url}/tables/{table}",
                params={"since": since, "limit": limit},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            latency_ms = (time.time() - start) * 1000

            if response.status_code == 200:
                stats.record_read(True, latency_ms, len(response.content))
            else:
                stats.record_read(False, latency_ms)
        except Exception:
            latency_ms = (time.time() - start) * 1000
            stats.record_read(False, latency_ms)


def run_read_benchmark(
    url: str,
    table: str,
    total_requests: int,
    workers: int,
) -> BenchmarkStats:
    """Run read latency benchmark."""
    stats = BenchmarkStats()
    iterations_per_worker = total_requests // workers

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(read_worker, url, table, iterations_per_worker, stats)
            for _ in range(workers)
        ]

        for f in futures:
            f.result()

    return stats


# =============================================================================
# STORAGE ANALYSIS
# =============================================================================

def get_s3_storage_stats(bucket: str, table: str) -> dict:
    """Get S3 storage statistics for a table."""
    try:
        result = subprocess.run(
            [
                "aws", "s3", "ls",
                f"s3://{bucket}/tables/{table}/",
                "--recursive", "--summarize"
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        lines = result.stdout.strip().split("\n")
        total_objects = 0
        total_size = 0

        for line in lines:
            if "Total Objects:" in line:
                total_objects = int(line.split(":")[1].strip())
            elif "Total Size:" in line:
                total_size = int(line.split(":")[1].strip())
            elif line.strip() and not line.startswith("Total"):
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        total_size += int(parts[2])
                        total_objects += 1
                    except ValueError:
                        pass

        return {
            "objects": total_objects,
            "total_bytes": total_size,
            "total_mb": total_size / 1024 / 1024,
        }
    except Exception as e:
        return {"error": str(e), "objects": 0, "total_bytes": 0, "total_mb": 0}


def check_iceberg_metadata(url: str, table: str) -> dict:
    """Check Iceberg table metadata via API."""
    try:
        response = requests.get(f"{url}/tables/{table}/metadata", timeout=10)
        if response.status_code == 200:
            return response.json()
        return {"error": f"Status {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# REPORTING
# =============================================================================

def print_stats(label: str, stats: BenchmarkStats, duration: float):
    """Print benchmark statistics."""
    percentiles = stats.get_percentiles()
    throughput = stats.throughput(duration)

    print(f"\n  {label}:")
    print(f"    Total Requests: {stats.total_sent + stats.total_errors:,}")
    print(f"    Successful: {stats.total_sent:,}")
    print(f"    Errors: {stats.total_errors:,}")
    print(f"    Throughput: {throughput:.1f} req/sec")
    print(f"    Bytes Sent: {stats.total_bytes_sent:,} ({stats.total_bytes_sent/1024/1024:.2f} MB)")

    if percentiles["p50"] > 0:
        print(f"    Latency P50: {percentiles['p50']:.2f} ms")
        print(f"    Latency P90: {percentiles['p90']:.2f} ms")
        print(f"    Latency P95: {percentiles['p95']:.2f} ms")
        print(f"    Latency P99: {percentiles['p99']:.2f} ms")
        print(f"    Latency Max: {percentiles['max']:.2f} ms")

    if stats.total_sent > 0:
        avg_size = stats.total_bytes_sent / stats.total_sent
        print(f"    Avg Request Size: {avg_size:.0f} bytes")


def print_comparison(json_stats: BenchmarkStats, proto_stats: BenchmarkStats, duration: float):
    """Print comparison summary."""
    json_throughput = json_stats.throughput(duration)
    proto_throughput = proto_stats.throughput(duration)

    json_p50 = json_stats.get_percentiles()["p50"]
    proto_p50 = proto_stats.get_percentiles()["p50"]

    throughput_diff = ((proto_throughput - json_throughput) / json_throughput * 100) if json_throughput > 0 else 0
    latency_diff = ((proto_p50 - json_p50) / json_p50 * 100) if json_p50 > 0 else 0

    json_avg_size = json_stats.total_bytes_sent / json_stats.total_sent if json_stats.total_sent > 0 else 0
    proto_avg_size = proto_stats.total_bytes_sent / proto_stats.total_sent if proto_stats.total_sent > 0 else 0
    size_diff = ((proto_avg_size - json_avg_size) / json_avg_size * 100) if json_avg_size > 0 else 0

    print("\n" + "=" * 70)
    print("COMPARISON SUMMARY")
    print("=" * 70)
    print(f"\n  Throughput:")
    print(f"    JSON:     {json_throughput:,.1f} req/sec")
    print(f"    Protobuf: {proto_throughput:,.1f} req/sec")
    print(f"    Difference: {throughput_diff:+.1f}%")

    print(f"\n  Latency (P50):")
    print(f"    JSON:     {json_p50:.2f} ms")
    print(f"    Protobuf: {proto_p50:.2f} ms")
    print(f"    Difference: {latency_diff:+.1f}%")

    print(f"\n  Request Size (avg):")
    print(f"    JSON:     {json_avg_size:.0f} bytes")
    print(f"    Protobuf: {proto_avg_size:.0f} bytes")
    print(f"    Difference: {size_diff:+.1f}%")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Zombi Proto vs JSON Performance Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Local quick test
    python proto_json_benchmark.py --url http://localhost:8080 --duration 30

    # EC2 full benchmark
    python proto_json_benchmark.py --url http://<EC2-IP>:8080 --duration 300 --workers 20

    # Write-only test
    python proto_json_benchmark.py --url http://localhost:8080 --skip-read --skip-storage

    # With custom S3 bucket
    python proto_json_benchmark.py --url http://localhost:8080 --s3-bucket my-zombi-bucket
        """,
    )

    parser.add_argument("--url", default="http://localhost:8080", help="Zombi server URL")
    parser.add_argument("--duration", type=int, default=60, help="Write test duration in seconds")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")
    parser.add_argument("--payload-size", type=int, default=200, help="Target payload size in bytes")
    parser.add_argument("--read-requests", type=int, default=1000, help="Number of read requests")
    parser.add_argument("--json-table", default="bench_json", help="Table name for JSON tests")
    parser.add_argument("--proto-table", default="bench_proto", help="Table name for Protobuf tests")
    parser.add_argument("--s3-bucket", default="zombi-events-test1", help="S3 bucket for storage analysis")
    parser.add_argument("--skip-read", action="store_true", help="Skip read benchmark")
    parser.add_argument("--skip-storage", action="store_true", help="Skip storage analysis")
    parser.add_argument("--flush-wait", type=int, default=15, help="Seconds to wait for S3 flush")

    args = parser.parse_args()

    print("=" * 70)
    print("ZOMBI PROTO VS JSON PERFORMANCE BENCHMARK")
    print("=" * 70)
    print(f"URL: {args.url}")
    print(f"Duration: {args.duration}s")
    print(f"Workers: {args.workers}")
    print(f"Payload Size: {args.payload_size} bytes")
    print(f"JSON Table: {args.json_table}")
    print(f"Proto Table: {args.proto_table}")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 70)

    # =========================================================================
    # WRITE BENCHMARKS
    # =========================================================================
    print("\n" + "=" * 70)
    print("WRITE BENCHMARKS")
    print("=" * 70)

    print(f"\n[1/2] JSON Write Test ({args.duration}s, {args.workers} workers)...")
    json_write_stats = run_write_benchmark(
        args.url, args.json_table, args.duration, args.workers,
        use_protobuf=False, payload_size=args.payload_size
    )
    print_stats("JSON Write Results", json_write_stats, args.duration)

    print(f"\n[2/2] Protobuf Write Test ({args.duration}s, {args.workers} workers)...")
    proto_write_stats = run_write_benchmark(
        args.url, args.proto_table, args.duration, args.workers,
        use_protobuf=True, payload_size=args.payload_size
    )
    print_stats("Protobuf Write Results", proto_write_stats, args.duration)

    print_comparison(json_write_stats, proto_write_stats, args.duration)

    # =========================================================================
    # READ BENCHMARKS
    # =========================================================================
    if not args.skip_read:
        print("\n" + "=" * 70)
        print("READ BENCHMARKS")
        print("=" * 70)

        print(f"\n[1/2] JSON Read Test ({args.read_requests} requests)...")
        json_read_stats = run_read_benchmark(
            args.url, args.json_table, args.read_requests, args.workers
        )
        read_duration = sum(json_read_stats.latencies_ms) / 1000 if json_read_stats.latencies_ms else 1
        print_stats("JSON Read Results", json_read_stats, read_duration)

        print(f"\n[2/2] Protobuf Read Test ({args.read_requests} requests)...")
        proto_read_stats = run_read_benchmark(
            args.url, args.proto_table, args.read_requests, args.workers
        )
        read_duration = sum(proto_read_stats.latencies_ms) / 1000 if proto_read_stats.latencies_ms else 1
        print_stats("Protobuf Read Results", proto_read_stats, read_duration)

    # =========================================================================
    # STORAGE ANALYSIS
    # =========================================================================
    if not args.skip_storage:
        print("\n" + "=" * 70)
        print("STORAGE ANALYSIS")
        print("=" * 70)

        print(f"\nWaiting {args.flush_wait}s for S3/Iceberg flush...")
        time.sleep(args.flush_wait)

        print("\nS3 Storage Stats:")
        for table, label in [(args.json_table, "JSON"), (args.proto_table, "Protobuf")]:
            stats = get_s3_storage_stats(args.s3_bucket, table)
            if "error" not in stats or stats.get("objects", 0) > 0:
                print(f"\n  {label} ({table}):")
                print(f"    Objects: {stats['objects']}")
                print(f"    Total Size: {stats['total_bytes']:,} bytes ({stats['total_mb']:.2f} MB)")
                if stats['objects'] > 0:
                    avg = stats['total_bytes'] / stats['objects']
                    print(f"    Avg Object Size: {avg:,.0f} bytes")
            else:
                print(f"\n  {label}: No data or error - {stats.get('error', 'unknown')}")

        print("\nIceberg Metadata:")
        for table, label in [(args.json_table, "JSON"), (args.proto_table, "Protobuf")]:
            metadata = check_iceberg_metadata(args.url, table)
            if "error" not in metadata:
                print(f"\n  {label} ({table}):")
                for key, value in metadata.items():
                    print(f"    {key}: {value}")
            else:
                print(f"\n  {label}: {metadata.get('error', 'No metadata')}")

    # =========================================================================
    # FINAL SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)

    json_tput = json_write_stats.throughput(args.duration)
    proto_tput = proto_write_stats.throughput(args.duration)
    winner = "Protobuf" if proto_tput > json_tput else "JSON"
    diff = abs(proto_tput - json_tput) / min(json_tput, proto_tput) * 100 if min(json_tput, proto_tput) > 0 else 0

    print(f"\n  Write Performance Winner: {winner} (+{diff:.1f}%)")
    print(f"  JSON Throughput:     {json_tput:,.1f} events/sec")
    print(f"  Protobuf Throughput: {proto_tput:,.1f} events/sec")

    json_size = json_write_stats.total_bytes_sent / json_write_stats.total_sent if json_write_stats.total_sent > 0 else 0
    proto_size = proto_write_stats.total_bytes_sent / proto_write_stats.total_sent if proto_write_stats.total_sent > 0 else 0
    size_savings = (json_size - proto_size) / json_size * 100 if json_size > 0 else 0

    print(f"\n  Wire Format Efficiency:")
    print(f"    JSON avg size:     {json_size:.0f} bytes")
    print(f"    Protobuf avg size: {proto_size:.0f} bytes")
    print(f"    Protobuf savings:  {size_savings:.1f}%")

    print("\n" + "=" * 70)
    print("Benchmark complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
