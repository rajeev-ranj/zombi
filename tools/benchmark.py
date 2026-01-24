#!/usr/bin/env python3
"""
Zombi Comprehensive Benchmark Tool

A unified benchmark suite for testing Zombi performance across multiple dimensions:
- Encoding: Proto vs JSON
- Operations: Writes, Reads, Roundtrip
- Payload sizes: Small (100B), Medium (1KB), Large (32KB)
- Storage: Hot (RocksDB) vs Cold (Iceberg/S3)

Usage:
    python benchmark.py --url http://localhost:8080 --suite quick
    python benchmark.py --url http://localhost:8080 --suite full
    python benchmark.py --url http://localhost:8080 --test proto-vs-json
"""

import argparse
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Try to import protobuf support
try:
    from google.protobuf import descriptor_pb2
    PROTO_AVAILABLE = True
except ImportError:
    PROTO_AVAILABLE = False


# ============================================================================
# Statistics Collection
# ============================================================================

@dataclass
class Stats:
    """Thread-safe statistics collector."""
    total: int = 0
    errors: int = 0
    bytes_total: int = 0
    latencies_ms: List[float] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, success: bool, latency_ms: float, bytes_sent: int = 0):
        with self.lock:
            self.total += 1
            if success:
                self.bytes_total += bytes_sent
                self.latencies_ms.append(latency_ms)
            else:
                self.errors += 1

    def percentile(self, p: float) -> float:
        with self.lock:
            if not self.latencies_ms:
                return 0.0
            sorted_lat = sorted(self.latencies_ms)
            idx = int(len(sorted_lat) * p / 100)
            return sorted_lat[min(idx, len(sorted_lat) - 1)]

    def summary(self) -> Dict:
        with self.lock:
            success = self.total - self.errors
            return {
                "total": self.total,
                "success": success,
                "errors": self.errors,
                "bytes": self.bytes_total,
                "p50_ms": self.percentile(50),
                "p95_ms": self.percentile(95),
                "p99_ms": self.percentile(99),
                "min_ms": min(self.latencies_ms) if self.latencies_ms else 0,
                "max_ms": max(self.latencies_ms) if self.latencies_ms else 0,
            }


# ============================================================================
# HTTP Client
# ============================================================================

def create_session(pool_size: int = 100) -> requests.Session:
    """Create HTTP session with connection pooling and retries."""
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ============================================================================
# Proto Encoding (manual implementation without generated code)
# ============================================================================

def encode_proto_event(payload: bytes, timestamp_ms: int, idempotency_key: str = "") -> bytes:
    """Manually encode a proto Event message.

    Proto wire format:
    - Field 1 (payload): bytes, tag = (1 << 3) | 2 = 0x0a
    - Field 2 (timestamp_ms): int64, tag = (2 << 3) | 0 = 0x10
    - Field 3 (idempotency_key): string, tag = (3 << 3) | 2 = 0x1a
    """
    result = bytearray()

    # Field 1: payload (bytes)
    result.append(0x0a)  # Tag: field 1, wire type 2 (length-delimited)
    result.extend(encode_varint(len(payload)))
    result.extend(payload)

    # Field 2: timestamp_ms (int64)
    result.append(0x10)  # Tag: field 2, wire type 0 (varint)
    result.extend(encode_varint(timestamp_ms))

    # Field 3: idempotency_key (string) - only if non-empty
    if idempotency_key:
        result.append(0x1a)  # Tag: field 3, wire type 2 (length-delimited)
        key_bytes = idempotency_key.encode('utf-8')
        result.extend(encode_varint(len(key_bytes)))
        result.extend(key_bytes)

    return bytes(result)


def encode_varint(value: int) -> bytes:
    """Encode an integer as a protobuf varint."""
    result = bytearray()
    while value > 127:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


# ============================================================================
# Test Payloads
# ============================================================================

def generate_payload(size: int = 100) -> dict:
    """Generate a realistic event payload of approximate size."""
    base = {
        "event_type": random.choice(["user_action", "order", "metric", "log"]),
        "user_id": f"user_{random.randint(1, 100000)}",
        "timestamp": int(time.time() * 1000),
    }

    # Pad to reach target size
    current_size = len(json.dumps(base))
    if size > current_size:
        padding_size = size - current_size - 15  # Account for "padding" key
        base["padding"] = "x" * max(0, padding_size)

    return base


# ============================================================================
# Benchmark Tests
# ============================================================================

class BenchmarkSuite:
    """Comprehensive benchmark suite for Zombi."""

    def __init__(self, url: str, table: str = "benchmark", s3_bucket: str = None):
        self.url = url.rstrip("/")
        self.table = table
        self.s3_bucket = s3_bucket
        self.session = create_session()
        self.results = {}

    def health_check(self) -> bool:
        """Check if Zombi is healthy."""
        try:
            r = self.session.get(f"{self.url}/health", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def get_stats(self) -> dict:
        """Get current stats from Zombi."""
        try:
            r = self.session.get(f"{self.url}/stats", timeout=5)
            return r.json() if r.status_code == 200 else {}
        except Exception:
            return {}

    # ---- Write Tests ----

    def write_json(self, payload: dict, partition: int = 0) -> Tuple[bool, float]:
        """Write a single event using JSON encoding."""
        data = {
            "topic": self.table,
            "partition": partition,
            "payload": payload,
            "timestamp_ms": int(time.time() * 1000),
        }
        start = time.perf_counter()
        try:
            r = self.session.post(
                f"{self.url}/tables/{self.table}",
                json=data,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            latency = (time.perf_counter() - start) * 1000
            return r.status_code in (200, 201, 202), latency
        except Exception:
            return False, (time.perf_counter() - start) * 1000

    def write_proto(self, payload: bytes, partition: int = 0) -> Tuple[bool, float]:
        """Write a single event using protobuf encoding."""
        proto_data = encode_proto_event(
            payload=payload,
            timestamp_ms=int(time.time() * 1000),
        )
        start = time.perf_counter()
        try:
            r = self.session.post(
                f"{self.url}/tables/{self.table}",
                data=proto_data,
                headers={
                    "Content-Type": "application/x-protobuf",
                    "X-Partition": str(partition),
                },
                timeout=10,
            )
            latency = (time.perf_counter() - start) * 1000
            return r.status_code in (200, 201, 202), latency
        except Exception:
            return False, (time.perf_counter() - start) * 1000

    # ---- Read Tests ----

    def read_records(self, since: int = 0, limit: int = 100) -> Tuple[List[dict], float]:
        """Read records from the table."""
        params = {"limit": limit}
        if since > 0:
            params["since"] = since

        start = time.perf_counter()
        try:
            r = self.session.get(
                f"{self.url}/tables/{self.table}",
                params=params,
                timeout=30,
            )
            latency = (time.perf_counter() - start) * 1000
            if r.status_code == 200:
                data = r.json()
                return data.get("records", []), latency
            return [], latency
        except Exception:
            return [], (time.perf_counter() - start) * 1000

    # ---- Benchmark Methods ----

    def test_proto_vs_json(self, duration_sec: int = 30, workers: int = 10) -> dict:
        """Compare proto and JSON encoding performance."""
        print("\n=== PROTO vs JSON COMPARISON ===")

        # Generate identical payloads for fair comparison
        test_payload = generate_payload(100)
        payload_json = json.dumps(test_payload).encode('utf-8')

        results = {}

        for encoding in ["json", "proto"]:
            print(f"\nTesting {encoding.upper()} encoding ({duration_sec}s)...")
            stats = Stats()
            stop_flag = threading.Event()

            def worker():
                local_session = create_session()
                while not stop_flag.is_set():
                    if encoding == "json":
                        success, latency = self.write_json(test_payload)
                        stats.record(success, latency, len(json.dumps(test_payload)))
                    else:
                        success, latency = self.write_proto(payload_json)
                        stats.record(success, latency, len(payload_json))

            threads = [threading.Thread(target=worker) for _ in range(workers)]
            for t in threads:
                t.start()

            time.sleep(duration_sec)
            stop_flag.set()

            for t in threads:
                t.join()

            summary = stats.summary()
            throughput = summary["success"] / duration_sec
            results[encoding] = {
                "throughput": throughput,
                "p95_ms": summary["p95_ms"],
                "errors": summary["errors"],
            }
            print(f"  {encoding.upper()}: {throughput:.0f} ev/s, P95: {summary['p95_ms']:.1f}ms, errors: {summary['errors']}")

        # Calculate improvement
        if results["json"]["throughput"] > 0:
            improvement = ((results["proto"]["throughput"] / results["json"]["throughput"]) - 1) * 100
            print(f"\n  Proto improvement: {improvement:+.1f}%")
            results["improvement_pct"] = improvement

        self.results["proto_vs_json"] = results
        return results

    def test_write_throughput(self, duration_sec: int = 30, workers: int = 10) -> dict:
        """Measure write throughput at max rate."""
        print("\n=== WRITE THROUGHPUT ===")

        stats = Stats()
        stop_flag = threading.Event()

        def worker():
            while not stop_flag.is_set():
                payload = generate_payload(100)
                success, latency = self.write_json(payload)
                stats.record(success, latency, 100)

        print(f"Running max throughput test ({duration_sec}s, {workers} workers)...")
        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for t in threads:
            t.start()

        time.sleep(duration_sec)
        stop_flag.set()

        for t in threads:
            t.join()

        summary = stats.summary()
        throughput = summary["success"] / duration_sec

        results = {
            "throughput": throughput,
            "total": summary["total"],
            "errors": summary["errors"],
            "p50_ms": summary["p50_ms"],
            "p95_ms": summary["p95_ms"],
            "p99_ms": summary["p99_ms"],
        }

        print(f"  Throughput: {throughput:,.0f} ev/s")
        print(f"  P50: {summary['p50_ms']:.1f}ms, P95: {summary['p95_ms']:.1f}ms, P99: {summary['p99_ms']:.1f}ms")
        print(f"  Errors: {summary['errors']}")

        self.results["write_throughput"] = results
        return results

    def test_read_throughput(self, num_reads: int = 100, batch_size: int = 100) -> dict:
        """Measure read throughput."""
        print("\n=== READ THROUGHPUT ===")

        latencies = []
        total_records = 0

        print(f"Running {num_reads} read requests (batch size {batch_size})...")
        start_total = time.perf_counter()

        for _ in range(num_reads):
            records, latency = self.read_records(limit=batch_size)
            latencies.append(latency)
            total_records += len(records)

        duration = time.perf_counter() - start_total

        results = {
            "requests": num_reads,
            "records_total": total_records,
            "records_per_sec": total_records / duration if duration > 0 else 0,
            "p50_ms": statistics.median(latencies) if latencies else 0,
            "p95_ms": sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0,
        }

        print(f"  Records/sec: {results['records_per_sec']:,.0f}")
        print(f"  P50: {results['p50_ms']:.1f}ms, P95: {results['p95_ms']:.1f}ms")

        self.results["read_throughput"] = results
        return results

    def test_write_read_lag(self, num_samples: int = 50) -> dict:
        """Measure write-to-read lag (hot storage only)."""
        print("\n=== WRITE-TO-READ LAG ===")

        lags = []
        unique_table = f"lag_test_{int(time.time())}"

        print(f"Testing {num_samples} write-read roundtrips...")

        for i in range(num_samples):
            marker = f"lag_test_{time.time()}_{i}"
            payload = {"marker": marker, "index": i}

            # Write
            write_time = time.perf_counter()
            success, _ = self.write_json(payload)
            if not success:
                continue

            # Read until found (with timeout)
            found = False
            for _ in range(100):  # Max 100 attempts
                records, _ = self.read_records(limit=10)
                for rec in records:
                    if isinstance(rec.get("payload"), str):
                        try:
                            p = json.loads(rec["payload"])
                            if p.get("marker") == marker:
                                found = True
                                break
                        except:
                            pass
                if found:
                    break
                time.sleep(0.001)  # 1ms between attempts

            if found:
                lag = (time.perf_counter() - write_time) * 1000
                lags.append(lag)

        if lags:
            results = {
                "samples": len(lags),
                "p50_ms": statistics.median(lags),
                "p95_ms": sorted(lags)[int(len(lags) * 0.95)],
                "p99_ms": sorted(lags)[int(len(lags) * 0.99)] if len(lags) >= 100 else sorted(lags)[-1],
                "max_ms": max(lags),
            }
        else:
            results = {"samples": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "max_ms": 0}

        print(f"  Samples: {results['samples']}")
        print(f"  P50: {results['p50_ms']:.1f}ms, P95: {results['p95_ms']:.1f}ms, Max: {results['max_ms']:.1f}ms")

        self.results["write_read_lag"] = results
        return results

    def test_payload_sizes(self, duration_sec: int = 20, workers: int = 10) -> dict:
        """Test impact of payload size on throughput."""
        print("\n=== PAYLOAD SIZE IMPACT ===")

        sizes = [100, 1024, 4096, 32768]  # 100B, 1KB, 4KB, 32KB
        results = {}

        for size in sizes:
            print(f"\nTesting {size} byte payloads ({duration_sec}s)...")
            stats = Stats()
            stop_flag = threading.Event()

            def worker():
                while not stop_flag.is_set():
                    payload = generate_payload(size)
                    success, latency = self.write_json(payload)
                    stats.record(success, latency, size)

            threads = [threading.Thread(target=worker) for _ in range(workers)]
            for t in threads:
                t.start()

            time.sleep(duration_sec)
            stop_flag.set()

            for t in threads:
                t.join()

            summary = stats.summary()
            throughput = summary["success"] / duration_sec
            mbps = (throughput * size * 8) / 1_000_000

            results[size] = {
                "throughput": throughput,
                "mbps": mbps,
                "p95_ms": summary["p95_ms"],
            }
            print(f"  {size}B: {throughput:,.0f} ev/s, {mbps:.1f} Mbps, P95: {summary['p95_ms']:.1f}ms")

        self.results["payload_sizes"] = results
        return results

    def test_iceberg_verification(self) -> dict:
        """Verify data appears in S3 (requires aws cli and bucket name)."""
        print("\n=== ICEBERG VERIFICATION ===")

        if not self.s3_bucket:
            print("  Skipped: No S3 bucket specified (use --s3-bucket)")
            return {"skipped": True}

        try:
            # Force a flush
            self.session.post(f"{self.url}/flush", timeout=30)
            time.sleep(2)  # Wait for flush to complete

            # Count S3 objects
            result = subprocess.run(
                ["aws", "s3", "ls", f"s3://{self.s3_bucket}/", "--recursive", "--summarize"],
                capture_output=True, text=True, timeout=30
            )

            lines = result.stdout.strip().split("\n")
            total_objects = 0
            total_size = 0

            for line in lines:
                if "Total Objects:" in line:
                    total_objects = int(line.split(":")[-1].strip())
                elif "Total Size:" in line:
                    size_str = line.split(":")[-1].strip()
                    total_size = int(size_str) if size_str.isdigit() else 0

            results = {
                "objects": total_objects,
                "size_bytes": total_size,
                "bucket": self.s3_bucket,
            }

            print(f"  S3 Objects: {total_objects}")
            print(f"  Total Size: {total_size / 1024 / 1024:.1f} MB")
            print(f"  Bucket: {self.s3_bucket}")

            self.results["iceberg"] = results
            return results

        except Exception as e:
            print(f"  Error: {e}")
            return {"error": str(e)}

    # ---- Suite Runners ----

    def run_quick(self) -> dict:
        """Run quick sanity check suite (~2 min)."""
        print("\n" + "=" * 60)
        print("QUICK BENCHMARK SUITE")
        print("=" * 60)

        if not self.health_check():
            print("ERROR: Zombi is not healthy!")
            return {"error": "health_check_failed"}

        self.test_write_throughput(duration_sec=30, workers=10)
        self.test_read_throughput(num_reads=50, batch_size=100)

        return self.results

    def run_full(self) -> dict:
        """Run full benchmark suite (~10 min)."""
        print("\n" + "=" * 60)
        print("FULL BENCHMARK SUITE")
        print("=" * 60)

        if not self.health_check():
            print("ERROR: Zombi is not healthy!")
            return {"error": "health_check_failed"}

        self.test_proto_vs_json(duration_sec=30, workers=10)
        self.test_write_throughput(duration_sec=30, workers=10)
        self.test_read_throughput(num_reads=100, batch_size=100)
        self.test_write_read_lag(num_samples=50)
        self.test_payload_sizes(duration_sec=20, workers=10)
        self.test_iceberg_verification()

        return self.results

    def run_stress(self) -> dict:
        """Run stress test suite (~30 min)."""
        print("\n" + "=" * 60)
        print("STRESS BENCHMARK SUITE")
        print("=" * 60)

        if not self.health_check():
            print("ERROR: Zombi is not healthy!")
            return {"error": "health_check_failed"}

        # Extended write test
        self.test_write_throughput(duration_sec=120, workers=20)

        # Extended proto vs json
        self.test_proto_vs_json(duration_sec=60, workers=20)

        # Extended payload sizes
        self.test_payload_sizes(duration_sec=60, workers=20)

        return self.results

    def print_report(self):
        """Print formatted benchmark report."""
        print("\n")
        print("=" * 60)
        print("ZOMBI BENCHMARK REPORT")
        print(f"Target: {self.url}")
        print(f"Date: {datetime.now().isoformat()}")
        print("=" * 60)

        # Get final stats from server
        stats = self.get_stats()
        if stats:
            print("\nSERVER STATS:")
            print(f"  Uptime: {stats.get('uptime_secs', 0):.1f}s")
            writes = stats.get('writes', {})
            print(f"  Total Writes: {writes.get('total', 0):,}")
            print(f"  Write Rate: {writes.get('rate_per_sec', 0):.1f} ev/s")
            print(f"  Avg Write Latency: {writes.get('avg_latency_us', 0) / 1000:.2f}ms")
            reads = stats.get('reads', {})
            print(f"  Total Reads: {reads.get('total', 0):,}")
            print(f"  Errors: {stats.get('errors_total', 0)}")

        print("\n" + "=" * 60)
        print("Results saved to: benchmark_results.json")

        # Save results to file
        with open("benchmark_results.json", "w") as f:
            json.dump({
                "url": self.url,
                "timestamp": datetime.now().isoformat(),
                "results": self.results,
                "server_stats": stats,
            }, f, indent=2)


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Zombi Benchmark Tool")
    parser.add_argument("--url", required=True, help="Zombi URL (e.g., http://localhost:8080)")
    parser.add_argument("--suite", choices=["quick", "full", "stress"], help="Run benchmark suite")
    parser.add_argument("--test", choices=[
        "proto-vs-json", "write-throughput", "read-throughput",
        "write-read-lag", "payload-sizes", "iceberg"
    ], help="Run specific test")
    parser.add_argument("--table", default="benchmark", help="Table name to use")
    parser.add_argument("--s3-bucket", help="S3 bucket for Iceberg verification")
    parser.add_argument("--duration", type=int, default=30, help="Test duration in seconds")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")

    args = parser.parse_args()

    bench = BenchmarkSuite(args.url, args.table, args.s3_bucket)

    if not bench.health_check():
        print(f"ERROR: Cannot connect to {args.url}")
        sys.exit(1)

    print(f"Connected to {args.url}")

    if args.suite:
        if args.suite == "quick":
            bench.run_quick()
        elif args.suite == "full":
            bench.run_full()
        elif args.suite == "stress":
            bench.run_stress()
        bench.print_report()

    elif args.test:
        if args.test == "proto-vs-json":
            bench.test_proto_vs_json(args.duration, args.workers)
        elif args.test == "write-throughput":
            bench.test_write_throughput(args.duration, args.workers)
        elif args.test == "read-throughput":
            bench.test_read_throughput()
        elif args.test == "write-read-lag":
            bench.test_write_read_lag()
        elif args.test == "payload-sizes":
            bench.test_payload_sizes(args.duration, args.workers)
        elif args.test == "iceberg":
            bench.test_iceberg_verification()

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
