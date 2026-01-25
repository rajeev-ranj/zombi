#!/usr/bin/env python3
"""
Zombi Bandwidth Test - Maximize MB/s throughput

This test focuses on saturating network bandwidth rather than maximizing request rate.
Uses large payloads and bulk writes to achieve maximum throughput.

Usage:
    python bandwidth_test.py --url http://localhost:8080
    python bandwidth_test.py --url http://localhost:8080 --payload-size 32768 --batch-size 100
    python bandwidth_test.py --url http://localhost:8080 --payload-size 65536 --concurrency 200
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class BandwidthResult:
    """Result from a bandwidth test run."""
    payload_size: int
    batch_size: int
    concurrency: int
    duration_secs: float
    total_requests: int
    total_events: int
    total_bytes: int
    requests_per_sec: float
    events_per_sec: float
    mb_per_sec: float
    gbps: float
    avg_latency_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    errors: int
    avg_cpu_percent: float = 0.0
    peak_cpu_percent: float = 0.0


def create_session(pool_size: int = 200) -> requests.Session:
    """Create HTTP session with large connection pool."""
    session = requests.Session()
    retry = Retry(total=1, backoff_factor=0.1)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_server_stats(url: str) -> Dict:
    """Get current server statistics."""
    try:
        r = requests.get(f"{url}/stats", timeout=5)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def get_zombi_pid() -> Optional[int]:
    """Get Zombi server PID for CPU monitoring."""
    try:
        # Try docker first
        result = subprocess.run(
            ["docker", "top", "zombi", "-o", "pid"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                return int(lines[1].strip())

        # Fallback to pgrep
        result = subprocess.run(["pgrep", "-x", "zombi"], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip().split('\n')[0])
    except Exception:
        pass
    return None


def monitor_cpu(pid: int, interval: float = 0.5) -> Tuple[threading.Event, List[float]]:
    """Start CPU monitoring in background thread."""
    stop_event = threading.Event()
    cpu_samples: List[float] = []

    def _monitor():
        while not stop_event.is_set():
            try:
                # Try docker stats first
                result = subprocess.run(
                    ["docker", "stats", "zombi", "--no-stream", "--format", "{{.CPUPerc}}"],
                    capture_output=True, text=True, timeout=2
                )
                if result.returncode == 0:
                    cpu_str = result.stdout.strip().replace('%', '')
                    if cpu_str:
                        cpu_samples.append(float(cpu_str))
                else:
                    # Fallback to ps
                    result = subprocess.run(
                        ["ps", "-p", str(pid), "-o", "%cpu"],
                        capture_output=True, text=True
                    )
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        if len(lines) > 1:
                            cpu_val = lines[1].strip()
                            if cpu_val:
                                cpu_samples.append(float(cpu_val))
            except Exception:
                pass
            time.sleep(interval)

    thread = threading.Thread(target=_monitor, daemon=True)
    thread.start()
    return stop_event, cpu_samples


def generate_large_payload(size: int) -> str:
    """Generate a payload of exact size in bytes."""
    base = {
        "event_type": "bandwidth_test",
        "timestamp": int(time.time() * 1000),
    }
    base_json = json.dumps(base)
    base_size = len(base_json)

    if size > base_size + 20:  # Account for "data" key
        padding_size = size - base_size - 12  # {"data":"..."}
        base["data"] = "x" * padding_size

    return json.dumps(base)


def create_bulk_payload(batch_size: int, payload_size: int) -> Tuple[str, int]:
    """Create a bulk write payload with specified batch size and payload size per record."""
    single_payload = generate_large_payload(payload_size)
    records = []
    for i in range(batch_size):
        records.append({
            "payload": single_payload,
            "partition": i % 8,
            "timestamp_ms": int(time.time() * 1000)
        })

    bulk_json = json.dumps({"records": records})
    return bulk_json, len(bulk_json)


def run_bandwidth_test_python(
    url: str,
    payload_size: int,
    batch_size: int,
    concurrency: int,
    duration: int,
    table: str = "bandwidth-test",
) -> BandwidthResult:
    """Run bandwidth test using Python requests (cross-platform)."""

    bulk_payload, payload_bytes = create_bulk_payload(batch_size, payload_size)
    endpoint = f"{url}/tables/{table}/bulk"

    stats_lock = threading.Lock()
    total_requests = 0
    total_errors = 0
    total_bytes_sent = 0
    latencies = []
    stop_flag = threading.Event()

    def worker():
        nonlocal total_requests, total_errors, total_bytes_sent
        session = create_session(concurrency)
        local_latencies = []
        local_requests = 0
        local_errors = 0
        local_bytes = 0

        while not stop_flag.is_set():
            start = time.perf_counter()
            try:
                r = session.post(
                    endpoint,
                    data=bulk_payload,
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                latency = (time.perf_counter() - start) * 1000

                if r.status_code in (200, 201, 202):
                    local_requests += 1
                    local_bytes += payload_bytes
                    local_latencies.append(latency)
                else:
                    local_errors += 1
            except Exception:
                local_errors += 1

        with stats_lock:
            total_requests += local_requests
            total_errors += local_errors
            total_bytes_sent += local_bytes
            latencies.extend(local_latencies)

    # Start workers
    threads = []
    for _ in range(concurrency):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    # Run for duration
    time.sleep(duration)
    stop_flag.set()

    # Wait for workers
    for t in threads:
        t.join(timeout=2)

    # Calculate results
    if latencies:
        sorted_lat = sorted(latencies)
        p50 = sorted_lat[len(sorted_lat) // 2]
        p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
        p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
        avg_lat = sum(latencies) / len(latencies)
    else:
        p50 = p95 = p99 = avg_lat = 0

    total_events = total_requests * batch_size
    mb_per_sec = (total_bytes_sent / duration) / (1024 * 1024)
    gbps = (total_bytes_sent * 8 / duration) / 1_000_000_000

    return BandwidthResult(
        payload_size=payload_size,
        batch_size=batch_size,
        concurrency=concurrency,
        duration_secs=duration,
        total_requests=total_requests,
        total_events=total_events,
        total_bytes=total_bytes_sent,
        requests_per_sec=total_requests / duration,
        events_per_sec=total_events / duration,
        mb_per_sec=mb_per_sec,
        gbps=gbps,
        avg_latency_ms=avg_lat,
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        errors=total_errors,
    )


def run_bandwidth_test_hey(
    url: str,
    payload_size: int,
    batch_size: int,
    concurrency: int,
    duration: int,
    table: str = "bandwidth-test",
) -> BandwidthResult:
    """Run bandwidth test using hey (faster, but requires hey installed)."""
    import re
    import shutil

    if not shutil.which("hey"):
        print("hey not found, falling back to Python implementation")
        return run_bandwidth_test_python(url, payload_size, batch_size, concurrency, duration, table)

    bulk_payload, payload_bytes = create_bulk_payload(batch_size, payload_size)

    # Write payload to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(bulk_payload)
        payload_file = f.name

    try:
        cmd = [
            "hey",
            "-z", f"{duration}s",
            "-c", str(concurrency),
            "-m", "POST",
            "-T", "application/json",
            "-D", payload_file,
            f"{url}/tables/{table}/bulk"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 60)
        output = result.stdout + result.stderr

        # Parse hey output
        total_requests = 0
        requests_per_sec = 0.0
        p50 = p95 = p99 = avg_lat = 0.0
        errors = 0
        actual_duration = duration

        if m := re.search(r'Total:\s+([\d.]+)\s+secs', output):
            actual_duration = float(m.group(1))
        if m := re.search(r'Requests/sec:\s+([\d.]+)', output):
            requests_per_sec = float(m.group(1))
        if m := re.search(r'Average:\s+([\d.]+)\s+secs', output):
            avg_lat = float(m.group(1)) * 1000
        if m := re.search(r'50%.*?in\s+([\d.]+)\s+secs', output):
            p50 = float(m.group(1)) * 1000
        if m := re.search(r'95%.*?in\s+([\d.]+)\s+secs', output):
            p95 = float(m.group(1)) * 1000
        if m := re.search(r'99%.*?in\s+([\d.]+)\s+secs', output):
            p99 = float(m.group(1)) * 1000

        for m in re.finditer(r'\[(\d+)\]\s+(\d+)\s+responses', output):
            code, count = int(m.group(1)), int(m.group(2))
            total_requests += count
            if code >= 400:
                errors += count

        successful_requests = total_requests - errors
        total_events = successful_requests * batch_size
        total_bytes = successful_requests * payload_bytes
        mb_per_sec = (total_bytes / actual_duration) / (1024 * 1024)
        gbps = (total_bytes * 8 / actual_duration) / 1_000_000_000

        return BandwidthResult(
            payload_size=payload_size,
            batch_size=batch_size,
            concurrency=concurrency,
            duration_secs=actual_duration,
            total_requests=successful_requests,
            total_events=total_events,
            total_bytes=total_bytes,
            requests_per_sec=requests_per_sec,
            events_per_sec=total_events / actual_duration,
            mb_per_sec=mb_per_sec,
            gbps=gbps,
            avg_latency_ms=avg_lat,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            errors=errors,
        )
    finally:
        os.unlink(payload_file)


def run_bandwidth_sweep(
    url: str,
    payload_sizes: List[int],
    batch_sizes: List[int],
    concurrency_levels: List[int],
    duration: int,
    verbose: bool = True,
) -> List[BandwidthResult]:
    """Run bandwidth tests across multiple configurations to find optimal settings."""

    results = []
    zombi_pid = get_zombi_pid()

    for payload_size in payload_sizes:
        for batch_size in batch_sizes:
            for concurrency in concurrency_levels:
                if verbose:
                    payload_kb = payload_size / 1024
                    print(f"\n{'='*70}")
                    print(f"Testing: payload={payload_kb:.0f}KB, batch={batch_size}, concurrency={concurrency}")
                    print(f"{'='*70}")

                # Start CPU monitoring
                cpu_samples = []
                stop_event = None
                if zombi_pid:
                    stop_event, cpu_samples = monitor_cpu(zombi_pid)
                    time.sleep(0.5)

                # Run test
                result = run_bandwidth_test_hey(
                    url=url,
                    payload_size=payload_size,
                    batch_size=batch_size,
                    concurrency=concurrency,
                    duration=duration,
                )

                # Stop CPU monitoring
                if stop_event:
                    stop_event.set()
                    time.sleep(0.5)
                    if cpu_samples:
                        result.avg_cpu_percent = sum(cpu_samples) / len(cpu_samples)
                        result.peak_cpu_percent = max(cpu_samples)

                results.append(result)

                if verbose:
                    print(f"\nResults:")
                    print(f"  Throughput: {result.mb_per_sec:.1f} MB/s ({result.gbps:.3f} Gbps)")
                    print(f"  Events/s: {result.events_per_sec:,.0f}")
                    print(f"  Requests/s: {result.requests_per_sec:,.0f}")
                    print(f"  Latency P50/P95/P99: {result.p50_ms:.1f}ms / {result.p95_ms:.1f}ms / {result.p99_ms:.1f}ms")
                    print(f"  Total data: {result.total_bytes / 1024 / 1024:.1f} MB")
                    if cpu_samples:
                        print(f"  CPU: {result.avg_cpu_percent:.0f}% avg, {result.peak_cpu_percent:.0f}% peak")

                time.sleep(2)  # Brief pause between tests

    return results


def print_summary(results: List[BandwidthResult]):
    """Print summary of all bandwidth test results."""
    print("\n")
    print("=" * 100)
    print("BANDWIDTH TEST SUMMARY")
    print("=" * 100)
    print()

    # Sort by MB/s
    sorted_results = sorted(results, key=lambda r: r.mb_per_sec, reverse=True)

    print(f"{'Payload':>10} {'Batch':>8} {'Conc':>6} {'MB/s':>10} {'Gbps':>8} {'Events/s':>12} {'P99':>10} {'CPU':>8}")
    print("-" * 100)

    for r in sorted_results:
        payload_kb = r.payload_size / 1024
        cpu_str = f"{r.peak_cpu_percent:.0f}%" if r.peak_cpu_percent else "N/A"
        print(f"{payload_kb:>9.0f}K {r.batch_size:>8} {r.concurrency:>6} {r.mb_per_sec:>9.1f} {r.gbps:>7.3f} {r.events_per_sec:>11,.0f} {r.p99_ms:>9.1f}ms {cpu_str:>8}")

    print("-" * 100)

    # Best result
    best = sorted_results[0]
    print()
    print(f"PEAK BANDWIDTH: {best.mb_per_sec:.1f} MB/s ({best.gbps:.3f} Gbps)")
    print(f"  Configuration: payload={best.payload_size/1024:.0f}KB, batch={best.batch_size}, concurrency={best.concurrency}")
    print(f"  Events/s: {best.events_per_sec:,.0f}")
    print(f"  P99 latency: {best.p99_ms:.1f}ms")

    # Network utilization estimate (assuming 5 Gbps for t3.micro)
    max_gbps = 5.0
    utilization = (best.gbps / max_gbps) * 100
    print(f"  Network utilization: {utilization:.1f}% of {max_gbps} Gbps")

    print("=" * 100)


def save_results(results: List[BandwidthResult], output_path: str):
    """Save results to JSON file."""
    data = {
        "timestamp": datetime.now().isoformat(),
        "results": [
            {
                "payload_size": r.payload_size,
                "batch_size": r.batch_size,
                "concurrency": r.concurrency,
                "duration_secs": r.duration_secs,
                "total_requests": r.total_requests,
                "total_events": r.total_events,
                "total_bytes": r.total_bytes,
                "requests_per_sec": r.requests_per_sec,
                "events_per_sec": r.events_per_sec,
                "mb_per_sec": r.mb_per_sec,
                "gbps": r.gbps,
                "p50_ms": r.p50_ms,
                "p95_ms": r.p95_ms,
                "p99_ms": r.p99_ms,
                "errors": r.errors,
                "avg_cpu_percent": r.avg_cpu_percent,
                "peak_cpu_percent": r.peak_cpu_percent,
            }
            for r in results
        ],
        "peak": {
            "mb_per_sec": max(r.mb_per_sec for r in results),
            "gbps": max(r.gbps for r in results),
        }
    }

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Zombi Bandwidth Test - Maximize MB/s throughput",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick bandwidth test
  python bandwidth_test.py --url http://localhost:8080

  # Custom payload size (32KB per event)
  python bandwidth_test.py --url http://localhost:8080 --payload-size 32768

  # Full sweep to find optimal configuration
  python bandwidth_test.py --url http://localhost:8080 --sweep

  # High concurrency with large payloads
  python bandwidth_test.py --url http://localhost:8080 --payload-size 65536 --batch-size 50 --concurrency 200
"""
    )

    parser.add_argument("--url", required=True, help="Zombi server URL")
    parser.add_argument("--payload-size", type=int, default=32768,
                        help="Payload size in bytes per event (default: 32768 = 32KB)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Number of events per bulk request (default: 100)")
    parser.add_argument("--concurrency", type=int, default=100,
                        help="Number of concurrent connections (default: 100)")
    parser.add_argument("--duration", type=int, default=30,
                        help="Test duration in seconds (default: 30)")
    parser.add_argument("--sweep", action="store_true",
                        help="Run sweep across multiple configurations")
    parser.add_argument("--output", help="Output file for JSON results")
    parser.add_argument("--quiet", action="store_true", help="Reduce output")

    args = parser.parse_args()

    # Health check
    try:
        r = requests.get(f"{args.url}/health", timeout=5)
        if r.status_code != 200:
            print(f"Error: Server health check failed")
            sys.exit(1)
        print(f"Connected to {args.url}")
    except Exception as e:
        print(f"Error: Cannot connect to {args.url}: {e}")
        sys.exit(1)

    if args.sweep:
        # Full configuration sweep
        payload_sizes = [4096, 16384, 32768, 65536]  # 4KB, 16KB, 32KB, 64KB
        batch_sizes = [50, 100, 200]
        concurrency_levels = [50, 100, 200]

        print(f"\nRunning bandwidth sweep:")
        print(f"  Payload sizes: {[f'{s/1024:.0f}KB' for s in payload_sizes]}")
        print(f"  Batch sizes: {batch_sizes}")
        print(f"  Concurrency levels: {concurrency_levels}")
        print(f"  Duration per test: {args.duration}s")
        print(f"  Total configurations: {len(payload_sizes) * len(batch_sizes) * len(concurrency_levels)}")

        results = run_bandwidth_sweep(
            url=args.url,
            payload_sizes=payload_sizes,
            batch_sizes=batch_sizes,
            concurrency_levels=concurrency_levels,
            duration=args.duration,
            verbose=not args.quiet,
        )
    else:
        # Single configuration test
        print(f"\nRunning bandwidth test:")
        print(f"  Payload size: {args.payload_size / 1024:.0f}KB")
        print(f"  Batch size: {args.batch_size}")
        print(f"  Concurrency: {args.concurrency}")
        print(f"  Duration: {args.duration}s")

        results = run_bandwidth_sweep(
            url=args.url,
            payload_sizes=[args.payload_size],
            batch_sizes=[args.batch_size],
            concurrency_levels=[args.concurrency],
            duration=args.duration,
            verbose=not args.quiet,
        )

    print_summary(results)

    if args.output:
        save_results(results, args.output)
    else:
        save_results(results, "bandwidth_results.json")


if __name__ == "__main__":
    main()
