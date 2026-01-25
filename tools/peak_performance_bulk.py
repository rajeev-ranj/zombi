#!/usr/bin/env python3
"""
Zombi Peak Performance Test - Bulk Write API

Measures maximum throughput for the bulk write API endpoint.
Each request writes multiple records in a single HTTP call.

Usage:
    python peak_performance_bulk.py --url http://localhost:8080
    python peak_performance_bulk.py --url http://localhost:8080 --batch-size 100 --concurrency 50,100,200
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple


@dataclass
class BulkPerfResult:
    """Result from a single bulk performance test run."""
    concurrency: int
    batch_size: int
    duration_secs: float
    total_requests: int
    requests_per_sec: float
    events_per_sec: float  # requests * batch_size
    avg_latency_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_latency_ms: float
    errors: int
    status_codes: Dict[int, int] = field(default_factory=dict)
    avg_cpu_percent: float = 0.0
    peak_cpu_percent: float = 0.0
    mb_per_sec: float = 0.0
    tool: str = "hey"


@dataclass
class BulkPeakPerfReport:
    """Complete bulk peak performance report."""
    url: str
    timestamp: str
    tool: str
    batch_size: int
    results: List[BulkPerfResult]
    server_stats_before: Dict
    server_stats_after: Dict
    peak_throughput: float = 0.0  # requests/sec
    peak_events_per_sec: float = 0.0  # events/sec
    peak_mb_per_sec: float = 0.0
    optimal_concurrency: int = 0


def check_dependencies() -> Tuple[Optional[str], str]:
    """Check for available HTTP load testing tools."""
    if shutil.which("hey"):
        return "hey", "hey is available"
    if shutil.which("wrk"):
        return "wrk", "wrk is available"
    return None, "No load testing tools found. Install with: brew install hey"


def get_server_stats(url: str) -> Dict:
    """Get current server statistics."""
    try:
        import requests
        r = requests.get(f"{url}/stats", timeout=5)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def get_zombi_pid() -> Optional[int]:
    """Get Zombi server PID (for CPU monitoring)."""
    try:
        result = subprocess.run(
            ["pgrep", "-x", "zombi"],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            return int(pids[0])

        result = subprocess.run(
            ["ps", "-eo", "pid,comm"],
            capture_output=True, text=True
        )
        for line in result.stdout.strip().split('\n'):
            if 'zombi' in line and 'grep' not in line:
                parts = line.strip().split()
                if parts:
                    return int(parts[0])
    except Exception:
        pass
    return None


def monitor_cpu(pid: int, interval: float = 0.2) -> Tuple[threading.Event, List[float]]:
    """Start CPU monitoring in background thread."""
    stop_event = threading.Event()
    cpu_samples: List[float] = []

    def _monitor():
        while not stop_event.is_set():
            try:
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


def create_bulk_payload(batch_size: int) -> str:
    """Create a bulk write payload with batch_size records."""
    records = []
    for i in range(batch_size):
        records.append({
            "payload": f"bulk-perf-test-{i}-{int(time.time() * 1000)}",
            "partition": i % 4,
            "timestamp_ms": int(time.time() * 1000)
        })
    return json.dumps({"records": records})


def parse_hey_output(output: str, batch_size: int) -> BulkPerfResult:
    """Parse hey command output."""
    result = BulkPerfResult(
        concurrency=0,
        batch_size=batch_size,
        duration_secs=0,
        total_requests=0,
        requests_per_sec=0,
        events_per_sec=0,
        avg_latency_ms=0,
        p50_ms=0,
        p95_ms=0,
        p99_ms=0,
        max_latency_ms=0,
        errors=0,
        tool="hey"
    )

    if m := re.search(r'Total:\s+([\d.]+)\s+secs', output):
        result.duration_secs = float(m.group(1))
    if m := re.search(r'Requests/sec:\s+([\d.]+)', output):
        result.requests_per_sec = float(m.group(1))
        result.events_per_sec = result.requests_per_sec * batch_size
    if m := re.search(r'Average:\s+([\d.]+)\s+secs', output):
        result.avg_latency_ms = float(m.group(1)) * 1000
    if m := re.search(r'Slowest:\s+([\d.]+)\s+secs', output):
        result.max_latency_ms = float(m.group(1)) * 1000

    if m := re.search(r'50%.*?in\s+([\d.]+)\s+secs', output):
        result.p50_ms = float(m.group(1)) * 1000
    if m := re.search(r'95%.*?in\s+([\d.]+)\s+secs', output):
        result.p95_ms = float(m.group(1)) * 1000
    if m := re.search(r'99%.*?in\s+([\d.]+)\s+secs', output):
        result.p99_ms = float(m.group(1)) * 1000

    for m in re.finditer(r'\[(\d+)\]\s+(\d+)\s+responses', output):
        code, count = int(m.group(1)), int(m.group(2))
        result.status_codes[code] = count
        result.total_requests += count
        if code >= 400:
            result.errors += count

    return result


def run_hey_bulk_test(
    url: str,
    concurrency: int,
    duration: int,
    batch_size: int,
) -> Tuple[str, BulkPerfResult]:
    """Run hey load test against bulk API."""
    payload = create_bulk_payload(batch_size)

    # Write payload to temp file for hey -D flag
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(payload)
        payload_file = f.name

    try:
        cmd = [
            "hey",
            "-z", f"{duration}s",
            "-c", str(concurrency),
            "-m", "POST",
            "-T", "application/json",
            "-D", payload_file,
            f"{url}/tables/bulk-perf-{concurrency}/bulk"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        output = result.stdout + result.stderr

        perf_result = parse_hey_output(output, batch_size)
        perf_result.concurrency = concurrency

        return output, perf_result
    finally:
        os.unlink(payload_file)


def run_bulk_peak_test(
    url: str,
    concurrency_levels: List[int],
    duration: int,
    batch_size: int,
    tool: str,
    verbose: bool = True,
) -> BulkPeakPerfReport:
    """Run bulk peak performance tests at multiple concurrency levels."""

    stats_before = get_server_stats(url)
    zombi_pid = get_zombi_pid()
    results: List[BulkPerfResult] = []

    for concurrency in concurrency_levels:
        if verbose:
            print(f"\n{'='*60}")
            print(f"Testing BULK API with {concurrency} concurrent connections...")
            print(f"Batch size: {batch_size} records per request")
            print(f"{'='*60}")

        # Get stats before this concurrency level
        stats_before_test = get_server_stats(url)

        # Start CPU monitoring
        cpu_samples: List[float] = []
        stop_event = None
        if zombi_pid:
            stop_event, cpu_samples = monitor_cpu(zombi_pid)
            time.sleep(0.5)

        # Run test
        output, perf_result = run_hey_bulk_test(url, concurrency, duration, batch_size)

        # Stop CPU monitoring
        if stop_event:
            stop_event.set()
            time.sleep(0.5)
            if cpu_samples:
                perf_result.avg_cpu_percent = sum(cpu_samples) / len(cpu_samples)
                perf_result.peak_cpu_percent = max(cpu_samples)

        # Get stats after and calculate MB/s
        stats_after_test = get_server_stats(url)
        bytes_before = stats_before_test.get("writes", {}).get("bytes_total", 0)
        bytes_after = stats_after_test.get("writes", {}).get("bytes_total", 0)
        bytes_written = bytes_after - bytes_before
        if perf_result.duration_secs > 0:
            perf_result.mb_per_sec = (bytes_written / perf_result.duration_secs) / (1024 * 1024)

        results.append(perf_result)

        if verbose:
            print(f"\nResults (c={concurrency}, batch={batch_size}):")
            print(f"  Requests/sec: {perf_result.requests_per_sec:,.0f}")
            print(f"  Events/sec: {perf_result.events_per_sec:,.0f}")
            print(f"  MB/s: {perf_result.mb_per_sec:.2f}")
            print(f"  Latency P50/P95/P99: {perf_result.p50_ms:.1f}ms / {perf_result.p95_ms:.1f}ms / {perf_result.p99_ms:.1f}ms")
            print(f"  Total requests: {perf_result.total_requests:,}")
            print(f"  Errors: {perf_result.errors}")
            if cpu_samples:
                print(f"  CPU: {perf_result.avg_cpu_percent:.1f}% avg, {perf_result.peak_cpu_percent:.1f}% peak")

        time.sleep(2)

    stats_after = get_server_stats(url)

    # Find peak throughput
    peak_result = max(results, key=lambda r: r.events_per_sec)
    peak_mb_result = max(results, key=lambda r: r.mb_per_sec)

    report = BulkPeakPerfReport(
        url=url,
        timestamp=datetime.now().isoformat(),
        tool=tool,
        batch_size=batch_size,
        results=results,
        server_stats_before=stats_before,
        server_stats_after=stats_after,
        peak_throughput=peak_result.requests_per_sec,
        peak_events_per_sec=peak_result.events_per_sec,
        peak_mb_per_sec=peak_mb_result.mb_per_sec,
        optimal_concurrency=peak_result.concurrency,
    )

    return report


def print_report(report: BulkPeakPerfReport):
    """Print formatted bulk performance report."""
    print("\n")
    print("=" * 95)
    print("ZOMBI BULK API PEAK PERFORMANCE REPORT")
    print("=" * 95)
    print(f"Target: {report.url}")
    print(f"Timestamp: {report.timestamp}")
    print(f"Tool: {report.tool}")
    print(f"Batch Size: {report.batch_size} records/request")
    print()

    print(f"{'Concurrency':>12} {'Req/s':>12} {'Events/s':>14} {'MB/s':>10} {'P50':>10} {'P95':>10} {'P99':>10} {'Peak CPU':>10}")
    print("-" * 95)
    for r in report.results:
        cpu_str = f"{r.peak_cpu_percent:.0f}%" if r.peak_cpu_percent else "N/A"
        print(f"{r.concurrency:>12} {r.requests_per_sec:>11,.0f} {r.events_per_sec:>13,.0f} {r.mb_per_sec:>9.1f} {r.p50_ms:>9.1f}ms {r.p95_ms:>9.1f}ms {r.p99_ms:>9.1f}ms {cpu_str:>10}")
    print("-" * 95)

    print()
    print(f"PEAK THROUGHPUT: {report.peak_throughput:,.0f} requests/sec")
    print(f"PEAK EVENTS: {report.peak_events_per_sec:,.0f} events/sec")
    print(f"PEAK BANDWIDTH: {report.peak_mb_per_sec:.2f} MB/s")
    print(f"Optimal concurrency: {report.optimal_concurrency}")

    if report.server_stats_before and report.server_stats_after:
        writes_before = report.server_stats_before.get("writes", {}).get("total", 0)
        writes_after = report.server_stats_after.get("writes", {}).get("total", 0)
        total_test_writes = writes_after - writes_before
        print(f"Total writes during test: {total_test_writes:,}")

        avg_latency = report.server_stats_after.get("writes", {}).get("avg_latency_us", 0)
        print(f"Server-side avg latency: {avg_latency:.1f} us")

    print()
    print("=" * 80)


def save_report(report: BulkPeakPerfReport, output_path: str):
    """Save report to JSON file."""
    data = {
        "url": report.url,
        "timestamp": report.timestamp,
        "tool": report.tool,
        "batch_size": report.batch_size,
        "peak_throughput": report.peak_throughput,
        "peak_events_per_sec": report.peak_events_per_sec,
        "peak_mb_per_sec": report.peak_mb_per_sec,
        "optimal_concurrency": report.optimal_concurrency,
        "results": [
            {
                "concurrency": r.concurrency,
                "batch_size": r.batch_size,
                "requests_per_sec": r.requests_per_sec,
                "events_per_sec": r.events_per_sec,
                "mb_per_sec": r.mb_per_sec,
                "total_requests": r.total_requests,
                "p50_ms": r.p50_ms,
                "p95_ms": r.p95_ms,
                "p99_ms": r.p99_ms,
                "max_latency_ms": r.max_latency_ms,
                "errors": r.errors,
                "avg_cpu_percent": r.avg_cpu_percent,
                "peak_cpu_percent": r.peak_cpu_percent,
            }
            for r in report.results
        ],
        "server_stats_before": report.server_stats_before,
        "server_stats_after": report.server_stats_after,
    }

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Results saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Zombi Bulk API Peak Performance Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick test with defaults
  python peak_performance_bulk.py --url http://localhost:8080

  # Custom batch size and concurrency
  python peak_performance_bulk.py --url http://localhost:8080 --batch-size 100 --concurrency 50,100,200

  # Longer duration
  python peak_performance_bulk.py --url http://localhost:8080 --duration 60

  # Save results
  python peak_performance_bulk.py --url http://localhost:8080 --output bulk_results.json
"""
    )

    parser.add_argument(
        "--url",
        required=True,
        help="Zombi server URL (e.g., http://localhost:8080)"
    )
    parser.add_argument(
        "--concurrency",
        default="50,100,200",
        help="Comma-separated concurrency levels (default: 50,100,200)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Test duration in seconds per concurrency level (default: 30)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of records per bulk request (default: 100)"
    )
    parser.add_argument(
        "--output",
        help="Output file for JSON results"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce output verbosity"
    )

    args = parser.parse_args()

    tool, msg = check_dependencies()
    if not tool:
        print(f"Error: {msg}")
        sys.exit(1)

    print(f"Using {tool} for load testing")
    print(f"Batch size: {args.batch_size} records per request")

    concurrency_levels = [int(c.strip()) for c in args.concurrency.split(",")]

    try:
        import requests
        r = requests.get(f"{args.url}/health", timeout=5)
        if r.status_code != 200:
            print(f"Error: Server health check failed")
            sys.exit(1)
        print(f"Connected to {args.url}")
    except Exception as e:
        print(f"Error: Cannot connect to {args.url}: {e}")
        sys.exit(1)

    report = run_bulk_peak_test(
        url=args.url,
        concurrency_levels=concurrency_levels,
        duration=args.duration,
        batch_size=args.batch_size,
        tool=tool,
        verbose=not args.quiet,
    )

    print_report(report)

    if args.output:
        save_report(report, args.output)
    else:
        save_report(report, "peak_performance_bulk_results.json")


if __name__ == "__main__":
    main()
