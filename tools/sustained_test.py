#!/usr/bin/env python3
"""
Zombi Sustained Performance Test with Resource Monitoring

Runs a sustained load test while monitoring resource consumption:
- CPU usage
- Memory usage
- Disk I/O
- Network I/O
- RocksDB stats

Usage:
    python sustained_test.py --url http://localhost:8080 --duration 600
    python sustained_test.py --url http://localhost:8080 --duration 600 --mode bulk
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import requests


@dataclass
class ResourceSample:
    """Single resource usage sample."""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    disk_read_mb: float
    disk_write_mb: float
    net_rx_mb: float
    net_tx_mb: float


@dataclass
class PerformanceSample:
    """Single performance sample."""
    timestamp: float
    requests_completed: int
    bytes_written: int
    avg_latency_us: float
    errors: int


@dataclass
class SustainedTestResult:
    """Complete sustained test result."""
    mode: str
    duration_secs: float
    concurrency: int
    batch_size: int
    payload_size: int

    # Performance metrics
    total_requests: int
    total_events: int
    total_bytes: int
    avg_requests_per_sec: float
    avg_events_per_sec: float
    avg_mb_per_sec: float
    avg_gbps: float

    # Resource metrics
    avg_cpu_percent: float
    peak_cpu_percent: float
    avg_memory_mb: float
    peak_memory_mb: float
    total_disk_read_mb: float
    total_disk_write_mb: float
    total_net_rx_mb: float
    total_net_tx_mb: float

    # Time series data
    resource_samples: List[ResourceSample] = field(default_factory=list)
    performance_samples: List[PerformanceSample] = field(default_factory=list)


def get_docker_stats() -> Dict:
    """Get Docker container stats."""
    try:
        result = subprocess.run(
            ["docker", "stats", "zombi", "--no-stream", "--format",
             "{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}}"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            parts = result.stdout.strip().split(',')
            if len(parts) >= 4:
                # Parse CPU
                cpu = float(parts[0].replace('%', ''))

                # Parse memory (e.g., "123.4MiB / 1GiB")
                mem_parts = parts[1].split('/')
                mem_used = mem_parts[0].strip()
                if 'GiB' in mem_used:
                    mem_mb = float(mem_used.replace('GiB', '').strip()) * 1024
                elif 'MiB' in mem_used:
                    mem_mb = float(mem_used.replace('MiB', '').strip())
                elif 'KiB' in mem_used:
                    mem_mb = float(mem_used.replace('KiB', '').strip()) / 1024
                else:
                    mem_mb = 0

                # Parse network I/O (e.g., "1.23MB / 4.56MB")
                net_parts = parts[2].split('/')
                net_rx = parse_size(net_parts[0].strip()) if len(net_parts) > 0 else 0
                net_tx = parse_size(net_parts[1].strip()) if len(net_parts) > 1 else 0

                # Parse block I/O (e.g., "1.23MB / 4.56MB")
                block_parts = parts[3].split('/')
                disk_read = parse_size(block_parts[0].strip()) if len(block_parts) > 0 else 0
                disk_write = parse_size(block_parts[1].strip()) if len(block_parts) > 1 else 0

                return {
                    'cpu_percent': cpu,
                    'memory_mb': mem_mb,
                    'net_rx_mb': net_rx,
                    'net_tx_mb': net_tx,
                    'disk_read_mb': disk_read,
                    'disk_write_mb': disk_write,
                }
    except Exception as e:
        pass
    return {}


def parse_size(s: str) -> float:
    """Parse size string like '1.23GB' to MB."""
    s = s.strip()
    if 'GB' in s:
        return float(s.replace('GB', '')) * 1024
    elif 'MB' in s:
        return float(s.replace('MB', ''))
    elif 'KB' in s or 'kB' in s:
        return float(s.replace('KB', '').replace('kB', '')) / 1024
    elif 'B' in s:
        return float(s.replace('B', '')) / (1024 * 1024)
    return 0


def get_server_stats(url: str) -> Dict:
    """Get Zombi server statistics."""
    try:
        r = requests.get(f"{url}/stats", timeout=5)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def resource_monitor(
    stop_event: threading.Event,
    samples: List[ResourceSample],
    interval: float = 1.0
):
    """Background thread to monitor resources."""
    start_time = time.time()
    prev_stats = get_docker_stats()

    while not stop_event.is_set():
        time.sleep(interval)
        stats = get_docker_stats()
        if stats:
            sample = ResourceSample(
                timestamp=time.time() - start_time,
                cpu_percent=stats.get('cpu_percent', 0),
                memory_mb=stats.get('memory_mb', 0),
                memory_percent=0,  # Will calculate later
                disk_read_mb=stats.get('disk_read_mb', 0),
                disk_write_mb=stats.get('disk_write_mb', 0),
                net_rx_mb=stats.get('net_rx_mb', 0),
                net_tx_mb=stats.get('net_tx_mb', 0),
            )
            samples.append(sample)


def performance_monitor(
    url: str,
    stop_event: threading.Event,
    samples: List[PerformanceSample],
    interval: float = 5.0
):
    """Background thread to monitor performance metrics."""
    start_time = time.time()
    prev_stats = get_server_stats(url)

    while not stop_event.is_set():
        time.sleep(interval)
        stats = get_server_stats(url)
        if stats:
            writes = stats.get('writes', {})
            sample = PerformanceSample(
                timestamp=time.time() - start_time,
                requests_completed=writes.get('total', 0),
                bytes_written=writes.get('bytes_total', 0),
                avg_latency_us=writes.get('avg_latency_us', 0),
                errors=stats.get('errors_total', 0),
            )
            samples.append(sample)


def generate_payload(size: int) -> str:
    """Generate a payload of specified size."""
    base = {
        "event_type": "sustained_test",
        "timestamp": int(time.time() * 1000),
    }
    base_json = json.dumps(base)
    if size > len(base_json) + 20:
        base["data"] = "x" * (size - len(base_json) - 12)
    return json.dumps(base)


def create_bulk_payload(batch_size: int, payload_size: int) -> str:
    """Create bulk write payload."""
    single = generate_payload(payload_size)
    records = [
        {"payload": single, "partition": i % 8, "timestamp_ms": int(time.time() * 1000)}
        for i in range(batch_size)
    ]
    return json.dumps({"records": records})


def run_sustained_test(
    url: str,
    duration: int,
    mode: str = "bulk",
    concurrency: int = 100,
    batch_size: int = 100,
    payload_size: int = 1024,
    verbose: bool = True,
) -> SustainedTestResult:
    """Run sustained performance test with resource monitoring."""

    # Prepare payload
    if mode == "bulk":
        payload = create_bulk_payload(batch_size, payload_size)
        endpoint = f"{url}/tables/sustained-test/bulk"
        events_per_request = batch_size
    else:
        payload = generate_payload(payload_size)
        endpoint = f"{url}/tables/sustained-test"
        events_per_request = 1

    payload_bytes = len(payload)

    if verbose:
        print(f"\n{'='*70}")
        print(f"SUSTAINED PERFORMANCE TEST")
        print(f"{'='*70}")
        print(f"Mode: {mode}")
        print(f"Duration: {duration}s ({duration/60:.1f} minutes)")
        print(f"Concurrency: {concurrency}")
        print(f"Batch size: {batch_size}")
        print(f"Payload size: {payload_size} bytes")
        print(f"Request size: {payload_bytes / 1024:.1f} KB")
        print()

    # Get initial stats
    stats_before = get_server_stats(url)
    initial_bytes = stats_before.get('writes', {}).get('bytes_total', 0)
    initial_requests = stats_before.get('writes', {}).get('total', 0)

    # Start resource monitoring
    resource_samples: List[ResourceSample] = []
    perf_samples: List[PerformanceSample] = []
    stop_event = threading.Event()

    resource_thread = threading.Thread(
        target=resource_monitor,
        args=(stop_event, resource_samples, 2.0),
        daemon=True
    )
    perf_thread = threading.Thread(
        target=performance_monitor,
        args=(url, stop_event, perf_samples, 5.0),
        daemon=True
    )

    resource_thread.start()
    perf_thread.start()

    # Write payload to temp file for hey
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(payload)
        payload_file = f.name

    try:
        # Run hey for the full duration
        if verbose:
            print(f"Starting load test at {datetime.now().strftime('%H:%M:%S')}...")
            print(f"Test will run for {duration} seconds...")
            print()

        start_time = time.time()

        cmd = [
            "hey",
            "-z", f"{duration}s",
            "-c", str(concurrency),
            "-m", "POST",
            "-T", "application/json",
            "-D", payload_file,
            endpoint
        ]

        # Run hey and show progress
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        # Print progress every 30 seconds
        last_progress = start_time
        while process.poll() is None:
            time.sleep(5)
            elapsed = time.time() - start_time
            if elapsed - (last_progress - start_time) >= 30:
                if resource_samples:
                    last_sample = resource_samples[-1]
                    print(f"  [{elapsed/60:.1f} min] CPU: {last_sample.cpu_percent:.0f}%, "
                          f"Mem: {last_sample.memory_mb:.0f} MB, "
                          f"Disk Write: {last_sample.disk_write_mb:.1f} MB")
                last_progress = time.time()

        stdout, _ = process.communicate()
        actual_duration = time.time() - start_time

    finally:
        os.unlink(payload_file)
        stop_event.set()
        resource_thread.join(timeout=2)
        perf_thread.join(timeout=2)

    # Get final stats
    stats_after = get_server_stats(url)
    final_bytes = stats_after.get('writes', {}).get('bytes_total', 0)
    final_requests = stats_after.get('writes', {}).get('total', 0)

    # Calculate results
    total_bytes = final_bytes - initial_bytes
    total_requests = final_requests - initial_requests
    total_events = total_requests * events_per_request

    avg_requests_per_sec = total_requests / actual_duration
    avg_events_per_sec = total_events / actual_duration
    avg_mb_per_sec = (total_bytes / actual_duration) / (1024 * 1024)
    avg_gbps = (total_bytes * 8 / actual_duration) / 1_000_000_000

    # Calculate resource averages
    if resource_samples:
        avg_cpu = sum(s.cpu_percent for s in resource_samples) / len(resource_samples)
        peak_cpu = max(s.cpu_percent for s in resource_samples)
        avg_memory = sum(s.memory_mb for s in resource_samples) / len(resource_samples)
        peak_memory = max(s.memory_mb for s in resource_samples)

        # Get final cumulative values
        final_sample = resource_samples[-1]
        total_disk_read = final_sample.disk_read_mb
        total_disk_write = final_sample.disk_write_mb
        total_net_rx = final_sample.net_rx_mb
        total_net_tx = final_sample.net_tx_mb
    else:
        avg_cpu = peak_cpu = avg_memory = peak_memory = 0
        total_disk_read = total_disk_write = total_net_rx = total_net_tx = 0

    result = SustainedTestResult(
        mode=mode,
        duration_secs=actual_duration,
        concurrency=concurrency,
        batch_size=batch_size,
        payload_size=payload_size,
        total_requests=total_requests,
        total_events=total_events,
        total_bytes=total_bytes,
        avg_requests_per_sec=avg_requests_per_sec,
        avg_events_per_sec=avg_events_per_sec,
        avg_mb_per_sec=avg_mb_per_sec,
        avg_gbps=avg_gbps,
        avg_cpu_percent=avg_cpu,
        peak_cpu_percent=peak_cpu,
        avg_memory_mb=avg_memory,
        peak_memory_mb=peak_memory,
        total_disk_read_mb=total_disk_read,
        total_disk_write_mb=total_disk_write,
        total_net_rx_mb=total_net_rx,
        total_net_tx_mb=total_net_tx,
        resource_samples=resource_samples,
        performance_samples=perf_samples,
    )

    return result


def print_result(result: SustainedTestResult):
    """Print detailed test results."""
    print(f"\n{'='*70}")
    print("SUSTAINED TEST RESULTS")
    print(f"{'='*70}")

    print("\nPERFORMANCE:")
    print(f"  Duration: {result.duration_secs:.1f}s ({result.duration_secs/60:.1f} minutes)")
    print(f"  Total requests: {result.total_requests:,}")
    print(f"  Total events: {result.total_events:,}")
    print(f"  Total data: {result.total_bytes / 1024 / 1024:.1f} MB")
    print()
    print(f"  Avg throughput: {result.avg_requests_per_sec:,.0f} req/s")
    print(f"  Avg events/s: {result.avg_events_per_sec:,.0f}")
    print(f"  Avg bandwidth: {result.avg_mb_per_sec:.1f} MB/s ({result.avg_gbps:.3f} Gbps)")

    print("\nRESOURCE CONSUMPTION:")
    print(f"  CPU: {result.avg_cpu_percent:.1f}% avg, {result.peak_cpu_percent:.1f}% peak")
    print(f"  Memory: {result.avg_memory_mb:.0f} MB avg, {result.peak_memory_mb:.0f} MB peak")
    print(f"  Disk read: {result.total_disk_read_mb:.1f} MB total")
    print(f"  Disk write: {result.total_disk_write_mb:.1f} MB total")
    print(f"  Network RX: {result.total_net_rx_mb:.1f} MB")
    print(f"  Network TX: {result.total_net_tx_mb:.1f} MB")

    # Calculate efficiency metrics
    if result.total_bytes > 0:
        bytes_per_cpu_pct = result.total_bytes / result.avg_cpu_percent if result.avg_cpu_percent > 0 else 0
        print("\nEFFICIENCY:")
        print(f"  MB/s per CPU%: {result.avg_mb_per_sec / result.avg_cpu_percent:.2f}" if result.avg_cpu_percent > 0 else "  MB/s per CPU%: N/A")
        print(f"  Write amplification: {result.total_disk_write_mb / (result.total_bytes / 1024 / 1024):.2f}x" if result.total_bytes > 0 else "")

    print(f"{'='*70}")


def save_result(result: SustainedTestResult, output_path: str):
    """Save result to JSON file."""
    data = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "mode": result.mode,
            "duration_secs": result.duration_secs,
            "concurrency": result.concurrency,
            "batch_size": result.batch_size,
            "payload_size": result.payload_size,
        },
        "performance": {
            "total_requests": result.total_requests,
            "total_events": result.total_events,
            "total_bytes": result.total_bytes,
            "avg_requests_per_sec": result.avg_requests_per_sec,
            "avg_events_per_sec": result.avg_events_per_sec,
            "avg_mb_per_sec": result.avg_mb_per_sec,
            "avg_gbps": result.avg_gbps,
        },
        "resources": {
            "avg_cpu_percent": result.avg_cpu_percent,
            "peak_cpu_percent": result.peak_cpu_percent,
            "avg_memory_mb": result.avg_memory_mb,
            "peak_memory_mb": result.peak_memory_mb,
            "total_disk_read_mb": result.total_disk_read_mb,
            "total_disk_write_mb": result.total_disk_write_mb,
            "total_net_rx_mb": result.total_net_rx_mb,
            "total_net_tx_mb": result.total_net_tx_mb,
        },
        "time_series": {
            "resources": [
                {
                    "t": s.timestamp,
                    "cpu": s.cpu_percent,
                    "mem": s.memory_mb,
                    "disk_w": s.disk_write_mb,
                    "net_tx": s.net_tx_mb,
                }
                for s in result.resource_samples
            ],
            "performance": [
                {
                    "t": s.timestamp,
                    "reqs": s.requests_completed,
                    "bytes": s.bytes_written,
                    "latency_us": s.avg_latency_us,
                }
                for s in result.performance_samples
            ],
        },
    }

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Zombi Sustained Performance Test with Resource Monitoring"
    )

    parser.add_argument("--url", required=True, help="Zombi server URL")
    parser.add_argument("--duration", type=int, default=600,
                        help="Test duration in seconds (default: 600 = 10 min)")
    parser.add_argument("--mode", choices=["single", "bulk"], default="bulk",
                        help="Test mode: single writes or bulk (default: bulk)")
    parser.add_argument("--concurrency", type=int, default=100,
                        help="Number of concurrent connections (default: 100)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Events per bulk request (default: 100)")
    parser.add_argument("--payload-size", type=int, default=1024,
                        help="Payload size in bytes (default: 1024)")
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

    # Run test
    result = run_sustained_test(
        url=args.url,
        duration=args.duration,
        mode=args.mode,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        payload_size=args.payload_size,
        verbose=not args.quiet,
    )

    print_result(result)

    output_path = args.output or "sustained_test_results.json"
    save_result(result, output_path)


if __name__ == "__main__":
    main()
