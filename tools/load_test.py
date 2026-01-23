#!/usr/bin/env python3
"""
Zombi Load Test Tool

Comprehensive load testing for Zombi with detailed metrics.
Tests write throughput and latency under various load patterns.

Usage:
    python load_test.py --url http://18.143.135.197:8080 --profile max --duration 300

Profiles:
    - steady: Constant rate (100 events/sec)
    - ramp: Linear ramp from 10 to max_rate over duration
    - spike: Baseline with periodic 10x spikes
    - max: Maximum sustainable throughput with auto-adjusting workers
"""

import argparse
import json
import random
import statistics
import sys
import time
import threading
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime
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
    latencies_ms: List[float] = field(default_factory=list)
    lock: threading.Lock = threading.Lock()

    def record(self, success: bool, latency_ms: float, bytes_sent: int = 0):
        with self.lock:
            if success:
                self.total_sent += 1
                self.total_bytes += bytes_sent
                self.latencies_ms.append(latency_ms)
            else:
                self.total_errors += 1

    def get_percentile(self, p: float) -> float:
        with self.lock:
            if not self.latencies_ms:
                return 0.0
            return statistics.quantiles(self.latencies_ms, n=100)[int(p) - 1]


def create_http_session(max_retries: int = 2) -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=max_retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def generate_event() -> dict:
    """Generate a realistic event payload matching Zombi's expected schema."""
    event_types = [
        "user_action",
        "order_created",
        "page_view",
        "metric",
        "system_event",
    ]
    event_type = random.choice(event_types)

    if event_type == "user_action":
        payload = {
            "event_type": "user_action",
            "user_id": f"user_{random.randint(1, 100000)}",
            "action": random.choice(["click", "scroll", "submit", "view"]),
            "page": random.choice(["/home", "/products", "/checkout", "/profile"]),
        }
    elif event_type == "order_created":
        payload = {
            "event_type": "order_created",
            "order_id": f"order_{random.randint(1, 1000000)}",
            "user_id": f"user_{random.randint(1, 100000)}",
            "total_cents": random.randint(1000, 50000),
            "items": random.randint(1, 10),
        }
    elif event_type == "page_view":
        payload = {
            "event_type": "page_view",
            "url": random.choice(["/", "/products", "/cart", "/about", "/contact"]),
            "referrer": random.choice(
                ["google.com", "twitter.com", "direct", "facebook.com"]
            ),
            "session_id": f"sess_{random.randint(1, 1000000)}",
        }
    elif event_type == "metric":
        payload = {
            "event_type": "metric",
            "metric_name": random.choice(
                ["cpu_usage", "memory_mb", "latency_ms", "error_rate"]
            ),
            "value": round(random.uniform(0, 100), 2),
            "host": f"server-{random.randint(1, 100)}",
        }
    else:
        payload = {
            "event_type": "system_event",
            "level": random.choice(["INFO", "WARNING", "ERROR"]),
            "message": f"System event {random.randint(1, 10000)}",
            "service": random.choice(["api", "worker", "scheduler", "ingestor"]),
        }

    return {"payload": json.dumps(payload), "timestamp_ms": int(time.time() * 1000)}


def send_event_worker(
    url: str,
    table: str,
    duration: float,
    stats: TestStats,
    target_rate: float = 0.0,
    thread_id: int = 0,
):
    """
    Worker that sends events continuously.

    Args:
        url: Zombi base URL
        table: Table name to write to
        duration: Test duration in seconds
        stats: Shared statistics object
        target_rate: Target events per second (0 = unlimited)
        thread_id: Worker identifier
    """
    session = create_http_session()
    end_time = time.time() + duration

    while time.time() < end_time:
        event = generate_event()
        event["partition"] = random.randint(0, 3)

        start = time.time()
        try:
            response = session.post(
                f"{url}/tables/{table}",
                json=event,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            latency_ms = (time.time() - start) * 1000
            bytes_sent = len(json.dumps(event).encode())

            if response.status_code in (200, 202):
                stats.record(True, latency_ms, bytes_sent)
            else:
                stats.record(False, latency_ms, bytes_sent)

        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            stats.record(False, latency_ms)

        # Rate limiting if target_rate is set
        if target_rate > 0:
            time.sleep(1.0 / target_rate)


def adaptive_load_worker(
    url: str, table: str, duration: float, stats: TestStats, thread_id: int = 0
):
    """
    Adaptive worker that adjusts rate based on latency.
    Increases load until P95 latency exceeds threshold.
    """
    session = create_http_session()
    end_time = time.time() + duration

    # Start with conservative rate
    current_rate = 50.0  # events/sec
    max_p95_latency = 500.0  # ms

    while time.time() < end_time:
        # Send burst of events
        batch_size = int(current_rate / 10)  # 10 batches per second
        batch_start = time.time()

        for _ in range(batch_size):
            event = generate_event()
            event["partition"] = random.randint(0, 3)

            start = time.time()
            try:
                response = session.post(
                    f"{url}/tables/{table}",
                    json=event,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                latency_ms = (time.time() - start) * 1000
                bytes_sent = len(json.dumps(event).encode())

                if response.status_code in (200, 202):
                    stats.record(True, latency_ms, bytes_sent)
                else:
                    stats.record(False, latency_ms, bytes_sent)

            except Exception:
                latency_ms = (time.time() - start) * 1000
                stats.record(False, latency_ms)

        batch_duration = time.time() - batch_start
        sleep_time = max(0, 0.1 - batch_duration)
        time.sleep(sleep_time)

        # Every second, adjust rate based on P95 latency
        p95 = stats.get_percentile(95)
        if p95 > max_p95_latency and current_rate > 10:
            current_rate *= 0.9  # Slow down
        elif p95 < max_p95_latency / 2 and current_rate < 10000:
            current_rate *= 1.1  # Speed up


def run_steady_load(args, stats: TestStats):
    """Steady load test with fixed rate."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for i in range(args.workers):
            future = executor.submit(
                send_event_worker,
                args.url,
                args.table,
                args.duration,
                stats,
                args.rate / args.workers,
                i,
            )
            futures.append(future)

        # Monitor progress
        last_report = time.time()
        last_sent = stats.total_sent
        while any(not f.done() for f in futures):
            time.sleep(1)
            current_time = time.time()
            if current_time - last_report >= 5:
                sent = stats.total_sent
                rate = (sent - last_sent) / (current_time - last_report)
                print(
                    f"  [{current_time - (last_report - 5):.0f}s] "
                    f"Rate: {rate:.1f}/s | Sent: {sent:,} | "
                    f"Errors: {stats.total_errors}"
                )
                last_report = current_time
                last_sent = sent

        for f in futures:
            f.result()


def run_ramp_load(args, stats: TestStats):
    """Ramp load from min_rate to max_rate."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        start_time = time.time()

        for i in range(args.workers):
            future = executor.submit(
                send_event_worker,
                args.url,
                args.table,
                args.duration,
                stats,
                0,  # Rate managed dynamically
                i,
            )
            futures.append(future)

        # Monitor progress
        last_report = time.time()
        last_sent = stats.total_sent
        while any(not f.done() for f in futures):
            time.sleep(1)
            current_time = time.time()
            elapsed = current_time - start_time

            # Calculate target rate based on ramp
            progress = elapsed / args.duration
            target_rate = args.min_rate + (args.max_rate - args.min_rate) * progress

            if current_time - last_report >= 5:
                sent = stats.total_sent
                rate = (sent - last_sent) / (current_time - last_report)
                print(
                    f"  [{elapsed:.0f}s] Target: {target_rate:.0f}/s | "
                    f"Actual: {rate:.1f}/s | Sent: {sent:,}"
                )
                last_report = current_time
                last_sent = sent

        for f in futures:
            f.result()


def run_spike_load(args, stats: TestStats):
    """Load test with periodic spikes."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        start_time = time.time()

        for i in range(args.workers):
            future = executor.submit(
                send_event_worker,
                args.url,
                args.table,
                args.duration,
                stats,
                0,  # Rate managed dynamically
                i,
            )
            futures.append(future)

        # Monitor progress
        last_report = time.time()
        last_sent = stats.total_sent
        while any(not f.done() for f in futures):
            time.sleep(1)
            current_time = time.time()
            elapsed = current_time - start_time

            # Trigger spike every 30 seconds
            if elapsed % 30 < 5:
                print(f"  [{elapsed:.0f}s] *** SPIKE MODE ***")
            elif elapsed % 30 < 7:
                print(f"  [{elapsed:.0f}s] Returning to baseline")

            if current_time - last_report >= 5:
                sent = stats.total_sent
                rate = (sent - last_sent) / (current_time - last_report)
                print(f"  [{elapsed:.0f}s] Rate: {rate:.1f}/s | Sent: {sent:,}")
                last_report = current_time
                last_sent = sent

        for f in futures:
            f.result()


def run_max_load(args, stats: TestStats):
    """Maximum throughput test with adaptive rate control."""
    print("Starting adaptive load test...")
    print("Workers will automatically adjust rate based on P95 latency")
    print(f"Target P95 latency: {args.max_latency_ms}ms")
    print()

    threads = []
    for i in range(args.workers):
        t = threading.Thread(
            target=adaptive_load_worker,
            args=(args.url, args.table, args.duration, stats, i),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Monitor progress
    start_time = time.time()
    last_report = time.time()
    last_sent = stats.total_sent

    while time.time() < start_time + args.duration:
        time.sleep(1)
        current_time = time.time()
        elapsed = current_time - start_time

        if current_time - last_report >= 5:
            with stats.lock:
                sent = stats.total_sent
                rate = (sent - last_sent) / (current_time - last_report)
                p50 = (
                    statistics.quantiles(stats.latencies_ms[-1000:], n=100)[49]
                    if len(stats.latencies_ms) >= 1000
                    else 0
                )
                p95 = (
                    statistics.quantiles(stats.latencies_ms[-1000:], n=100)[94]
                    if len(stats.latencies_ms) >= 1000
                    else 0
                )
                p99 = (
                    statistics.quantiles(stats.latencies_ms[-1000:], n=100)[98]
                    if len(stats.latencies_ms) >= 1000
                    else 0
                )

            print(
                f"  [{elapsed:.0f}s] Rate: {rate:.1f}/s | Sent: {sent:,} | "
                f"P50: {p50:.1f}ms | P95: {p95:.1f}ms | P99: {p99:.1f}ms"
            )
            last_report = current_time
            last_sent = sent

    for t in threads:
        t.join(timeout=1)


def print_summary(stats: TestStats, duration: float):
    """Print final test summary."""
    print("\n" + "=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)

    total_requests = stats.total_sent + stats.total_errors
    error_rate = (
        (stats.total_errors / total_requests * 100) if total_requests > 0 else 0
    )
    avg_rate = stats.total_sent / duration

    print(f"Duration: {duration:.1f}s")
    print(f"Total Requests: {total_requests:,}")
    print(f"  Successful: {stats.total_sent:,} ({100 - error_rate:.1f}%)")
    print(f"  Errors: {stats.total_errors:,} ({error_rate:.1f}%)")
    print(f"Throughput: {avg_rate:.1f} events/sec")
    print(
        f"Total Bytes: {stats.total_bytes:,} ({stats.total_bytes / 1024 / 1024:.2f} MB)"
    )
    print(f"Avg Event Size: {stats.total_bytes / max(stats.total_sent, 1):.0f} bytes")

    with stats.lock:
        if stats.latencies_ms:
            percentiles = statistics.quantiles(stats.latencies_ms, n=100)
            print("\nLatency (ms):")
            print(f"  Min: {min(stats.latencies_ms):.1f}")
            print(f"  P50: {percentiles[49]:.1f}")
            print(f"  P90: {percentiles[89]:.1f}")
            print(f"  P95: {percentiles[94]:.1f}")
            print(f"  P99: {percentiles[98]:.1f}")
            print(f"  Max: {max(stats.latencies_ms):.1f}")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Zombi Load Test Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Steady load at 1000 events/sec for 60s
  python load_test.py --url http://18.143.135.197:8080 --profile steady --rate 1000 --duration 60

  # Ramp from 100 to 5000 events/sec over 5 minutes
  python load_test.py --url http://18.143.135.197:8080 --profile ramp --min-rate 100 --max-rate 5000 --duration 300

  # Find maximum sustainable throughput (adaptive)
  python load_test.py --url http://18.143.135.197:8080 --profile max --duration 300
        """,
    )

    parser.add_argument(
        "--url", default="http://18.143.135.197:8080", help="Zombi base URL"
    )
    parser.add_argument("--table", default="test", help="Table name to write to")
    parser.add_argument(
        "--duration", type=float, default=60, help="Test duration in seconds"
    )
    parser.add_argument(
        "--workers", type=int, default=10, help="Number of worker threads"
    )

    # Profile-specific arguments
    parser.add_argument(
        "--profile",
        choices=["steady", "ramp", "spike", "max"],
        default="max",
        help="Load profile",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=1000,
        help="Target rate for steady profile (events/sec)",
    )
    parser.add_argument(
        "--min-rate", type=float, default=100, help="Minimum rate for ramp profile"
    )
    parser.add_argument(
        "--max-rate",
        type=float,
        default=5000,
        help="Maximum rate for ramp/spike profile",
    )
    parser.add_argument(
        "--max-latency-ms",
        type=float,
        default=500,
        help="Target P95 latency for max profile",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("ZOMBI LOAD TEST")
    print("=" * 70)
    print(f"URL: {args.url}")
    print(f"Table: {args.table}")
    print(f"Profile: {args.profile}")
    print(f"Duration: {args.duration}s")
    print(f"Workers: {args.workers}")
    if args.profile == "steady":
        print(f"Target Rate: {args.rate}/s")
    elif args.profile == "ramp":
        print(f"Rate Range: {args.min_rate}/s -> {args.max_rate}/s")
    elif args.profile == "spike":
        print(f"Baseline Rate: {args.min_rate}/s")
        print(f"Spike Rate: {args.max_rate}/s")
    elif args.profile == "max":
        print(f"Target P95 Latency: {args.max_latency_ms}ms")
    print("=" * 70)
    print()

    stats = TestStats()

    try:
        if args.profile == "steady":
            run_steady_load(args, stats)
        elif args.profile == "ramp":
            run_ramp_load(args, stats)
        elif args.profile == "spike":
            run_spike_load(args, stats)
        elif args.profile == "max":
            run_max_load(args, stats)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")

    print_summary(stats, args.duration)


if __name__ == "__main__":
    main()
