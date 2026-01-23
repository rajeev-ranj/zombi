#!/usr/bin/env python3
"""
Zombi Load Test Tool - Protobuf Version

Uses protobuf encoding instead of JSON for more efficient wire format.
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
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import protobuf
try:
    from . import events_pb2 as pb
except ImportError:
    import events_pb2 as pb

from . import load_test

# Use same TestStats and create_http_session from load_test
TestStats = load_test.TestStats
create_http_session = load_test.create_http_session


def generate_user_event() -> pb.UserEvent:
    """Generate a random user event."""
    return pb.UserEvent(
        event_type=random.choice(["signup", "login", "logout", "profile_update"]),
        user_id=f"user_{random.randint(1, 100000)}",
        email=f"test{random.randint(1, 99999)}@example.com",
        ip_address=f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}",
        user_agent=random.choice(
            [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/17.0",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Mobile/15E148",
            ]
        ),
        metadata={
            "source": random.choice(["web", "mobile", "api"]),
            "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
        },
    )


def generate_order_event() -> pb.OrderEvent:
    """Generate a random order event."""
    items = []
    total = 0
    for _ in range(random.randint(1, 5)):
        product = random.choice(
            [
                ("laptop", 99900),
                ("phone", 79900),
                ("tablet", 49900),
                ("headphones", 19900),
                ("keyboard", 9900),
                ("mouse", 4900),
            ]
        )
        qty = random.randint(1, 3)
        items.append(
            pb.OrderItem(
                product_id=f"prod_{random.randint(1, 100)}",
                name=product[0],
                quantity=qty,
                price_cents=product[1],
            )
        )
        total += product[1] * qty

    return pb.OrderEvent(
        event_type=random.choice(
            ["created", "paid", "shipped", "delivered", "cancelled"]
        ),
        order_id=f"order_{random.randint(1, 1000000)}",
        user_id=f"user_{random.randint(1, 100000)}",
        items=items,
        total_cents=total,
        currency="USD",
        status=random.choice(["pending", "processing", "completed"]),
    )


def generate_clickstream_event() -> pb.ClickstreamEvent:
    """Generate a random clickstream event."""
    return pb.ClickstreamEvent(
        session_id=f"sess_{random.randint(1, 1000000)}",
        user_id=f"user_{random.randint(1, 100000)}" if random.random() > 0.3 else "",
        page_url=random.choice(
            ["/", "/products", "/cart", "/checkout", "/profile", "/orders", "/search"]
        ),
        referrer=random.choice(
            ["https://google.com", "https://twitter.com", "", "direct"]
        ),
        action=random.choice(
            ["pageview", "click", "scroll", "form_submit", "add_to_cart"]
        ),
        element_id=f"btn_{random.randint(1, 50)}" if random.random() > 0.5 else "",
        viewport_width=random.choice([1920, 1440, 1280, 375, 414]),
        viewport_height=random.choice([1080, 900, 720, 667, 896]),
    )


def generate_metric_event() -> pb.MetricEvent:
    """Generate a random metric event."""
    metric_name = random.choice(
        ["cpu_usage", "memory_mb", "request_latency_ms", "error_rate", "queue_depth"]
    )

    if metric_name == "cpu_usage":
        value = random.uniform(0, 100)
        unit = "percent"
    elif metric_name == "memory_mb":
        value = random.uniform(100, 8000)
        unit = "megabytes"
    elif metric_name == "request_latency_ms":
        value = random.expovariate(1 / 50)
        unit = "milliseconds"
    elif metric_name == "error_rate":
        value = random.uniform(0, 5)
        unit = "percent"
    else:
        value = random.randint(0, 1000)
        unit = "count"

    return pb.MetricEvent(
        metric_name=metric_name,
        value=value,
        unit=unit,
        tags={
            "host": f"server-{random.randint(1, 100)}",
            "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
            "env": random.choice(["prod", "staging", "dev"]),
        },
    )


# Event generators
EVENT_GENERATORS = {
    "users": (generate_user_event, "UserEvent"),
    "orders": (generate_order_event, "OrderEvent"),
    "clickstream": (generate_clickstream_event, "ClickstreamEvent"),
    "metrics": (generate_metric_event, "MetricEvent"),
}


def generate_event(table: str) -> pb.Event:
    """Generate a protobuf Event wrapper."""
    generator, event_type_name = EVENT_GENERATORS.get(
        table, (generate_user_event, "UserEvent")
    )
    domain_event = generator()

    event = pb.Event(
        payload=domain_event.SerializeToString(),
        timestamp_ms=int(time.time() * 1000),
        idempotency_key=f"{table}_{random.randint(1, 1000000000)}",
        headers={
            "event_type": event_type_name,
            "schema_version": "1",
            "source": "load_test",
            "table": table,
        },
    )
    return event


def send_event_worker(
    url: str,
    table: str,
    duration: float,
    stats: TestStats,
    target_rate: float = 0.0,
    thread_id: int = 0,
):
    """Worker that sends protobuf events to Zombi."""
    session = create_http_session()
    end_time = time.time() + duration

    while time.time() < end_time:
        event = generate_event(table)

        start = time.time()
        try:
            response = session.post(
                f"{url}/tables/{table}",
                data=event.SerializeToString(),
                headers={
                    "Content-Type": "application/x-protobuf",
                    "X-Partition": str(random.randint(0, 3)),
                },
                timeout=10,
            )
            latency_ms = (time.time() - start) * 1000
            bytes_sent = len(event.SerializeToString())

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
    """Adaptive worker that adjusts rate based on latency."""
    session = create_http_session()
    end_time = time.time() + duration

    current_rate = 50.0  # Start with 50 events/sec
    max_p95_latency = 500.0  # ms

    while time.time() < end_time:
        # Send burst of events
        batch_size = int(current_rate / 10)
        batch_start = time.time()

        for _ in range(batch_size):
            event = generate_event(table)

            start = time.time()
            try:
                response = session.post(
                    f"{url}/tables/{table}",
                    data=event.SerializeToString(),
                    headers={
                        "Content-Type": "application/x-protobuf",
                        "X-Partition": str(random.randint(0, 3)),
                    },
                    timeout=10,
                )
                latency_ms = (time.time() - start) * 1000
                bytes_sent = len(event.SerializeToString())

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
    """Steady load test with protobuf."""
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


def run_max_load(args, stats: TestStats):
    """Maximum throughput test with adaptive rate control."""
    threads = []
    for i in range(args.workers):
        t = threading.Thread(
            target=adaptive_load_worker,
            args=(args.url, args.table, args.duration, stats, i),
            daemon=True,
        )
        t.start()
        threads.append(t)

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
                if len(stats.latencies_ms) >= 1000:
                    percentiles = statistics.quantiles(
                        stats.latencies_ms[-1000:], n=100
                    )
                    p50 = percentiles[49]
                    p95 = percentiles[94]
                    p99 = percentiles[98]
                else:
                    p50 = p95 = p99 = 0

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
    load_test.print_summary(stats, duration)


def main():
    parser = argparse.ArgumentParser(
        description="Zombi Load Test Tool - Protobuf Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find maximum throughput (protobuf)
  python load_test_proto.py --url http://localhost:8080 --profile max --duration 300

  # Steady load at 1000 events/sec (protobuf)
  python load_test_proto.py --url http://localhost:8080 --profile steady --rate 1000 --duration 60
        """,
    )

    parser.add_argument("--url", default="http://localhost:8080", help="Zombi URL")
    parser.add_argument("--table", default="test", help="Table name to write to")
    parser.add_argument(
        "--duration", type=float, default=60, help="Test duration in seconds"
    )
    parser.add_argument(
        "--workers", type=int, default=10, help="Number of worker threads"
    )
    parser.add_argument(
        "--profile", choices=["steady", "max"], default="max", help="Load profile"
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1000,
        help="Target rate for steady profile (events/sec)",
    )
    parser.add_argument(
        "--max-latency-ms",
        type=float,
        default=500,
        help="Target P95 latency for max profile",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("ZOMBI LOAD TEST - PROTOBUF VERSION")
    print("=" * 70)
    print(f"URL: {args.url}")
    print(f"Table: {args.table}")
    print(f"Profile: {args.profile}")
    print(f"Duration: {args.duration}s")
    print(f"Workers: {args.workers}")
    print(f"Encoding: Protobuf (binary)")
    if args.profile == "steady":
        print(f"Target Rate: {args.rate}/s")
    elif args.profile == "max":
        print(f"Target P95 Latency: {args.max_latency_ms}ms")
    print("=" * 70)
    print()

    stats = TestStats()

    try:
        if args.profile == "steady":
            run_steady_load(args, stats)
        elif args.profile == "max":
            run_max_load(args, stats)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")

    print_summary(stats, args.duration)


if __name__ == "__main__":
    main()
