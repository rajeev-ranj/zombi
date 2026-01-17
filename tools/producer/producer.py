#!/usr/bin/env python3
"""
Zombi Test Producer

Sends protobuf events to Zombi with variable burst patterns.
Simulates realistic traffic with multiple event schemas.

Usage:
    python producer.py [--url URL] [--tables TABLES] [--duration SECS] [--profile PROFILE]

Profiles:
    - steady: Constant 10 events/sec
    - bursty: Random bursts of 50-200 events, then quiet periods
    - spike: Normal traffic with occasional 10x spikes
    - stress: Maximum throughput
"""

import argparse
import random
import time
import uuid
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Callable
import requests

# Generate protobuf code if not exists
try:
    import events_pb2 as pb
except ImportError:
    import subprocess
    print("Generating protobuf code...")
    subprocess.run([
        "protoc",
        "--python_out=.",
        "events.proto"
    ], cwd=sys.path[0] or ".", check=True)
    import events_pb2 as pb


@dataclass
class Stats:
    sent: int = 0
    errors: int = 0
    bytes_sent: int = 0
    start_time: float = 0

    def reset(self):
        self.sent = 0
        self.errors = 0
        self.bytes_sent = 0
        self.start_time = time.time()

    def print_stats(self):
        elapsed = time.time() - self.start_time
        rate = self.sent / elapsed if elapsed > 0 else 0
        print(f"\r[{elapsed:.1f}s] Sent: {self.sent} | Errors: {self.errors} | "
              f"Rate: {rate:.1f}/s | Bytes: {self.bytes_sent:,}", end="", flush=True)


# Fake data generators
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
PRODUCTS = [
    ("laptop", 99900), ("phone", 79900), ("tablet", 49900),
    ("headphones", 19900), ("keyboard", 9900), ("mouse", 4900)
]
PAGES = ["/", "/products", "/cart", "/checkout", "/profile", "/orders", "/search"]
ACTIONS = ["pageview", "click", "scroll", "form_submit", "add_to_cart"]
METRICS = ["cpu_usage", "memory_mb", "request_latency_ms", "error_rate", "queue_depth"]


def generate_user_event() -> pb.Event:
    """Generate a random user event."""
    user = pb.UserEvent(
        event_type=random.choice(["signup", "login", "logout", "profile_update"]),
        user_id=f"user_{random.randint(1, 10000)}",
        email=f"{random.choice(FIRST_NAMES).lower()}_{random.randint(1,999)}@example.com",
        ip_address=f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}",
        user_agent=random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/17.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Mobile/15E148",
        ]),
        metadata={"source": random.choice(["web", "mobile", "api"])}
    )

    event = pb.Event(
        payload=user.SerializeToString(),
        timestamp_ms=int(time.time() * 1000),
        idempotency_key=str(uuid.uuid4()),
        headers={"event_type": "user", "schema_version": "1"}
    )
    return event


def generate_order_event() -> pb.Event:
    """Generate a random order event."""
    num_items = random.randint(1, 5)
    items = []
    total = 0

    for _ in range(num_items):
        product, price = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        items.append(pb.OrderItem(
            product_id=f"prod_{random.randint(1, 100)}",
            name=product,
            quantity=qty,
            price_cents=price
        ))
        total += price * qty

    order = pb.OrderEvent(
        event_type=random.choice(["created", "paid", "shipped", "delivered", "cancelled"]),
        order_id=f"order_{uuid.uuid4().hex[:12]}",
        user_id=f"user_{random.randint(1, 10000)}",
        items=items,
        total_cents=total,
        currency="USD",
        status=random.choice(["pending", "processing", "completed"])
    )

    event = pb.Event(
        payload=order.SerializeToString(),
        timestamp_ms=int(time.time() * 1000),
        idempotency_key=str(uuid.uuid4()),
        headers={"event_type": "order", "schema_version": "1"}
    )
    return event


def generate_clickstream_event() -> pb.Event:
    """Generate a random clickstream event."""
    click = pb.ClickstreamEvent(
        session_id=f"sess_{uuid.uuid4().hex[:16]}",
        user_id=f"user_{random.randint(1, 10000)}" if random.random() > 0.3 else "",
        page_url=random.choice(PAGES),
        referrer=random.choice(["https://google.com", "https://twitter.com", "", "direct"]),
        action=random.choice(ACTIONS),
        element_id=f"btn_{random.randint(1, 50)}" if random.random() > 0.5 else "",
        viewport_width=random.choice([1920, 1440, 1280, 375, 414]),
        viewport_height=random.choice([1080, 900, 720, 667, 896])
    )

    event = pb.Event(
        payload=click.SerializeToString(),
        timestamp_ms=int(time.time() * 1000),
        headers={"event_type": "clickstream", "schema_version": "1"}
    )
    return event


def generate_metric_event() -> pb.Event:
    """Generate a random metric event."""
    metric_name = random.choice(METRICS)

    if metric_name == "cpu_usage":
        value = random.uniform(0, 100)
        unit = "percent"
    elif metric_name == "memory_mb":
        value = random.uniform(100, 8000)
        unit = "megabytes"
    elif metric_name == "request_latency_ms":
        value = random.expovariate(1/50)  # Exponential distribution, mean 50ms
        unit = "milliseconds"
    elif metric_name == "error_rate":
        value = random.uniform(0, 5)
        unit = "percent"
    else:
        value = random.randint(0, 1000)
        unit = "count"

    metric = pb.MetricEvent(
        metric_name=metric_name,
        value=value,
        unit=unit,
        tags={
            "host": f"server-{random.randint(1, 10)}",
            "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
            "env": random.choice(["prod", "staging"])
        }
    )

    event = pb.Event(
        payload=metric.SerializeToString(),
        timestamp_ms=int(time.time() * 1000),
        headers={"event_type": "metric", "schema_version": "1"}
    )
    return event


# Event generators by table
EVENT_GENERATORS = {
    "users": generate_user_event,
    "orders": generate_order_event,
    "clickstream": generate_clickstream_event,
    "metrics": generate_metric_event,
}


class Producer:
    def __init__(self, base_url: str, tables: List[str], stats: Stats):
        self.base_url = base_url.rstrip("/")
        self.tables = tables
        self.stats = stats
        self.session = requests.Session()

    def send_event(self, table: str) -> bool:
        """Send a single event to a table."""
        generator = EVENT_GENERATORS.get(table, generate_user_event)
        event = generator()

        try:
            data = event.SerializeToString()
            resp = self.session.post(
                f"{self.base_url}/tables/{table}",
                data=data,
                headers={
                    "Content-Type": "application/x-protobuf",
                    "X-Partition": str(random.randint(0, 3))  # 4 partitions
                },
                timeout=5
            )

            if resp.status_code in (200, 201, 202):
                self.stats.sent += 1
                self.stats.bytes_sent += len(data)
                return True
            else:
                self.stats.errors += 1
                return False
        except Exception as e:
            self.stats.errors += 1
            return False

    def send_batch(self, count: int):
        """Send a batch of events across random tables."""
        for _ in range(count):
            table = random.choice(self.tables)
            self.send_event(table)


# Traffic profiles
def profile_steady(producer: Producer, duration: float):
    """Steady 10 events/sec."""
    end_time = time.time() + duration
    while time.time() < end_time:
        producer.send_batch(1)
        time.sleep(0.1)


def profile_bursty(producer: Producer, duration: float):
    """Random bursts of 50-200 events, then 1-5 second quiet periods."""
    end_time = time.time() + duration
    while time.time() < end_time:
        # Burst
        burst_size = random.randint(50, 200)
        producer.send_batch(burst_size)
        producer.stats.print_stats()

        # Quiet period
        quiet_time = random.uniform(1, 5)
        time.sleep(quiet_time)


def profile_spike(producer: Producer, duration: float):
    """Normal traffic (10/sec) with occasional 10x spikes."""
    end_time = time.time() + duration
    while time.time() < end_time:
        if random.random() < 0.05:  # 5% chance of spike
            print("\n[SPIKE!]", end="")
            producer.send_batch(100)
        else:
            producer.send_batch(1)
        time.sleep(0.1)


def profile_stress(producer: Producer, duration: float):
    """Maximum throughput with thread pool."""
    end_time = time.time() + duration

    def worker():
        while time.time() < end_time:
            table = random.choice(producer.tables)
            producer.send_event(table)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker) for _ in range(10)]

        while time.time() < end_time:
            producer.stats.print_stats()
            time.sleep(0.5)


PROFILES = {
    "steady": profile_steady,
    "bursty": profile_bursty,
    "spike": profile_spike,
    "stress": profile_stress,
}


def main():
    parser = argparse.ArgumentParser(description="Zombi Test Producer")
    parser.add_argument("--url", default="http://localhost:8080", help="Zombi URL")
    parser.add_argument("--tables", default="users,orders,clickstream,metrics",
                        help="Comma-separated table names")
    parser.add_argument("--duration", type=float, default=60, help="Duration in seconds")
    parser.add_argument("--profile", choices=PROFILES.keys(), default="bursty",
                        help="Traffic profile")
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",")]
    stats = Stats()
    stats.reset()

    producer = Producer(args.url, tables, stats)

    print(f"Zombi Producer")
    print(f"  URL: {args.url}")
    print(f"  Tables: {tables}")
    print(f"  Profile: {args.profile}")
    print(f"  Duration: {args.duration}s")
    print("-" * 50)

    try:
        profile_fn = PROFILES[args.profile]
        profile_fn(producer, args.duration)
    except KeyboardInterrupt:
        print("\n\nInterrupted!")

    # Final stats
    elapsed = time.time() - stats.start_time
    print(f"\n\nFinal Stats:")
    print(f"  Total sent: {stats.sent:,}")
    print(f"  Total errors: {stats.errors}")
    print(f"  Bytes sent: {stats.bytes_sent:,}")
    print(f"  Avg rate: {stats.sent/elapsed:.1f} events/sec")
    print(f"  Avg size: {stats.bytes_sent/max(stats.sent,1):.0f} bytes/event")


if __name__ == "__main__":
    main()
