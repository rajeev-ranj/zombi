#!/usr/bin/env python3
"""
Zombi Test Consumer

Continuously reads from Zombi tables and tracks consumption metrics.

Usage:
    python consumer.py [--url URL] [--tables TABLES] [--interval SECS]
"""

import argparse
import time
import requests
from dataclasses import dataclass


@dataclass
class Stats:
    reads: int = 0
    records: int = 0
    errors: int = 0
    start_time: float = 0

    def reset(self):
        self.reads = 0
        self.records = 0
        self.errors = 0
        self.start_time = time.time()

    def print_stats(self):
        elapsed = time.time() - self.start_time
        rate = self.records / elapsed if elapsed > 0 else 0
        print(f"\r[{elapsed:.1f}s] Reads: {self.reads} | Records: {self.records:,} | "
              f"Rate: {rate:.1f} rec/s | Errors: {self.errors}", end="", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Zombi Test Consumer")
    parser.add_argument("--url", default="http://localhost:8080", help="Zombi URL")
    parser.add_argument("--tables", default="users,orders,clickstream,metrics",
                        help="Comma-separated table names")
    parser.add_argument("--interval", type=float, default=0.5, help="Poll interval in seconds")
    parser.add_argument("--duration", type=float, default=60, help="Duration in seconds")
    parser.add_argument("--limit", type=int, default=100, help="Records per read")
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",")]
    stats = Stats()
    stats.reset()

    session = requests.Session()
    base_url = args.url.rstrip("/")

    # Track last seen timestamp per table
    last_seen = {table: 0 for table in tables}

    print(f"Zombi Consumer")
    print(f"  URL: {args.url}")
    print(f"  Tables: {tables}")
    print(f"  Interval: {args.interval}s")
    print(f"  Duration: {args.duration}s")
    print("-" * 50)

    end_time = time.time() + args.duration

    try:
        while time.time() < end_time:
            for table in tables:
                try:
                    # Read with since parameter to get new records
                    params = {"limit": args.limit}
                    if last_seen[table] > 0:
                        params["since"] = last_seen[table] + 1

                    resp = session.get(
                        f"{base_url}/tables/{table}",
                        params=params,
                        timeout=5
                    )

                    if resp.status_code == 200:
                        data = resp.json()
                        record_count = data.get("count", 0)
                        records = data.get("records", [])

                        stats.reads += 1
                        stats.records += record_count

                        # Update last seen timestamp
                        if records:
                            last_seen[table] = max(r["timestamp_ms"] for r in records)
                    else:
                        stats.errors += 1

                except Exception as e:
                    stats.errors += 1

            stats.print_stats()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\nInterrupted!")

    # Final stats
    elapsed = time.time() - stats.start_time
    print(f"\n\nFinal Stats:")
    print(f"  Total reads: {stats.reads:,}")
    print(f"  Total records: {stats.records:,}")
    print(f"  Total errors: {stats.errors}")
    print(f"  Avg rate: {stats.records/elapsed:.1f} records/sec")


if __name__ == "__main__":
    main()
