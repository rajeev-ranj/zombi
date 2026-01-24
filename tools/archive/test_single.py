#!/usr/bin/env python3
import time
import requests


def main():
    url = "http://18.143.135.197:8080"
    duration = 30
    workers = 5

    print("=" * 70)
    print("ZOMBI JSON THROUGHPUT TEST")
    print("=" * 70)
    print(f"URL: {url}")
    print(f"Duration: {duration}s")
    print(f"Workers: {workers}")
    print("=" * 70)
    print()

    # Single test first
    print("Running single test...")
    sent = 0
    start = time.time()
    session = requests.Session()

    while time.time() - start < duration:
        try:
            r = session.post(
                f"{url}/tables/test_quick",
                json={"payload": json.dumps({"t": "x" * 80}), "partition": 0},
                timeout=10,
            )
            if r.status_code in (200, 202):
                sent += 1
        except:
            pass  # Count errors as not success

    elapsed = time.time() - start
    rate = sent / elapsed
    print(f"\nSINGLE TEST RESULTS:")
    print(f"  Sent: {sent:,} events in {elapsed:.1f}s")
    print(f"  Throughput: {rate:.1f} events/sec")
    print(f" Errors: {elapsed - sent:,} (assumed network errors)")
    print(f"\n" + "=" * 70)


if __name__ == "__main__":
    main()
