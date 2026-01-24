#!/usr/bin/env python3
import time
import json

# Very simple test
def main():
    url = "http://18.143.135.197:8080"
    duration = 30
    
    print("=" * 60)
    print(f"ZOMBI SIMPLE TEST")
    print("=" * 60)
    print(f"Duration: {duration}s")
    print("=" * 60)
    
    sent = 0
    start = time.time()
    
    while time.time() - start < duration:
        try:
            resp = requests.post(
                f"{url}/tables/test_simple",
                json={"payload": json.dumps({"test": "data" * 100}), "timestamp_ms": int(time.time() * 1000), "partition": 0},
                timeout=10
            )
            if resp.status_code in (200, 202):
                sent += 1
                if sent % 100 == 0:
                    print(f"  Events: {sent}")
        except Exception:
            pass
    
    elapsed = time.time() - start
    print()
    print(f"\nRESULTS")
    print("=" * 60)
    print(f"Sent: {sent:,}")
    print(f"Duration: {elapsed:.1f}s")
    print(f"Rate: {sent/elapsed:.1f} events/sec")
    print("=" * 60)

if __name__ == "__main__":
    main()
