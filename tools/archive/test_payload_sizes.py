import time, json, random, statistics
from concurrent.futures import ThreadPoolExecutor
import requests


def test_size(size_name, payload_size, duration=10, workers=5):
    sent = 0
    errors = 0
    latencies = []
    bytes_total = 0

    session = requests.Session()
    end = time.time() + duration

    while time.time() < end:
        event = {
            "payload": json.dumps({"t": "test", "d": "x" * (payload_size - 60)}),
            "timestamp_ms": int(time.time() * 1000),
            "partition": random.randint(0, 3),
        }
        start = time.time()
        try:
            resp = session.post(
                "http://localhost:8080/tables/test", json=event, timeout=10
            )
            lat = (time.time() - start) * 1000
            b = len(json.dumps(event))
            if resp.status_code in (200, 202):
                sent += 1
                latencies.append(lat)
                bytes_total += b
            else:
                errors += 1
                latencies.append(lat)
        except:
            errors += 1
            latencies.append(999)

    p50 = statistics.quantiles(latencies, n=100)[49] if latencies else 0
    avg_size = bytes_total / sent if sent > 0 else 0
    print(
        f"{size_name:<20} {sent / duration:>7.1f} e/s     {p50:>6.1f}ms     {avg_size:>8.0f} b"
    )


print(f"{'Size':<20} {'Events/s':>12} {'P50 (ms)':>12} {'Bytes':>10}")
print("-" * 54)
test_size("178 bytes (normal)", 178)
test_size("100 bytes", 100)
test_size("60 bytes (small)", 60)
test_size("40 bytes (proto-like)", 40)
