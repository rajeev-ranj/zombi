#!/usr/bin/env python3
"""Quick JSON throughput test for Zombi on EC2"""
import time
import json
from concurrent.futures import ThreadPoolExecutor
import requests

def main():
    url = "http://localhost:8080"
    table = "test_quick"
    duration = 60  # seconds
    workers = 5
    
    print("="*70)
    print("ZOMBI JSON THROUGHPUT TEST")
    print("="*70)
    print(f"URL: {url}")
    print(f"Table: {table}")
    print(f"Duration: {duration}s")
    print(f"Workers: {workers}")
    print("="*70)
    print()
    
    def worker():
        sent = 0
        errors = 0
        latencies = []
        session = requests.Session()
        end = time.time() + duration
        
        while time.time() < end:
            start = time.time()
            try:
                event = {
                    "payload": json.dumps({"t": "test", "d": "x" * 100}),
                    "timestamp_ms": int(time.time() * 1000),
                    "partition": 0
                }
                
                resp = session.post(
                    f"{url}/tables/{table}",
                    json=event,
                    timeout=10
                )
                latency = (time.time() - start) * 1000
                
                if resp.status_code in (200, 202):
                    sent += 1
                    latencies.append(latency)
                else:
                    errors += 1
                    latencies.append(latency)
            except Exception as e:
                errors += 1
                latencies.append(999)
        
        return sent, errors, latencies
    
    # Run workers
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker) for _ in range(workers)]
        
        # Monitor every 5 seconds
        last_sent = 0
        while any(not f.done() for f in futures):
            time.sleep(5)
            
            sent = sum(f.result()[0] for f in futures)
            if sent > last_sent + 10:  # Print every 10+ events
                errors = sum(f.result()[1] for f in futures)
                p50 = statistics.quantiles(
                    [l for f.result()[2] for f in futures for l in f.result()[2] if l < 990 else l],
                    n=100
                )[49]
                
                if len(p50) > 1:
                    print(f"[{(time.time() - start_time):.0f}s] Rate: {(sent - last_sent)/5:.1f} e/s | "
                          f"Errors: {errors} | P50: {p50:.1f}ms")
                    last_sent = sent
    
    # Wait for flush
    print()
    print("Waiting 10s for S3 flush...")
    time.sleep(10)
    
    # Check S3
    import subprocess
    print()
    print("Checking S3 storage...")
    result = subprocess.run(
        ["aws", "s3", "ls", "s3://zombi-events-test1/tables/test_quick/data/", "--recursive"],
        capture_output=True,
        text=True
    )
    
    objects = [line for line in result.stdout.split('\n') if line.strip()]
    count = len(objects)
    print(f"  S3 Objects: {count}")
    
    if count > 0:
        avg_size = sum(len(l.split()[2]) for l in objects if len(l.split()) >= 3) / count
        print(f"  Total Size: {avg_size:,} bytes")
        print(f"  Avg Object Size: {avg_size/1024/1024:.2f} MB")
        print()
    
    print("="*70)
    print("FINAL RESULTS")
    print("="*70)
    print()
    all_sent = sum(f.result()[0] for f in futures)
    all_errors = sum(f.result()[1] for f in futures)
    all_latencies = [l for f.result()[2] for f in futures for l in f.result()[2] for l in f.result()[2] if l < 990 else l]
    
    if all_latencies:
        import statistics
        p = statistics.quantiles(all_latencies, n=100)
        print(f"Total: {all_sent + all_errors:,} requests")
        print(f"Successful: {all_sent:,} ({100 * all_sent / (all_sent + all_errors) if (all_sent + all_errors) > 0 else 100:.1f}%)")
        print(f"Errors: {all_errors:,}")
        print()
        print(f"Throughput: {all_sent / duration:.1f} events/sec")
        print()
        if len(all_latencies) >= 10:
            print("Latency (ms):")
            print(f"  Min: {min(all_latencies):.1f}")
            print(f"  P50: {p[49]:.1f}")
            print(f"  P90: {p[89]:.1f}")
            print(f"  P95: {p[94]:.1f}")
            print(f"  P99: {p[98]:.1f}")
            print(f"  Max: {max(all_latencies):.1f}")
            print()
        
    print("="*70)
