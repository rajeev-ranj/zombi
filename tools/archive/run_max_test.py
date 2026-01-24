#!/usr/bin/env python3
"""
Max throughput test with multiple tables and EC2 monitoring
"""

import subprocess
import threading
import time
import sys

EC2_HOST = "ubuntu@18.143.135.197"
SSH_KEY = "~/.ssh/id_ed25519"
ZOMBI_URL = "http://18.143.135.197:8080"
TABLES = ["events_a", "events_b", "events_c", "events_d", "events_e"]
TEST_DURATION = 300  # 5 minutes


def run_command(cmd, description):
    """Run a command and print output."""
    print(f"\n[{time.strftime('%H:%M:%S')}] {description}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(result.stdout.strip())
    else:
        print(f"Error: {result.stderr.strip()}")
    return result.stdout


def monitor_ec2():
    """Monitor EC2 stats in a background thread."""
    print("\n" + "=" * 70)
    print("EC2 MONITORING (refreshing every 10 seconds)")
    print("=" * 70)

    ssh_prefix = f"ssh -o StrictHostKeyChecking=no -i {SSH_KEY}"

    while True:
        print(f"\n[{time.strftime('%H:%M:%S')}] --- EC2 Stats ---")

        # Docker stats
        result = run_command(
            f"{ssh_prefix} {EC2_HOST} 'sudo docker stats --no-stream zombi'",
            "Docker Stats:",
        )

        # CPU/Memory
        result = run_command(
            f"{ssh_prefix} {EC2_HOST} 'top -b -n1 | head -5'", "System Top:"
        )

        # Zombi logs (last 10 flush lines)
        result = run_command(
            f"{ssh_prefix} {EC2_HOST} 'sudo docker logs zombi 2>&1 | grep -E \"Committed|Flushed\" | tail -5'",
            "Recent Flushes:",
        )

        time.sleep(10)


def run_load_test():
    """Run load test on multiple tables."""
    print("\n" + "=" * 70)
    print("RUNNING LOAD TEST")
    print("=" * 70)
    print(f"URL: {ZOMBI_URL}")
    print(f"Tables: {TABLES}")
    print(f"Duration: {TEST_DURATION}s")
    print("=" * 70)

    # Start load tests for each table in parallel
    processes = []
    for table in TABLES:
        cmd = [
            sys.executable,
            "load_test.py",
            "--url",
            ZOMBI_URL,
            "--table",
            table,
            "--profile",
            "max",
            "--duration",
            str(TEST_DURATION),
            "--workers",
            "5",  # 5 workers per table
        ]
        print(f"\nStarting test for table: {table}")
        p = subprocess.Popen(cmd)
        processes.append(p)

    # Wait for all tests to complete
    for i, p in enumerate(processes):
        p.wait()
        print(f"\n[Completed] Table {TABLES[i]} test finished")

    print("\n" + "=" * 70)
    print("ALL TESTS COMPLETED")
    print("=" * 70)


def check_s3_after_test():
    """Check S3 after test completes."""
    print("\n" + "=" * 70)
    print("S3 SUMMARY")
    print("=" * 70)

    for table in TABLES:
        result = run_command(
            f"aws s3 ls s3://zombi-events-test1/tables/{table}/data/ --recursive | wc -l",
            f"S3 objects for {table}:",
        )


if __name__ == "__main__":
    try:
        # Start EC2 monitoring in background
        monitor_thread = threading.Thread(target=monitor_ec2, daemon=True)
        monitor_thread.start()

        # Run load test
        run_load_test()

        # Check S3
        check_s3_after_test()

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
