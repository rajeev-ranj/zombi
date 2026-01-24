#!/usr/bin/env python3
"""
Quick runner for Zombi load tests.

This script provides a simple interactive menu for running common load tests.
"""

import subprocess
import sys


def run_command(cmd: list, description: str):
    """Run a command and display output."""
    print(f"\n{'=' * 70}")
    print(f"{description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'=' * 70}\n")

    result = subprocess.run(cmd)
    return result.returncode


def main():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                   Zombi Load Test Runner                         ║
╚══════════════════════════════════════════════════════════════════╝

Testing EC2 instance: http://18.143.135.197:8080
""")

    while True:
        print("""
Select a test:

  1. Quick sanity check (30s, 100 events/sec)
  2. Steady load test (2 min, 500 events/sec)
  3. Ramp test (5 min, 100 -> 2000 events/sec)
  4. Max throughput test (5 min, adaptive)
  5. Spike test (2 min, baseline 100 -> spike 1000)
  6. Extended max test (10 min, adaptive)
  7. Stress test (30 min, adaptive)
  8. Custom test (run load_test.py with custom args)
  9. Monitor EC2 (show logs and stats)
  0. Exit
""")

        choice = input("Enter choice [0-9]: ").strip()

        base_url = "http://18.143.135.197:8080"

        if choice == "1":
            run_command(
                [
                    sys.executable,
                    "load_test.py",
                    "--url",
                    base_url,
                    "--profile",
                    "steady",
                    "--rate",
                    "100",
                    "--duration",
                    "30",
                ],
                "Quick Sanity Check",
            )

        elif choice == "2":
            run_command(
                [
                    sys.executable,
                    "load_test.py",
                    "--url",
                    base_url,
                    "--profile",
                    "steady",
                    "--rate",
                    "500",
                    "--duration",
                    "120",
                ],
                "Steady Load Test",
            )

        elif choice == "3":
            run_command(
                [
                    sys.executable,
                    "load_test.py",
                    "--url",
                    base_url,
                    "--profile",
                    "ramp",
                    "--min-rate",
                    "100",
                    "--max-rate",
                    "2000",
                    "--duration",
                    "300",
                ],
                "Ramp Test",
            )

        elif choice == "4":
            run_command(
                [
                    sys.executable,
                    "load_test.py",
                    "--url",
                    base_url,
                    "--profile",
                    "max",
                    "--duration",
                    "300",
                ],
                "Max Throughput Test",
            )

        elif choice == "5":
            run_command(
                [
                    sys.executable,
                    "load_test.py",
                    "--url",
                    base_url,
                    "--profile",
                    "spike",
                    "--min-rate",
                    "100",
                    "--max-rate",
                    "1000",
                    "--duration",
                    "120",
                ],
                "Spike Test",
            )

        elif choice == "6":
            run_command(
                [
                    sys.executable,
                    "load_test.py",
                    "--url",
                    base_url,
                    "--profile",
                    "max",
                    "--duration",
                    "600",
                ],
                "Extended Max Test",
            )

        elif choice == "7":
            run_command(
                [
                    sys.executable,
                    "load_test.py",
                    "--url",
                    base_url,
                    "--profile",
                    "max",
                    "--duration",
                    "1800",
                ],
                "Stress Test",
            )

        elif choice == "8":
            print("\nEnter custom load_test.py arguments:")
            args = input("> load_test.py ").strip()
            if args:
                cmd = [sys.executable, "load_test.py"] + args.split()
                run_command(cmd, "Custom Test")
            else:
                print("No arguments provided.")

        elif choice == "9":
            print("\nMonitoring EC2 instance...")
            print("Press Ctrl+C to stop monitoring\n")

            try:
                while True:
                    print("\n" + "=" * 70)
                    print("Zombi Logs (last 20 lines):")
                    print("=" * 70)
                    subprocess.run(
                        [
                            "ssh",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-i",
                            f"{sys.path[0]}/../.ssh/id_ed25519",
                            "ubuntu@18.143.135.197",
                            "sudo docker logs zombi --tail 20",
                        ]
                    )

                    print("\n" + "=" * 70)
                    print("Container Stats:")
                    print("=" * 70)
                    subprocess.run(
                        [
                            "ssh",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-i",
                            f"{sys.path[0]}/../.ssh/id_ed25519",
                            "ubuntu@18.143.135.197",
                            "sudo docker stats --no-stream zombi",
                        ]
                    )

                    print("\n" + "=" * 70)
                    print("System Top:")
                    print("=" * 70)
                    subprocess.run(
                        [
                            "ssh",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-i",
                            f"{sys.path[0]}/../.ssh/id_ed25519",
                            "ubuntu@18.143.135.197",
                            "top -b -n1 | head -20",
                        ]
                    )

                    input("\nPress Enter to refresh (or Ctrl+C to exit)...")

            except KeyboardInterrupt:
                print("\nMonitoring stopped.")

        elif choice == "0":
            print("\nGoodbye!")
            break

        else:
            print("\nInvalid choice. Please try again.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted. Goodbye!")
        sys.exit(0)
