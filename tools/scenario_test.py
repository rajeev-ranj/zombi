#!/usr/bin/env python3
"""
Zombi Scenario Test Tool

DEPRECATED: This tool is deprecated in favor of zombi_load.py.
Use the unified CLI instead:

    python tools/zombi_load.py run --profile quick
    python tools/zombi_load.py run --profile full
    python tools/zombi_load.py run --scenario multi-producer
    python tools/zombi_load.py run --scenario consistency

This file is kept for backwards compatibility.

---

Comprehensive load testing with realistic scenarios for Zombi.

Usage:
    # Run single scenario
    python scenario_test.py --url http://localhost:8080 --scenario multi-producer

    # Run scenario suite
    python scenario_test.py --url http://localhost:8080 --suite quick
    python scenario_test.py --url http://localhost:8080 --suite full

    # With cold storage verification (requires S3/MinIO)
    python scenario_test.py --url http://localhost:8080 --suite full \
        --s3-bucket zombi-events --s3-endpoint http://localhost:9000

Scenarios:
    multi-producer  - Multiple producers writing to multiple topics/partitions
    consumer        - Consumer offset tracking and order verification
    mixed           - Concurrent read/write workload
    backpressure    - Overload testing with 503 verification
    cold-storage    - Iceberg/S3 flush verification (requires S3)
    consistency     - Data consistency invariant verification

Suites:
    quick   - Fast sanity check (producer + consistency)
    full    - All scenarios except endurance
    stress  - Extended duration tests
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

# Import scenarios
from scenarios import (
    BaseScenario,
    ScenarioConfig,
    ScenarioResult,
    MultiProducerScenario,
    ConsumerScenario,
    MixedWorkloadScenario,
    BackpressureScenario,
    ColdStorageScenario,
    ConsistencyScenario,
)
from scenarios.producer import ProducerConfig
from scenarios.consumer import ConsumerConfig
from scenarios.mixed import MixedConfig
from scenarios.backpressure import BackpressureConfig
from scenarios.cold_storage import ColdStorageConfig
from scenarios.consistency import ConsistencyConfig


# ============================================================================
# Suite Definitions
# ============================================================================

SUITES = {
    "quick": {
        "description": "Fast sanity check (~3 min)",
        "scenarios": ["multi-producer", "consistency"],
        "duration_multiplier": 0.5,
    },
    "full": {
        "description": "Complete test suite (~30 min)",
        "scenarios": [
            "multi-producer",
            "consumer",
            "mixed",
            "backpressure",
            "cold-storage",
            "consistency",
        ],
        "duration_multiplier": 1.0,
    },
    "stress": {
        "description": "Extended stress testing (~2 hours)",
        "scenarios": [
            "multi-producer",
            "mixed",
            "backpressure",
            "consistency",
        ],
        "duration_multiplier": 4.0,
    },
}


# ============================================================================
# Environment Detection
# ============================================================================

def detect_environment() -> str:
    """Detect if running locally or on EC2."""
    endpoint = os.environ.get("ZOMBI_S3_ENDPOINT", "")
    if endpoint.startswith("http://localhost") or endpoint.startswith("http://127."):
        return "local-minio"
    elif os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("S3_BUCKET"):
        return "ec2-s3"
    else:
        return "hot-only"


def print_banner():
    """Print the scenario test banner."""
    print("=" * 70)
    print("ZOMBI SCENARIO TEST SUITE")
    print("=" * 70)
    print(f"Date: {datetime.now().isoformat()}")
    print(f"Environment: {detect_environment()}")
    print("=" * 70)


# ============================================================================
# Scenario Factory
# ============================================================================

def create_scenario(
    name: str,
    url: str,
    encoding: str = "proto",
    duration_secs: int = 60,
    s3_bucket: Optional[str] = None,
    s3_endpoint: Optional[str] = None,
    s3_region: str = "us-east-1",
    **kwargs,
) -> BaseScenario:
    """
    Create a scenario instance by name.

    Args:
        name: Scenario name
        url: Zombi server URL
        encoding: "proto" or "json"
        duration_secs: Base duration for the scenario
        s3_bucket: S3 bucket for cold storage tests
        s3_endpoint: S3 endpoint (for MinIO)
        s3_region: S3 region
        **kwargs: Additional scenario-specific config

    Returns:
        Configured scenario instance
    """
    base_config = {
        "url": url,
        "encoding": encoding,
        "duration_secs": duration_secs,
        "s3_bucket": s3_bucket,
        "s3_endpoint": s3_endpoint,
        "s3_region": s3_region,
    }

    if name == "multi-producer":
        config = ProducerConfig(
            **base_config,
            num_producers=kwargs.get("num_producers", 10),
            topics=kwargs.get("topics", ["events", "orders", "metrics"]),
            partitions_per_topic=kwargs.get("partitions_per_topic", 4),
            events_per_second=kwargs.get("events_per_second", 100),
        )
        return MultiProducerScenario(config)

    elif name == "consumer":
        config = ConsumerConfig(
            **base_config,
            num_consumers=kwargs.get("num_consumers", 5),
            consumer_group=kwargs.get("consumer_group", "test-group"),
            topic=kwargs.get("topic", "consumer-test"),
            num_partitions=kwargs.get("num_partitions", 4),
            seed_events=kwargs.get("seed_events", 10000),
        )
        return ConsumerScenario(config)

    elif name == "mixed":
        config = MixedConfig(
            **base_config,
            writer_count=kwargs.get("writer_count", 10),
            reader_count=kwargs.get("reader_count", 5),
            write_ratio=kwargs.get("write_ratio", 0.7),
            topic=kwargs.get("topic", "mixed-test"),
        )
        return MixedWorkloadScenario(config)

    elif name == "backpressure":
        config = BackpressureConfig(
            **base_config,
            burst_workers=kwargs.get("burst_workers", 200),
            burst_duration_secs=kwargs.get("burst_duration_secs", 30),
            recovery_workers=kwargs.get("recovery_workers", 10),
            recovery_duration_secs=kwargs.get("recovery_duration_secs", 30),
        )
        return BackpressureScenario(config)

    elif name == "cold-storage":
        if not s3_bucket:
            raise ValueError("cold-storage scenario requires --s3-bucket")
        config = ColdStorageConfig(
            **base_config,
            num_events=kwargs.get("num_events", 50000),
            topic=kwargs.get("topic", "cold-storage-test"),
            wait_for_flush_secs=kwargs.get("wait_for_flush_secs", 120),
        )
        return ColdStorageScenario(config)

    elif name == "consistency":
        config = ConsistencyConfig(
            **base_config,
            num_events=kwargs.get("num_events", 10000),
            num_partitions=kwargs.get("num_partitions", 4),
            topic=kwargs.get("topic", "consistency-test"),
        )
        return ConsistencyScenario(config)

    else:
        raise ValueError(f"Unknown scenario: {name}")


# ============================================================================
# Report Generation
# ============================================================================

@dataclass
class SuiteResult:
    """Results from running a test suite."""
    suite_name: str
    start_time: str
    duration_secs: float
    environment: str
    scenarios: List[ScenarioResult]
    passed: int
    failed: int
    skipped: int

    def to_dict(self) -> Dict:
        return {
            "suite_name": self.suite_name,
            "start_time": self.start_time,
            "duration_secs": self.duration_secs,
            "environment": self.environment,
            "summary": {
                "passed": self.passed,
                "failed": self.failed,
                "skipped": self.skipped,
                "total": self.passed + self.failed + self.skipped,
            },
            "scenarios": [s.to_dict() for s in self.scenarios],
        }


def print_scenario_result(result: ScenarioResult):
    """Print a scenario result summary."""
    status = "PASS" if result.success else "FAIL"
    print(f"\n{'=' * 60}")
    print(f"[{status}] {result.scenario_name}")
    print(f"  Duration: {result.duration_secs:.1f}s")
    print(f"  Events: {result.total_events:,} ({result.events_per_sec:.0f}/s)")
    print(f"  Errors: {result.total_errors}")
    print(f"  P95 Latency: {result.p95_ms:.1f}ms")
    if result.errors:
        print(f"  Error messages:")
        for err in result.errors[:5]:
            print(f"    - {err}")


def print_suite_summary(suite_result: SuiteResult):
    """Print suite summary."""
    print("\n" + "=" * 70)
    print("SUITE SUMMARY")
    print("=" * 70)
    print(f"Suite: {suite_result.suite_name}")
    print(f"Duration: {suite_result.duration_secs:.1f}s")
    print(f"Environment: {suite_result.environment}")
    print()
    print(f"Results:")
    print(f"  Passed:  {suite_result.passed}")
    print(f"  Failed:  {suite_result.failed}")
    print(f"  Skipped: {suite_result.skipped}")
    print()

    if suite_result.failed > 0:
        print("Failed scenarios:")
        for s in suite_result.scenarios:
            if not s.success:
                print(f"  - {s.scenario_name}")
                for err in s.errors[:2]:
                    print(f"      {err}")

    overall = "PASSED" if suite_result.failed == 0 else "FAILED"
    print()
    print(f"Overall: {overall}")
    print("=" * 70)


def save_results(suite_result: SuiteResult, output_path: Optional[str] = None, output_dir: str = "results"):
    """Save results to JSON file."""
    if output_path:
        # Use specified output path directly
        filename = output_path
        # Create parent directory if needed
        parent_dir = os.path.dirname(filename)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
    else:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/scenario_results_{timestamp}.json"

    with open(filename, "w") as f:
        json.dump(suite_result.to_dict(), f, indent=2)

    print(f"\nResults saved to: {filename}")
    return filename


# ============================================================================
# Main Runner
# ============================================================================

def run_scenario(
    name: str,
    args: argparse.Namespace,
    duration_multiplier: float = 1.0,
) -> Optional[ScenarioResult]:
    """Run a single scenario."""
    try:
        duration = int(args.duration * duration_multiplier)

        scenario = create_scenario(
            name=name,
            url=args.url,
            encoding=args.encoding,
            duration_secs=duration,
            s3_bucket=args.s3_bucket,
            s3_endpoint=args.s3_endpoint,
            s3_region=args.s3_region,
        )

        return scenario.run()

    except ValueError as e:
        print(f"\nSkipping {name}: {e}")
        return None
    except Exception as e:
        print(f"\nError in {name}: {e}")
        import traceback
        traceback.print_exc()
        return ScenarioResult(
            scenario_name=name,
            config={},
            success=False,
            start_time=datetime.now().isoformat(),
            duration_secs=0,
            errors=[str(e)],
        )


def run_suite(suite_name: str, args: argparse.Namespace) -> SuiteResult:
    """Run a test suite."""
    suite = SUITES.get(suite_name)
    if not suite:
        raise ValueError(f"Unknown suite: {suite_name}")

    print(f"\nRunning suite: {suite_name}")
    print(f"Description: {suite['description']}")
    print(f"Scenarios: {', '.join(suite['scenarios'])}")
    print()

    start_time = datetime.now()
    start = time.time()

    results = []
    passed = 0
    failed = 0
    skipped = 0

    for scenario_name in suite["scenarios"]:
        # Skip cold-storage if no S3 bucket
        if scenario_name == "cold-storage" and not args.s3_bucket:
            print(f"\nSkipping {scenario_name}: No S3 bucket specified")
            skipped += 1
            continue

        result = run_scenario(
            scenario_name,
            args,
            duration_multiplier=suite["duration_multiplier"],
        )

        if result is None:
            skipped += 1
        elif result.success:
            passed += 1
            results.append(result)
            print_scenario_result(result)
        else:
            failed += 1
            results.append(result)
            print_scenario_result(result)

    duration = time.time() - start

    return SuiteResult(
        suite_name=suite_name,
        start_time=start_time.isoformat(),
        duration_secs=duration,
        environment=detect_environment(),
        scenarios=results,
        passed=passed,
        failed=failed,
        skipped=skipped,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Zombi Scenario Test Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Connection
    parser.add_argument(
        "--url",
        default="http://localhost:8080",
        help="Zombi server URL",
    )

    # Test selection
    parser.add_argument(
        "--scenario",
        choices=[
            "multi-producer",
            "consumer",
            "mixed",
            "backpressure",
            "cold-storage",
            "consistency",
        ],
        help="Run a single scenario",
    )
    parser.add_argument(
        "--suite",
        choices=list(SUITES.keys()),
        help="Run a test suite",
    )

    # Encoding
    parser.add_argument(
        "--encoding",
        choices=["proto", "json"],
        default="proto",
        help="Event encoding format (default: proto)",
    )

    # Timing
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Base duration for scenarios in seconds (default: 60)",
    )

    # S3/Cold Storage
    parser.add_argument(
        "--s3-bucket",
        help="S3 bucket for cold storage verification",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 endpoint URL (for MinIO)",
    )
    parser.add_argument(
        "--s3-region",
        default="us-east-1",
        help="S3 region (default: us-east-1)",
    )

    # Output
    parser.add_argument(
        "--output-dir",
        default="results",
        help="Directory for result files (default: results)",
    )
    parser.add_argument(
        "--output",
        help="Output file for JSON results (overrides --output-dir)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to file",
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.scenario and not args.suite:
        parser.print_help()
        print("\nError: Specify either --scenario or --suite")
        sys.exit(1)

    print_banner()
    print(f"Target: {args.url}")
    print(f"Encoding: {args.encoding}")
    if args.s3_bucket:
        print(f"S3 Bucket: {args.s3_bucket}")
        if args.s3_endpoint:
            print(f"S3 Endpoint: {args.s3_endpoint}")

    # Run tests
    if args.scenario:
        result = run_scenario(args.scenario, args)
        if result:
            print_scenario_result(result)
            if not args.no_save:
                suite_result = SuiteResult(
                    suite_name=f"single-{args.scenario}",
                    start_time=result.start_time,
                    duration_secs=result.duration_secs,
                    environment=detect_environment(),
                    scenarios=[result],
                    passed=1 if result.success else 0,
                    failed=0 if result.success else 1,
                    skipped=0,
                )
                save_results(suite_result, output_path=args.output, output_dir=args.output_dir)

            sys.exit(0 if result.success else 1)
        else:
            sys.exit(1)

    elif args.suite:
        suite_result = run_suite(args.suite, args)
        print_suite_summary(suite_result)

        if not args.no_save:
            save_results(suite_result, output_path=args.output, output_dir=args.output_dir)

        sys.exit(0 if suite_result.failed == 0 else 1)


if __name__ == "__main__":
    main()
