"""
Scenario runner that wraps existing scenario implementations.
"""

import sys
import time
from datetime import datetime
from typing import Optional

# Add tools directory to path for imports
sys.path.insert(0, str(__file__).rsplit("/", 3)[0])

from scenarios import (
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

from benchmark import BenchmarkSuite

from output import ScenarioOutput
from .base import BaseRunner, RunnerConfig


# Map scenario names to their implementations
SCENARIO_MAP = {
    # New unified names
    "single-write": "multi-producer",
    "bulk-write": "benchmark-bulk",  # Fixed: uses /tables/{table}/bulk endpoint
    "read-throughput": "benchmark-read",
    "write-read-lag": "benchmark-lag",
    "mixed-workload": "mixed",
    "backpressure": "backpressure",
    "cold-storage": "cold-storage",
    "consistency": "consistency",
    # Legacy names (direct mapping)
    "multi-producer": "multi-producer",
    "consumer": "consumer",
    "mixed": "mixed",
    # Keep old name for backwards compatibility
    "benchmark-write": "benchmark-write",
}


class ScenarioRunner(BaseRunner):
    """Runner that wraps existing scenario implementations."""

    def __init__(self, scenario_name: str, config: RunnerConfig):
        super().__init__(config)
        self._name = scenario_name
        self._impl_name = SCENARIO_MAP.get(scenario_name, scenario_name)

    @property
    def name(self) -> str:
        return self._name

    def run(self) -> ScenarioOutput:
        """Execute the scenario and return standardized output."""
        # Check for benchmark-based scenarios
        if self._impl_name.startswith("benchmark-"):
            return self._run_benchmark_scenario()

        # Run scenario-based test
        return self._run_scenario()

    def _run_scenario(self) -> ScenarioOutput:
        """Run a scenario from the scenarios package."""
        try:
            scenario = self._create_scenario()
            if scenario is None:
                return self.create_output(
                    name=self.name,
                    success=False,
                    duration_secs=0,
                    error_messages=[f"Unknown scenario: {self._impl_name}"],
                )

            # Check health
            if not scenario.health_check():
                return self.create_output(
                    name=self.name,
                    success=False,
                    duration_secs=0,
                    error_messages=["Health check failed"],
                )

            # Run the scenario
            print(f"\n{'='*60}")
            print(f"Running scenario: {self.name}")
            print(f"{'='*60}")

            result = scenario.run()

            # Convert ScenarioResult to ScenarioOutput
            return self._convert_result(result)

        except Exception as e:
            import traceback
            return self.create_output(
                name=self.name,
                success=False,
                duration_secs=0,
                error_messages=[str(e), traceback.format_exc()],
            )

    def _create_scenario(self):
        """Create the appropriate scenario instance."""
        base_config = {
            "url": self.config.url,
            "encoding": self.config.encoding,
            "duration_secs": self.config.duration_secs,
            "warmup_secs": self.config.warmup_secs,
            "s3_bucket": self.config.s3_bucket,
            "s3_endpoint": self.config.s3_endpoint,
            "s3_region": self.config.s3_region,
            "payload_size": self.config.payload_size,
        }

        if self._impl_name == "multi-producer":
            config = ProducerConfig(
                **base_config,
                num_producers=self.config.num_workers,
                topics=["events", "orders", "metrics"],
                partitions_per_topic=4,
                events_per_second=0,  # 0 = max rate
            )
            return MultiProducerScenario(config)

        elif self._impl_name == "consumer":
            config = ConsumerConfig(
                **base_config,
                num_consumers=5,
                consumer_group="test-group",
                topic="consumer-test",
                num_partitions=4,
                seed_events=10000,
            )
            return ConsumerScenario(config)

        elif self._impl_name == "mixed":
            config = MixedConfig(
                **base_config,
                writer_count=self.config.num_workers,
                reader_count=max(1, self.config.num_workers // 2),
                write_ratio=0.7,
                topic="mixed-test",
            )
            return MixedWorkloadScenario(config)

        elif self._impl_name == "backpressure":
            config = BackpressureConfig(
                **base_config,
                burst_workers=200,
                burst_duration_secs=min(30, self.config.duration_secs // 2),
                recovery_workers=10,
                recovery_duration_secs=min(30, self.config.duration_secs // 2),
            )
            return BackpressureScenario(config)

        elif self._impl_name == "cold-storage":
            if not self.config.s3_bucket:
                raise ValueError("cold-storage scenario requires s3_bucket configuration")
            config = ColdStorageConfig(
                **base_config,
                num_events=50000,
                topic="cold-storage-test",
                wait_for_flush_secs=120,
            )
            return ColdStorageScenario(config)

        elif self._impl_name == "consistency":
            config = ConsistencyConfig(
                **base_config,
                num_events=10000,
                num_partitions=4,
                topic="consistency-test",
            )
            return ConsistencyScenario(config)

        return None

    def _run_benchmark_scenario(self) -> ScenarioOutput:
        """Run a scenario using the benchmark suite."""
        bench = BenchmarkSuite(
            url=self.config.url,
            table=f"bench_{int(time.time())}",
            s3_bucket=self.config.s3_bucket,
        )

        if not bench.health_check():
            return self.create_output(
                name=self.name,
                success=False,
                duration_secs=0,
                error_messages=["Health check failed"],
            )

        print(f"\n{'='*60}")
        print(f"Running scenario: {self.name}")
        print(f"{'='*60}")

        start_time = time.time()

        if self._impl_name == "benchmark-write":
            result = bench.test_write_throughput(
                duration_sec=self.config.duration_secs,
                workers=self.config.num_workers,
            )
            duration = time.time() - start_time

            return self.create_output(
                name=self.name,
                success=result.get("errors", 0) == 0,
                duration_secs=duration,
                events_per_sec=result.get("throughput", 0),
                bytes_total=result.get("total", 0) * self.config.payload_size,
                p50_ms=result.get("p50_ms", 0),
                p95_ms=result.get("p95_ms", 0),
                p99_ms=result.get("p99_ms", 0),
                errors=result.get("errors", 0),
            )

        elif self._impl_name == "benchmark-read":
            result = bench.test_read_throughput(
                num_reads=100,
                batch_size=100,
            )
            duration = time.time() - start_time

            return self.create_output(
                name=self.name,
                success=True,
                duration_secs=duration,
                events_per_sec=result.get("records_per_sec", 0),
                p50_ms=result.get("p50_ms", 0),
                p95_ms=result.get("p95_ms", 0),
                details={"records_total": result.get("records_total", 0)},
            )

        elif self._impl_name == "benchmark-lag":
            result = bench.test_write_read_lag(num_samples=50)
            duration = time.time() - start_time

            return self.create_output(
                name=self.name,
                success=True,
                duration_secs=duration,
                p50_ms=result.get("p50_ms", 0),
                p95_ms=result.get("p95_ms", 0),
                p99_ms=result.get("p99_ms", 0),
                details={"samples": result.get("samples", 0)},
            )

        elif self._impl_name == "benchmark-bulk":
            # Use the bulk API endpoint for true bulk write performance
            result = bench.test_bulk_write_throughput(
                duration_sec=self.config.duration_secs,
                workers=self.config.num_workers,
                batch_size=100,  # 100 events per request
            )
            duration = time.time() - start_time

            return self.create_output(
                name=self.name,
                success=result.get("errors", 0) == 0,
                duration_secs=duration,
                events_per_sec=result.get("events_per_sec", 0),
                bytes_total=result.get("total_events", 0) * self.config.payload_size,
                p50_ms=result.get("p50_ms", 0),
                p95_ms=result.get("p95_ms", 0),
                p99_ms=result.get("p99_ms", 0),
                errors=result.get("errors", 0),
                details={
                    "batch_size": result.get("batch_size", 100),
                    "requests_per_sec": result.get("requests_per_sec", 0),
                },
            )

        return self.create_output(
            name=self.name,
            success=False,
            duration_secs=0,
            error_messages=[f"Unknown benchmark scenario: {self._impl_name}"],
        )

    def _convert_result(self, result: ScenarioResult) -> ScenarioOutput:
        """Convert ScenarioResult to ScenarioOutput."""
        return self.create_output(
            name=self.name,
            success=result.success,
            duration_secs=result.duration_secs,
            events_per_sec=result.events_per_sec,
            bytes_total=result.bytes_total,
            p50_ms=result.p50_ms,
            p95_ms=result.p95_ms,
            p99_ms=result.p99_ms,
            min_ms=result.min_ms,
            max_ms=result.max_ms,
            errors=result.total_errors,
            details=result.details,
            timeline=result.timeline,
            error_messages=result.errors,
        )


def run_scenario(
    scenario_name: str,
    url: str,
    encoding: str = "proto",
    duration_secs: int = 60,
    num_workers: int = 10,
    s3_bucket: Optional[str] = None,
    s3_endpoint: Optional[str] = None,
    s3_region: str = "us-east-1",
) -> ScenarioOutput:
    """
    Convenience function to run a single scenario.

    Args:
        scenario_name: Name of scenario to run
        url: Zombi server URL
        encoding: "proto" or "json"
        duration_secs: Test duration
        num_workers: Number of concurrent workers
        s3_bucket: S3 bucket for cold storage tests
        s3_endpoint: S3 endpoint (for MinIO)
        s3_region: S3 region

    Returns:
        ScenarioOutput with test results
    """
    config = RunnerConfig(
        url=url,
        encoding=encoding,
        duration_secs=duration_secs,
        num_workers=num_workers,
        s3_bucket=s3_bucket,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
    )

    runner = ScenarioRunner(scenario_name, config)
    return runner.run()
