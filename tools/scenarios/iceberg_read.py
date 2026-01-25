"""
Iceberg Read Scenario

Tests cold storage (Iceberg/Parquet) read performance.
Measures query latency and throughput when reading from S3.
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import BaseScenario, ScenarioConfig, ScenarioResult


@dataclass
class IcebergReadConfig(ScenarioConfig):
    """Configuration for Iceberg read scenario."""
    topic: str = "iceberg-read-test"
    num_warmup_reads: int = 5
    num_reads: int = 50
    batch_size: int = 1000
    read_from_cold: bool = True  # Force reads from cold storage


class IcebergReadScenario(BaseScenario):
    """
    Scenario for testing Iceberg/cold storage read performance.

    This scenario:
    1. Writes events to trigger a flush to cold storage
    2. Waits for flush to complete
    3. Performs multiple reads measuring latency and throughput
    4. Reports read performance from cold storage
    """

    def __init__(self, config: IcebergReadConfig):
        super().__init__(config)
        self.iceberg_config = config

    @property
    def name(self) -> str:
        return "iceberg-read"

    def run(self) -> ScenarioResult:
        """Execute the Iceberg read scenario."""
        start_time = datetime.now()
        start = time.time()
        errors = []

        # Check S3 configuration
        if not self.config.s3_bucket:
            return self.create_result(
                success=False,
                start_time=start_time,
                duration=0,
                error_messages=["S3 bucket not configured - required for iceberg-read scenario"],
            )

        print(f"\n{'='*60}")
        print(f"Running Iceberg Read Scenario")
        print(f"{'='*60}")
        print(f"Topic: {self.iceberg_config.topic}")
        print(f"Reads: {self.iceberg_config.num_reads}")
        print(f"Batch size: {self.iceberg_config.batch_size}")

        # Health check
        if not self.health_check():
            return self.create_result(
                success=False,
                start_time=start_time,
                duration=0,
                error_messages=["Health check failed"],
            )

        # Step 1: Seed data and trigger flush
        print(f"\nStep 1: Seeding data for cold storage test...")
        seed_result = self._seed_data()
        if not seed_result["success"]:
            return self.create_result(
                success=False,
                start_time=start_time,
                duration=time.time() - start,
                error_messages=seed_result.get("errors", ["Seeding failed"]),
            )

        print(f"  Seeded {seed_result['events_written']} events")

        # Step 2: Trigger flush
        print(f"\nStep 2: Triggering flush to cold storage...")
        flush_result = self._trigger_flush()
        if not flush_result["success"]:
            errors.append("Flush may not have completed")

        print(f"  Flush completed in {flush_result.get('duration_secs', 0):.1f}s")

        # Step 3: Verify data in S3
        print(f"\nStep 3: Verifying data in S3...")
        verify_result = self._verify_s3_data()
        if verify_result["file_count"] == 0:
            return self.create_result(
                success=False,
                start_time=start_time,
                duration=time.time() - start,
                error_messages=["No data files found in S3"],
            )

        print(f"  Found {verify_result['file_count']} files, {verify_result['total_size_mb']:.2f} MB")

        # Step 4: Warmup reads
        print(f"\nStep 4: Warming up with {self.iceberg_config.num_warmup_reads} reads...")
        for _ in range(self.iceberg_config.num_warmup_reads):
            self.read_events(
                self.iceberg_config.topic,
                limit=self.iceberg_config.batch_size,
            )

        # Step 5: Benchmark reads
        print(f"\nStep 5: Benchmarking {self.iceberg_config.num_reads} reads...")
        read_results = self._benchmark_reads()

        duration = time.time() - start

        # Calculate metrics
        total_records = sum(r["records"] for r in read_results)
        total_latency = sum(r["latency_ms"] for r in read_results)
        avg_latency = total_latency / len(read_results) if read_results else 0

        latencies = sorted([r["latency_ms"] for r in read_results])
        p50 = latencies[len(latencies) // 2] if latencies else 0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

        records_per_sec = total_records / (total_latency / 1000) if total_latency > 0 else 0

        # Estimate MB scanned
        mb_scanned = verify_result["total_size_mb"]

        print(f"\nResults:")
        print(f"  Total reads: {len(read_results)}")
        print(f"  Total records: {total_records:,}")
        print(f"  Records/sec: {records_per_sec:,.0f}")
        print(f"  Avg latency: {avg_latency:.1f}ms")
        print(f"  P95 latency: {p95:.1f}ms")
        print(f"  MB scanned: {mb_scanned:.2f}")

        # Record stats
        for r in read_results:
            self.stats.record(True, r["latency_ms"], r["records"] * 100)  # Estimate bytes

        return self.create_result(
            success=True,
            start_time=start_time,
            duration=duration,
            details={
                "reads_completed": len(read_results),
                "records_total": total_records,
                "records_per_sec": records_per_sec,
                "avg_latency_ms": avg_latency,
                "mb_scanned": mb_scanned,
                "s3_files": verify_result["file_count"],
                "s3_size_mb": verify_result["total_size_mb"],
            },
            error_messages=errors if errors else None,
        )

    def _seed_data(self) -> Dict[str, Any]:
        """Seed data for the cold storage test."""
        num_events = 10000  # Enough to trigger a flush
        events_written = 0
        errors = []

        for i in range(num_events):
            payload = {
                "event_type": "iceberg_test",
                "index": i,
                "data": f"test-data-{i}",
                "timestamp": int(time.time() * 1000),
            }
            success, latency, _ = self.write_event(
                self.iceberg_config.topic,
                partition=i % 4,
                payload_dict=payload,
            )
            if success:
                events_written += 1
            else:
                errors.append(f"Write failed for event {i}")

            if i % 1000 == 0:
                print(f"  Progress: {i}/{num_events}")

        return {
            "success": events_written > num_events * 0.9,  # 90% success rate
            "events_written": events_written,
            "errors": errors[:5] if errors else [],
        }

    def _trigger_flush(self) -> Dict[str, Any]:
        """Trigger a flush to cold storage."""
        start = time.time()
        try:
            r = self.session.post(f"{self.config.url}/flush", timeout=120)
            duration = time.time() - start

            # Wait for flush to complete
            time.sleep(5)

            return {
                "success": r.status_code in (200, 202, 204),
                "duration_secs": duration,
            }
        except Exception as e:
            return {
                "success": False,
                "duration_secs": time.time() - start,
                "error": str(e),
            }

    def _verify_s3_data(self) -> Dict[str, Any]:
        """Verify data exists in S3."""
        try:
            from lib.s3_verifier import S3ParquetVerifier

            verifier = S3ParquetVerifier(
                bucket=self.config.s3_bucket,
                endpoint=self.config.s3_endpoint,
                region=self.config.s3_region,
            )

            files = verifier.list_parquet_files()

            total_size = sum(f.size_bytes for f in files)

            return {
                "file_count": len(files),
                "total_size_mb": total_size / 1024 / 1024,
                "files": [{"path": f.path, "size_mb": f.size_bytes / 1024 / 1024} for f in files[:10]],
            }

        except ImportError:
            # Fallback to aws cli
            import subprocess

            try:
                result = subprocess.run(
                    ["aws", "s3", "ls", f"s3://{self.config.s3_bucket}/", "--recursive", "--summarize"],
                    capture_output=True, text=True, timeout=30,
                    env={**dict(__import__('os').environ), "AWS_ENDPOINT_URL": self.config.s3_endpoint or ""}
                )

                file_count = 0
                total_size = 0

                for line in result.stdout.strip().split("\n"):
                    if "Total Objects:" in line:
                        file_count = int(line.split(":")[-1].strip())
                    elif "Total Size:" in line:
                        size_str = line.split(":")[-1].strip()
                        if size_str.isdigit():
                            total_size = int(size_str)

                return {
                    "file_count": file_count,
                    "total_size_mb": total_size / 1024 / 1024,
                    "files": [],
                }

            except Exception as e:
                return {
                    "file_count": 0,
                    "total_size_mb": 0,
                    "files": [],
                    "error": str(e),
                }

    def _benchmark_reads(self) -> List[Dict[str, Any]]:
        """Benchmark read performance."""
        results = []

        for i in range(self.iceberg_config.num_reads):
            events, latency, _ = self.read_events(
                self.iceberg_config.topic,
                limit=self.iceberg_config.batch_size,
            )

            results.append({
                "read_index": i,
                "records": len(events),
                "latency_ms": latency,
            })

            if i % 10 == 0:
                print(f"  Progress: {i}/{self.iceberg_config.num_reads}")

        return results
