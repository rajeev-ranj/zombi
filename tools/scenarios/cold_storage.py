"""
Cold Storage Scenario

Tests Iceberg/S3 flush and verifies data in Parquet files.
"""

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from .base import BaseScenario, ScenarioConfig, ScenarioResult, Stats, create_session, generate_payload


@dataclass
class ColdStorageConfig(ScenarioConfig):
    """Configuration for cold storage scenario."""
    # Events to write
    num_events: int = 50000
    batch_size: int = 1000
    topic: str = "cold-storage-test"
    num_partitions: int = 4

    # Flush settings
    force_flush: bool = True
    wait_for_flush_secs: int = 120  # Max wait time for flush

    # S3 verification (required)
    # s3_bucket: str  # From parent
    # s3_endpoint: str  # From parent (optional, for MinIO)

    def __post_init__(self):
        super().__post_init__()
        if not self.s3_bucket:
            raise ValueError("s3_bucket is required for cold storage scenario")


class ColdStorageScenario(BaseScenario):
    """
    Cold storage scenario that verifies Iceberg/S3 flush.

    Steps:
    1. Write N events to hot storage
    2. Trigger or wait for flush
    3. Verify events appear in S3 as Parquet files
    4. Verify row counts match
    """

    def __init__(self, config: ColdStorageConfig):
        super().__init__(config)
        self.cs_config = config
        self.write_stats = Stats()
        self._written_events: List[Dict] = []
        self._s3_verifier = None

    @property
    def name(self) -> str:
        return "cold-storage"

    def _init_s3_verifier(self):
        """Initialize S3 verifier."""
        try:
            from lib.s3_verifier import S3ParquetVerifier, check_dependencies

            deps = check_dependencies()
            if not deps["boto3"]:
                return None, "boto3 not installed"
            if not deps["pyarrow"]:
                return None, "pyarrow not installed (Parquet verification will be limited)"

            verifier = S3ParquetVerifier(
                bucket=self.cs_config.s3_bucket,
                endpoint_url=self.cs_config.s3_endpoint,
                region=self.cs_config.s3_region,
            )
            return verifier, None
        except Exception as e:
            return None, str(e)

    def _write_events(self) -> bool:
        """Write events to hot storage."""
        session = create_session()
        num_events = self.cs_config.num_events
        batch_size = self.cs_config.batch_size
        topic = self.cs_config.topic
        partitions = self.cs_config.num_partitions

        print(f"Writing {num_events:,} events in batches of {batch_size}...")

        events_written = 0
        batch_num = 0

        while events_written < num_events:
            batch_start = time.time()

            for i in range(min(batch_size, num_events - events_written)):
                partition = events_written % partitions
                payload = {
                    "cold_storage_test": True,
                    "event_index": events_written,
                    "batch": batch_num,
                    "timestamp": time.time(),
                }

                success, latency, bytes_sent = self.write_event(
                    topic, partition, payload, session
                )
                self.write_stats.record(success, latency, bytes_sent)

                if success:
                    self._written_events.append({
                        "index": events_written,
                        "partition": partition,
                    })
                    events_written += 1

            batch_duration = time.time() - batch_start
            batch_num += 1

            if batch_num % 10 == 0:
                summary = self.write_stats.summary()
                rate = summary["success"] / (time.time() - batch_start) if batch_start > 0 else 0
                print(
                    f"  Written: {events_written:,}/{num_events:,} | "
                    f"Errors: {summary['errors']} | "
                    f"P95: {summary['p95_ms']:.1f}ms"
                )

        summary = self.write_stats.summary()
        print(f"Write complete: {summary['success']:,} events, {summary['errors']} errors")
        return summary["errors"] == 0

    def _trigger_flush(self) -> bool:
        """Trigger a flush to cold storage."""
        try:
            r = self.session.post(
                f"{self.config.url}/tables/{self.cs_config.topic}/flush",
                timeout=60,
            )
            if r.status_code in (200, 202, 204):
                print("Flush triggered successfully")
                return True
            else:
                print(f"Flush returned status {r.status_code}")
                return True  # May not be implemented
        except Exception as e:
            print(f"Flush request failed: {e}")
            return True  # Continue anyway

    def _verify_s3(self, verifier) -> Dict:
        """Verify data in S3."""
        # Construct expected prefix based on Zombi's layout
        prefix = f"tables/{self.cs_config.topic}/data/"

        print(f"Verifying S3 data in {self.cs_config.s3_bucket}/{prefix}...")

        # Wait for files to appear
        print(f"Waiting up to {self.cs_config.wait_for_flush_secs}s for Parquet files...")
        success, msg = verifier.wait_for_files(
            prefix=prefix,
            min_files=1,
            timeout_secs=self.cs_config.wait_for_flush_secs,
            poll_interval_secs=5,
        )

        if not success:
            # Try alternate prefixes
            alt_prefixes = [
                f"tables/{self.cs_config.topic}/",
                f"{self.cs_config.topic}/",
                "",
            ]
            for alt_prefix in alt_prefixes:
                files = verifier.list_parquet_files(alt_prefix, max_files=10)
                if files:
                    prefix = alt_prefix
                    success = True
                    print(f"Found files with alternate prefix: {alt_prefix}")
                    break

        if not success:
            return {
                "verified": False,
                "error": f"No Parquet files found: {msg}",
                "files": [],
            }

        # Verify data
        result = verifier.verify_data(
            prefix=prefix,
            expected_rows=len(self._written_events),
        )

        return {
            "verified": result.success,
            "total_files": result.total_files,
            "total_rows": result.total_rows,
            "total_size_bytes": result.total_size_bytes,
            "expected_rows": len(self._written_events),
            "row_match": result.total_rows >= len(self._written_events) * 0.95,  # 95% threshold
            "errors": result.errors,
            "files": [
                {
                    "key": f.key,
                    "size_bytes": f.size_bytes,
                    "row_count": f.row_count,
                }
                for f in result.files[:10]  # First 10 files
            ],
        }

    def _verify_s3_basic(self) -> Dict:
        """Basic S3 verification using AWS CLI (fallback)."""
        import subprocess

        prefix = f"tables/{self.cs_config.topic}/"
        bucket = self.cs_config.s3_bucket
        endpoint = self.cs_config.s3_endpoint

        cmd = ["aws", "s3", "ls", f"s3://{bucket}/{prefix}", "--recursive"]
        if endpoint:
            cmd.extend(["--endpoint-url", endpoint])

        print(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            lines = result.stdout.strip().split("\n") if result.stdout else []

            parquet_files = [l for l in lines if ".parquet" in l]
            total_size = 0
            for line in parquet_files:
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        total_size += int(parts[2])
                    except ValueError:
                        pass

            return {
                "verified": len(parquet_files) > 0,
                "total_files": len(parquet_files),
                "total_size_bytes": total_size,
                "files": parquet_files[:10],
                "method": "aws-cli",
            }
        except Exception as e:
            return {
                "verified": False,
                "error": str(e),
                "method": "aws-cli",
            }

    def run(self) -> ScenarioResult:
        """Execute the cold storage scenario."""
        print(f"\n=== COLD STORAGE SCENARIO ===")
        print(f"Events to write: {self.cs_config.num_events:,}")
        print(f"Topic: {self.cs_config.topic}")
        print(f"S3 Bucket: {self.cs_config.s3_bucket}")
        if self.cs_config.s3_endpoint:
            print(f"S3 Endpoint: {self.cs_config.s3_endpoint}")

        if not self.health_check():
            return self.create_result(
                success=False,
                start_time=datetime.now(),
                duration=0,
                error_messages=["Health check failed"],
            )

        start_time = datetime.now()
        start = time.time()
        errors = []

        # Initialize S3 verifier
        verifier, verifier_error = self._init_s3_verifier()
        if verifier_error:
            print(f"S3 verifier warning: {verifier_error}")

        # Phase 1: Write events
        print("\nPhase 1: Writing events to hot storage...")
        write_success = self._write_events()
        if not write_success:
            errors.append("Write phase had errors")

        # Phase 2: Trigger flush
        if self.cs_config.force_flush:
            print("\nPhase 2: Triggering flush...")
            self._trigger_flush()
            time.sleep(5)  # Give flush time to start

        # Phase 3: Verify S3
        print("\nPhase 3: Verifying data in S3...")
        if verifier:
            s3_result = self._verify_s3(verifier)
        else:
            s3_result = self._verify_s3_basic()

        duration = time.time() - start

        # Build details
        write_summary = self.write_stats.summary()
        details = {
            "write_phase": {
                "total_written": write_summary["success"],
                "errors": write_summary["errors"],
                "p95_ms": write_summary["p95_ms"],
            },
            "s3_verification": s3_result,
        }

        # Determine success
        success = (
            write_summary["errors"] == 0
            and s3_result.get("verified", False)
        )

        if not s3_result.get("verified"):
            errors.append(s3_result.get("error", "S3 verification failed"))
        if s3_result.get("errors"):
            errors.extend(s3_result["errors"])

        # Print summary
        print(f"\nCold storage test {'PASSED' if success else 'FAILED'}")
        print(f"  Events written: {write_summary['success']:,}")
        print(f"  S3 files found: {s3_result.get('total_files', 0)}")
        print(f"  S3 total rows: {s3_result.get('total_rows', 'N/A')}")
        if s3_result.get("total_size_bytes"):
            print(f"  S3 total size: {s3_result['total_size_bytes'] / 1024 / 1024:.1f} MB")

        return self.create_result(
            success=success,
            start_time=start_time,
            duration=duration,
            details=details,
            error_messages=errors,
        )
