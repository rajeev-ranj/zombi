"""
Mixed Workload Scenario

Tests concurrent read and write operations at configurable ratios.
"""

import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from .base import BaseScenario, ScenarioConfig, ScenarioResult, Stats, create_session, generate_payload


@dataclass
class MixedConfig(ScenarioConfig):
    """Configuration for mixed workload scenario."""
    writer_count: int = 10
    reader_count: int = 5
    write_ratio: float = 0.7  # 70% writes, 30% reads
    topic: str = "mixed-test"
    num_partitions: int = 4
    read_batch_size: int = 100


class MixedWorkloadScenario(BaseScenario):
    """
    Mixed workload scenario that tests concurrent reads and writes.

    Features:
    - Configurable writer/reader counts
    - Configurable write/read ratio
    - Separate stats for reads and writes
    - Tests for deadlocks and resource contention
    """

    def __init__(self, config: MixedConfig):
        super().__init__(config)
        self.mixed_config = config
        self.write_stats = Stats()
        self.read_stats = Stats()
        self._active_writers = 0
        self._active_readers = 0
        self._counter_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "mixed-workload"

    def _writer_worker(self, writer_id: int, duration: float):
        """Worker function for a writer."""
        session = create_session()
        topic = self.mixed_config.topic
        partitions = self.mixed_config.num_partitions
        end_time = time.time() + duration

        with self._counter_lock:
            self._active_writers += 1

        try:
            while time.time() < end_time and not self.should_stop():
                partition = random.randint(0, partitions - 1)
                payload = generate_payload(self.config.payload_size)
                payload["writer_id"] = writer_id

                success, latency, bytes_sent = self.write_event(
                    topic, partition, payload, session
                )
                self.write_stats.record(success, latency, bytes_sent)

                # Small random delay to vary load
                time.sleep(random.uniform(0, 0.01))

        finally:
            with self._counter_lock:
                self._active_writers -= 1

    def _reader_worker(self, reader_id: int, duration: float):
        """Worker function for a reader."""
        session = create_session()
        topic = self.mixed_config.topic
        partitions = self.mixed_config.num_partitions
        batch_size = self.mixed_config.read_batch_size
        end_time = time.time() + duration

        with self._counter_lock:
            self._active_readers += 1

        # Track offsets per partition
        offsets = {p: 0 for p in range(partitions)}

        try:
            while time.time() < end_time and not self.should_stop():
                partition = random.randint(0, partitions - 1)
                offset = offsets[partition]

                start = time.perf_counter()
                events, latency, next_offset = self.read_events(
                    topic, partition, offset, batch_size, session
                )
                self.read_stats.record(len(events) > 0, latency, len(events))

                if next_offset is not None:
                    offsets[partition] = next_offset

                # Small delay if no events found
                if not events:
                    time.sleep(0.05)

        finally:
            with self._counter_lock:
                self._active_readers -= 1

    def run(self) -> ScenarioResult:
        """Execute the mixed workload scenario."""
        print(f"\n=== MIXED WORKLOAD SCENARIO ===")
        print(f"Writers: {self.mixed_config.writer_count}")
        print(f"Readers: {self.mixed_config.reader_count}")
        print(f"Write ratio: {self.mixed_config.write_ratio * 100:.0f}%")
        print(f"Topic: {self.mixed_config.topic}")
        print(f"Duration: {self.config.duration_secs}s")
        print(f"Encoding: {self.config.encoding}")

        if not self.health_check():
            return self.create_result(
                success=False,
                start_time=datetime.now(),
                duration=0,
                error_messages=["Health check failed"],
            )

        start_time = datetime.now()
        start = time.time()

        # Start writer threads
        writer_threads = []
        for i in range(self.mixed_config.writer_count):
            t = threading.Thread(
                target=self._writer_worker,
                args=(i, self.config.duration_secs),
                daemon=True,
            )
            t.start()
            writer_threads.append(t)

        # Start reader threads
        reader_threads = []
        for i in range(self.mixed_config.reader_count):
            t = threading.Thread(
                target=self._reader_worker,
                args=(i, self.config.duration_secs),
                daemon=True,
            )
            t.start()
            reader_threads.append(t)

        # Monitor progress
        last_report = time.time()
        while time.time() - start < self.config.duration_secs:
            time.sleep(1)
            elapsed = time.time() - start

            if time.time() - last_report >= 5:
                write_summary = self.write_stats.summary()
                read_summary = self.read_stats.summary()

                # Record combined timeline
                self.stats.record(True, 0, 0)  # Just for timeline tracking
                self.record_timeline_point(
                    elapsed,
                    self._active_writers + self._active_readers
                )

                print(
                    f"  [{elapsed:.0f}s] "
                    f"Writers: {self._active_writers} ({write_summary['success']:,} events, "
                    f"{write_summary['errors']} errs) | "
                    f"Readers: {self._active_readers} ({read_summary['total']:,} reads, "
                    f"P95: {read_summary['p95_ms']:.1f}ms)"
                )
                last_report = time.time()

        # Wait for threads
        self.stop()
        for t in writer_threads + reader_threads:
            t.join(timeout=5)

        duration = time.time() - start

        # Check for deadlock (all threads should have finished)
        still_running = sum(1 for t in writer_threads + reader_threads if t.is_alive())
        deadlock_detected = still_running > 0

        # Build result
        write_summary = self.write_stats.summary()
        read_summary = self.read_stats.summary()

        total_ops = write_summary["success"] + read_summary["total"]
        actual_write_ratio = (
            write_summary["success"] / total_ops if total_ops > 0 else 0
        )

        details = {
            "writers": self.mixed_config.writer_count,
            "readers": self.mixed_config.reader_count,
            "target_write_ratio": self.mixed_config.write_ratio,
            "actual_write_ratio": actual_write_ratio,
            "write_stats": {
                "total": write_summary["success"],
                "errors": write_summary["errors"],
                "events_per_sec": write_summary["success"] / duration if duration > 0 else 0,
                "p95_ms": write_summary["p95_ms"],
            },
            "read_stats": {
                "total": read_summary["total"],
                "errors": read_summary["errors"],
                "reads_per_sec": read_summary["total"] / duration if duration > 0 else 0,
                "p95_ms": read_summary["p95_ms"],
            },
            "deadlock_detected": deadlock_detected,
        }

        success = (
            not deadlock_detected
            and write_summary["errors"] == 0
            and read_summary["errors"] < read_summary["total"] * 0.01  # <1% read errors ok
        )

        errors = []
        if deadlock_detected:
            errors.append(f"Deadlock detected: {still_running} threads still running")
        if write_summary["errors"] > 0:
            errors.append(f"Write errors: {write_summary['errors']}")

        print(f"\nMixed workload test {'PASSED' if success else 'FAILED'}")
        print(f"  Total writes: {write_summary['success']:,}")
        print(f"  Total reads: {read_summary['total']:,}")
        print(f"  Write P95: {write_summary['p95_ms']:.1f}ms")
        print(f"  Read P95: {read_summary['p95_ms']:.1f}ms")
        if deadlock_detected:
            print(f"  WARNING: Deadlock detected!")

        return self.create_result(
            success=success,
            start_time=start_time,
            duration=duration,
            details=details,
            error_messages=errors,
        )
