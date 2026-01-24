"""
Consumer Scenario

Tests consumer functionality with offset tracking and order verification.
"""

import json
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set

from .base import BaseScenario, ScenarioConfig, ScenarioResult, Stats, create_session, generate_payload


@dataclass
class ConsumerConfig(ScenarioConfig):
    """Configuration for consumer scenario."""
    num_consumers: int = 5
    consumer_group: str = "test-group"
    topic: str = "consumer-test"
    num_partitions: int = 4
    commit_interval_secs: float = 5.0
    verify_ordering: bool = True
    # Pre-populate data
    seed_events: int = 10000


class ConsumerScenario(BaseScenario):
    """
    Consumer scenario that tests:
    - Reading events from offset
    - Committing offsets
    - Verifying event ordering (INV-3)
    - Consumer group coordination
    """

    def __init__(self, config: ConsumerConfig):
        super().__init__(config)
        self.consumer_config = config
        self.write_stats = Stats()
        self.read_stats = Stats()
        self._ordering_errors: List[str] = []
        self._ordering_lock = threading.Lock()
        self._committed_offsets: Dict[int, int] = {}
        self._offset_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "consumer"

    def _seed_data(self) -> bool:
        """Seed the topic with test data."""
        print(f"Seeding {self.consumer_config.seed_events} events...")

        session = create_session()
        events_per_partition = self.consumer_config.seed_events // self.consumer_config.num_partitions

        for partition in range(self.consumer_config.num_partitions):
            for i in range(events_per_partition):
                payload = {
                    "sequence_marker": f"p{partition}-{i}",
                    "partition": partition,
                    "index": i,
                    "timestamp": time.time(),
                }
                success, latency, bytes_sent = self.write_event(
                    self.consumer_config.topic,
                    partition,
                    payload,
                    session,
                )
                self.write_stats.record(success, latency, bytes_sent)

                if not success:
                    print(f"  Warning: Failed to write event p{partition}-{i}")

            if partition % 2 == 1:
                print(f"  Seeded partitions 0-{partition}")

        summary = self.write_stats.summary()
        print(f"Seeded {summary['success']} events ({summary['errors']} errors)")
        return summary["errors"] == 0

    def _consumer_worker(
        self,
        consumer_id: int,
        partition: int,
        duration: float,
    ):
        """Worker function for a single consumer."""
        session = create_session()
        topic = self.consumer_config.topic
        group = self.consumer_config.consumer_group
        end_time = time.time() + duration

        # Get starting offset (committed or 0)
        offset, _ = self.get_committed_offset(group, topic, partition, session)
        current_offset = offset if offset is not None else 0

        last_commit = time.time()
        events_read = 0
        last_sequence: Optional[int] = None

        while time.time() < end_time and not self.should_stop():
            # Read batch of events
            events, latency, next_offset = self.read_events(
                topic, partition, current_offset, limit=100, session=session
            )
            self.read_stats.record(len(events) > 0, latency, 0)

            if events:
                events_read += len(events)

                # Verify ordering
                if self.consumer_config.verify_ordering:
                    for event in events:
                        seq = event.get("sequence")
                        if seq is not None:
                            if last_sequence is not None and seq <= last_sequence:
                                with self._ordering_lock:
                                    self._ordering_errors.append(
                                        f"Consumer {consumer_id}: Out of order - "
                                        f"got {seq} after {last_sequence}"
                                    )
                            last_sequence = seq

                # Update offset
                if next_offset is not None:
                    current_offset = next_offset

            # Commit offset periodically
            if time.time() - last_commit >= self.consumer_config.commit_interval_secs:
                success, _ = self.commit_offset(
                    group, topic, partition, current_offset, session
                )
                if success:
                    with self._offset_lock:
                        self._committed_offsets[partition] = current_offset
                last_commit = time.time()

            # Small delay if no events
            if not events:
                time.sleep(0.1)

        # Final commit
        self.commit_offset(group, topic, partition, current_offset, session)
        with self._offset_lock:
            self._committed_offsets[partition] = current_offset

    def run(self) -> ScenarioResult:
        """Execute the consumer scenario."""
        print(f"\n=== CONSUMER SCENARIO ===")
        print(f"Consumers: {self.consumer_config.num_consumers}")
        print(f"Consumer group: {self.consumer_config.consumer_group}")
        print(f"Topic: {self.consumer_config.topic}")
        print(f"Partitions: {self.consumer_config.num_partitions}")
        print(f"Duration: {self.config.duration_secs}s")

        if not self.health_check():
            return self.create_result(
                success=False,
                start_time=datetime.now(),
                duration=0,
                error_messages=["Health check failed"],
            )

        start_time = datetime.now()

        # Phase 1: Seed data
        if not self._seed_data():
            return self.create_result(
                success=False,
                start_time=start_time,
                duration=time.time() - start_time.timestamp(),
                error_messages=["Failed to seed data"],
            )

        # Phase 2: Run consumers
        print(f"\nRunning consumers for {self.config.duration_secs}s...")
        start = time.time()

        threads = []
        for i in range(self.consumer_config.num_consumers):
            # Assign partition (round-robin)
            partition = i % self.consumer_config.num_partitions
            t = threading.Thread(
                target=self._consumer_worker,
                args=(i, partition, self.config.duration_secs),
                daemon=True,
            )
            t.start()
            threads.append(t)

        # Monitor progress
        last_report = time.time()
        while time.time() - start < self.config.duration_secs:
            time.sleep(1)
            elapsed = time.time() - start

            if time.time() - last_report >= 5:
                summary = self.read_stats.summary()
                print(
                    f"  [{elapsed:.0f}s] Reads: {summary['total']} | "
                    f"P95: {summary['p95_ms']:.1f}ms | "
                    f"Ordering errors: {len(self._ordering_errors)}"
                )
                last_report = time.time()

        # Wait for threads
        self.stop()
        for t in threads:
            t.join(timeout=5)

        duration = time.time() - start

        # Verify committed offsets
        print("\nVerifying committed offsets...")
        offset_verified = True
        for partition, expected_offset in self._committed_offsets.items():
            actual, _ = self.get_committed_offset(
                self.consumer_config.consumer_group,
                self.consumer_config.topic,
                partition,
            )
            if actual != expected_offset:
                print(f"  Partition {partition}: expected {expected_offset}, got {actual}")
                offset_verified = False
            else:
                print(f"  Partition {partition}: offset {actual} verified")

        # Build result
        read_summary = self.read_stats.summary()
        write_summary = self.write_stats.summary()

        details = {
            "seed_events": write_summary["success"],
            "read_operations": read_summary["total"],
            "committed_offsets": self._committed_offsets,
            "ordering_errors": len(self._ordering_errors),
            "offset_verified": offset_verified,
        }

        success = (
            len(self._ordering_errors) == 0
            and offset_verified
            and read_summary["errors"] == 0
        )

        errors = self._ordering_errors.copy()
        if not offset_verified:
            errors.append("Offset verification failed")

        print(f"\nConsumer test {'PASSED' if success else 'FAILED'}")
        print(f"  Ordering errors: {len(self._ordering_errors)}")
        print(f"  Offset verified: {offset_verified}")

        return self.create_result(
            success=success,
            start_time=start_time,
            duration=duration + (time.time() - start),  # Include seed time
            details=details,
            error_messages=errors,
        )
