"""
Multi-Producer Scenario

Tests multiple producers writing to multiple topics and partitions concurrently.
"""

import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from .base import BaseScenario, ScenarioConfig, ScenarioResult, create_session, generate_payload


@dataclass
class ProducerConfig(ScenarioConfig):
    """Configuration for multi-producer scenario."""
    num_producers: int = 10
    topics: List[str] = None
    partitions_per_topic: int = 4
    events_per_second: int = 100  # Per producer

    def __post_init__(self):
        super().__post_init__()
        if self.topics is None:
            self.topics = ["events", "orders", "metrics"]


class MultiProducerScenario(BaseScenario):
    """
    Multi-producer scenario that simulates realistic production load.

    Features:
    - Multiple producers writing concurrently
    - Multiple topics with multiple partitions
    - Configurable rate per producer
    - Rate limiting to target events/second
    """

    def __init__(self, config: ProducerConfig):
        super().__init__(config)
        self.producer_config = config
        self._active_producers = 0
        self._producer_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "multi-producer"

    def _producer_worker(
        self,
        producer_id: int,
        target_rate: float,
        duration: float,
    ):
        """Worker function for a single producer."""
        session = create_session()
        topics = self.producer_config.topics
        partitions = self.producer_config.partitions_per_topic
        end_time = time.time() + duration

        # Track rate limiting
        events_sent = 0
        start_time = time.time()

        with self._producer_lock:
            self._active_producers += 1

        try:
            while time.time() < end_time and not self.should_stop():
                # Select random topic and partition
                topic = random.choice(topics)
                partition = random.randint(0, partitions - 1)

                # Generate payload
                payload = generate_payload(self.config.payload_size)
                payload["producer_id"] = producer_id

                # Write event
                success, latency, bytes_sent = self.write_event(
                    topic, partition, payload, session
                )
                self.stats.record(success, latency, bytes_sent)

                events_sent += 1

                # Rate limiting
                if target_rate > 0:
                    elapsed = time.time() - start_time
                    expected_events = elapsed * target_rate
                    if events_sent > expected_events:
                        sleep_time = (events_sent - expected_events) / target_rate
                        if sleep_time > 0.001:  # Only sleep if > 1ms
                            time.sleep(sleep_time)

        finally:
            with self._producer_lock:
                self._active_producers -= 1

    def run(self) -> ScenarioResult:
        """Execute the multi-producer scenario."""
        print(f"\n=== MULTI-PRODUCER SCENARIO ===")
        print(f"Producers: {self.producer_config.num_producers}")
        print(f"Topics: {self.producer_config.topics}")
        print(f"Partitions/topic: {self.producer_config.partitions_per_topic}")
        print(f"Target rate/producer: {self.producer_config.events_per_second}/s")
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

        # Start producer threads
        threads = []
        for i in range(self.producer_config.num_producers):
            t = threading.Thread(
                target=self._producer_worker,
                args=(
                    i,
                    self.producer_config.events_per_second,
                    self.config.duration_secs,
                ),
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
                self.record_timeline_point(elapsed, self._active_producers)
                self.print_progress(elapsed, self.config.duration_secs)
                last_report = time.time()

        # Wait for threads to finish
        self.stop()
        for t in threads:
            t.join(timeout=5)

        duration = time.time() - start

        # Calculate per-topic stats
        summary = self.stats.summary()
        target_total = (
            self.producer_config.num_producers
            * self.producer_config.events_per_second
            * self.config.duration_secs
        )
        achievement_pct = (summary["success"] / target_total * 100) if target_total > 0 else 0

        details = {
            "num_producers": self.producer_config.num_producers,
            "topics": self.producer_config.topics,
            "partitions_per_topic": self.producer_config.partitions_per_topic,
            "target_events_per_producer": self.producer_config.events_per_second,
            "target_total": target_total,
            "achieved_total": summary["success"],
            "achievement_pct": achievement_pct,
        }

        print(f"\nCompleted: {summary['success']:,} events "
              f"({achievement_pct:.1f}% of target)")
        print(f"Throughput: {summary['success'] / duration:.0f} events/sec")

        return self.create_result(
            success=summary["errors"] == 0 and achievement_pct > 90,
            start_time=start_time,
            duration=duration,
            details=details,
        )
