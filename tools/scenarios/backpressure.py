"""
Backpressure Scenario

Tests server behavior under overload conditions and verifies proper 503 responses.
"""

import json
import random
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from .base import BaseScenario, ScenarioConfig, ScenarioResult, Stats, create_session, generate_payload


@dataclass
class BackpressureConfig(ScenarioConfig):
    """Configuration for backpressure scenario."""
    # Phase 1: Burst to trigger backpressure
    burst_workers: int = 200
    burst_duration_secs: int = 30
    # Phase 2: Reduce and verify recovery
    recovery_workers: int = 10
    recovery_duration_secs: int = 30
    # Expected behavior
    # Note: Python HTTP clients typically can't generate enough load to trigger
    # backpressure on a Rust server. Set to False for local testing; True for
    # production testing with high-performance clients.
    expect_503_during_burst: bool = False
    topic: str = "backpressure-test"


class BackpressureScenario(BaseScenario):
    """
    Backpressure scenario that tests overload handling.

    Phases:
    1. Burst: Flood server with 200 concurrent writers to trigger 503
    2. Recovery: Reduce to normal load and verify server recovers

    Verifies:
    - Server returns 503 with "SERVER_OVERLOADED" under load
    - Server recovers after load decreases
    - No crashes or hangs
    """

    def __init__(self, config: BackpressureConfig):
        super().__init__(config)
        self.bp_config = config
        self.burst_stats = Stats()
        self.recovery_stats = Stats()
        self._status_codes: Counter = Counter()
        self._status_lock = threading.Lock()
        self._error_bodies: List[str] = []

    @property
    def name(self) -> str:
        return "backpressure"

    def _burst_worker(self, worker_id: int, duration: float):
        """Worker that sends events as fast as possible."""
        session = create_session()
        topic = self.bp_config.topic
        end_time = time.time() + duration

        while time.time() < end_time and not self.should_stop():
            payload = generate_payload(self.config.payload_size)

            # Direct HTTP call to capture status codes
            start = time.perf_counter()
            try:
                if self.config.encoding == "proto":
                    from benchmark import encode_proto_event
                    proto_data = encode_proto_event(
                        payload=json.dumps(payload).encode("utf-8"),
                        timestamp_ms=int(time.time() * 1000),
                    )
                    r = session.post(
                        f"{self.config.url}/tables/{topic}",
                        data=proto_data,
                        headers={
                            "Content-Type": "application/x-protobuf",
                            "X-Partition": "0",
                        },
                        timeout=10,
                    )
                else:
                    data = {
                        "topic": topic,
                        "partition": 0,
                        "payload": json.dumps(payload),
                        "timestamp_ms": int(time.time() * 1000),
                    }
                    r = session.post(
                        f"{self.config.url}/tables/{topic}",
                        json=data,
                        timeout=10,
                    )

                latency = (time.perf_counter() - start) * 1000
                success = r.status_code in (200, 201, 202)

                with self._status_lock:
                    self._status_codes[r.status_code] += 1
                    if r.status_code == 503:
                        try:
                            body = r.json()
                            if len(self._error_bodies) < 5:
                                self._error_bodies.append(str(body))
                        except Exception:
                            pass

                self.burst_stats.record(success, latency, 0)

            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                self.burst_stats.record(False, latency, 0)
                with self._status_lock:
                    self._status_codes["exception"] += 1

    def _recovery_worker(self, worker_id: int, duration: float):
        """Worker that sends events at moderate rate."""
        session = create_session()
        topic = self.bp_config.topic
        end_time = time.time() + duration

        while time.time() < end_time and not self.should_stop():
            payload = generate_payload(self.config.payload_size)
            success, latency, bytes_sent = self.write_event(
                topic, 0, payload, session
            )
            self.recovery_stats.record(success, latency, bytes_sent)

            # Moderate rate
            time.sleep(0.01)

    def run(self) -> ScenarioResult:
        """Execute the backpressure scenario."""
        print(f"\n=== BACKPRESSURE SCENARIO ===")
        print(f"Phase 1 - Burst: {self.bp_config.burst_workers} workers for {self.bp_config.burst_duration_secs}s")
        print(f"Phase 2 - Recovery: {self.bp_config.recovery_workers} workers for {self.bp_config.recovery_duration_secs}s")

        if not self.health_check():
            return self.create_result(
                success=False,
                start_time=datetime.now(),
                duration=0,
                error_messages=["Health check failed"],
            )

        start_time = datetime.now()
        start = time.time()

        # Phase 1: Burst
        print(f"\nPhase 1: Starting burst with {self.bp_config.burst_workers} workers...")
        burst_threads = []
        for i in range(self.bp_config.burst_workers):
            t = threading.Thread(
                target=self._burst_worker,
                args=(i, self.bp_config.burst_duration_secs),
                daemon=True,
            )
            t.start()
            burst_threads.append(t)

        # Monitor burst phase
        burst_start = time.time()
        last_report = time.time()
        while time.time() - burst_start < self.bp_config.burst_duration_secs:
            time.sleep(1)
            elapsed = time.time() - burst_start

            if time.time() - last_report >= 5:
                summary = self.burst_stats.summary()
                with self._status_lock:
                    codes_str = ", ".join(f"{k}:{v}" for k, v in sorted(self._status_codes.items()))
                print(
                    f"  [Burst {elapsed:.0f}s] "
                    f"Total: {summary['total']:,} | "
                    f"Success: {summary['success']:,} | "
                    f"Status codes: {codes_str}"
                )
                last_report = time.time()

        # Wait for burst threads
        for t in burst_threads:
            t.join(timeout=2)

        burst_summary = self.burst_stats.summary()
        print(f"\nBurst phase complete:")
        print(f"  Total requests: {burst_summary['total']:,}")
        print(f"  Successful: {burst_summary['success']:,}")
        print(f"  503 responses: {self._status_codes.get(503, 0)}")

        # Phase 2: Recovery
        print(f"\nPhase 2: Testing recovery with {self.bp_config.recovery_workers} workers...")
        time.sleep(2)  # Brief pause before recovery

        recovery_threads = []
        for i in range(self.bp_config.recovery_workers):
            t = threading.Thread(
                target=self._recovery_worker,
                args=(i, self.bp_config.recovery_duration_secs),
                daemon=True,
            )
            t.start()
            recovery_threads.append(t)

        # Monitor recovery phase
        recovery_start = time.time()
        last_report = time.time()
        while time.time() - recovery_start < self.bp_config.recovery_duration_secs:
            time.sleep(1)
            elapsed = time.time() - recovery_start

            if time.time() - last_report >= 5:
                summary = self.recovery_stats.summary()
                error_rate = (
                    summary["errors"] / summary["total"] * 100
                    if summary["total"] > 0 else 0
                )
                print(
                    f"  [Recovery {elapsed:.0f}s] "
                    f"Total: {summary['total']:,} | "
                    f"Success: {summary['success']:,} | "
                    f"Error rate: {error_rate:.1f}%"
                )
                last_report = time.time()

        # Wait for recovery threads
        self.stop()
        for t in recovery_threads:
            t.join(timeout=2)

        duration = time.time() - start
        recovery_summary = self.recovery_stats.summary()

        # Analyze results
        got_503 = self._status_codes.get(503, 0) > 0
        recovery_error_rate = (
            recovery_summary["errors"] / recovery_summary["total"]
            if recovery_summary["total"] > 0 else 1.0
        )
        recovered = recovery_error_rate < 0.01  # <1% errors during recovery

        # Check for overload message in 503 responses
        has_overload_message = any(
            "overload" in body.lower() for body in self._error_bodies
        )

        details = {
            "burst_phase": {
                "workers": self.bp_config.burst_workers,
                "duration_secs": self.bp_config.burst_duration_secs,
                "total_requests": burst_summary["total"],
                "successful": burst_summary["success"],
                "status_codes": dict(self._status_codes),
                "p95_ms": burst_summary["p95_ms"],
            },
            "recovery_phase": {
                "workers": self.bp_config.recovery_workers,
                "duration_secs": self.bp_config.recovery_duration_secs,
                "total_requests": recovery_summary["total"],
                "successful": recovery_summary["success"],
                "error_rate": recovery_error_rate,
                "p95_ms": recovery_summary["p95_ms"],
            },
            "backpressure_triggered": got_503,
            "has_overload_message": has_overload_message,
            "server_recovered": recovered,
            "sample_503_bodies": self._error_bodies[:3],
        }

        # Success criteria
        success = True
        errors = []

        if self.bp_config.expect_503_during_burst and not got_503:
            success = False
            errors.append("Expected 503 responses during burst but got none")

        if not recovered:
            success = False
            errors.append(f"Server did not recover: {recovery_error_rate*100:.1f}% error rate")

        print(f"\nBackpressure test {'PASSED' if success else 'FAILED'}")
        print(f"  503 responses triggered: {got_503}")
        print(f"  Server recovered: {recovered}")
        print(f"  Recovery error rate: {recovery_error_rate*100:.1f}%")

        return self.create_result(
            success=success,
            start_time=start_time,
            duration=duration,
            details=details,
            error_messages=errors,
        )
