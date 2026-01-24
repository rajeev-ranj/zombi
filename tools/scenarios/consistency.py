"""
Data Consistency Scenario

Verifies data consistency invariants INV-2 (no data loss) and INV-3 (order preserved).
"""

import json
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from .base import BaseScenario, ScenarioConfig, ScenarioResult, Stats, create_session, generate_payload


@dataclass
class ConsistencyConfig(ScenarioConfig):
    """Configuration for consistency scenario."""
    num_events: int = 10000
    num_partitions: int = 4
    topic: str = "consistency-test"
    verify_timeout_secs: int = 60
    # Use unique markers for verification
    test_id: str = ""

    def __post_init__(self):
        super().__post_init__()
        if not self.test_id:
            self.test_id = str(uuid.uuid4())[:8]


class ConsistencyScenario(BaseScenario):
    """
    Data consistency scenario that verifies Zombi invariants.

    Invariants tested:
    - INV-2: No data loss after ACK - all acknowledged writes must be readable
    - INV-3: Order preserved within partition - events read in write order

    Approach:
    1. Write events with unique sequence markers per partition
    2. Track all acknowledged writes
    3. Read back all events
    4. Verify all acknowledged events are present and ordered
    """

    def __init__(self, config: ConsistencyConfig):
        super().__init__(config)
        self.cons_config = config
        self.write_stats = Stats()
        self.read_stats = Stats()

        # Track writes per partition: partition -> list of (marker, server_offset)
        self._acknowledged_writes: Dict[int, List[Tuple[str, Optional[int]]]] = defaultdict(list)
        self._write_lock = threading.Lock()

        # Track reads per partition: partition -> list of markers in order read
        self._read_events: Dict[int, List[str]] = defaultdict(list)
        self._read_lock = threading.Lock()

        # Track write start timestamp for time-based reads
        self._write_start_timestamp: int = 0

    @property
    def name(self) -> str:
        return "consistency"

    def _write_phase(self) -> Dict:
        """Write events with sequence markers."""
        session = create_session()
        num_events = self.cons_config.num_events
        partitions = self.cons_config.num_partitions
        topic = self.cons_config.topic
        test_id = self.cons_config.test_id

        # Track start timestamp for time-based reads
        self._write_start_timestamp = int(time.time() * 1000)

        print(f"Writing {num_events:,} events with test_id={test_id}...")

        events_per_partition = num_events // partitions

        for partition in range(partitions):
            for i in range(events_per_partition):
                marker = f"{test_id}-p{partition}-{i}"
                payload = {
                    "test_id": test_id,
                    "marker": marker,
                    "partition": partition,
                    "index": i,
                    "timestamp": time.time(),
                }

                # Direct write to capture offset
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
                                "X-Partition": str(partition),
                            },
                            timeout=10,
                        )
                    else:
                        data = {
                            "topic": topic,
                            "partition": partition,
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

                    if success:
                        try:
                            resp = r.json()
                            offset = resp.get("offset")
                        except Exception:
                            offset = None

                        with self._write_lock:
                            self._acknowledged_writes[partition].append((marker, offset))

                    self.write_stats.record(success, latency, 0)

                except Exception as e:
                    latency = (time.perf_counter() - start) * 1000
                    self.write_stats.record(False, latency, 0)

            print(f"  Partition {partition}: {len(self._acknowledged_writes[partition])} events written")

        summary = self.write_stats.summary()
        return {
            "total_acknowledged": sum(len(v) for v in self._acknowledged_writes.values()),
            "errors": summary["errors"],
            "p95_ms": summary["p95_ms"],
        }

    def _read_phase(self) -> Dict:
        """Read back all events using large batch reads.

        Note: The API reads from all partitions merged by timestamp.
        Due to timestamp-based pagination limitations with duplicate timestamps,
        we use a simple approach: read large batches and collect all events.
        """
        session = create_session()
        topic = self.cons_config.topic
        test_id = self.cons_config.test_id
        timeout = self.cons_config.verify_timeout_secs

        print(f"Reading events back (timeout={timeout}s)...")

        # Track total events found for this test
        total_expected = sum(len(v) for v in self._acknowledged_writes.values())
        read_start = time.time()
        events_found = 0
        seen_markers: Set[str] = set()

        # Read in large batches using time-based pagination
        # Use write start timestamp as the starting point
        since_ts = self._write_start_timestamp
        batch_size = 5000  # Large batch to minimize pagination issues
        no_progress_iterations = 0

        while time.time() - read_start < timeout:
            events, latency, last_ts = self.read_events(
                topic, limit=batch_size, session=session, since=since_ts
            )
            self.read_stats.record(len(events) > 0, latency, 0)

            if not events:
                if events_found >= total_expected:
                    break
                no_progress_iterations += 1
                if no_progress_iterations > 5:
                    break
                time.sleep(0.5)
                continue

            found_new_this_batch = 0
            for event in events:
                payload_str = event.get("payload", "")
                try:
                    if isinstance(payload_str, str):
                        payload = json.loads(payload_str)
                    else:
                        payload = payload_str

                    if payload.get("test_id") == test_id:
                        marker = payload.get("marker")
                        partition = payload.get("partition", 0)
                        if marker and marker not in seen_markers:
                            seen_markers.add(marker)
                            with self._read_lock:
                                self._read_events[partition].append(marker)
                            events_found += 1
                            found_new_this_batch += 1
                except Exception:
                    pass

            if found_new_this_batch > 0:
                no_progress_iterations = 0
                print(f"  Progress: {events_found:,}/{total_expected:,} events found (+{found_new_this_batch})")
            else:
                no_progress_iterations += 1

            # Move to next page
            if last_ts is not None:
                # If we found new events, stay at this timestamp to catch any more
                # If no new events, advance past it
                if found_new_this_batch > 0:
                    since_ts = last_ts
                else:
                    since_ts = last_ts + 1

            if no_progress_iterations > 10:
                break

            if events_found >= total_expected:
                break

        # Print partition summary
        for partition in range(self.cons_config.num_partitions):
            found = len(self._read_events.get(partition, []))
            print(f"  Partition {partition}: read {found} events")

        summary = self.read_stats.summary()
        return {
            "total_read": sum(len(v) for v in self._read_events.values()),
            "reads": summary["total"],
            "p95_ms": summary["p95_ms"],
        }

    def _verify_consistency(self) -> Dict:
        """Verify INV-2 (no data loss) and INV-3 (order preserved)."""
        print("Verifying consistency invariants...")

        inv2_errors = []  # No data loss
        inv3_errors = []  # Order preserved

        total_acknowledged = 0
        total_found = 0
        total_missing = 0
        total_order_violations = 0

        for partition in range(self.cons_config.num_partitions):
            acknowledged = self._acknowledged_writes.get(partition, [])
            read_markers = self._read_events.get(partition, [])

            total_acknowledged += len(acknowledged)

            # INV-2: Check all acknowledged writes are present
            acknowledged_markers = set(m for m, _ in acknowledged)
            read_marker_set = set(read_markers)

            missing = acknowledged_markers - read_marker_set
            total_missing += len(missing)
            total_found += len(acknowledged_markers & read_marker_set)

            if missing:
                inv2_errors.append(
                    f"Partition {partition}: {len(missing)} missing events "
                    f"(first 5: {list(missing)[:5]})"
                )

            # INV-3: Check ordering within partition
            # Events should be read in the same order they were written
            acknowledged_order = [m for m, _ in acknowledged]

            # Build mapping of marker -> expected position
            expected_positions = {m: i for i, m in enumerate(acknowledged_order)}

            # Check that markers appear in increasing expected position
            last_expected_pos = -1
            for marker in read_markers:
                if marker in expected_positions:
                    expected_pos = expected_positions[marker]
                    if expected_pos < last_expected_pos:
                        total_order_violations += 1
                        if len(inv3_errors) < 10:
                            inv3_errors.append(
                                f"Partition {partition}: Order violation - "
                                f"{marker} (pos {expected_pos}) after pos {last_expected_pos}"
                            )
                    last_expected_pos = max(last_expected_pos, expected_pos)

            print(f"  Partition {partition}: "
                  f"acked={len(acknowledged)}, found={len(read_marker_set & acknowledged_markers)}, "
                  f"missing={len(missing)}, order_violations={total_order_violations}")

        return {
            "inv2_no_data_loss": {
                "pass": len(inv2_errors) == 0,
                "total_acknowledged": total_acknowledged,
                "total_found": total_found,
                "total_missing": total_missing,
                "errors": inv2_errors,
            },
            "inv3_order_preserved": {
                "pass": len(inv3_errors) == 0,
                "total_violations": total_order_violations,
                "errors": inv3_errors,
            },
        }

    def run(self) -> ScenarioResult:
        """Execute the consistency scenario."""
        print(f"\n=== DATA CONSISTENCY SCENARIO ===")
        print(f"Test ID: {self.cons_config.test_id}")
        print(f"Events: {self.cons_config.num_events:,}")
        print(f"Partitions: {self.cons_config.num_partitions}")
        print(f"Topic: {self.cons_config.topic}")

        if not self.health_check():
            return self.create_result(
                success=False,
                start_time=datetime.now(),
                duration=0,
                error_messages=["Health check failed"],
            )

        start_time = datetime.now()
        start = time.time()

        # Phase 1: Write
        print("\nPhase 1: Write")
        write_result = self._write_phase()

        # Phase 2: Read
        print("\nPhase 2: Read")
        read_result = self._read_phase()

        # Phase 3: Verify
        print("\nPhase 3: Verify")
        verify_result = self._verify_consistency()

        duration = time.time() - start

        # Build result
        inv2_pass = verify_result["inv2_no_data_loss"]["pass"]
        inv3_pass = verify_result["inv3_order_preserved"]["pass"]
        success = inv2_pass and inv3_pass

        details = {
            "test_id": self.cons_config.test_id,
            "write_phase": write_result,
            "read_phase": read_result,
            "verification": verify_result,
        }

        errors = []
        if not inv2_pass:
            errors.append("INV-2 FAILED: Data loss detected")
            errors.extend(verify_result["inv2_no_data_loss"]["errors"][:5])
        if not inv3_pass:
            errors.append("INV-3 FAILED: Order violations detected")
            errors.extend(verify_result["inv3_order_preserved"]["errors"][:5])

        # Print summary
        print(f"\nConsistency test {'PASSED' if success else 'FAILED'}")
        print(f"  INV-2 (No data loss): {'PASS' if inv2_pass else 'FAIL'}")
        print(f"    Acknowledged: {verify_result['inv2_no_data_loss']['total_acknowledged']}")
        print(f"    Found: {verify_result['inv2_no_data_loss']['total_found']}")
        print(f"    Missing: {verify_result['inv2_no_data_loss']['total_missing']}")
        print(f"  INV-3 (Order preserved): {'PASS' if inv3_pass else 'FAIL'}")
        print(f"    Violations: {verify_result['inv3_order_preserved']['total_violations']}")

        return self.create_result(
            success=success,
            start_time=start_time,
            duration=duration,
            details=details,
            error_messages=errors,
        )
