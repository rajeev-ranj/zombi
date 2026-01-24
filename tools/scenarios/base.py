"""
Base Scenario Class

Abstract base class for all scenario tests. Imports utilities from benchmark.py.
"""

import json
import sys
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# Add tools directory to path for imports
sys.path.insert(0, str(__file__).rsplit("/", 2)[0])

# Import reusable utilities from benchmark.py
from benchmark import (
    Stats,
    create_session,
    encode_proto_event,
    encode_varint,
    generate_payload,
)


class Encoding(Enum):
    """Supported event encoding formats."""
    JSON = "json"
    PROTO = "proto"


@dataclass
class ScenarioConfig:
    """Configuration for a scenario test."""
    # Connection
    url: str = "http://localhost:8080"

    # Encoding
    encoding: str = "proto"  # "proto" or "json"

    # S3/Cold Storage (optional)
    s3_bucket: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_region: str = "us-east-1"

    # Timing
    duration_secs: int = 60
    warmup_secs: int = 5

    # Common settings
    num_workers: int = 10
    payload_size: int = 100  # bytes

    def __post_init__(self):
        """Validate configuration."""
        if self.encoding not in ("proto", "json"):
            raise ValueError(f"Invalid encoding: {self.encoding}")
        self.url = self.url.rstrip("/")


@dataclass
class TimelineEntry:
    """A single point in the timeline of metrics."""
    timestamp: float
    elapsed_secs: float
    events_per_sec: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    errors: int
    active_workers: int = 0


@dataclass
class ScenarioResult:
    """Result of a scenario test."""
    scenario_name: str
    config: Dict[str, Any]
    success: bool
    start_time: str
    duration_secs: float

    # Aggregate stats
    total_events: int = 0
    total_errors: int = 0
    events_per_sec: float = 0.0
    bytes_total: int = 0

    # Latency percentiles
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0

    # Timeline for visualization
    timeline: List[Dict] = field(default_factory=list)

    # Scenario-specific data
    details: Dict[str, Any] = field(default_factory=dict)

    # Error messages
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "scenario_name": self.scenario_name,
            "config": self.config,
            "success": self.success,
            "start_time": self.start_time,
            "duration_secs": self.duration_secs,
            "total_events": self.total_events,
            "total_errors": self.total_errors,
            "events_per_sec": self.events_per_sec,
            "bytes_total": self.bytes_total,
            "latency": {
                "p50_ms": self.p50_ms,
                "p95_ms": self.p95_ms,
                "p99_ms": self.p99_ms,
                "min_ms": self.min_ms,
                "max_ms": self.max_ms,
            },
            "timeline": self.timeline,
            "details": self.details,
            "errors": self.errors,
        }


class BaseScenario(ABC):
    """
    Abstract base class for scenario tests.

    Subclasses must implement:
    - name: Scenario name
    - run(): Execute the scenario
    """

    def __init__(self, config: ScenarioConfig):
        self.config = config
        self.session = create_session()
        self.stats = Stats()
        self.timeline: List[TimelineEntry] = []
        self._stop_flag = threading.Event()
        self._timeline_lock = threading.Lock()

    @property
    @abstractmethod
    def name(self) -> str:
        """Return scenario name."""
        pass

    @abstractmethod
    def run(self) -> ScenarioResult:
        """Execute the scenario and return results."""
        pass

    def health_check(self) -> bool:
        """Check if Zombi is healthy."""
        try:
            r = self.session.get(f"{self.config.url}/health", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def get_server_stats(self) -> Dict:
        """Get current stats from Zombi server."""
        try:
            r = self.session.get(f"{self.config.url}/stats", timeout=5)
            return r.json() if r.status_code == 200 else {}
        except Exception:
            return {}

    def write_json(
        self,
        topic: str,
        partition: int,
        payload: dict,
        session=None,
    ) -> Tuple[bool, float, int]:
        """
        Write event using JSON encoding.

        Returns: (success, latency_ms, bytes_sent)
        """
        s = session or self.session
        data = {
            "topic": topic,
            "partition": partition,
            "payload": json.dumps(payload),
            "timestamp_ms": int(time.time() * 1000),
        }
        bytes_sent = len(json.dumps(data))
        start = time.perf_counter()
        try:
            r = s.post(
                f"{self.config.url}/tables/{topic}",
                json=data,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            latency = (time.perf_counter() - start) * 1000
            return r.status_code in (200, 201, 202), latency, bytes_sent
        except Exception:
            return False, (time.perf_counter() - start) * 1000, bytes_sent

    def write_proto(
        self,
        topic: str,
        partition: int,
        payload: bytes,
        session=None,
    ) -> Tuple[bool, float, int]:
        """
        Write event using protobuf encoding.

        Returns: (success, latency_ms, bytes_sent)
        """
        s = session or self.session
        proto_data = encode_proto_event(
            payload=payload,
            timestamp_ms=int(time.time() * 1000),
        )
        start = time.perf_counter()
        try:
            r = s.post(
                f"{self.config.url}/tables/{topic}",
                data=proto_data,
                headers={
                    "Content-Type": "application/x-protobuf",
                    "X-Partition": str(partition),
                },
                timeout=10,
            )
            latency = (time.perf_counter() - start) * 1000
            return r.status_code in (200, 201, 202), latency, len(proto_data)
        except Exception:
            return False, (time.perf_counter() - start) * 1000, len(proto_data)

    def write_event(
        self,
        topic: str,
        partition: int,
        payload_dict: dict,
        session=None,
    ) -> Tuple[bool, float, int]:
        """
        Write event using configured encoding.

        Returns: (success, latency_ms, bytes_sent)
        """
        if self.config.encoding == "proto":
            payload_bytes = json.dumps(payload_dict).encode("utf-8")
            return self.write_proto(topic, partition, payload_bytes, session)
        else:
            return self.write_json(topic, partition, payload_dict, session)

    def read_events(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        limit: int = 100,
        session=None,
        since: Optional[int] = None,
    ) -> Tuple[List[Dict], float, Optional[int]]:
        """
        Read events from a topic.

        Note: The API uses time-based reads (since parameter), not offset-based.
        The partition and offset parameters are kept for interface compatibility
        but the current API reads from all partitions sorted by timestamp.

        Returns: (events, latency_ms, last_timestamp for pagination)
        """
        s = session or self.session
        params = {"limit": limit}
        if since is not None:
            params["since"] = since
        start = time.perf_counter()
        try:
            r = s.get(
                f"{self.config.url}/tables/{topic}",
                params=params,
                timeout=30,
            )
            latency = (time.perf_counter() - start) * 1000
            if r.status_code == 200:
                data = r.json()
                # API returns "records", not "events"
                events = data.get("records", [])
                has_more = data.get("has_more", False)
                # Return the last timestamp for pagination (caller handles dedup)
                if events:
                    last_ts = events[-1].get("timestamp_ms", 0)
                    # Return last_ts (not +1) - caller will handle duplicates
                    return events, latency, last_ts if has_more else None
                return [], latency, None
            return [], latency, None
        except Exception:
            return [], (time.perf_counter() - start) * 1000, None

    def commit_offset(
        self,
        group: str,
        topic: str,
        partition: int,
        offset: int,
        session=None,
    ) -> Tuple[bool, float]:
        """
        Commit consumer offset.

        Returns: (success, latency_ms)
        """
        s = session or self.session
        data = {"topic": topic, "partition": partition, "offset": offset}
        start = time.perf_counter()
        try:
            r = s.post(
                f"{self.config.url}/consumers/{group}/commit",
                json=data,
                timeout=10,
            )
            latency = (time.perf_counter() - start) * 1000
            return r.status_code in (200, 201, 202, 204), latency
        except Exception:
            return False, (time.perf_counter() - start) * 1000

    def get_committed_offset(
        self,
        group: str,
        topic: str,
        partition: int,
        session=None,
    ) -> Tuple[Optional[int], float]:
        """
        Get committed offset for a consumer group.

        Returns: (offset, latency_ms)
        """
        s = session or self.session
        params = {"topic": topic, "partition": partition}
        start = time.perf_counter()
        try:
            r = s.get(
                f"{self.config.url}/consumers/{group}/offset",
                params=params,
                timeout=10,
            )
            latency = (time.perf_counter() - start) * 1000
            if r.status_code == 200:
                data = r.json()
                return data.get("offset"), latency
            return None, latency
        except Exception:
            return None, (time.perf_counter() - start) * 1000

    def record_timeline_point(self, elapsed_secs: float, active_workers: int = 0):
        """Record a point in the timeline for metrics visualization."""
        with self._timeline_lock:
            summary = self.stats.summary()

            # Calculate events per second over last period
            if self.timeline:
                last = self.timeline[-1]
                time_delta = elapsed_secs - last.elapsed_secs
                events_delta = summary["success"] - (last.events_per_sec * last.elapsed_secs if len(self.timeline) > 1 else 0)
                events_per_sec = events_delta / time_delta if time_delta > 0 else 0
            else:
                events_per_sec = summary["success"] / elapsed_secs if elapsed_secs > 0 else 0

            entry = TimelineEntry(
                timestamp=time.time(),
                elapsed_secs=elapsed_secs,
                events_per_sec=events_per_sec,
                p50_ms=summary["p50_ms"],
                p95_ms=summary["p95_ms"],
                p99_ms=summary["p99_ms"],
                errors=summary["errors"],
                active_workers=active_workers,
            )
            self.timeline.append(entry)

    def create_result(
        self,
        success: bool,
        start_time: datetime,
        duration: float,
        details: Optional[Dict] = None,
        error_messages: Optional[List[str]] = None,
    ) -> ScenarioResult:
        """Create a ScenarioResult from collected stats."""
        summary = self.stats.summary()

        return ScenarioResult(
            scenario_name=self.name,
            config={
                "url": self.config.url,
                "encoding": self.config.encoding,
                "duration_secs": self.config.duration_secs,
                "num_workers": self.config.num_workers,
                "payload_size": self.config.payload_size,
            },
            success=success,
            start_time=start_time.isoformat(),
            duration_secs=duration,
            total_events=summary["success"],
            total_errors=summary["errors"],
            events_per_sec=summary["success"] / duration if duration > 0 else 0,
            bytes_total=summary["bytes"],
            p50_ms=summary["p50_ms"],
            p95_ms=summary["p95_ms"],
            p99_ms=summary["p99_ms"],
            min_ms=summary["min_ms"],
            max_ms=summary["max_ms"],
            timeline=[
                {
                    "elapsed_secs": e.elapsed_secs,
                    "events_per_sec": e.events_per_sec,
                    "p95_ms": e.p95_ms,
                    "errors": e.errors,
                }
                for e in self.timeline
            ],
            details=details or {},
            errors=error_messages or [],
        )

    def print_progress(self, elapsed: float, total: float):
        """Print progress during scenario execution."""
        summary = self.stats.summary()
        pct = (elapsed / total) * 100 if total > 0 else 0
        rate = summary["success"] / elapsed if elapsed > 0 else 0
        print(
            f"  [{elapsed:.0f}s/{total:.0f}s] {pct:.0f}% | "
            f"Rate: {rate:.0f}/s | "
            f"Total: {summary['success']:,} | "
            f"Errors: {summary['errors']} | "
            f"P95: {summary['p95_ms']:.1f}ms"
        )

    def stop(self):
        """Signal scenario to stop."""
        self._stop_flag.set()

    def should_stop(self) -> bool:
        """Check if scenario should stop."""
        return self._stop_flag.is_set()
