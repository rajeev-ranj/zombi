"""
Base runner class for zombi-load scenarios.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from output import (
    ScenarioOutput,
    ScenarioMetrics,
    ThroughputMetrics,
    LatencyMetrics,
    CpuMetrics,
)


@dataclass
class RunnerConfig:
    """Configuration for a runner."""
    url: str = "http://localhost:8080"
    encoding: str = "proto"
    duration_secs: int = 60
    warmup_secs: int = 5
    num_workers: int = 10
    payload_size: int = 100

    # Cold storage settings
    s3_bucket: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_region: str = "us-east-1"

    # Peak testing settings
    concurrency_levels: List[int] = field(default_factory=lambda: [50, 100, 200, 500])
    batch_size: int = 100

    def __post_init__(self):
        self.url = self.url.rstrip("/")


class BaseRunner(ABC):
    """Base class for all runners."""

    def __init__(self, config: RunnerConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self, pool_size: int = 100) -> requests.Session:
        """Create HTTP session with connection pooling and retries."""
        session = requests.Session()
        retry = Retry(total=2, backoff_factor=0.1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=pool_size,
            pool_maxsize=pool_size
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the runner/scenario name."""
        pass

    @abstractmethod
    def run(self) -> ScenarioOutput:
        """Execute the runner and return results."""
        pass

    def health_check(self, timeout: int = 5) -> bool:
        """Check if Zombi is healthy."""
        try:
            r = self.session.get(f"{self.config.url}/health", timeout=timeout)
            return r.status_code == 200
        except Exception:
            return False

    def get_server_stats(self) -> Dict[str, Any]:
        """Get current stats from Zombi server."""
        try:
            r = self.session.get(f"{self.config.url}/stats", timeout=5)
            return r.json() if r.status_code == 200 else {}
        except Exception:
            return {}

    @staticmethod
    def create_output(
        name: str,
        success: bool,
        duration_secs: float,
        events_per_sec: float = 0.0,
        bytes_total: int = 0,
        p50_ms: float = 0.0,
        p95_ms: float = 0.0,
        p99_ms: float = 0.0,
        min_ms: float = 0.0,
        max_ms: float = 0.0,
        cpu_avg: float = 0.0,
        cpu_peak: float = 0.0,
        errors: int = 0,
        details: Optional[Dict[str, Any]] = None,
        timeline: Optional[List[Dict]] = None,
        error_messages: Optional[List[str]] = None,
    ) -> ScenarioOutput:
        """Create a standardized ScenarioOutput."""
        # Calculate MB/s
        mb_per_sec = (bytes_total / duration_secs / 1024 / 1024) if duration_secs > 0 else 0

        # Calculate error rate
        total = int(events_per_sec * duration_secs) + errors
        error_rate = (errors / total * 100) if total > 0 else 0

        return ScenarioOutput(
            name=name,
            success=success,
            duration_secs=duration_secs,
            metrics=ScenarioMetrics(
                throughput=ThroughputMetrics(
                    events_per_sec=events_per_sec,
                    mb_per_sec=mb_per_sec,
                    bytes_total=bytes_total,
                ),
                latency=LatencyMetrics(
                    p50_ms=p50_ms,
                    p95_ms=p95_ms,
                    p99_ms=p99_ms,
                    min_ms=min_ms,
                    max_ms=max_ms,
                ),
                cpu=CpuMetrics(
                    avg_percent=cpu_avg,
                    peak_percent=cpu_peak,
                ),
                errors=errors,
                error_rate=error_rate,
                details=details or {},
            ),
            timeline=timeline or [],
            error_messages=error_messages or [],
        )
