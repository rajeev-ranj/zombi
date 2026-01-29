#!/usr/bin/env python3
"""
Utilization Sweep Tool — Find Optimal t3.micro Load Configuration.

Runs a 6-phase smart sweep that systematically tests payload sizes, batch sizes,
concurrency levels, encodings, and workload mixes to find the configuration that
maximizes machine utilization on a t3.micro (or any target).

Each phase sweeps one dimension, carries the winner forward, and narrows the
search space — reducing 600+ grid combos down to ~24 experiments (~22 min).

Usage:
    # Full sweep (~22 min)
    python tools/utilization_sweep.py --url http://localhost:8080

    # Quick mode (~10 min)
    python tools/utilization_sweep.py --url http://ec2:8080 --quick

    # With node_exporter for richer resource metrics
    python tools/utilization_sweep.py --url http://ec2:8080 \
        --node-exporter http://ec2:9100
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Imports from sibling modules
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmark import create_session, encode_proto_event, generate_payload
from output import generate_run_id


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SweepConfig:
    """Top-level configuration for the entire sweep run."""
    url: str = "http://localhost:8080"
    output_dir: str = "./results"
    experiment_duration: int = 45
    champion_duration: int = 180
    node_exporter_url: Optional[str] = None
    use_docker: bool = True
    skip_phases: List[str] = field(default_factory=list)
    quick: bool = False
    quiet: bool = False

    def __post_init__(self):
        self.url = self.url.rstrip("/")
        if self.quick:
            self.experiment_duration = min(self.experiment_duration, 20)
            if "encoding" not in self.skip_phases:
                self.skip_phases.append("encoding")
            if "mix" not in self.skip_phases:
                self.skip_phases.append("mix")


@dataclass
class ExperimentConfig:
    """Parameters for a single experiment."""
    phase: int
    label: str
    payload_size: int = 256
    batch_size: int = 1
    concurrency: int = 50
    encoding: str = "json"
    workload: str = "write-only"
    read_ratio: float = 0.0
    duration: int = 45
    table_suffix: str = ""

    @property
    def table_name(self) -> str:
        suffix = self.table_suffix or self.label
        safe = re.sub(r"[^a-zA-Z0-9_-]", "", suffix)
        return f"sweep-p{self.phase}-{safe}"


@dataclass
class ResourceSnapshot:
    """Single point-in-time resource measurement."""
    timestamp: float = 0.0
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    disk_read_mb_s: float = 0.0
    disk_write_mb_s: float = 0.0
    net_rx_mb_s: float = 0.0
    net_tx_mb_s: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ResourceMetrics:
    """Aggregated resource utilisation over an experiment."""
    cpu_avg: float = 0.0
    cpu_peak: float = 0.0
    memory_avg_mb: float = 0.0
    memory_peak_mb: float = 0.0
    disk_io_avg_mb_s: float = 0.0
    disk_io_peak_mb_s: float = 0.0
    net_avg_mb_s: float = 0.0
    net_peak_mb_s: float = 0.0
    snapshots: List[ResourceSnapshot] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["snapshots"] = [s.to_dict() for s in self.snapshots]
        return d


@dataclass
class ExperimentResult:
    """Outcome of a single experiment."""
    config: ExperimentConfig
    requests_per_sec: float = 0.0
    events_per_sec: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    max_latency_ms: float = 0.0
    errors: int = 0
    total_requests: int = 0
    error_rate: float = 0.0
    resources: ResourceMetrics = field(default_factory=ResourceMetrics)
    utilization_score: float = 0.0
    raw_output: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": asdict(self.config),
            "requests_per_sec": self.requests_per_sec,
            "events_per_sec": self.events_per_sec,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "max_latency_ms": self.max_latency_ms,
            "errors": self.errors,
            "total_requests": self.total_requests,
            "error_rate": self.error_rate,
            "resources": self.resources.to_dict(),
            "utilization_score": self.utilization_score,
        }


@dataclass
class PhaseResult:
    """Results from one sweep phase."""
    phase: int
    name: str
    dimension: str
    results: List[ExperimentResult] = field(default_factory=list)
    winner: Optional[ExperimentResult] = None
    skipped: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "phase": self.phase,
            "name": self.name,
            "dimension": self.dimension,
            "skipped": self.skipped,
            "results": [r.to_dict() for r in self.results],
            "winner": self.winner.to_dict() if self.winner else None,
        }


@dataclass
class SweepReport:
    """Full sweep output with champion."""
    sweep_id: str
    url: str
    start_time: str = ""
    end_time: str = ""
    duration_secs: float = 0.0
    phases: List[PhaseResult] = field(default_factory=list)
    champion: Optional[ExperimentResult] = None
    champion_timeseries: List[ResourceSnapshot] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sweep_id": self.sweep_id,
            "url": self.url,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_secs": self.duration_secs,
            "phases": [p.to_dict() for p in self.phases],
            "champion": self.champion.to_dict() if self.champion else None,
            "champion_timeseries": [s.to_dict() for s in self.champion_timeseries],
        }


# ============================================================================
# UtilizationScorer
# ============================================================================

class UtilizationScorer:
    """Compute a weighted utilisation score in [0.0, 1.0].

    Weights and maxima are calibrated for t3.micro (2 vCPU burst, 1 GB RAM).
    """

    WEIGHTS = {
        "cpu": 0.30,
        "memory": 0.20,
        "disk_io": 0.25,
        "network": 0.10,
        "throughput": 0.15,
    }

    MAXIMA = {
        "cpu_percent": 200.0,       # 2 vCPU = 200%
        "memory_mb": 1024.0,        # 1 GB
        "disk_io_mb_s": 128.0,      # gp2 burst
        "net_mb_s": 625.0,          # 5 Gbps burst
        "events_per_sec": 200_000,  # bulk baseline
    }

    @classmethod
    def score(cls, result: ExperimentResult) -> float:
        r = result.resources
        components = {
            "cpu": min(r.cpu_avg / cls.MAXIMA["cpu_percent"], 1.0),
            "memory": min(r.memory_avg_mb / cls.MAXIMA["memory_mb"], 1.0),
            "disk_io": min(r.disk_io_avg_mb_s / cls.MAXIMA["disk_io_mb_s"], 1.0),
            "network": min(r.net_avg_mb_s / cls.MAXIMA["net_mb_s"], 1.0),
            "throughput": min(result.events_per_sec / cls.MAXIMA["events_per_sec"], 1.0),
        }
        return sum(cls.WEIGHTS[k] * v for k, v in components.items())


# ============================================================================
# MetricsCollector — 3-tier fallback
# ============================================================================

class MetricsCollector:
    """Collect resource metrics via node_exporter, docker stats, or ps fallback."""

    def __init__(self, cfg: SweepConfig):
        self._node_url = cfg.node_exporter_url
        self._use_docker = cfg.use_docker
        self._url = cfg.url
        self._session = _make_session()
        self._stop = threading.Event()
        self._snapshots: List[ResourceSnapshot] = []
        self._thread: Optional[threading.Thread] = None
        self._tier: str = "none"

        # Detect which tier is available
        if self._node_url and self._probe_node_exporter():
            self._tier = "node_exporter"
        elif self._use_docker and self._probe_docker():
            self._tier = "docker"
        else:
            self._tier = "fallback"

    @property
    def tier(self) -> str:
        return self._tier

    # -- Probes ---------------------------------------------------------------

    def _probe_node_exporter(self) -> bool:
        try:
            r = self._session.get(f"{self._node_url}/metrics", timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    @staticmethod
    def _probe_docker() -> bool:
        try:
            result = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}"],
                capture_output=True, text=True, timeout=5,
            )
            return result.returncode == 0 and result.stdout.strip() != ""
        except Exception:
            return False

    # -- Sampling -------------------------------------------------------------

    def start(self, interval: float = 2.0):
        self._stop.clear()
        self._snapshots = []
        self._thread = threading.Thread(
            target=self._loop, args=(interval,), daemon=True,
        )
        self._thread.start()

    def stop(self) -> ResourceMetrics:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        return self._aggregate()

    def _loop(self, interval: float):
        while not self._stop.is_set():
            snap = self._sample()
            if snap:
                self._snapshots.append(snap)
            self._stop.wait(timeout=interval)

    def _sample(self) -> Optional[ResourceSnapshot]:
        if self._tier == "node_exporter":
            return self._sample_node_exporter()
        elif self._tier == "docker":
            return self._sample_docker()
        return self._sample_fallback()

    # -- Tier 1: node_exporter ------------------------------------------------

    def _sample_node_exporter(self) -> Optional[ResourceSnapshot]:
        try:
            r = self._session.get(f"{self._node_url}/metrics", timeout=3)
            text = r.text
            snap = ResourceSnapshot(timestamp=time.time())

            # CPU: node_cpu_seconds_total — compute instant usage from idle
            idle = _prom_value(text, 'node_cpu_seconds_total{.*mode="idle".*}')
            total = _prom_value(text, 'node_cpu_seconds_total{.*}')
            if total and idle:
                snap.cpu_percent = max(0.0, (1.0 - idle / total) * 200.0)

            # Memory
            mem_total = _prom_value(text, "node_memory_MemTotal_bytes")
            mem_avail = _prom_value(text, "node_memory_MemAvailable_bytes")
            if mem_total and mem_avail:
                snap.memory_mb = (mem_total - mem_avail) / 1024 / 1024

            # Disk I/O (bytes read+written per sec — approximation via total)
            disk_r = _prom_value(text, "node_disk_read_bytes_total")
            disk_w = _prom_value(text, "node_disk_written_bytes_total")
            if disk_r is not None:
                snap.disk_read_mb_s = disk_r / 1024 / 1024  # cumulative; we diff later
            if disk_w is not None:
                snap.disk_write_mb_s = disk_w / 1024 / 1024

            # Network
            net_rx = _prom_value(text, "node_network_receive_bytes_total")
            net_tx = _prom_value(text, "node_network_transmit_bytes_total")
            if net_rx is not None:
                snap.net_rx_mb_s = net_rx / 1024 / 1024
            if net_tx is not None:
                snap.net_tx_mb_s = net_tx / 1024 / 1024

            return snap
        except Exception:
            return None

    # -- Tier 2: docker stats -------------------------------------------------

    def _sample_docker(self) -> Optional[ResourceSnapshot]:
        try:
            result = subprocess.run(
                [
                    "docker", "stats", "--no-stream",
                    "--format", "{{.CPUPerc}}\t{{.MemUsage}}\t{{.BlockIO}}\t{{.NetIO}}",
                ],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode != 0:
                return None

            snap = ResourceSnapshot(timestamp=time.time())
            for line in result.stdout.strip().splitlines():
                parts = line.split("\t")
                if len(parts) < 4:
                    continue
                # CPU
                cpu_str = parts[0].strip().rstrip("%")
                snap.cpu_percent += _safe_float(cpu_str)
                # Memory: "123.4MiB / 1GiB"
                mem_parts = parts[1].split("/")
                if mem_parts:
                    snap.memory_mb += _parse_mem(mem_parts[0].strip())
                # Block I/O: "1.23MB / 4.56MB"
                bio = parts[2].split("/")
                if len(bio) == 2:
                    snap.disk_read_mb_s += _parse_mem(bio[0].strip())
                    snap.disk_write_mb_s += _parse_mem(bio[1].strip())
                # Net I/O
                nio = parts[3].split("/")
                if len(nio) == 2:
                    snap.net_rx_mb_s += _parse_mem(nio[0].strip())
                    snap.net_tx_mb_s += _parse_mem(nio[1].strip())
            return snap
        except Exception:
            return None

    # -- Tier 3: fallback (ps + /stats) ---------------------------------------

    def _sample_fallback(self) -> Optional[ResourceSnapshot]:
        snap = ResourceSnapshot(timestamp=time.time())
        # CPU from ps
        try:
            pid = _get_zombi_pid()
            if pid:
                result = subprocess.run(
                    ["ps", "-p", str(pid), "-o", "%cpu,%mem"],
                    capture_output=True, text=True,
                )
                if result.returncode == 0:
                    lines = result.stdout.strip().splitlines()
                    if len(lines) > 1:
                        parts = lines[1].strip().split()
                        if len(parts) >= 1:
                            snap.cpu_percent = _safe_float(parts[0])
                        if len(parts) >= 2:
                            snap.memory_mb = _safe_float(parts[1]) * 10.24  # %mem → approx MB on 1GB
        except Exception:
            pass

        # Server stats
        try:
            r = self._session.get(f"{self._url}/stats", timeout=2)
            if r.status_code == 200:
                stats = r.json()
                writes = stats.get("writes", {})
                snap.disk_write_mb_s = writes.get("bytes_per_sec", 0) / 1024 / 1024
        except Exception:
            pass

        return snap

    # -- Aggregation ----------------------------------------------------------

    def _aggregate(self) -> ResourceMetrics:
        if not self._snapshots:
            return ResourceMetrics()

        # For node_exporter cumulative counters, convert to rates
        snaps = self._snapshots
        if self._tier == "node_exporter" and len(snaps) >= 2:
            snaps = self._compute_rates(snaps)

        cpus = [s.cpu_percent for s in snaps if s.cpu_percent > 0]
        mems = [s.memory_mb for s in snaps if s.memory_mb > 0]
        disk_r = [s.disk_read_mb_s for s in snaps]
        disk_w = [s.disk_write_mb_s for s in snaps]
        disk_io = [r + w for r, w in zip(disk_r, disk_w)]
        nets = [s.net_rx_mb_s + s.net_tx_mb_s for s in snaps]

        return ResourceMetrics(
            cpu_avg=_avg(cpus),
            cpu_peak=max(cpus) if cpus else 0.0,
            memory_avg_mb=_avg(mems),
            memory_peak_mb=max(mems) if mems else 0.0,
            disk_io_avg_mb_s=_avg(disk_io),
            disk_io_peak_mb_s=max(disk_io) if disk_io else 0.0,
            net_avg_mb_s=_avg(nets),
            net_peak_mb_s=max(nets) if nets else 0.0,
            snapshots=list(self._snapshots),
        )

    @staticmethod
    def _compute_rates(snaps: List[ResourceSnapshot]) -> List[ResourceSnapshot]:
        """Convert cumulative node_exporter counters into per-second rates."""
        rated: List[ResourceSnapshot] = []
        for i in range(1, len(snaps)):
            dt = snaps[i].timestamp - snaps[i - 1].timestamp
            if dt <= 0:
                continue
            rated.append(ResourceSnapshot(
                timestamp=snaps[i].timestamp,
                cpu_percent=snaps[i].cpu_percent,
                memory_mb=snaps[i].memory_mb,
                disk_read_mb_s=max(0, snaps[i].disk_read_mb_s - snaps[i - 1].disk_read_mb_s) / dt,
                disk_write_mb_s=max(0, snaps[i].disk_write_mb_s - snaps[i - 1].disk_write_mb_s) / dt,
                net_rx_mb_s=max(0, snaps[i].net_rx_mb_s - snaps[i - 1].net_rx_mb_s) / dt,
                net_tx_mb_s=max(0, snaps[i].net_tx_mb_s - snaps[i - 1].net_tx_mb_s) / dt,
            ))
        return rated


# ============================================================================
# ExperimentRunner — invoke hey, capture metrics, return result
# ============================================================================

class ExperimentRunner:
    """Run a single experiment and collect metrics."""

    def __init__(self, cfg: SweepConfig, collector: MetricsCollector):
        self._cfg = cfg
        self._collector = collector
        self._session = _make_session()

    def run(self, exp: ExperimentConfig) -> ExperimentResult:
        result = ExperimentResult(config=exp)
        url = self._cfg.url

        if exp.workload == "write-only":
            result = self._run_write(exp, url)
        elif exp.workload.startswith("mixed"):
            result = self._run_mixed(exp, url)
        else:
            result = self._run_write(exp, url)

        # Score
        result.utilization_score = UtilizationScorer.score(result)
        return result

    # -- Writers --------------------------------------------------------------

    def _run_write(self, exp: ExperimentConfig, url: str) -> ExperimentResult:
        """Run a write-only experiment using hey."""
        self._collector.start(interval=2.0)

        if exp.batch_size > 1:
            hey_result = self._hey_bulk(exp, url)
        elif exp.encoding == "proto":
            hey_result = self._hey_proto(exp, url)
        else:
            hey_result = self._hey_single(exp, url)

        resources = self._collector.stop()

        result = ExperimentResult(
            config=exp,
            requests_per_sec=hey_result.get("requests_per_sec", 0),
            events_per_sec=hey_result.get("events_per_sec", hey_result.get("requests_per_sec", 0)),
            p50_ms=hey_result.get("p50_ms", 0),
            p95_ms=hey_result.get("p95_ms", 0),
            p99_ms=hey_result.get("p99_ms", 0),
            max_latency_ms=hey_result.get("max_latency_ms", 0),
            errors=hey_result.get("errors", 0),
            total_requests=hey_result.get("total_requests", 0),
            resources=resources,
            raw_output=hey_result.get("raw", ""),
        )
        total = result.total_requests
        result.error_rate = (result.errors / total * 100) if total > 0 else 0
        return result

    def _run_mixed(self, exp: ExperimentConfig, url: str) -> ExperimentResult:
        """Run mixed read/write workload."""
        self._collector.start(interval=2.0)

        # Start writes with hey
        write_exp = ExperimentConfig(
            phase=exp.phase,
            label=exp.label,
            payload_size=exp.payload_size,
            batch_size=exp.batch_size,
            concurrency=max(1, int(exp.concurrency * (1 - exp.read_ratio))),
            encoding=exp.encoding,
            duration=exp.duration,
            table_suffix=exp.table_suffix,
        )
        if write_exp.batch_size > 1:
            hey_result = self._hey_bulk(write_exp, url)
        else:
            hey_result = self._hey_single(write_exp, url)

        # Concurrent reads via thread pool
        read_concurrency = max(1, int(exp.concurrency * exp.read_ratio))
        read_count = [0]
        read_errors = [0]
        stop_reads = threading.Event()

        def _reader():
            s = _make_session()
            while not stop_reads.is_set():
                try:
                    r = s.get(
                        f"{url}/tables/{exp.table_name}",
                        params={"limit": 100},
                        timeout=5,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        read_count[0] += len(data.get("records", []))
                    else:
                        read_errors[0] += 1
                except Exception:
                    read_errors[0] += 1

        # Run reads in background for experiment duration
        with ThreadPoolExecutor(max_workers=read_concurrency) as pool:
            futures = [pool.submit(_reader) for _ in range(read_concurrency)]
            # hey already ran for the full duration; stop reads now
            stop_reads.set()
            for f in futures:
                f.result(timeout=10)

        resources = self._collector.stop()

        write_eps = hey_result.get("events_per_sec", hey_result.get("requests_per_sec", 0))
        read_rps = read_count[0] / max(1, exp.duration)

        result = ExperimentResult(
            config=exp,
            requests_per_sec=hey_result.get("requests_per_sec", 0),
            events_per_sec=write_eps + read_rps,
            p50_ms=hey_result.get("p50_ms", 0),
            p95_ms=hey_result.get("p95_ms", 0),
            p99_ms=hey_result.get("p99_ms", 0),
            max_latency_ms=hey_result.get("max_latency_ms", 0),
            errors=hey_result.get("errors", 0) + read_errors[0],
            total_requests=hey_result.get("total_requests", 0),
            resources=resources,
            raw_output=hey_result.get("raw", ""),
        )
        total = result.total_requests + read_count[0] + read_errors[0]
        result.error_rate = (result.errors / total * 100) if total > 0 else 0
        return result

    # -- hey invocations ------------------------------------------------------

    def _hey_single(self, exp: ExperimentConfig, url: str) -> Dict:
        payload_dict = generate_payload(exp.payload_size)
        payload_body = json.dumps({
            "payload": json.dumps(payload_dict),
            "timestamp_ms": int(time.time() * 1000),
        })
        target = f"{url}/tables/{exp.table_name}"
        cmd = [
            "hey",
            "-z", f"{exp.duration}s",
            "-c", str(exp.concurrency),
            "-m", "POST",
            "-T", "application/json",
            "-d", payload_body,
            target,
        ]
        return self._exec_hey(cmd, batch_size=1)

    def _hey_proto(self, exp: ExperimentConfig, url: str) -> Dict:
        payload_dict = generate_payload(exp.payload_size)
        payload_bytes = json.dumps(payload_dict).encode("utf-8")
        proto_data = encode_proto_event(
            payload=payload_bytes,
            timestamp_ms=int(time.time() * 1000),
        )
        target = f"{url}/tables/{exp.table_name}"

        tmpf = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
        try:
            tmpf.write(proto_data)
            tmpf.close()
            cmd = [
                "hey",
                "-z", f"{exp.duration}s",
                "-c", str(exp.concurrency),
                "-m", "POST",
                "-T", "application/x-protobuf",
                "-D", tmpf.name,
                "-H", "X-Partition: 0",
                target,
            ]
            return self._exec_hey(cmd, batch_size=1)
        finally:
            _safe_unlink(tmpf.name)

    def _hey_bulk(self, exp: ExperimentConfig, url: str) -> Dict:
        records = []
        for i in range(exp.batch_size):
            p = generate_payload(exp.payload_size)
            records.append({
                "payload": json.dumps(p),
                "partition": i % 4,
                "timestamp_ms": int(time.time() * 1000),
            })
        body = json.dumps({"records": records})
        target = f"{url}/tables/{exp.table_name}/bulk"

        tmpf = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
        try:
            tmpf.write(body)
            tmpf.close()
            cmd = [
                "hey",
                "-z", f"{exp.duration}s",
                "-c", str(exp.concurrency),
                "-m", "POST",
                "-T", "application/json",
                "-D", tmpf.name,
                target,
            ]
            result = self._exec_hey(cmd, batch_size=exp.batch_size)
            result["events_per_sec"] = result.get("requests_per_sec", 0) * exp.batch_size
            return result
        finally:
            _safe_unlink(tmpf.name)

    @staticmethod
    def _exec_hey(cmd: List[str], batch_size: int = 1) -> Dict:
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600,
            )
            output = proc.stdout + proc.stderr
            perf = _parse_hey_output(output)
            perf["raw"] = output
            return perf
        except subprocess.TimeoutExpired:
            return {"raw": "TIMEOUT", "errors": 1}
        except Exception as e:
            return {"raw": str(e), "errors": 1}


# ============================================================================
# PhasedSweep — 6-phase orchestrator
# ============================================================================

class PhasedSweep:
    """Run the 6-phase sweep and return a SweepReport."""

    PHASE_NAMES = {
        1: ("payload", "Payload Size"),
        2: ("batch", "Batch Size"),
        3: ("concurrency", "Concurrency"),
        4: ("encoding", "Encoding"),
        5: ("mix", "Workload Mix"),
        6: ("champion", "Champion Validation"),
    }

    def __init__(self, cfg: SweepConfig):
        self._cfg = cfg
        self._collector = MetricsCollector(cfg)
        self._runner = ExperimentRunner(cfg, self._collector)
        self._quiet = cfg.quiet

    def run(self) -> SweepReport:
        sweep_id = generate_run_id()
        report = SweepReport(
            sweep_id=sweep_id,
            url=self._cfg.url,
            start_time=datetime.now().isoformat(),
        )
        overall_start = time.time()

        # Health check
        if not self._health_check():
            _log("ERROR: Zombi health check failed. Aborting.")
            report.end_time = datetime.now().isoformat()
            report.duration_secs = time.time() - overall_start
            return report

        _log(f"Metrics collection tier: {self._collector.tier}")

        # Carry-forward optimal values
        opt_payload = 256
        opt_batch = 1
        opt_concurrency = 50
        opt_encoding = "json"
        opt_workload = "write-only"
        opt_read_ratio = 0.0

        # Phase 1: Payload Size
        phase1 = self._run_phase_1(opt_batch, opt_concurrency)
        report.phases.append(phase1)
        if phase1.winner:
            opt_payload = phase1.winner.config.payload_size

        # Phase 2: Batch Size
        phase2 = self._run_phase_2(opt_payload, opt_concurrency)
        report.phases.append(phase2)
        if phase2.winner:
            opt_batch = phase2.winner.config.batch_size

        # Phase 3: Concurrency
        phase3 = self._run_phase_3(opt_payload, opt_batch)
        report.phases.append(phase3)
        if phase3.winner:
            opt_concurrency = phase3.winner.config.concurrency

        # Phase 4: Encoding
        phase4 = self._run_phase_4(opt_payload, opt_batch, opt_concurrency)
        report.phases.append(phase4)
        if phase4.winner:
            opt_encoding = phase4.winner.config.encoding

        # Phase 5: Workload Mix
        phase5 = self._run_phase_5(opt_payload, opt_batch, opt_concurrency, opt_encoding)
        report.phases.append(phase5)
        if phase5.winner:
            opt_workload = phase5.winner.config.workload
            opt_read_ratio = phase5.winner.config.read_ratio

        # Phase 6: Champion Validation
        phase6 = self._run_phase_6(
            opt_payload, opt_batch, opt_concurrency, opt_encoding,
            opt_workload, opt_read_ratio,
        )
        report.phases.append(phase6)
        if phase6.winner:
            report.champion = phase6.winner
            report.champion_timeseries = phase6.winner.resources.snapshots

        report.end_time = datetime.now().isoformat()
        report.duration_secs = time.time() - overall_start

        _log(f"\nSweep completed in {report.duration_secs:.0f}s")
        return report

    # -- Phase implementations ------------------------------------------------

    def _run_phase_1(self, batch: int, concurrency: int) -> PhaseResult:
        """Phase 1: Sweep payload sizes — maximize effective bandwidth."""
        phase = PhaseResult(phase=1, name="Payload Size", dimension="payload_size")
        if self._skip("payload"):
            phase.skipped = True
            return phase

        _phase_header(1, "Payload Size")
        sizes = [64, 256, 1024, 4096, 16384]

        for size in sizes:
            exp = ExperimentConfig(
                phase=1, label=f"{size}B",
                payload_size=size, batch_size=batch,
                concurrency=concurrency, duration=self._cfg.experiment_duration,
            )
            _log(f"  Testing payload={size}B ...")
            result = self._runner.run(exp)
            phase.results.append(result)
            self._print_brief(result)
            self._cooldown()

        # Winner: maximize events/s × payload_bytes (effective bandwidth)
        phase.winner = max(
            phase.results,
            key=lambda r: r.events_per_sec * r.config.payload_size,
        )
        _log(f"  ★ Winner: {phase.winner.config.label} "
             f"({phase.winner.events_per_sec:,.0f} ev/s)")
        return phase

    def _run_phase_2(self, payload: int, concurrency: int) -> PhaseResult:
        """Phase 2: Sweep batch sizes — maximize events/s."""
        phase = PhaseResult(phase=2, name="Batch Size", dimension="batch_size")
        if self._skip("batch"):
            phase.skipped = True
            return phase

        _phase_header(2, "Batch Size")
        batches = [1, 10, 50, 100, 500]

        for bs in batches:
            exp = ExperimentConfig(
                phase=2, label=f"b{bs}",
                payload_size=payload, batch_size=bs,
                concurrency=concurrency, duration=self._cfg.experiment_duration,
            )
            _log(f"  Testing batch={bs} ...")
            result = self._runner.run(exp)
            phase.results.append(result)
            self._print_brief(result)
            self._cooldown()

        phase.winner = max(phase.results, key=lambda r: r.events_per_sec)
        _log(f"  ★ Winner: batch={phase.winner.config.batch_size} "
             f"({phase.winner.events_per_sec:,.0f} ev/s)")
        return phase

    def _run_phase_3(self, payload: int, batch: int) -> PhaseResult:
        """Phase 3: Sweep concurrency — maximize events/s with p99 < 1s gate."""
        phase = PhaseResult(phase=3, name="Concurrency", dimension="concurrency")
        if self._skip("concurrency"):
            phase.skipped = True
            return phase

        _phase_header(3, "Concurrency")
        levels = [10, 50, 100, 200]

        for c in levels:
            exp = ExperimentConfig(
                phase=3, label=f"c{c}",
                payload_size=payload, batch_size=batch,
                concurrency=c, duration=self._cfg.experiment_duration,
            )
            _log(f"  Testing concurrency={c} ...")
            result = self._runner.run(exp)
            phase.results.append(result)
            self._print_brief(result)
            self._cooldown()

        # Winner: max events/s WHERE p99 < 1000ms
        gated = [r for r in phase.results if r.p99_ms < 1000]
        if not gated:
            gated = phase.results  # fallback: ignore gate
        phase.winner = max(gated, key=lambda r: r.events_per_sec)
        _log(f"  ★ Winner: concurrency={phase.winner.config.concurrency} "
             f"({phase.winner.events_per_sec:,.0f} ev/s, p99={phase.winner.p99_ms:.0f}ms)")
        return phase

    def _run_phase_4(self, payload: int, batch: int, concurrency: int) -> PhaseResult:
        """Phase 4: JSON vs Proto encoding — maximize events/s."""
        phase = PhaseResult(phase=4, name="Encoding", dimension="encoding")
        if self._skip("encoding"):
            phase.skipped = True
            return phase

        _phase_header(4, "Encoding")
        # Proto is single-API only (no bulk proto)
        test_batch = 1 if batch <= 1 else batch
        encodings = ["json", "proto"] if test_batch == 1 else ["json"]

        for enc in encodings:
            exp = ExperimentConfig(
                phase=4, label=enc,
                payload_size=payload, batch_size=test_batch,
                concurrency=concurrency, encoding=enc,
                duration=self._cfg.experiment_duration,
            )
            _log(f"  Testing encoding={enc} ...")
            result = self._runner.run(exp)
            phase.results.append(result)
            self._print_brief(result)
            self._cooldown()

        phase.winner = max(phase.results, key=lambda r: r.events_per_sec)
        _log(f"  ★ Winner: {phase.winner.config.encoding} "
             f"({phase.winner.events_per_sec:,.0f} ev/s)")
        return phase

    def _run_phase_5(
        self, payload: int, batch: int, concurrency: int, encoding: str,
    ) -> PhaseResult:
        """Phase 5: Workload mix — maximize combined throughput."""
        phase = PhaseResult(phase=5, name="Workload Mix", dimension="workload")
        if self._skip("mix"):
            phase.skipped = True
            return phase

        _phase_header(5, "Workload Mix")
        mixes = [
            ("write-only", 0.0),
            ("mixed-90-10", 0.10),
            ("mixed-70-30", 0.30),
        ]

        for name, read_ratio in mixes:
            exp = ExperimentConfig(
                phase=5, label=name,
                payload_size=payload, batch_size=batch,
                concurrency=concurrency, encoding=encoding,
                workload=name, read_ratio=read_ratio,
                duration=self._cfg.experiment_duration,
            )
            _log(f"  Testing workload={name} ...")
            result = self._runner.run(exp)
            phase.results.append(result)
            self._print_brief(result)
            self._cooldown()

        phase.winner = max(phase.results, key=lambda r: r.events_per_sec)
        _log(f"  ★ Winner: {phase.winner.config.label} "
             f"({phase.winner.events_per_sec:,.0f} ev/s)")
        return phase

    def _run_phase_6(
        self, payload: int, batch: int, concurrency: int,
        encoding: str, workload: str, read_ratio: float,
    ) -> PhaseResult:
        """Phase 6: Champion validation — sustained run with time-series snapshots."""
        phase = PhaseResult(phase=6, name="Champion Validation", dimension="champion")
        if self._skip("champion"):
            phase.skipped = True
            return phase

        _phase_header(6, "Champion Validation")
        _log(f"  Config: payload={payload}B batch={batch} c={concurrency} "
             f"enc={encoding} workload={workload}")
        _log(f"  Duration: {self._cfg.champion_duration}s (sustained)")

        exp = ExperimentConfig(
            phase=6, label="champion",
            payload_size=payload, batch_size=batch,
            concurrency=concurrency, encoding=encoding,
            workload=workload, read_ratio=read_ratio,
            duration=self._cfg.champion_duration,
            table_suffix="champion",
        )
        result = self._runner.run(exp)
        phase.results.append(result)
        phase.winner = result
        self._print_brief(result)

        _log(f"  Utilization score: {result.utilization_score:.3f}")
        return phase

    # -- Helpers --------------------------------------------------------------

    def _skip(self, dimension: str) -> bool:
        if dimension in self._cfg.skip_phases:
            _log(f"  [skipped by --skip-phase]")
            return True
        return False

    def _cooldown(self):
        _log("  (3s cooldown)")
        time.sleep(3)

    def _health_check(self) -> bool:
        try:
            r = _make_session().get(f"{self._cfg.url}/health", timeout=10)
            return r.status_code == 200
        except Exception:
            return False

    def _print_brief(self, r: ExperimentResult):
        if self._quiet:
            return
        _log(f"    → {r.events_per_sec:>10,.0f} ev/s | "
             f"p50={r.p50_ms:>6.1f}ms p99={r.p99_ms:>6.1f}ms | "
             f"err={r.errors} | score={r.utilization_score:.3f}")


# ============================================================================
# ReportGenerator
# ============================================================================

class ReportGenerator:
    """Generate JSON + Markdown output files."""

    @staticmethod
    def save(report: SweepReport, output_dir: str) -> Tuple[str, str]:
        base = Path(output_dir) / "utilization-sweep" / report.sweep_id
        base.mkdir(parents=True, exist_ok=True)

        json_path = base / "results.json"
        with open(json_path, "w") as f:
            json.dump(report.to_dict(), f, indent=2)

        md_path = base / "REPORT.md"
        with open(md_path, "w") as f:
            f.write(ReportGenerator._markdown(report))

        return str(json_path), str(md_path)

    @staticmethod
    def _markdown(report: SweepReport) -> str:
        lines: List[str] = []
        lines.append("# Utilization Sweep Report")
        lines.append("")
        lines.append(f"**Sweep ID:** {report.sweep_id}")
        lines.append(f"**Target:** {report.url}")
        lines.append(f"**Start:** {report.start_time}")
        lines.append(f"**Duration:** {report.duration_secs:.0f}s")
        lines.append("")

        # Champion summary
        lines.append("## Champion Configuration")
        lines.append("")
        if report.champion:
            c = report.champion.config
            lines.append("| Parameter | Value |")
            lines.append("|-----------|-------|")
            lines.append(f"| Payload size | {c.payload_size} B |")
            lines.append(f"| Batch size | {c.batch_size} |")
            lines.append(f"| Concurrency | {c.concurrency} |")
            lines.append(f"| Encoding | {c.encoding} |")
            lines.append(f"| Workload | {c.workload} |")
            lines.append("")
            lines.append("| Metric | Value |")
            lines.append("|--------|-------|")
            lines.append(f"| Events/s | {report.champion.events_per_sec:,.0f} |")
            lines.append(f"| Requests/s | {report.champion.requests_per_sec:,.0f} |")
            lines.append(f"| P50 latency | {report.champion.p50_ms:.1f} ms |")
            lines.append(f"| P95 latency | {report.champion.p95_ms:.1f} ms |")
            lines.append(f"| P99 latency | {report.champion.p99_ms:.1f} ms |")
            lines.append(f"| Errors | {report.champion.errors} |")
            lines.append(f"| Utilization score | {report.champion.utilization_score:.3f} |")
            lines.append("")

            r = report.champion.resources
            lines.append("| Resource | Avg | Peak |")
            lines.append("|----------|-----|------|")
            lines.append(f"| CPU | {r.cpu_avg:.1f}% | {r.cpu_peak:.1f}% |")
            lines.append(f"| Memory | {r.memory_avg_mb:.0f} MB | {r.memory_peak_mb:.0f} MB |")
            lines.append(f"| Disk I/O | {r.disk_io_avg_mb_s:.1f} MB/s | {r.disk_io_peak_mb_s:.1f} MB/s |")
            lines.append(f"| Network | {r.net_avg_mb_s:.1f} MB/s | {r.net_peak_mb_s:.1f} MB/s |")
            lines.append("")
        else:
            lines.append("*No champion determined.*")
            lines.append("")

        # Phase-by-phase tables
        for phase in report.phases:
            lines.append(f"## Phase {phase.phase}: {phase.name}")
            lines.append("")
            if phase.skipped:
                lines.append("*Skipped*")
                lines.append("")
                continue

            if not phase.results:
                lines.append("*No results*")
                lines.append("")
                continue

            lines.append(f"**Dimension:** `{phase.dimension}`")
            if phase.winner:
                lines.append(f"**Winner:** `{phase.winner.config.label}`")
            lines.append("")

            lines.append("| Config | Events/s | Req/s | P50 ms | P95 ms | P99 ms | Errors | Score |")
            lines.append("|--------|----------|-------|--------|--------|--------|--------|-------|")
            for r in phase.results:
                marker = " ★" if (phase.winner and r.config.label == phase.winner.config.label) else ""
                lines.append(
                    f"| {r.config.label}{marker} "
                    f"| {r.events_per_sec:,.0f} "
                    f"| {r.requests_per_sec:,.0f} "
                    f"| {r.p50_ms:.1f} "
                    f"| {r.p95_ms:.1f} "
                    f"| {r.p99_ms:.1f} "
                    f"| {r.errors} "
                    f"| {r.utilization_score:.3f} |"
                )
            lines.append("")

        # ASCII utilization heatmap
        lines.append("## Utilization Heatmap")
        lines.append("")
        lines.append("Scores across all experiments (higher = better utilization):")
        lines.append("")
        lines.append("```")
        for phase in report.phases:
            if phase.skipped or not phase.results:
                continue
            label = f"P{phase.phase}"
            bars = ""
            for r in phase.results:
                bar_len = int(r.utilization_score * 20)
                bars += f"  {r.config.label:>12} {'█' * bar_len}{'░' * (20 - bar_len)} {r.utilization_score:.3f}"
                bars += "\n"
            lines.append(f"{label} ({phase.name}):")
            lines.append(bars.rstrip())
        lines.append("```")
        lines.append("")

        # Bottleneck analysis
        lines.append("## Recommendations")
        lines.append("")
        if report.champion:
            r = report.champion.resources
            bottlenecks = []
            if r.cpu_avg > 150:
                bottlenecks.append("**CPU-bound**: Average CPU >75% of 2-vCPU capacity. "
                                   "Consider a larger instance or optimizing hot paths.")
            if r.memory_avg_mb > 800:
                bottlenecks.append("**Memory-bound**: Average memory >800 MB on 1 GB instance. "
                                   "Reduce batch sizes or increase instance memory.")
            if r.disk_io_avg_mb_s > 80:
                bottlenecks.append("**Disk I/O bound**: Disk throughput >80 MB/s. "
                                   "Consider io1/io2 EBS or reduce write amplification.")
            if r.net_avg_mb_s > 400:
                bottlenecks.append("**Network-bound**: Network >400 MB/s. "
                                   "Likely hitting burst limits. Use larger instance type.")

            if bottlenecks:
                for b in bottlenecks:
                    lines.append(f"- {b}")
            else:
                lines.append("- No resource saturation detected. The workload may be "
                             "client-limited or the server has headroom.")
            lines.append("")
            score = report.champion.utilization_score
            if score < 0.3:
                lines.append(f"- Overall utilization score ({score:.3f}) is low. "
                             "The server has significant spare capacity.")
            elif score < 0.6:
                lines.append(f"- Moderate utilization ({score:.3f}). "
                             "Good balance of throughput and headroom.")
            else:
                lines.append(f"- High utilization ({score:.3f}). "
                             "The server is well-loaded; watch for saturation under burst.")
        else:
            lines.append("- No data available for recommendations.")
        lines.append("")

        lines.append("---")
        lines.append("*Generated by `tools/utilization_sweep.py`*")
        return "\n".join(lines)


# ============================================================================
# Helpers (module-level)
# ============================================================================

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _parse_hey_output(output: str) -> Dict[str, Any]:
    """Parse hey command output — reuses patterns from runners/peak_runner.py."""
    perf: Dict[str, Any] = {
        "total_requests": 0,
        "requests_per_sec": 0.0,
        "avg_latency_ms": 0.0,
        "p50_ms": 0.0,
        "p95_ms": 0.0,
        "p99_ms": 0.0,
        "max_latency_ms": 0.0,
        "errors": 0,
    }

    if m := re.search(r'Requests/sec:\s+([\d.]+)', output):
        perf["requests_per_sec"] = float(m.group(1))
    if m := re.search(r'Average:\s+([\d.]+)\s+secs', output):
        perf["avg_latency_ms"] = float(m.group(1)) * 1000
    if m := re.search(r'Slowest:\s+([\d.]+)\s+secs', output):
        perf["max_latency_ms"] = float(m.group(1)) * 1000
    if m := re.search(r'50%.*?in\s+([\d.]+)\s+secs', output):
        perf["p50_ms"] = float(m.group(1)) * 1000
    if m := re.search(r'95%.*?in\s+([\d.]+)\s+secs', output):
        perf["p95_ms"] = float(m.group(1)) * 1000
    if m := re.search(r'99%.*?in\s+([\d.]+)\s+secs', output):
        perf["p99_ms"] = float(m.group(1)) * 1000

    for m in re.finditer(r'\[(\d+)\]\s+(\d+)\s+responses', output):
        code, count = int(m.group(1)), int(m.group(2))
        perf["total_requests"] += count
        if code >= 400:
            perf["errors"] += count

    return perf


def _get_zombi_pid() -> Optional[int]:
    """Find Zombi server PID."""
    try:
        result = subprocess.run(
            ["pgrep", "-x", "zombi"], capture_output=True, text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip().splitlines()[0])
        result = subprocess.run(
            ["ps", "-eo", "pid,comm"], capture_output=True, text=True,
        )
        for line in result.stdout.strip().splitlines():
            if "zombi" in line and "grep" not in line:
                parts = line.strip().split()
                if parts:
                    return int(parts[0])
    except Exception:
        pass
    return None


def _prom_value(text: str, metric_pattern: str) -> Optional[float]:
    """Extract the first numeric value matching a Prometheus metric pattern."""
    m = re.search(rf'^{metric_pattern}\s+([\d.eE+\-]+)', text, re.MULTILINE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _parse_mem(s: str) -> float:
    """Parse a memory string like '123.4MiB' or '1.5GB' into MB."""
    s = s.strip()
    m = re.match(r'([\d.]+)\s*(B|KB|KiB|MB|MiB|GB|GiB|TB|TiB)?', s, re.IGNORECASE)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = (m.group(2) or "B").upper()
    multipliers = {
        "B": 1 / 1024 / 1024,
        "KB": 1 / 1024, "KIB": 1 / 1024,
        "MB": 1.0, "MIB": 1.0,
        "GB": 1024, "GIB": 1024,
        "TB": 1024 * 1024, "TIB": 1024 * 1024,
    }
    return val * multipliers.get(unit, 1.0)


def _safe_float(s: str) -> float:
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _safe_unlink(path: str):
    try:
        os.unlink(path)
    except OSError:
        pass


def _log(msg: str):
    print(msg, flush=True)


def _phase_header(num: int, name: str):
    _log(f"\n{'─' * 60}")
    _log(f"  Phase {num}: {name}")
    _log(f"{'─' * 60}")


# ============================================================================
# CLI (Click)
# ============================================================================

@click.command("utilization-sweep")
@click.option("--url", "-u", default="http://localhost:8080",
              help="Zombi server URL.")
@click.option("--output-dir", "-o", default="./results",
              help="Output directory for results.")
@click.option("--experiment-duration", "-d", default=45, type=int,
              help="Seconds per experiment (default 45).")
@click.option("--champion-duration", default=180, type=int,
              help="Champion validation duration in seconds (default 180).")
@click.option("--node-exporter", default=None,
              help="node_exporter URL for system metrics (e.g. http://host:9100).")
@click.option("--no-docker", is_flag=True, default=False,
              help="Disable docker stats collection.")
@click.option("--skip-phase", multiple=True,
              type=click.Choice(["payload", "batch", "concurrency", "encoding", "mix", "champion"]),
              help="Skip specific phase(s).")
@click.option("--quick", is_flag=True, default=False,
              help="Quick mode: 20s experiments, skip encoding + mix phases.")
@click.option("--quiet", "-q", is_flag=True, default=False,
              help="Reduce output verbosity.")
def cli(url, output_dir, experiment_duration, champion_duration,
        node_exporter, no_docker, skip_phase, quick, quiet):
    """Utilization Sweep — find the optimal load configuration for Zombi.

    Runs a 6-phase smart sweep: payload sizes → batch sizes → concurrency →
    encoding → workload mix → champion validation. Each phase carries the
    winner forward to narrow the search space.
    """
    # Check hey dependency
    if not shutil.which("hey"):
        click.echo("ERROR: 'hey' HTTP load generator not found.", err=True)
        click.echo("Install with: brew install hey  (macOS)", err=True)
        click.echo("         or: go install github.com/rakyll/hey@latest", err=True)
        sys.exit(1)

    cfg = SweepConfig(
        url=url,
        output_dir=output_dir,
        experiment_duration=experiment_duration,
        champion_duration=champion_duration,
        node_exporter_url=node_exporter,
        use_docker=not no_docker,
        skip_phases=list(skip_phase),
        quick=quick,
        quiet=quiet,
    )

    _log("╔══════════════════════════════════════════════════════════╗")
    _log("║          Zombi Utilization Sweep                       ║")
    _log("╚══════════════════════════════════════════════════════════╝")
    _log(f"  Target:     {cfg.url}")
    _log(f"  Duration:   {cfg.experiment_duration}s per experiment")
    _log(f"  Champion:   {cfg.champion_duration}s validation")
    _log(f"  Quick mode: {'yes' if cfg.quick else 'no'}")
    _log(f"  Output:     {cfg.output_dir}")
    if cfg.skip_phases:
        _log(f"  Skipping:   {', '.join(cfg.skip_phases)}")
    _log("")

    sweep = PhasedSweep(cfg)
    report = sweep.run()

    # Save results
    json_path, md_path = ReportGenerator.save(report, cfg.output_dir)
    _log(f"\nResults saved:")
    _log(f"  JSON:     {json_path}")
    _log(f"  Report:   {md_path}")

    # Print champion summary
    if report.champion:
        c = report.champion
        _log("")
        _log("═══════════════════════════════════════════")
        _log("  CHAMPION CONFIGURATION")
        _log("═══════════════════════════════════════════")
        _log(f"  Payload:     {c.config.payload_size} B")
        _log(f"  Batch:       {c.config.batch_size}")
        _log(f"  Concurrency: {c.config.concurrency}")
        _log(f"  Encoding:    {c.config.encoding}")
        _log(f"  Workload:    {c.config.workload}")
        _log(f"  Events/s:    {c.events_per_sec:,.0f}")
        _log(f"  P99 latency: {c.p99_ms:.1f} ms")
        _log(f"  Score:       {c.utilization_score:.3f}")
        _log("═══════════════════════════════════════════")


if __name__ == "__main__":
    cli()
