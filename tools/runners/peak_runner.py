"""
Peak performance runner - consolidates single and bulk peak testing.
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
from typing import Dict, List, Optional, Tuple

from output import ScenarioOutput
from .base import BaseRunner, RunnerConfig


class PeakRunner(BaseRunner):
    """
    Runner for peak performance tests.

    Supports both single-event and bulk API testing using native HTTP
    load testing tools (hey, wrk, or ab).
    """

    def __init__(self, mode: str, config: RunnerConfig):
        """
        Initialize peak runner.

        Args:
            mode: "single" for single-event API, "bulk" for bulk API
            config: Runner configuration
        """
        super().__init__(config)
        self._mode = mode
        self._tool = self._detect_tool()

    @property
    def name(self) -> str:
        return f"peak-{self._mode}"

    def _detect_tool(self) -> Optional[str]:
        """Detect available HTTP load testing tool."""
        for tool in ["hey", "wrk", "ab"]:
            if shutil.which(tool):
                return tool
        return None

    def run(self) -> ScenarioOutput:
        """Execute peak performance test."""
        if not self._tool:
            return self.create_output(
                name=self.name,
                success=False,
                duration_secs=0,
                error_messages=["No load testing tool found. Install with: brew install hey"],
            )

        if not self.health_check():
            return self.create_output(
                name=self.name,
                success=False,
                duration_secs=0,
                error_messages=["Health check failed"],
            )

        print(f"\n{'='*60}")
        print(f"Running {self.name} with {self._tool}")
        print(f"Concurrency levels: {self.config.concurrency_levels}")
        print(f"{'='*60}")

        stats_before = self.get_server_stats()
        zombi_pid = self._get_zombi_pid()
        results = []
        start_time = time.time()

        for concurrency in self.config.concurrency_levels:
            print(f"\nTesting with {concurrency} concurrent connections...")

            # Start CPU monitoring
            cpu_samples = []
            stop_event = None
            if zombi_pid:
                stop_event, cpu_samples = self._monitor_cpu(zombi_pid)
                time.sleep(0.5)

            # Run test
            if self._mode == "bulk":
                output, perf = self._run_bulk_test(concurrency)
            else:
                output, perf = self._run_single_test(concurrency)

            # Stop CPU monitoring
            if stop_event:
                stop_event.set()
                time.sleep(0.5)
                if cpu_samples:
                    perf["avg_cpu_percent"] = sum(cpu_samples) / len(cpu_samples)
                    perf["peak_cpu_percent"] = max(cpu_samples)

            results.append(perf)
            self._print_result(perf)
            time.sleep(2)

        stats_after = self.get_server_stats()
        duration = time.time() - start_time

        # Find peak result
        if self._mode == "bulk":
            peak = max(results, key=lambda r: r.get("events_per_sec", 0))
            peak_throughput = peak.get("events_per_sec", 0)
        else:
            peak = max(results, key=lambda r: r.get("requests_per_sec", 0))
            peak_throughput = peak.get("requests_per_sec", 0)

        # Calculate total bytes
        total_requests = sum(r.get("total_requests", 0) for r in results)
        bytes_total = total_requests * self.config.payload_size
        if self._mode == "bulk":
            bytes_total *= self.config.batch_size

        return self.create_output(
            name=self.name,
            success=True,
            duration_secs=duration,
            events_per_sec=peak_throughput,
            bytes_total=bytes_total,
            p50_ms=peak.get("p50_ms", 0),
            p95_ms=peak.get("p95_ms", 0),
            p99_ms=peak.get("p99_ms", 0),
            cpu_avg=peak.get("avg_cpu_percent", 0),
            cpu_peak=peak.get("peak_cpu_percent", 0),
            errors=sum(r.get("errors", 0) for r in results),
            details={
                "tool": self._tool,
                "optimal_concurrency": peak.get("concurrency", 0),
                "all_results": results,
                "server_stats_before": stats_before,
                "server_stats_after": stats_after,
            },
        )

    def _run_single_test(self, concurrency: int) -> Tuple[str, Dict]:
        """Run single-event throughput test."""
        payload = json.dumps({
            "payload": f"peak-perf-test-{int(time.time() * 1000)}",
            "timestamp_ms": int(time.time() * 1000)
        })

        if self._tool == "hey":
            return self._run_hey_test(
                f"{self.config.url}/tables/peak-single-{concurrency}",
                concurrency,
                self.config.duration_secs,
                payload,
            )
        elif self._tool == "wrk":
            return self._run_wrk_test(
                f"{self.config.url}/tables/peak-single-{concurrency}",
                concurrency,
                self.config.duration_secs,
                payload,
            )
        return "", {"concurrency": concurrency, "requests_per_sec": 0}

    def _run_bulk_test(self, concurrency: int) -> Tuple[str, Dict]:
        """Run bulk API throughput test."""
        records = []
        for i in range(self.config.batch_size):
            records.append({
                "payload": f"bulk-perf-{i}-{int(time.time() * 1000)}",
                "partition": i % 4,
                "timestamp_ms": int(time.time() * 1000)
            })
        payload = json.dumps({"records": records})

        if self._tool == "hey":
            output, perf = self._run_hey_test(
                f"{self.config.url}/tables/peak-bulk-{concurrency}/bulk",
                concurrency,
                self.config.duration_secs,
                payload,
                use_file=True,  # Bulk payloads are large, use file
            )
            # Calculate events per second
            perf["events_per_sec"] = perf.get("requests_per_sec", 0) * self.config.batch_size
            return output, perf

        return "", {"concurrency": concurrency, "requests_per_sec": 0, "events_per_sec": 0}

    def _run_hey_test(
        self,
        url: str,
        concurrency: int,
        duration: int,
        payload: str,
        use_file: bool = False,
    ) -> Tuple[str, Dict]:
        """Run hey load test."""
        payload_file = None

        try:
            cmd = [
                "hey",
                "-z", f"{duration}s",
                "-c", str(concurrency),
                "-m", "POST",
                "-T", "application/json",
            ]

            if use_file:
                # Write payload to temp file for large payloads
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    f.write(payload)
                    payload_file = f.name
                cmd.extend(["-D", payload_file])
            else:
                cmd.extend(["-d", payload])

            cmd.append(url)

            result = subprocess.run(cmd, capture_output=True, text=True)
            output = result.stdout + result.stderr

            perf = self._parse_hey_output(output)
            perf["concurrency"] = concurrency

            return output, perf

        finally:
            if payload_file and os.path.exists(payload_file):
                os.unlink(payload_file)

    def _run_wrk_test(
        self,
        url: str,
        concurrency: int,
        duration: int,
        payload: str,
    ) -> Tuple[str, Dict]:
        """Run wrk load test."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.lua', delete=False) as f:
            f.write(f'''
wrk.method = "POST"
wrk.body = '{payload}'
wrk.headers["Content-Type"] = "application/json"
''')
            lua_script = f.name

        try:
            cmd = [
                "wrk",
                "-t", str(min(concurrency, 16)),
                "-c", str(concurrency),
                "-d", f"{duration}s",
                "-s", lua_script,
                url
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)
            output = result.stdout + result.stderr

            perf = self._parse_wrk_output(output)
            perf["concurrency"] = concurrency

            return output, perf

        finally:
            os.unlink(lua_script)

    def _parse_hey_output(self, output: str) -> Dict:
        """Parse hey command output."""
        perf = {
            "total_requests": 0,
            "requests_per_sec": 0,
            "avg_latency_ms": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "max_latency_ms": 0,
            "errors": 0,
        }

        if m := re.search(r'Requests/sec:\s+([\d.]+)', output):
            perf["requests_per_sec"] = float(m.group(1))
        if m := re.search(r'Average:\s+([\d.]+)\s+secs', output):
            perf["avg_latency_ms"] = float(m.group(1)) * 1000
        if m := re.search(r'Slowest:\s+([\d.]+)\s+secs', output):
            perf["max_latency_ms"] = float(m.group(1)) * 1000

        # Parse latency distribution
        if m := re.search(r'50%.*?in\s+([\d.]+)\s+secs', output):
            perf["p50_ms"] = float(m.group(1)) * 1000
        if m := re.search(r'95%.*?in\s+([\d.]+)\s+secs', output):
            perf["p95_ms"] = float(m.group(1)) * 1000
        if m := re.search(r'99%.*?in\s+([\d.]+)\s+secs', output):
            perf["p99_ms"] = float(m.group(1)) * 1000

        # Parse status codes and count total requests
        for m in re.finditer(r'\[(\d+)\]\s+(\d+)\s+responses', output):
            code, count = int(m.group(1)), int(m.group(2))
            perf["total_requests"] += count
            if code >= 400:
                perf["errors"] += count

        return perf

    def _parse_wrk_output(self, output: str) -> Dict:
        """Parse wrk command output."""
        perf = {
            "total_requests": 0,
            "requests_per_sec": 0,
            "avg_latency_ms": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "max_latency_ms": 0,
            "errors": 0,
        }

        if m := re.search(r'(\d+)\s+requests\s+in\s+([\d.]+)([smh])', output):
            perf["total_requests"] = int(m.group(1))

        if m := re.search(r'Requests/sec:\s+([\d.]+)', output):
            perf["requests_per_sec"] = float(m.group(1))

        if m := re.search(r'Latency\s+([\d.]+)([um]?s)', output):
            latency = float(m.group(1))
            if m.group(2) == 'us':
                latency /= 1000
            elif m.group(2) == 's':
                latency *= 1000
            perf["avg_latency_ms"] = latency

        return perf

    def _get_zombi_pid(self) -> Optional[int]:
        """Get Zombi server PID for CPU monitoring."""
        try:
            result = subprocess.run(
                ["pgrep", "-x", "zombi"],
                capture_output=True, text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                return int(pids[0])

            # Fallback
            result = subprocess.run(
                ["ps", "-eo", "pid,comm"],
                capture_output=True, text=True
            )
            for line in result.stdout.strip().split('\n'):
                if 'zombi' in line and 'grep' not in line:
                    parts = line.strip().split()
                    if parts:
                        return int(parts[0])
        except Exception:
            pass
        return None

    def _monitor_cpu(self, pid: int, interval: float = 0.2) -> Tuple[threading.Event, List[float]]:
        """Start CPU monitoring in background thread."""
        stop_event = threading.Event()
        cpu_samples = []

        def _monitor():
            while not stop_event.is_set():
                try:
                    result = subprocess.run(
                        ["ps", "-p", str(pid), "-o", "%cpu"],
                        capture_output=True, text=True
                    )
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        if len(lines) > 1:
                            cpu_val = lines[1].strip()
                            if cpu_val:
                                cpu_samples.append(float(cpu_val))
                except Exception:
                    pass
                time.sleep(interval)

        thread = threading.Thread(target=_monitor, daemon=True)
        thread.start()
        return stop_event, cpu_samples

    def _print_result(self, perf: Dict):
        """Print a single test result."""
        cpu_str = f"{perf.get('avg_cpu_percent', 0):.0f}%" if perf.get('avg_cpu_percent') else "N/A"
        if self._mode == "bulk":
            print(f"  Concurrency: {perf.get('concurrency')}")
            print(f"  Requests/s: {perf.get('requests_per_sec', 0):,.0f}")
            print(f"  Events/s: {perf.get('events_per_sec', 0):,.0f}")
        else:
            print(f"  Concurrency: {perf.get('concurrency')}")
            print(f"  Requests/s: {perf.get('requests_per_sec', 0):,.0f}")
        print(f"  P50/P95/P99: {perf.get('p50_ms', 0):.1f}ms / {perf.get('p95_ms', 0):.1f}ms / {perf.get('p99_ms', 0):.1f}ms")
        print(f"  CPU: {cpu_str}")


def run_peak_test(
    mode: str,
    url: str,
    concurrency_levels: Optional[List[int]] = None,
    duration_secs: int = 30,
    batch_size: int = 100,
) -> ScenarioOutput:
    """
    Convenience function to run peak performance test.

    Args:
        mode: "single" or "bulk"
        url: Zombi server URL
        concurrency_levels: List of concurrency levels to test
        duration_secs: Duration per concurrency level
        batch_size: Batch size for bulk mode

    Returns:
        ScenarioOutput with test results
    """
    config = RunnerConfig(
        url=url,
        duration_secs=duration_secs,
        concurrency_levels=concurrency_levels or [50, 100, 200, 500],
        batch_size=batch_size,
    )

    runner = PeakRunner(mode, config)
    return runner.run()
