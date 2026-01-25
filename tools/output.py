"""
Output formatting and result comparison for zombi-load CLI.
"""

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ThroughputMetrics:
    """Standard throughput metrics."""
    events_per_sec: float = 0.0
    mb_per_sec: float = 0.0
    bytes_total: int = 0


@dataclass
class LatencyMetrics:
    """Standard latency metrics."""
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0


@dataclass
class CpuMetrics:
    """CPU utilization metrics."""
    avg_percent: float = 0.0
    peak_percent: float = 0.0


@dataclass
class ScenarioMetrics:
    """Standardized scenario metrics."""
    throughput: ThroughputMetrics = field(default_factory=ThroughputMetrics)
    latency: LatencyMetrics = field(default_factory=LatencyMetrics)
    cpu: CpuMetrics = field(default_factory=CpuMetrics)
    errors: int = 0
    error_rate: float = 0.0

    # Scenario-specific metrics
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScenarioOutput:
    """Output from a single scenario run."""
    name: str
    success: bool
    duration_secs: float
    metrics: ScenarioMetrics = field(default_factory=ScenarioMetrics)
    timeline: List[Dict] = field(default_factory=list)
    error_messages: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "success": self.success,
            "duration_secs": self.duration_secs,
            "metrics": {
                "throughput": {
                    "events_per_sec": self.metrics.throughput.events_per_sec,
                    "mb_per_sec": self.metrics.throughput.mb_per_sec,
                    "bytes_total": self.metrics.throughput.bytes_total,
                },
                "latency": {
                    "p50_ms": self.metrics.latency.p50_ms,
                    "p95_ms": self.metrics.latency.p95_ms,
                    "p99_ms": self.metrics.latency.p99_ms,
                    "min_ms": self.metrics.latency.min_ms,
                    "max_ms": self.metrics.latency.max_ms,
                },
                "cpu": {
                    "avg_percent": self.metrics.cpu.avg_percent,
                    "peak_percent": self.metrics.cpu.peak_percent,
                },
                "errors": self.metrics.errors,
                "error_rate": self.metrics.error_rate,
                "details": self.metrics.details,
            },
            "timeline": self.timeline,
            "error_messages": self.error_messages,
        }


@dataclass
class RunMetadata:
    """Metadata for a test run."""
    run_id: str
    profile: Optional[str] = None
    scenarios_requested: List[str] = field(default_factory=list)
    start_time: str = ""
    end_time: str = ""
    duration_secs: float = 0.0
    url: str = ""
    encoding: str = "proto"
    config_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "profile": self.profile,
            "scenarios_requested": self.scenarios_requested,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_secs": self.duration_secs,
            "url": self.url,
            "encoding": self.encoding,
            "config_path": self.config_path,
        }


@dataclass
class RunSummary:
    """Summary of a test run."""
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    peak_events_per_sec: float = 0.0
    peak_mb_per_sec: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "total": self.passed + self.failed + self.skipped,
            "peak_events_per_sec": self.peak_events_per_sec,
            "peak_mb_per_sec": self.peak_mb_per_sec,
        }


@dataclass
class RunOutput:
    """Complete output from a test run."""
    metadata: RunMetadata
    summary: RunSummary = field(default_factory=RunSummary)
    scenarios: List[ScenarioOutput] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "summary": self.summary.to_dict(),
            "scenarios": [s.to_dict() for s in self.scenarios],
        }


def generate_run_id() -> str:
    """Generate a unique run ID."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def save_results(output: RunOutput, output_dir: str, format: str = "json") -> str:
    """
    Save test results to files.

    Returns the path to the summary file.
    """
    # Create output directory structure
    run_dir = Path(output_dir) / f"{output.metadata.run_id}_{output.metadata.profile or 'custom'}"
    run_dir.mkdir(parents=True, exist_ok=True)
    scenarios_dir = run_dir / "scenarios"
    scenarios_dir.mkdir(exist_ok=True)

    # Save individual scenario results
    for scenario in output.scenarios:
        scenario_path = scenarios_dir / f"{scenario.name}.json"
        with open(scenario_path, "w") as f:
            json.dump(scenario.to_dict(), f, indent=2)

    # Save summary JSON
    summary_path = run_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(output.to_dict(), f, indent=2)

    # Generate markdown if requested
    if format in ("markdown", "both"):
        md_path = run_dir / "summary.md"
        with open(md_path, "w") as f:
            f.write(format_markdown(output))

    return str(summary_path)


def format_markdown(output: RunOutput) -> str:
    """Format results as markdown."""
    lines = []

    # Header
    lines.append(f"# Zombi Load Test Report")
    lines.append("")
    lines.append(f"**Run ID:** {output.metadata.run_id}")
    lines.append(f"**Profile:** {output.metadata.profile or 'custom'}")
    lines.append(f"**Target:** {output.metadata.url}")
    lines.append(f"**Date:** {output.metadata.start_time}")
    lines.append(f"**Duration:** {output.metadata.duration_secs:.1f}s")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Passed | {output.summary.passed} |")
    lines.append(f"| Failed | {output.summary.failed} |")
    lines.append(f"| Skipped | {output.summary.skipped} |")
    lines.append(f"| Peak Events/s | {output.summary.peak_events_per_sec:,.0f} |")
    lines.append(f"| Peak MB/s | {output.summary.peak_mb_per_sec:.2f} |")
    lines.append("")

    # Scenario results table
    lines.append("## Scenario Results")
    lines.append("")
    lines.append("| Scenario | Status | Events/s | MB/s | P95 (ms) | Errors |")
    lines.append("|----------|--------|----------|------|----------|--------|")

    for s in output.scenarios:
        status = "PASS" if s.success else "FAIL"
        lines.append(
            f"| {s.name} | {status} | "
            f"{s.metrics.throughput.events_per_sec:,.0f} | "
            f"{s.metrics.throughput.mb_per_sec:.2f} | "
            f"{s.metrics.latency.p95_ms:.1f} | "
            f"{s.metrics.errors} |"
        )
    lines.append("")

    # Detailed results
    lines.append("## Detailed Results")
    lines.append("")

    for s in output.scenarios:
        lines.append(f"### {s.name}")
        lines.append("")
        status = "PASS" if s.success else "FAIL"
        lines.append(f"**Status:** {status}")
        lines.append(f"**Duration:** {s.duration_secs:.1f}s")
        lines.append("")

        lines.append("**Throughput:**")
        lines.append(f"- Events/s: {s.metrics.throughput.events_per_sec:,.0f}")
        lines.append(f"- MB/s: {s.metrics.throughput.mb_per_sec:.2f}")
        lines.append(f"- Total bytes: {s.metrics.throughput.bytes_total:,}")
        lines.append("")

        lines.append("**Latency:**")
        lines.append(f"- P50: {s.metrics.latency.p50_ms:.1f}ms")
        lines.append(f"- P95: {s.metrics.latency.p95_ms:.1f}ms")
        lines.append(f"- P99: {s.metrics.latency.p99_ms:.1f}ms")
        lines.append("")

        if s.metrics.details:
            lines.append("**Details:**")
            for k, v in s.metrics.details.items():
                lines.append(f"- {k}: {v}")
            lines.append("")

        if s.error_messages:
            lines.append("**Errors:**")
            for err in s.error_messages[:5]:
                lines.append(f"- {err}")
            lines.append("")

    return "\n".join(lines)


def print_summary(output: RunOutput):
    """Print a concise summary to console."""
    print()
    print("=" * 70)
    print("ZOMBI LOAD TEST SUMMARY")
    print("=" * 70)
    print(f"Run ID:   {output.metadata.run_id}")
    print(f"Profile:  {output.metadata.profile or 'custom'}")
    print(f"Target:   {output.metadata.url}")
    print(f"Duration: {output.metadata.duration_secs:.1f}s")
    print()

    # Results table
    print(f"{'Scenario':<20} {'Status':<8} {'Events/s':>12} {'MB/s':>8} {'P95':>10}")
    print("-" * 70)

    for s in output.scenarios:
        status = "PASS" if s.success else "FAIL"
        print(
            f"{s.name:<20} {status:<8} "
            f"{s.metrics.throughput.events_per_sec:>12,.0f} "
            f"{s.metrics.throughput.mb_per_sec:>8.2f} "
            f"{s.metrics.latency.p95_ms:>9.1f}ms"
        )

    print("-" * 70)
    print()

    # Summary
    total = output.summary.passed + output.summary.failed + output.summary.skipped
    print(f"Results: {output.summary.passed}/{total} passed, {output.summary.failed} failed, {output.summary.skipped} skipped")
    print(f"Peak:    {output.summary.peak_events_per_sec:,.0f} events/s, {output.summary.peak_mb_per_sec:.2f} MB/s")
    print()

    overall = "PASSED" if output.summary.failed == 0 else "FAILED"
    print(f"Overall: {overall}")
    print("=" * 70)


def load_results(path: str) -> RunOutput:
    """Load results from a JSON file."""
    with open(path) as f:
        data = json.load(f)

    metadata = RunMetadata(
        run_id=data["metadata"]["run_id"],
        profile=data["metadata"].get("profile"),
        scenarios_requested=data["metadata"].get("scenarios_requested", []),
        start_time=data["metadata"].get("start_time", ""),
        end_time=data["metadata"].get("end_time", ""),
        duration_secs=data["metadata"].get("duration_secs", 0),
        url=data["metadata"].get("url", ""),
        encoding=data["metadata"].get("encoding", "proto"),
        config_path=data["metadata"].get("config_path"),
    )

    summary = RunSummary(
        passed=data["summary"]["passed"],
        failed=data["summary"]["failed"],
        skipped=data["summary"].get("skipped", 0),
        peak_events_per_sec=data["summary"].get("peak_events_per_sec", 0),
        peak_mb_per_sec=data["summary"].get("peak_mb_per_sec", 0),
    )

    scenarios = []
    for s in data.get("scenarios", []):
        metrics = ScenarioMetrics(
            throughput=ThroughputMetrics(
                events_per_sec=s["metrics"]["throughput"]["events_per_sec"],
                mb_per_sec=s["metrics"]["throughput"]["mb_per_sec"],
                bytes_total=s["metrics"]["throughput"].get("bytes_total", 0),
            ),
            latency=LatencyMetrics(
                p50_ms=s["metrics"]["latency"]["p50_ms"],
                p95_ms=s["metrics"]["latency"]["p95_ms"],
                p99_ms=s["metrics"]["latency"]["p99_ms"],
                min_ms=s["metrics"]["latency"].get("min_ms", 0),
                max_ms=s["metrics"]["latency"].get("max_ms", 0),
            ),
            cpu=CpuMetrics(
                avg_percent=s["metrics"]["cpu"].get("avg_percent", 0),
                peak_percent=s["metrics"]["cpu"].get("peak_percent", 0),
            ),
            errors=s["metrics"].get("errors", 0),
            error_rate=s["metrics"].get("error_rate", 0),
            details=s["metrics"].get("details", {}),
        )

        scenarios.append(ScenarioOutput(
            name=s["name"],
            success=s["success"],
            duration_secs=s["duration_secs"],
            metrics=metrics,
            timeline=s.get("timeline", []),
            error_messages=s.get("error_messages", []),
        ))

    return RunOutput(metadata=metadata, summary=summary, scenarios=scenarios)


@dataclass
class ComparisonResult:
    """Result of comparing two test runs."""
    baseline_id: str
    current_id: str
    improvements: List[Dict[str, Any]] = field(default_factory=list)
    regressions: List[Dict[str, Any]] = field(default_factory=list)
    unchanged: List[Dict[str, Any]] = field(default_factory=list)


def compare_results(baseline_path: str, current_path: str, threshold_pct: float = 5.0) -> ComparisonResult:
    """
    Compare two test runs and identify regressions/improvements.

    Args:
        baseline_path: Path to baseline summary.json
        current_path: Path to current summary.json
        threshold_pct: Percentage threshold for significance (default 5%)

    Returns:
        ComparisonResult with categorized differences
    """
    baseline = load_results(baseline_path)
    current = load_results(current_path)

    result = ComparisonResult(
        baseline_id=baseline.metadata.run_id,
        current_id=current.metadata.run_id,
    )

    # Build lookup for baseline scenarios
    baseline_scenarios = {s.name: s for s in baseline.scenarios}

    for curr_scenario in current.scenarios:
        if curr_scenario.name not in baseline_scenarios:
            continue

        base_scenario = baseline_scenarios[curr_scenario.name]

        # Compare throughput
        base_eps = base_scenario.metrics.throughput.events_per_sec
        curr_eps = curr_scenario.metrics.throughput.events_per_sec

        if base_eps > 0:
            pct_change = ((curr_eps - base_eps) / base_eps) * 100

            comparison = {
                "scenario": curr_scenario.name,
                "metric": "events_per_sec",
                "baseline": base_eps,
                "current": curr_eps,
                "change_pct": pct_change,
            }

            if pct_change > threshold_pct:
                result.improvements.append(comparison)
            elif pct_change < -threshold_pct:
                result.regressions.append(comparison)
            else:
                result.unchanged.append(comparison)

        # Compare P95 latency (lower is better)
        base_p95 = base_scenario.metrics.latency.p95_ms
        curr_p95 = curr_scenario.metrics.latency.p95_ms

        if base_p95 > 0:
            pct_change = ((curr_p95 - base_p95) / base_p95) * 100

            comparison = {
                "scenario": curr_scenario.name,
                "metric": "p95_ms",
                "baseline": base_p95,
                "current": curr_p95,
                "change_pct": pct_change,
            }

            # For latency, lower is better, so flip the logic
            if pct_change < -threshold_pct:
                result.improvements.append(comparison)
            elif pct_change > threshold_pct:
                result.regressions.append(comparison)
            else:
                result.unchanged.append(comparison)

    return result


def print_comparison(result: ComparisonResult):
    """Print comparison results to console."""
    print()
    print("=" * 70)
    print("COMPARISON REPORT")
    print("=" * 70)
    print(f"Baseline: {result.baseline_id}")
    print(f"Current:  {result.current_id}")
    print()

    if result.regressions:
        print("REGRESSIONS:")
        for r in result.regressions:
            direction = "slower" if r["metric"] == "p95_ms" else "lower"
            print(f"  {r['scenario']}.{r['metric']}: {r['baseline']:.1f} -> {r['current']:.1f} ({r['change_pct']:+.1f}% {direction})")
        print()

    if result.improvements:
        print("IMPROVEMENTS:")
        for i in result.improvements:
            direction = "faster" if i["metric"] == "p95_ms" else "higher"
            print(f"  {i['scenario']}.{i['metric']}: {i['baseline']:.1f} -> {i['current']:.1f} ({i['change_pct']:+.1f}% {direction})")
        print()

    if not result.regressions and not result.improvements:
        print("No significant changes detected.")
        print()

    summary = "REGRESSION DETECTED" if result.regressions else "NO REGRESSIONS"
    print(f"Summary: {summary}")
    print("=" * 70)
