#!/usr/bin/env python3
"""
zombi-load: Unified Load Testing CLI for Zombi

A single command-line tool for running any test scenario with consistent
configuration and standardized output.

Usage:
    # Quick sanity check
    zombi-load run --profile quick

    # Full test suite
    zombi-load run --profile full --url http://ec2:8080

    # Specific scenario with overrides
    zombi-load run --scenario single-write --workers 100 --duration 120

    # Compare results
    zombi-load compare results/run1/summary.json results/run2/summary.json

    # List available scenarios and profiles
    zombi-load list scenarios
    zombi-load list profiles
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Ensure tools directory is in path
tools_dir = Path(__file__).parent
sys.path.insert(0, str(tools_dir))

try:
    import click
except ImportError:
    print("Error: click is required. Install with: pip install click")
    sys.exit(1)

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from config import (
    Config,
    load_config,
    list_scenarios,
    list_profiles,
    get_scenario_info,
    resolve_scenario_name,
    DEFAULT_PROFILES,
    SCENARIOS,
)
from output import (
    RunOutput,
    RunMetadata,
    RunSummary,
    ScenarioOutput,
    generate_run_id,
    save_results,
    print_summary,
    compare_results,
    print_comparison,
)
from runners import ScenarioRunner, PeakRunner, RunnerConfig


@click.group()
@click.option("--config", "-c", "config_path", help="Path to config file (zombi-load.yaml)")
@click.option("--url", "-u", help="Zombi server URL (overrides config)")
@click.option("--quiet", "-q", is_flag=True, help="Reduce output verbosity")
@click.pass_context
def cli(ctx, config_path: Optional[str], url: Optional[str], quiet: bool):
    """zombi-load: Unified Load Testing CLI for Zombi"""
    ctx.ensure_object(dict)

    # Load configuration
    config = load_config(config_path)

    # Apply URL override
    if url:
        config.target.url = url.rstrip("/")

    ctx.obj["config"] = config
    ctx.obj["quiet"] = quiet


@cli.command()
@click.option("--profile", "-p", help="Test profile to run (quick, full, stress, peak, iceberg)")
@click.option("--scenario", "-s", multiple=True, help="Specific scenario(s) to run")
@click.option("--duration", "-d", type=int, help="Override test duration (seconds)")
@click.option("--workers", "-w", type=int, help="Override number of workers")
@click.option("--encoding", type=click.Choice(["proto", "json"]), help="Event encoding format")
@click.option("--output-dir", "-o", help="Output directory for results")
@click.option("--no-save", is_flag=True, help="Don't save results to files")
@click.pass_context
def run(
    ctx,
    profile: Optional[str],
    scenario: tuple,
    duration: Optional[int],
    workers: Optional[int],
    encoding: Optional[str],
    output_dir: Optional[str],
    no_save: bool,
):
    """Run load tests."""
    config: Config = ctx.obj["config"]
    quiet = ctx.obj["quiet"]

    # Determine scenarios to run
    scenarios_to_run = []

    if profile:
        profile_config = config.profiles.get(profile) or DEFAULT_PROFILES.get(profile)
        if not profile_config:
            click.echo(f"Error: Unknown profile '{profile}'", err=True)
            click.echo(f"Available profiles: {', '.join(list_profiles())}", err=True)
            sys.exit(1)
        scenarios_to_run = profile_config.scenarios
        duration_multiplier = profile_config.duration_multiplier
        concurrency_levels = profile_config.concurrency_levels
    elif scenario:
        scenarios_to_run = list(scenario)
        duration_multiplier = 1.0
        concurrency_levels = None
    else:
        click.echo("Error: Specify either --profile or --scenario", err=True)
        sys.exit(1)

    # Apply overrides
    base_duration = duration or config.settings.default_duration
    num_workers = workers or config.settings.num_workers
    event_encoding = encoding or config.settings.encoding
    result_output_dir = output_dir or config.output.directory

    # Print banner
    if not quiet:
        click.echo()
        click.echo("=" * 70)
        click.echo("ZOMBI LOAD TEST")
        click.echo("=" * 70)
        click.echo(f"Target:     {config.target.url}")
        click.echo(f"Profile:    {profile or 'custom'}")
        click.echo(f"Scenarios:  {', '.join(scenarios_to_run)}")
        click.echo(f"Duration:   {base_duration}s (x{duration_multiplier})")
        click.echo(f"Workers:    {num_workers}")
        click.echo(f"Encoding:   {event_encoding}")
        if config.cold_storage.enabled:
            click.echo(f"S3 Bucket:  {config.cold_storage.s3_bucket}")
        click.echo("=" * 70)

    # Check server health
    import requests
    try:
        r = requests.get(f"{config.target.url}/health", timeout=config.target.health_timeout)
        if r.status_code != 200:
            click.echo(f"Error: Server health check failed (status {r.status_code})", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"Error: Cannot connect to {config.target.url}: {e}", err=True)
        sys.exit(1)

    if not quiet:
        click.echo(f"Connected to {config.target.url}")

    # Initialize run output
    run_id = generate_run_id()
    start_time = datetime.now()

    run_output = RunOutput(
        metadata=RunMetadata(
            run_id=run_id,
            profile=profile,
            scenarios_requested=scenarios_to_run,
            start_time=start_time.isoformat(),
            url=config.target.url,
            encoding=event_encoding,
            config_path=config.config_path,
        ),
        summary=RunSummary(),
    )

    # Run scenarios
    for scenario_name in scenarios_to_run:
        # Resolve alias
        canonical_name = resolve_scenario_name(scenario_name)

        # Check if scenario requires cold storage
        scenario_info = get_scenario_info(canonical_name)
        if scenario_info and scenario_info.get("cold_storage_required"):
            if not config.cold_storage.enabled:
                if not quiet:
                    click.echo(f"\nSkipping {scenario_name}: S3 not configured")
                run_output.summary.skipped += 1
                continue

        # Calculate duration for this scenario
        scenario_duration = int(base_duration * duration_multiplier)

        # Build runner config
        runner_config = RunnerConfig(
            url=config.target.url,
            encoding=event_encoding,
            duration_secs=scenario_duration,
            warmup_secs=config.settings.warmup_secs,
            num_workers=num_workers,
            payload_size=config.settings.payload_size,
            s3_bucket=config.cold_storage.s3_bucket if config.cold_storage.enabled else None,
            s3_endpoint=config.cold_storage.s3_endpoint,
            s3_region=config.cold_storage.s3_region,
            concurrency_levels=concurrency_levels or [50, 100, 200, 500],
        )

        # Select runner
        if canonical_name == "peak-single":
            runner = PeakRunner("single", runner_config)
        elif canonical_name == "peak-bulk":
            runner = PeakRunner("bulk", runner_config)
        else:
            runner = ScenarioRunner(canonical_name, runner_config)

        # Run scenario
        try:
            result = runner.run()
            run_output.scenarios.append(result)

            if result.success:
                run_output.summary.passed += 1
            else:
                run_output.summary.failed += 1

            # Update peak metrics
            if result.metrics.throughput.events_per_sec > run_output.summary.peak_events_per_sec:
                run_output.summary.peak_events_per_sec = result.metrics.throughput.events_per_sec
            if result.metrics.throughput.mb_per_sec > run_output.summary.peak_mb_per_sec:
                run_output.summary.peak_mb_per_sec = result.metrics.throughput.mb_per_sec

        except Exception as e:
            click.echo(f"\nError running {scenario_name}: {e}", err=True)
            run_output.summary.failed += 1

    # Finalize
    end_time = datetime.now()
    run_output.metadata.end_time = end_time.isoformat()
    run_output.metadata.duration_secs = (end_time - start_time).total_seconds()

    # Print summary
    print_summary(run_output)

    # Save results
    if not no_save:
        summary_path = save_results(
            run_output,
            result_output_dir,
            config.output.format,
        )
        click.echo(f"\nResults saved to: {summary_path}")

    # Exit with appropriate code
    sys.exit(0 if run_output.summary.failed == 0 else 1)


@cli.command("list")
@click.argument("what", type=click.Choice(["scenarios", "profiles"]))
@click.pass_context
def list_items(ctx, what: str):
    """List available scenarios or profiles."""
    if what == "scenarios":
        click.echo("\nAvailable Scenarios:")
        click.echo("-" * 60)
        for name in list_scenarios():
            info = get_scenario_info(name)
            if info:
                cold = " [S3]" if info.get("cold_storage_required") else ""
                click.echo(f"  {name:<20} {info['description']}{cold}")
        click.echo()
        click.echo("  [S3] = Requires S3/cold storage configuration")

    elif what == "profiles":
        click.echo("\nAvailable Profiles:")
        click.echo("-" * 60)
        for name, profile in DEFAULT_PROFILES.items():
            scenarios = ", ".join(profile.scenarios[:3])
            if len(profile.scenarios) > 3:
                scenarios += f" (+{len(profile.scenarios) - 3} more)"
            click.echo(f"  {name:<12} {scenarios}")
            click.echo(f"             Duration multiplier: {profile.duration_multiplier}x")

    click.echo()


@cli.command()
@click.argument("baseline", type=click.Path(exists=True))
@click.argument("current", type=click.Path(exists=True))
@click.option("--threshold", "-t", type=float, default=5.0, help="Significance threshold (%)")
@click.pass_context
def compare(ctx, baseline: str, current: str, threshold: float):
    """Compare two test runs for regression detection."""
    result = compare_results(baseline, current, threshold)
    print_comparison(result)

    # Exit with error if regressions found
    sys.exit(1 if result.regressions else 0)


@cli.command()
@click.pass_context
def version(ctx):
    """Show version information."""
    click.echo("zombi-load 1.0.0")
    click.echo("Unified Load Testing CLI for Zombi")


@cli.command()
@click.pass_context
def init(ctx):
    """Create a sample configuration file."""
    sample_config = """\
# zombi-load configuration
# See: https://github.com/yourorg/zombi/tree/main/tools

target:
  url: http://localhost:8080
  health_timeout: 30

cold_storage:
  enabled: false
  s3_bucket: zombi-events
  s3_endpoint: http://localhost:9000  # For MinIO
  s3_region: us-east-1

settings:
  encoding: proto  # proto or json
  warmup_secs: 5
  default_duration: 60
  payload_size: 100
  num_workers: 10

# Custom profiles (optional - defaults are provided)
# profiles:
#   my-profile:
#     scenarios: [single-write, read-throughput]
#     duration_multiplier: 2.0

output:
  directory: ./results
  format: json  # json, markdown, or both
"""

    output_path = Path.cwd() / "zombi-load.yaml"
    if output_path.exists():
        if not click.confirm(f"{output_path} already exists. Overwrite?"):
            click.echo("Aborted.")
            return

    with open(output_path, "w") as f:
        f.write(sample_config)

    click.echo(f"Created {output_path}")
    click.echo("\nEdit this file to configure your tests, then run:")
    click.echo("  python tools/zombi_load.py run --profile quick")


def main():
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
