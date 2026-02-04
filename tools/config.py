"""
Configuration management for zombi-load CLI.

Loads configuration from YAML files and environment variables.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class TargetConfig:
    """Target server configuration."""
    url: str = "http://localhost:8080"
    health_timeout: int = 30

    def __post_init__(self):
        self.url = self.url.rstrip("/")


@dataclass
class ColdStorageConfig:
    """Cold storage (S3/Iceberg) configuration."""
    enabled: bool = False
    s3_bucket: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_region: str = "us-east-1"


@dataclass
class SettingsConfig:
    """Global test settings."""
    encoding: str = "proto"
    warmup_secs: int = 5
    default_duration: int = 60
    payload_size: int = 100
    num_workers: int = 10

    def __post_init__(self):
        if self.encoding not in ("proto", "json"):
            raise ValueError(f"Invalid encoding: {self.encoding}")


@dataclass
class ProfileConfig:
    """Profile configuration."""
    scenarios: List[str]
    duration_multiplier: float = 1.0
    concurrency_levels: Optional[List[int]] = None
    workers_override: Optional[int] = None


@dataclass
class OutputConfig:
    """Output configuration."""
    directory: str = "./results"
    format: str = "json"

    def __post_init__(self):
        if self.format not in ("json", "markdown", "both"):
            raise ValueError(f"Invalid output format: {self.format}")


@dataclass
class Config:
    """Complete configuration for zombi-load."""
    target: TargetConfig = field(default_factory=TargetConfig)
    cold_storage: ColdStorageConfig = field(default_factory=ColdStorageConfig)
    settings: SettingsConfig = field(default_factory=SettingsConfig)
    profiles: Dict[str, ProfileConfig] = field(default_factory=dict)
    output: OutputConfig = field(default_factory=OutputConfig)

    # Metadata
    config_path: Optional[str] = None


# Default profiles
DEFAULT_PROFILES = {
    "quick": ProfileConfig(
        scenarios=["single-write", "read-throughput", "consistency"],
        duration_multiplier=0.5,
    ),
    "full": ProfileConfig(
        scenarios=[
            "single-write", "bulk-write", "read-throughput",
            "write-read-lag", "mixed-workload", "backpressure",
            "cold-storage", "consistency",
        ],
        duration_multiplier=1.0,
    ),
    "stress": ProfileConfig(
        scenarios=["single-write", "mixed-workload", "backpressure", "consistency"],
        duration_multiplier=4.0,
    ),
    "peak": ProfileConfig(
        scenarios=["peak-single", "peak-bulk"],
        duration_multiplier=0.5,
        concurrency_levels=[50, 100, 200, 500],
    ),
    "iceberg": ProfileConfig(
        scenarios=["cold-storage", "iceberg-read"],
        duration_multiplier=1.0,
    ),
    "baseline": ProfileConfig(
        scenarios=[
            "peak-single", "peak-bulk",
            "single-write", "bulk-write",
            "read-throughput", "write-read-lag",
            "mixed-workload", "backpressure",
            "cold-storage", "consistency",
        ],
        duration_multiplier=3.0,
        concurrency_levels=[50, 100, 200],
    ),
}

# Scenario metadata
SCENARIOS = {
    "single-write": {
        "description": "Single event write throughput",
        "type": "write",
        "metrics": ["events_per_sec", "mb_per_sec", "p50_ms", "p95_ms", "p99_ms"],
        "cold_storage_required": False,
    },
    "bulk-write": {
        "description": "Bulk API write throughput",
        "type": "write",
        "metrics": ["events_per_sec", "mb_per_sec", "batch_req_per_sec"],
        "cold_storage_required": False,
    },
    "read-throughput": {
        "description": "Read throughput from hot storage",
        "type": "read",
        "metrics": ["records_per_sec", "mb_per_sec", "p50_ms", "p95_ms"],
        "cold_storage_required": False,
    },
    "write-read-lag": {
        "description": "Write to read visibility latency",
        "type": "lag",
        "metrics": ["p50_ms", "p95_ms", "p99_ms"],
        "cold_storage_required": False,
    },
    "mixed-workload": {
        "description": "Concurrent read/write workload",
        "type": "mixed",
        "metrics": ["read_events_per_sec", "write_events_per_sec", "mb_per_sec"],
        "cold_storage_required": False,
    },
    "backpressure": {
        "description": "Overload testing with 503 verification",
        "type": "stress",
        "metrics": ["error_rate", "recovery_time_sec"],
        "cold_storage_required": False,
    },
    "cold-storage": {
        "description": "Cold storage flush verification",
        "type": "iceberg",
        "metrics": ["files_created", "rows_written", "file_sizes_mb"],
        "cold_storage_required": True,
    },
    "peak-single": {
        "description": "Peak single-event throughput (requires hey/wrk)",
        "type": "peak",
        "metrics": ["max_req_per_sec", "max_mb_per_sec", "optimal_concurrency"],
        "cold_storage_required": False,
    },
    "peak-bulk": {
        "description": "Peak bulk-event throughput (requires hey/wrk)",
        "type": "peak",
        "metrics": ["max_events_per_sec", "max_mb_per_sec", "optimal_concurrency"],
        "cold_storage_required": False,
    },
    "iceberg-read": {
        "description": "Cold storage read performance",
        "type": "iceberg",
        "metrics": ["query_latency_ms", "mb_scanned"],
        "cold_storage_required": True,
    },
    "consistency": {
        "description": "Data consistency invariant verification",
        "type": "invariant",
        "metrics": ["inv2_violations", "inv3_violations"],
        "cold_storage_required": False,
    },
    # Map legacy scenario names to new names
    "multi-producer": {
        "description": "Multi-producer write throughput (alias for single-write)",
        "type": "write",
        "metrics": ["events_per_sec", "mb_per_sec", "p50_ms", "p95_ms", "p99_ms"],
        "cold_storage_required": False,
        "alias_for": "single-write",
    },
    "consumer": {
        "description": "Consumer offset tracking and order verification",
        "type": "read",
        "metrics": ["records_per_sec", "p50_ms", "p95_ms"],
        "cold_storage_required": False,
    },
    "mixed": {
        "description": "Alias for mixed-workload",
        "type": "mixed",
        "metrics": ["read_events_per_sec", "write_events_per_sec", "mb_per_sec"],
        "cold_storage_required": False,
        "alias_for": "mixed-workload",
    },
}


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from YAML file and environment variables.

    Priority (highest to lowest):
    1. Environment variables (ZOMBI_*)
    2. Config file
    3. Defaults
    """
    config = Config()

    # Find config file
    if config_path is None:
        # Search order: current dir, tools dir, home dir
        search_paths = [
            Path.cwd() / "zombi-load.yaml",
            Path.cwd() / "zombi-load.yml",
            Path(__file__).parent / "zombi-load.yaml",
            Path.home() / ".zombi-load.yaml",
        ]
        for path in search_paths:
            if path.exists():
                config_path = str(path)
                break

    # Load from file
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
        config = _parse_config(data)
        config.config_path = config_path

    # Apply environment variable overrides
    config = _apply_env_overrides(config)

    # Apply default profiles if none defined
    if not config.profiles:
        config.profiles = DEFAULT_PROFILES

    return config


def _parse_config(data: Dict[str, Any]) -> Config:
    """Parse configuration from dictionary."""
    config = Config()

    # Target
    if "target" in data:
        t = data["target"]
        config.target = TargetConfig(
            url=t.get("url", config.target.url),
            health_timeout=t.get("health_timeout", config.target.health_timeout),
        )

    # Cold storage
    if "cold_storage" in data:
        cs = data["cold_storage"]
        config.cold_storage = ColdStorageConfig(
            enabled=cs.get("enabled", False),
            s3_bucket=cs.get("s3_bucket"),
            s3_endpoint=cs.get("s3_endpoint"),
            s3_region=cs.get("s3_region", "us-east-1"),
        )

    # Settings
    if "settings" in data:
        s = data["settings"]
        config.settings = SettingsConfig(
            encoding=s.get("encoding", "proto"),
            warmup_secs=s.get("warmup_secs", 5),
            default_duration=s.get("default_duration", 60),
            payload_size=s.get("payload_size", 100),
            num_workers=s.get("num_workers", 10),
        )

    # Profiles
    if "profiles" in data:
        for name, p in data["profiles"].items():
            config.profiles[name] = ProfileConfig(
                scenarios=p.get("scenarios", []),
                duration_multiplier=p.get("duration_multiplier", 1.0),
                concurrency_levels=p.get("concurrency_levels"),
                workers_override=p.get("workers_override"),
            )

    # Output
    if "output" in data:
        o = data["output"]
        config.output = OutputConfig(
            directory=o.get("directory", "./results"),
            format=o.get("format", "json"),
        )

    return config


def _apply_env_overrides(config: Config) -> Config:
    """Apply environment variable overrides."""
    # Target URL
    if url := os.environ.get("ZOMBI_URL"):
        config.target.url = url.rstrip("/")

    # S3 settings
    if bucket := os.environ.get("ZOMBI_S3_BUCKET"):
        config.cold_storage.s3_bucket = bucket
        config.cold_storage.enabled = True
    if endpoint := os.environ.get("ZOMBI_S3_ENDPOINT"):
        config.cold_storage.s3_endpoint = endpoint
    if region := os.environ.get("ZOMBI_S3_REGION"):
        config.cold_storage.s3_region = region

    # Encoding
    if encoding := os.environ.get("ZOMBI_ENCODING"):
        config.settings.encoding = encoding

    # Output directory
    if output_dir := os.environ.get("ZOMBI_OUTPUT_DIR"):
        config.output.directory = output_dir

    return config


def get_scenario_info(name: str) -> Optional[Dict[str, Any]]:
    """Get scenario metadata by name."""
    return SCENARIOS.get(name)


def list_scenarios() -> List[str]:
    """List all available scenario names."""
    return sorted([k for k, v in SCENARIOS.items() if "alias_for" not in v])


def list_profiles() -> List[str]:
    """List all available profile names."""
    return list(DEFAULT_PROFILES.keys())


def resolve_scenario_name(name: str) -> str:
    """Resolve scenario aliases to canonical names."""
    info = SCENARIOS.get(name, {})
    return info.get("alias_for", name)
