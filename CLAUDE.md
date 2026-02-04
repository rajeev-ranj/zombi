# CLAUDE.md

> Instructions for Claude Code when working on Zombi.

---

## Project Overview

Zombi is an Iceberg-first event streaming system written in Rust. It provides the lowest-cost path from events to Iceberg, with optional streaming support via RocksDB buffering.

**Current Version:** See `Cargo.toml`

---

## Code Style

### Rust Conventions
- Use `rustfmt` defaults (run `cargo fmt`)
- All warnings as errors (`cargo clippy -- -D warnings`)
- No `unwrap()` or `expect()` in library code — use `?` or explicit error handling
- Use `thiserror` for error types
- Use `tracing` for logging, not `println!`

### Naming
- Types: `PascalCase`
- Functions/methods: `snake_case`
- Constants: `SCREAMING_SNAKE_CASE`
- Modules match file names

---

## Architecture Rules

### Module Boundaries

```
src/
├── contracts/   # Trait definitions — STABLE, change rarely
├── storage/     # Storage implementations (RocksDB, S3, Iceberg)
├── flusher/     # Background flush logic
├── api/         # HTTP handlers
└── metrics/     # Metrics collection and reporting
```

**Rules:**
- `api/` can import from `contracts/`, `storage/`, `metrics/`
- `storage/` can import from `contracts/`
- `flusher/` can import from `contracts/`, `storage/`
- `metrics/` can import from `contracts/`
- `contracts/` imports from NOTHING (except std)
- Never import from `api/` into `storage/`

### Key Storage Files

| File | Purpose |
|------|---------|
| `storage/rocksdb.rs` | Hot storage (RocksDB) |
| `storage/s3.rs` | Cold storage (S3 JSON) |
| `storage/iceberg_storage.rs` | Cold storage (Iceberg/Parquet) |
| `storage/cold_storage_backend.rs` | Unified backend enum |
| `storage/parquet.rs` | Parquet writing utilities |
| `storage/iceberg.rs` | Iceberg metadata structures |
| `storage/compaction.rs` | File compaction logic |
| `storage/catalog.rs` | Iceberg catalog registration |
| `storage/retry.rs` | Retry logic for storage operations |
| `storage/payload_extractor.rs` | Event payload field extraction |
| `storage/sequence.rs` | Sequence number generation |

### Trait-First Development

Always implement traits from `contracts/`, don't invent new public APIs:

```rust
// contracts/storage.rs
pub trait HotStorage: Send + Sync {
    fn write(&self, topic: &str, partition: u32, ...) -> Result<u64, StorageError>;
    fn read(&self, topic: &str, partition: u32, offset: u64, limit: usize) -> Result<Vec<StoredEvent>, StorageError>;
}

// storage/rocksdb.rs
impl HotStorage for RocksDbStorage {
    // Implement exactly the trait, nothing more
}
```

---

## Invariants

See `SPEC.md` Invariants section. Run `cargo test` to verify all invariants hold.

---

## Testing Requirements

### Every PR Must Have:
1. Unit tests for new pure functions
2. Integration test if new I/O path
3. Property test if touching invariants

### Test Naming
```rust
#[test]
fn write_returns_monotonic_sequence() { ... }

#[test]
fn read_returns_empty_for_future_offset() { ... }

#[tokio::test]
async fn flush_writes_to_s3() { ... }
```

### Running Tests
```bash
# All tests
cargo test

# Specific test file
cargo test --test integration_tests

# With verbose output
cargo test -- --nocapture
```

### Load Testing

**Two entry points:**

#### Local (docker-compose)
```bash
docker-compose -f docker-compose.monitoring.yml up --build -d
# Grafana: http://localhost:3000 (admin/admin)

python tools/zombi_load.py run --profile quick --url http://localhost:8080
python tools/zombi_load.py run --profile full
python tools/zombi_load.py list   # show profiles and scenarios
```

#### AWS (auto-deploys infra + local Grafana)
```bash
# Full baseline with cleanup (~75 min)
./tools/aws_peak_performance.sh baseline yes t3.micro
# Grafana auto-starts at http://localhost:3000 (no SSH needed)

# Keep infra running for inspection
./tools/aws_peak_performance.sh baseline no t3.micro

# Other modes: quick (~10 min), sustained (~12 min), comprehensive (~40 min)
./tools/aws_peak_performance.sh quick yes t3.micro
```

**Prerequisites:** Terraform, AWS CLI, Docker, SSH key at `~/.ssh/id_ed25519`

Results saved to `tools/results/<branch>/<timestamp>/` with auto-comparison across branches.

### Tools Reference

| File | Purpose |
|------|---------|
| `tools/zombi_load.py` | **Local CLI** — profiles: quick, full, stress, peak, baseline |
| `tools/aws_peak_performance.sh` | **AWS CLI** — deploy, test, Grafana, compare, cleanup |
| `tools/config.py` | Configuration loader (profiles, scenarios) |
| `tools/output.py` | Result formatting and comparison |
| `tools/peak_performance.py` | Single-write throughput (used by AWS scripts on EC2) |
| `tools/peak_performance_bulk.py` | Bulk-write throughput (used by AWS scripts on EC2) |
| `tools/benchmark.py` | Shared library + read/lag/encoding tests |
| `tools/sustained_test.py` | Long-running sustained load |
| `tools/scenario_test.py` | Scenario orchestrator (mixed, consistency) |
| `tools/bandwidth_test.py` | Bandwidth maximization |

---

## Workflow Rules

### Feature Branches for Issues
All code resolving a GitHub issue **must** be developed on a feature branch — never pushed directly to `main`. Use the naming convention from `docs/BRANCHING_STRATEGY_SIMPLE.md`:
```
feature/<short-description>   # New features
fix/<short-description>        # Bug fixes
refactor/<short-description>   # Refactoring
```

### Auto-Close Issues via PR
PR descriptions **must** include a `Closes #<issue>` line so the linked issue is automatically closed when the PR is merged:
```
## Summary
- Added column projection for read queries

Closes #38
```
If a PR resolves multiple issues, list each one:
```
Closes #36
Closes #38
```

### CI Notes
- Benchmarks require the `performance` label or `[benchmark]` in the PR title
- Docs-only changes skip CI

---

## Common Tasks

### Adding a New Endpoint

1. Add handler in `src/api/handlers.rs`
2. Register route in `src/api/mod.rs`
3. Add integration test in `tests/integration_tests.rs`
4. Update `SPEC.md` with new endpoint

### Adding a New Storage Operation

1. Add method to trait in `src/contracts/storage.rs` or `cold_storage.rs`
2. Implement in all storage backends
3. Add property test if it affects invariants
4. Add integration test

### Creating a PR

- Use `.github/PULL_REQUEST_TEMPLATE.md` as the template
- Include `Closes #<issue>` in the description

### Updating Documentation

- Run `./scripts/check-docs-coherence.sh` to verify cross-file consistency
- For spec changes, also check `SPEC.md`, `CHANGELOG.md`, and `testing_strategy.md`

---

## Do NOT

- Add new public APIs without updating `contracts/`
- Use `unwrap()` or `expect()` in library code
- Skip tests for "simple" changes
- Change invariant behavior without discussion
- Add dependencies without justification
- Mix refactoring with feature changes in one PR

---

## Product Vision

Zombi's mission is to provide the **lowest-cost, lowest-operational-overhead path from production data to data lake (Iceberg)**.

### Core Principles

1. **Low Cost**: Fully utilize server resources — performance regressions are critical bugs
2. **Low Ops**: Minimal configuration, auto-registration, self-healing
3. **Data Freshness**: Close the gap between production and analytics

### Architecture Layers

| Layer | Purpose | Technology |
|-------|---------|------------|
| Hot Storage | Sub-second reads of recent events | RocksDB |
| Cold Storage | Durable analytics-ready data | Iceberg on S3 |
| Unified API | Abstracts hot/cold for consumers | Zombi read endpoint |

### Roadmap

- **Phase 1 (Current)**: Event ingestion (clickstream, application events)
- **Phase 2**: Unified consumer API (data available before S3 via streaming endpoint OR unified Iceberg view)
- **Phase 3**: Binlog ingestion (MySQL CDC, Postgres logical replication)
- **Phase 4**: Third-party connectors (Kafka, Kinesis, webhooks)

---

## Performance Invariants

These performance baselines are **minimum acceptable** on t3.micro (2 vCPU, 1GB RAM).
Any PR causing >30% regression should be flagged and investigated.

| ID | Metric | Baseline | How to Verify |
|----|--------|----------|---------------|
| PERF-1 | Single-event writes | >10,000 req/s | `hey` single-write test |
| PERF-2 | Bulk writes (100/batch) | >100,000 ev/s | `hey` bulk-write test |
| PERF-3 | Server write latency | <10 μs | `/stats` endpoint `avg_latency_us` |
| PERF-4 | Iceberg snapshot commit | <500 ms | Flush timing logs |
| PERF-5 | Clickstream bulk (100 diverse events) | >50,000 ev/s | `hey` clickstream bulk test |

---

## Debugging

### Logging
```bash
RUST_LOG=zombi=debug cargo run
RUST_LOG=zombi::storage=trace cargo run  # Verbose storage logs
```

### Check Iceberg Data
```bash
# List files in MinIO
aws --endpoint-url http://localhost:9000 s3 ls s3://zombi-events/ --recursive

# Check for metadata and data directories
aws --endpoint-url http://localhost:9000 s3 ls s3://zombi-events/tables/events/
```

---

## Bash Guidelines

### Avoid output buffering issues
- DO NOT pipe output through `head`, `tail`, `less`, or `more` when monitoring
- Let commands complete fully, or use command-specific flags (e.g., `git log -n 10`)
- Avoid chained pipes that can cause output to buffer indefinitely
