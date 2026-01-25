# CLAUDE.md

> Instructions for Claude Code when working on Zombi.

---

## Project Overview

Zombi is a Kafka-replacement event streaming system written in Rust. It uses RocksDB for hot storage and S3/Iceberg for cold storage.

**Current Version:** 0.2.0

**Read these files first:**
- `SPEC.md` — Full specification and API reference
- `CHANGELOG.md` — Version history and feature status
- `testing_strategy.md` — Comprehensive testing approach
- `docs/BRANCHING_STRATEGY_SIMPLE.md` — Current development workflow (v0.x)

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

### Error Handling
```rust
// Good
pub fn write(&self, event: Event) -> Result<u64, StorageError> {
    let seq = self.seq_gen.next()?;
    self.db.put(seq.to_be_bytes(), event.encode()?)?;
    Ok(seq)
}

// Bad
pub fn write(&self, event: Event) -> u64 {
    let seq = self.seq_gen.next().unwrap();  // NO!
    self.db.put(seq.to_be_bytes(), event.encode().unwrap()).unwrap();
    seq
}
```

---

## Architecture Rules

### Module Boundaries

```
src/
├── contracts/   # Trait definitions — STABLE, change rarely
├── storage/     # Storage implementations (RocksDB, S3, Iceberg)
├── flusher/     # Background flush logic
├── api/         # HTTP handlers
└── proto/       # Generated protobuf code
```

**Rules:**
- `api/` can import from `contracts/`, `storage/`
- `storage/` can import from `contracts/`
- `flusher/` can import from `contracts/`, `storage/`
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

These must NEVER be violated. If you're unsure, ask.

| ID | Invariant | How to Verify |
|----|-----------|---------------|
| INV-1 | Sequences are monotonic | Property test: `sequences_are_monotonic` |
| INV-2 | No data loss after flush | Property test: `no_data_loss` |
| INV-3 | Order preserved | Property test: `order_preserved` |
| INV-4 | Idempotent writes | Property test: `idempotent_writes` |
| INV-5 | Partition isolation | Property test: `partition_isolation` |

**Before merging any storage change, verify all tests pass:**
```bash
cargo test
```

---

## Testing Requirements

### Test Suite (100 tests)
- **63 unit tests** - Storage, flusher, API
- **9 concurrency tests** - Parallel writes, race conditions
- **11 crash recovery tests** - Data persistence
- **12 integration tests** - HTTP endpoints
- **5 property tests** - Invariants

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

For load/performance testing, use the unified `zombi-load` CLI:

```bash
# Quick sanity check (~3 min)
python tools/zombi_load.py run --profile quick --url http://localhost:8080

# Full suite (~30 min)
python tools/zombi_load.py run --profile full

# Specific scenario
python tools/zombi_load.py run --scenario single-write --workers 50
```

See `tools/README.md` for profiles, scenarios, and configuration options.

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

### Changing Protobuf Schema

1. Edit `proto/event.proto`
2. Run `cargo build` (triggers prost-build)
3. Update any affected serialization code
4. Ensure backward compatibility (add fields, don't remove)

---

## Do NOT

- Add new public APIs without updating `contracts/`
- Use `unwrap()` or `expect()` in library code
- Skip tests for "simple" changes
- Change invariant behavior without discussion
- Add dependencies without justification
- Mix refactoring with feature changes in one PR

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

## Performance Notes

- RocksDB writes should be <100μs in hot path
- WAL is disabled for throughput (data durable after S3 flush)
- Avoid allocations in write path
- Use `bytes::Bytes` for zero-copy when possible
- Batch S3 writes (min 1000 events or 5 seconds)
- Iceberg mode uses size-based batching (target 128MB files)

---

## Questions?

If unclear about:
- Architecture decisions → Check `SPEC.md`
- Version history → Check `CHANGELOG.md`
- Test strategy → Check `testing_strategy.md`
- Implementation details → Check existing code patterns
