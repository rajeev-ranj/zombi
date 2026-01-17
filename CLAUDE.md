# CLAUDE.md

> Instructions for Claude Code when working on Zombi.

---

## Project Overview

Zombi is a Kafka-replacement event streaming proxy written in Rust. It uses RocksDB for hot storage and S3 for cold storage.

**Read these files first:**
- `SPEC.md` — Full specification
- `v0.1_plan.md` — Current development plan
- `hld.md` — North star architecture (v1.0 vision)

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
├── storage/     # Storage implementations
├── flusher/     # Background flush logic
├── api/         # HTTP handlers
└── internal/    # Private utilities
```

**Rules:**
- `api/` can import from `contracts/`, `storage/`
- `storage/` can import from `contracts/`, `internal/`
- `flusher/` can import from `contracts/`, `storage/`, `internal/`
- `contracts/` imports from NOTHING (except std)
- Never import from `api/` into `storage/`

### Trait-First Development

Always implement traits from `contracts/`, don't invent new public APIs:

```rust
// contracts/storage.rs
pub trait HotStorage: Send + Sync {
    fn write(&self, partition: u32, event: Event) -> Result<u64, StorageError>;
    fn read(&self, partition: u32, offset: u64, limit: usize) -> Result<Vec<Event>, StorageError>;
    fn high_watermark(&self, partition: u32) -> Result<u64, StorageError>;
}

// storage/rocksdb.rs
impl HotStorage for RocksDBStorage {
    // Implement exactly the trait, nothing more
}
```

---

## Invariants

These must NEVER be violated. If you're unsure, ask.

| ID | Invariant | How to Verify |
|----|-----------|---------------|
| INV-1 | Sequences are monotonic | Property test: `sequences_are_monotonic` |
| INV-2 | No data loss | Property test: `no_data_loss` |
| INV-3 | Order preserved | Property test: `order_preserved` |
| INV-4 | Idempotent writes | Property test: `idempotent_writes` |
| INV-5 | Partition isolation | Property test: `partition_isolation` |
| INV-6 | Crash recovery | Crash test: `tests/crash/recovery.rs` |

**Before merging any storage change, verify all property tests pass:**
```bash
cargo test --test property_tests
```

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

# Only property tests
cargo test --test property_tests

# With S3 (needs MinIO running)
docker run -d -p 9000:9000 minio/minio server /data
cargo test --test integration_tests
```

---

## Common Tasks

### Adding a New Endpoint

1. Add handler in `src/api/handlers.rs`
2. Register route in `src/api/mod.rs`
3. Add integration test in `tests/integration_tests.rs`
4. Update `SPEC.md` with new endpoint

### Adding a New Storage Operation

1. Add method to trait in `src/contracts/storage.rs`
2. Implement in `src/storage/rocksdb.rs`
3. Add property test if it affects invariants
4. Add integration test

### Changing Protobuf Schema

1. Edit `proto/event.proto`
2. Run `cargo build` (triggers prost-build)
3. Update any affected serialization code
4. Ensure backward compatibility (add fields, don't remove)

---

## Do NOT

- ❌ Add new public APIs without updating `contracts/`
- ❌ Use `unwrap()` or `expect()` in library code
- ❌ Skip tests for "simple" changes
- ❌ Change invariant behavior without discussion
- ❌ Add dependencies without justification
- ❌ Mix refactoring with feature changes in one PR

---

## File Templates

### New Trait
```rust
// src/contracts/new_trait.rs

use crate::error::ZombiError;

/// Brief description of what this trait does.
/// 
/// # Invariants
/// - List any invariants this trait must maintain
pub trait NewTrait: Send + Sync {
    /// Brief description of method.
    fn method(&self, arg: Type) -> Result<ReturnType, ZombiError>;
}
```

### New Handler
```rust
// src/api/handlers.rs

use axum::{extract::Path, Json};
use crate::storage::Storage;

pub async fn handle_new_endpoint(
    Path(topic): Path<String>,
    storage: Storage,
) -> Result<Json<Response>, ApiError> {
    let result = storage.some_operation(&topic).await?;
    Ok(Json(Response { data: result }))
}
```

### New Test
```rust
// tests/integration_tests.rs

#[tokio::test]
async fn test_new_feature() {
    let storage = setup_test_storage().await;
    
    // Arrange
    let event = Event::new(b"test".to_vec());
    
    // Act
    let result = storage.new_operation(&event).await;
    
    // Assert
    assert!(result.is_ok());
    assert_eq!(result.unwrap().some_field, expected_value);
}
```

---

## Debugging

### Logging
```bash
RUST_LOG=zombi=debug cargo run
RUST_LOG=zombi::storage=trace cargo run  # Verbose storage logs
```

### RocksDB Inspection
```bash
# List keys
cargo run --bin debug-rocks -- --path ./data/rocks --list

# Dump key
cargo run --bin debug-rocks -- --path ./data/rocks --key "topic:0:00000001"
```

---

## Performance Notes

- RocksDB writes should be <100μs in hot path
- Avoid allocations in write path
- Use `bytes::Bytes` for zero-copy when possible
- Batch S3 writes (min 1000 events or 5 seconds)

---

## Questions?

If unclear about:
- Architecture decisions → Check `SPEC.md` or `hld.md`
- Implementation approach → Check existing code patterns
- Test strategy → Check `testing_strategy.md`
- Roadmap → Check `roadmap.md`
