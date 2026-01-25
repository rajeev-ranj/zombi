# Zombi Testing Strategy

> Comprehensive testing approach for a storage system where correctness is critical.

---

## Testing Pyramid

```
                          ┌───────────────┐
                          │  Chaos (L7)   │  ← Distributed failures
                         ─┼───────────────┼─
                        / │  Fuzz (L6)    │ \  ← Random edge cases
                       /  │               │  \
                      ─┼──┴───────────────┴──┼─
                     /                        \
                    /      Load (L5)           \  ← Performance limits
                   ─┼──────────────────────────┼─
                  /                              \
                 /       Crash (L4)               \  ← Recovery correct
                ─┼────────────────────────────────┼─
               /                                    \
              /     Concurrency (L3)                 \  ← No races
             ─┼──────────────────────────────────────┼─
            /                                          \
           /        Property-Based (L2)                 \  ← Invariants hold
          ─┼────────────────────────────────────────────┼─
         /                                                \
        /            Integration (L1)                      \  ← Components work
       ─┼──────────────────────────────────────────────────┼─
      /                                                      \
     /                  Unit (L0)                             \  ← Pure logic
    ───────────────────────────────────────────────────────────
```

---

## Level 0: Unit Tests

**Purpose:** Test pure functions with no I/O.

**What to test:**
- Sequence number generation
- Partition key hashing
- Event serialization/deserialization
- Watermark calculations
- Configuration parsing

**Tools:** `cargo test`

**Example:**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_hash_is_deterministic() {
        let key = "user:123";
        let p1 = partition_for_key(key, 8);
        let p2 = partition_for_key(key, 8);
        assert_eq!(p1, p2);
    }

    #[test]
    fn partition_distributes_evenly() {
        let mut counts = [0u32; 8];
        for i in 0..10000 {
            let key = format!("key:{}", i);
            let p = partition_for_key(&key, 8);
            counts[p as usize] += 1;
        }
        // Each partition should have ~1250 ± 20%
        for count in counts {
            assert!(count > 1000 && count < 1500);
        }
    }

    #[test]
    fn watermark_comparison() {
        let w1 = Watermark { topic: "t".into(), partition: 0, offset: 100 };
        let w2 = Watermark { topic: "t".into(), partition: 0, offset: 200 };
        assert!(w1 < w2);
    }
}
```

**Coverage target:** 95%+ for pure functions.

**Runs:** Every commit, <10 seconds.

---

## Level 1: Integration Tests

**Purpose:** Test real components with real I/O.

**What to test:**
- RocksDB write/read cycle
- S3 upload/download
- HTTP API endpoints
- Protobuf serialization through wire

**Tools:** `testcontainers-rs`, MinIO (S3-compatible)

**Example:**

```rust
// tests/integration/storage_test.rs

use testcontainers::{clients::Cli, images::minio::MinIO};

#[tokio::test]
async fn write_read_cycle() {
    let docker = Cli::default();
    let minio = docker.run(MinIO::default());
    
    let config = StorageConfig {
        s3_endpoint: format!("http://localhost:{}", minio.get_host_port(9000)),
        rocksdb_path: tempdir().path().to_path_buf(),
        ..Default::default()
    };
    
    let storage = Storage::new(config).await.unwrap();
    
    // Write
    let event = Event::new(b"test payload".to_vec());
    let seq = storage.write("topic", 0, &event).await.unwrap();
    
    // Read
    let events = storage.read("topic", 0, seq, 1).await.unwrap();
    
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, event.payload);
}

#[tokio::test]
async fn flush_to_s3_and_read_back() {
    // ... setup ...
    
    // Write 1000 events
    for i in 0..1000 {
        storage.write("topic", 0, &Event::new(vec![i as u8])).await.unwrap();
    }
    
    // Force flush
    storage.flush().await.unwrap();
    
    // Clear local cache
    storage.clear_hot_cache().await.unwrap();
    
    // Read from cold (S3)
    let events = storage.read("topic", 0, 0, 1000).await.unwrap();
    assert_eq!(events.len(), 1000);
}
```

**Runs:** Every commit, <2 minutes.

---

## Level 2: Property-Based Tests

**Purpose:** Verify invariants hold for all possible inputs.

**Critical invariants:**

| ID | Invariant | Description |
|----|-----------|-------------|
| INV-1 | Monotonic sequences | `seq[n+1] > seq[n]` |
| INV-2 | No data loss | `write(x) → x ∈ read()` |
| INV-3 | Order preserved | `write([a,b,c]) → read() = [a,b,c]` |
| INV-4 | Idempotency | `write(x, id) twice → count(x) = 1` |
| INV-5 | Partition isolation | Events in partition P stay in P |

**Tools:** `proptest`

**Example:**

```rust
// tests/property/invariants.rs

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10000))]

    /// INV-1: Sequence numbers are always increasing
    #[test]
    fn sequences_are_monotonic(
        events in prop::collection::vec(any::<Vec<u8>>(), 1..1000)
    ) {
        let storage = create_test_storage();
        let mut last_seq = 0u64;

        for payload in events {
            let seq = storage.write(0, Event::new(payload)).unwrap();
            prop_assert!(seq > last_seq, "Sequence went backwards: {} -> {}", last_seq, seq);
            last_seq = seq;
        }
    }

    /// INV-2: Every written event can be read back
    #[test]
    fn no_data_loss(
        events in prop::collection::vec(any::<Vec<u8>>(), 1..500)
    ) {
        let storage = create_test_storage();
        let mut written = vec![];

        for payload in events {
            let seq = storage.write(0, Event::new(payload.clone())).unwrap();
            written.push((seq, payload));
        }

        storage.flush().unwrap();

        for (seq, expected_payload) in written {
            let read = storage.read(0, seq, 1).unwrap();
            prop_assert_eq!(read.len(), 1);
            prop_assert_eq!(&read[0].payload, &expected_payload);
        }
    }

    /// INV-3: Read returns events in sequence order
    #[test]
    fn read_preserves_order(
        events in prop::collection::vec(any::<Vec<u8>>(), 2..500)
    ) {
        let storage = create_test_storage();

        for payload in &events {
            storage.write(0, Event::new(payload.clone())).unwrap();
        }

        let read = storage.read(0, 0, events.len()).unwrap();

        for window in read.windows(2) {
            prop_assert!(
                window[0].sequence < window[1].sequence,
                "Out of order: {} >= {}", window[0].sequence, window[1].sequence
            );
        }
    }

    /// INV-4: Idempotent writes with same key store once
    #[test]
    fn idempotent_writes(
        payload in any::<Vec<u8>>(),
        idempotency_key in "[a-z]{8}"
    ) {
        let storage = create_test_storage();

        let seq1 = storage.write_idempotent(0, Event::new(payload.clone()), &idempotency_key).unwrap();
        let seq2 = storage.write_idempotent(0, Event::new(payload.clone()), &idempotency_key).unwrap();

        prop_assert_eq!(seq1, seq2, "Different sequences for same idempotency key");

        let all = storage.read(0, 0, 100).unwrap();
        prop_assert_eq!(all.len(), 1, "Event stored multiple times");
    }

    /// INV-5: Events stay in their partition
    #[test]
    fn partition_isolation(
        events_p0 in prop::collection::vec(any::<Vec<u8>>(), 1..100),
        events_p1 in prop::collection::vec(any::<Vec<u8>>(), 1..100)
    ) {
        let storage = create_test_storage();

        for payload in &events_p0 {
            storage.write(0, Event::new(payload.clone())).unwrap();
        }
        for payload in &events_p1 {
            storage.write(1, Event::new(payload.clone())).unwrap();
        }

        let read_p0 = storage.read(0, 0, 1000).unwrap();
        let read_p1 = storage.read(1, 0, 1000).unwrap();

        prop_assert_eq!(read_p0.len(), events_p0.len());
        prop_assert_eq!(read_p1.len(), events_p1.len());
    }
}
```

**Runs:** Every commit, ~30 seconds (10k cases).

---

## Level 3: Concurrency Tests

**Purpose:** Find race conditions, deadlocks, data corruption.

**Tools:** `loom` (exhaustive state exploration)

**What to test:**
- Parallel writes to same partition
- Read during write
- Flush during write
- Multiple flushers

**Example:**

```rust
// tests/concurrency/parallel_writes.rs

use loom::sync::Arc;
use loom::thread;

#[test]
fn parallel_writes_no_corruption() {
    loom::model(|| {
        let storage = Arc::new(LoomTestStorage::new());
        
        let handles: Vec<_> = (0..3).map(|i| {
            let s = storage.clone();
            thread::spawn(move || {
                s.write(0, Event::new(vec![i])).unwrap()
            })
        }).collect();

        let seqs: Vec<u64> = handles.into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        // All sequences must be unique
        let mut sorted = seqs.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), seqs.len(), "Duplicate sequences!");

        // All events must be readable
        let events = storage.read(0, 0, 10).unwrap();
        assert_eq!(events.len(), 3);
    });
}

#[test]
fn read_during_write_sees_consistent_state() {
    loom::model(|| {
        let storage = Arc::new(LoomTestStorage::new());
        let s1 = storage.clone();
        let s2 = storage.clone();

        let writer = thread::spawn(move || {
            for i in 0..5 {
                s1.write(0, Event::new(vec![i])).unwrap();
            }
        });

        let reader = thread::spawn(move || {
            let events = s2.read(0, 0, 100).unwrap();
            // Must be in order, no gaps
            for window in events.windows(2) {
                assert!(window[0].sequence < window[1].sequence);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}
```

**Runs:** Every commit, ~1 minute.

---

## Level 4: Crash Recovery Tests

**Purpose:** Verify data survives crashes and restarts.

**Crash points:**

| Point | Description |
|-------|-------------|
| CP-1 | After buffer write, before RocksDB |
| CP-2 | After RocksDB, before S3 flush |
| CP-3 | During S3 multipart upload |
| CP-4 | After S3, before watermark update |

**Technique:** Fault injection + process restart

**Example:**

```rust
// tests/crash/recovery.rs

#[tokio::test]
async fn recover_after_crash_before_s3() {
    let data_dir = tempdir().unwrap();
    
    // Phase 1: Write events, crash before flush
    {
        let storage = Storage::new(StorageConfig {
            rocksdb_path: data_dir.path().join("rocks"),
            ..Default::default()
        }).await.unwrap();

        for i in 0..100 {
            storage.write("topic", 0, &Event::new(vec![i])).await.unwrap();
        }

        // Simulate crash: drop without flush
        // RocksDB WAL should have the data
        drop(storage);
    }

    // Phase 2: Restart and verify recovery
    {
        let storage = Storage::new(StorageConfig {
            rocksdb_path: data_dir.path().join("rocks"),
            ..Default::default()
        }).await.unwrap();

        // Data should be recovered from RocksDB WAL
        let events = storage.read("topic", 0, 0, 100).await.unwrap();
        assert_eq!(events.len(), 100, "Lost events after crash!");
    }
}

#[tokio::test]
async fn recover_after_crash_during_s3_upload() {
    let data_dir = tempdir().unwrap();
    
    {
        let storage = Storage::new_with_fault_injection(
            StorageConfig { ... },
            FaultConfig {
                fail_at: FaultPoint::DuringS3Upload,
                fail_after_bytes: 1024,
            }
        ).await.unwrap();

        for i in 0..100 {
            storage.write("topic", 0, &Event::new(vec![i])).await.unwrap();
        }

        // This should fail mid-upload
        let result = storage.flush().await;
        assert!(result.is_err());
        
        drop(storage);
    }

    // Restart
    {
        let storage = Storage::new(StorageConfig { ... }).await.unwrap();
        
        // Flush should complete now
        storage.flush().await.unwrap();
        
        // All data should be there
        let events = storage.read("topic", 0, 0, 100).await.unwrap();
        assert_eq!(events.len(), 100);
    }
}
```

**Runs:** Every PR, ~5 minutes.

---

## Level 5: Load Tests

**Purpose:** Find performance limits, memory leaks, degradation.

**Scenarios:**

| Scenario | Target | Duration |
|----------|--------|----------|
| Sustained writes | 10k events/sec | 1 hour |
| Burst writes | 100k events/sec | 10 seconds |
| Mixed read/write | 50%/50% | 30 minutes |
| Large events | 1MB each, 100/sec | 10 minutes |
| Many partitions | 1000 partitions | 30 minutes |

**Tools:** `criterion`, `k6`

**Example (Rust benchmark):**

```rust
// benches/write_throughput.rs

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

fn write_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = rt.block_on(create_test_storage());
    let event = Event::new(vec![0u8; 1024]); // 1KB event
    
    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Elements(1));
    group.sample_size(10000);
    
    group.bench_function("single_1kb", |b| {
        b.iter(|| {
            rt.block_on(storage.write("topic", 0, &event)).unwrap();
        });
    });
    
    group.finish();
}

criterion_group!(benches, write_throughput);
criterion_main!(benches);
```

**Example (k6 HTTP load test):**

```javascript
// tests/load/write.js

import http from 'k6/http';
import { check } from 'k6';

export const options = {
    scenarios: {
        sustained: {
            executor: 'constant-arrival-rate',
            rate: 10000,
            timeUnit: '1s',
            duration: '5m',
            preAllocatedVUs: 100,
        },
    },
};

export default function () {
    const payload = {
        payload: 'x'.repeat(1024),
    };
    
    const res = http.post('http://localhost:8080/events/test-topic', 
        JSON.stringify(payload),
        { headers: { 'Content-Type': 'application/json' } }
    );
    
    check(res, {
        'status is 202': (r) => r.status === 202,
        'has offset': (r) => r.json().offset !== undefined,
    });
}
```

**Runs:** Nightly, ~2 hours.

### Implementation: zombi-load CLI

The load testing strategy is implemented via the `zombi-load` CLI tool:

```bash
# Profiles map to testing levels:
python tools/zombi_load.py run --profile quick      # CI smoke tests
python tools/zombi_load.py run --profile full       # Release validation
python tools/zombi_load.py run --profile stress     # Extended stability

# Compare runs for regression detection
python tools/zombi_load.py compare results/baseline.json results/current.json
```

**Available Profiles:**
| Profile | Duration | Use Case |
|---------|----------|----------|
| `quick` | ~3 min | CI/CD pipelines |
| `full` | ~30 min | Release validation |
| `stress` | ~2 hours | Stability testing |
| `peak` | ~10 min | Max throughput (requires hey) |

See `tools/README.md` for complete documentation.

---

## Level 6: Fuzz Tests

**Purpose:** Find crashes, panics, undefined behavior with random input.

**Tools:** `cargo-fuzz` (libFuzzer)

**Fuzz targets:**

| Target | Input |
|--------|-------|
| `fuzz_write` | Random event payloads |
| `fuzz_read` | Random offset/limit combinations |
| `fuzz_proto` | Random protobuf bytes |
| `fuzz_api` | Random HTTP requests |

**Example:**

```rust
// fuzz/fuzz_targets/fuzz_write.rs

#![no_main]
use libfuzzer_sys::fuzz_target;
use zombi::Event;

fuzz_target!(|data: &[u8]| {
    let storage = create_temp_storage();
    
    // Should never panic, even with garbage input
    let _ = storage.write(0, Event::new(data.to_vec()));
});
```

```rust
// fuzz/fuzz_targets/fuzz_proto.rs

#![no_main]
use libfuzzer_sys::fuzz_target;
use zombi::proto::Event;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    // Should handle malformed protobuf gracefully
    let _ = Event::decode(data);
});
```

**Runs:** Nightly, 4+ hours.

---

## Level 7: Chaos Tests (Future, for HA)

**Purpose:** Test distributed failure scenarios.

**Scenarios:**
- Network partition between nodes
- Slow network (latency injection)
- Node crashes and restarts
- S3 temporary unavailability
- Clock skew between nodes

**Tools:** `toxiproxy`, Docker network manipulation

---

## Test Organization

```
tests/
├── unit/                      # L0: Fast, pure functions
│   ├── sequence_test.rs
│   ├── partition_test.rs
│   └── watermark_test.rs
│
├── integration/               # L1: Real I/O
│   ├── storage_test.rs
│   ├── api_test.rs
│   └── consumer_test.rs
│
├── property/                  # L2: Invariants
│   └── invariants.rs
│
├── concurrency/               # L3: Race conditions
│   └── parallel_writes.rs
│
├── crash/                     # L4: Recovery
│   └── recovery.rs
│
├── load/                      # L5: Performance (k6 scripts)
│   ├── write.js
│   └── mixed.js
│
├── adr/                       # ADR compliance
│   ├── adr_001_storage.rs
│   └── adr_004_ordering.rs
│
└── fixtures/
    ├── mod.rs
    └── test_helpers.rs

fuzz/
└── fuzz_targets/              # L6: Fuzzing
    ├── fuzz_write.rs
    └── fuzz_proto.rs

benches/                       # Performance tracking
├── write_throughput.rs
└── read_latency.rs
```

---

## CI Integration

```yaml
# .github/workflows/test.yml
name: Test Suite
on: [push, pull_request]

jobs:
  quick:  # <1 min, every commit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --lib  # Unit tests

  full:  # <5 min, every commit
    runs-on: ubuntu-latest
    needs: quick
    services:
      minio:
        image: minio/minio:latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --all  # All tests except slow

  property:  # ~1 min
    runs-on: ubuntu-latest
    needs: quick
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --test 'property_*'

  concurrency:  # ~2 min
    runs-on: ubuntu-latest
    needs: quick
    steps:
      - uses: actions/checkout@v4
      - run: RUSTFLAGS="--cfg loom" cargo test --test 'concurrency_*'

  bench-check:  # Compile only
    runs-on: ubuntu-latest
    needs: quick
    steps:
      - uses: actions/checkout@v4
      - run: cargo bench --no-run

# Nightly jobs
  fuzz:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - run: cargo +nightly fuzz run fuzz_write -- -max_total_time=3600

  load:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - run: ./scripts/run_load_tests.sh
```

---

## Coverage Tracking

**Tools:** `cargo-llvm-cov`, Codecov

```yaml
# In CI
- name: Coverage
  run: |
    cargo llvm-cov --all-features --lcov --output-path lcov.info
    
- name: Upload to Codecov
  uses: codecov/codecov-action@v3
  with:
    files: lcov.info
```

**Targets:**
- Overall: 80%+
- `contracts/`: 95%+
- `storage/`: 90%+
- `api/`: 85%+

---

## Summary

| Level | Purpose | Frequency | Blocks Merge? |
|-------|---------|-----------|---------------|
| L0 Unit | Pure logic | Every commit | ✅ |
| L1 Integration | I/O works | Every commit | ✅ |
| L2 Property | Invariants | Every commit | ✅ |
| L3 Concurrency | No races | Every commit | ✅ |
| L4 Crash | Recovery | Every PR | ✅ |
| L5 Load | Performance | Nightly | ⚠️ Alert |
| L6 Fuzz | Edge cases | Nightly | ⚠️ Issue |
| L7 Chaos | HA failures | Manual | ❌ |
