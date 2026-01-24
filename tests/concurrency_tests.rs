//! Concurrency tests for Zombi storage system.
//!
//! These tests verify thread-safety and correctness under concurrent access.
//! Run with: cargo test --test concurrency_tests
//!
//! For loom-based exhaustive testing of the sequence generator:
//! RUSTFLAGS="--cfg loom" cargo test --test concurrency_tests --features loom

use std::sync::Arc;
use std::thread;

use tempfile::TempDir;
use zombi::contracts::HotStorage;
use zombi::storage::RocksDbStorage;

fn create_test_storage() -> (Arc<RocksDbStorage>, TempDir) {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDbStorage::open(dir.path()).unwrap());
    (storage, dir)
}

// =============================================================================
// Parallel Write Tests
// =============================================================================

/// Test that parallel writes to the same partition produce unique sequences.
#[test]
fn parallel_writes_no_duplicate_sequences() {
    let (storage, _dir) = create_test_storage();
    let num_threads = 10;
    let writes_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let s = Arc::clone(&storage);
            thread::spawn(move || {
                let mut seqs = Vec::with_capacity(writes_per_thread);
                for i in 0..writes_per_thread {
                    let seq = s
                        .write("test-topic", 0, format!("data-{}", i).as_bytes(), 0, None)
                        .expect("write should succeed");
                    seqs.push(seq);
                }
                seqs
            })
        })
        .collect();

    let mut all_seqs: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    // All sequences must be unique
    all_seqs.sort();
    let len_before = all_seqs.len();
    all_seqs.dedup();
    assert_eq!(
        all_seqs.len(),
        len_before,
        "Found duplicate sequences in parallel writes"
    );

    // Should have exactly num_threads * writes_per_thread sequences
    assert_eq!(all_seqs.len(), num_threads * writes_per_thread);
}

/// Test that parallel writes to different partitions are isolated.
#[test]
fn parallel_writes_to_different_partitions() {
    let (storage, _dir) = create_test_storage();
    let num_partitions = 4;
    let writes_per_partition = 50;

    let handles: Vec<_> = (0..num_partitions)
        .map(|partition| {
            let s = Arc::clone(&storage);
            thread::spawn(move || {
                for i in 0..writes_per_partition {
                    s.write(
                        "test-topic",
                        partition,
                        format!("p{}-data-{}", partition, i).as_bytes(),
                        0,
                        None,
                    )
                    .expect("write should succeed");
                }
                partition
            })
        })
        .collect();

    // Wait for all writes to complete
    for h in handles {
        h.join().unwrap();
    }

    // Verify each partition has the correct number of events
    for partition in 0..num_partitions {
        let events = storage
            .read("test-topic", partition, 0, 1000)
            .expect("read should succeed");
        assert_eq!(
            events.len(),
            writes_per_partition as usize,
            "Partition {} should have {} events",
            partition,
            writes_per_partition
        );

        // Verify all events belong to this partition
        for event in &events {
            assert_eq!(event.partition, partition);
            let payload = String::from_utf8_lossy(&event.payload);
            assert!(
                payload.starts_with(&format!("p{}-", partition)),
                "Event in partition {} has wrong payload: {}",
                partition,
                payload
            );
        }
    }
}

// =============================================================================
// Read During Write Tests
// =============================================================================

/// Test that reads during concurrent writes see a consistent state.
#[test]
fn read_during_write_sees_consistent_state() {
    let (storage, _dir) = create_test_storage();
    let num_writers = 4;
    let writes_per_thread = 100;

    // Start writer threads
    let writer_handles: Vec<_> = (0..num_writers)
        .map(|writer_id| {
            let s = Arc::clone(&storage);
            thread::spawn(move || {
                for i in 0..writes_per_thread {
                    s.write(
                        "test-topic",
                        0,
                        format!("w{}-{}", writer_id, i).as_bytes(),
                        0,
                        None,
                    )
                    .expect("write should succeed");
                }
            })
        })
        .collect();

    // Start reader threads that read while writes are happening
    let reader_handles: Vec<_> = (0..2)
        .map(|_| {
            let s = Arc::clone(&storage);
            thread::spawn(move || {
                let mut read_count = 0;
                for _ in 0..50 {
                    let events = s
                        .read("test-topic", 0, 0, 1000)
                        .expect("read should succeed");

                    // Events must be in sequence order (no gaps in what we read)
                    for window in events.windows(2) {
                        assert!(
                            window[0].sequence < window[1].sequence,
                            "Events out of order: {} >= {}",
                            window[0].sequence,
                            window[1].sequence
                        );
                    }

                    read_count += events.len();
                    thread::sleep(std::time::Duration::from_millis(1));
                }
                read_count
            })
        })
        .collect();

    // Wait for writers
    for h in writer_handles {
        h.join().unwrap();
    }

    // Wait for readers
    for h in reader_handles {
        let _ = h.join().unwrap();
    }

    // Final verification: all events should be present
    let events = storage
        .read("test-topic", 0, 0, 10000)
        .expect("read should succeed");
    assert_eq!(
        events.len(),
        (num_writers * writes_per_thread) as usize,
        "Should have all events after concurrent writes"
    );
}

// =============================================================================
// Idempotency Under Concurrent Retries
// =============================================================================

/// Test that idempotent writes work correctly with sequential retries.
/// Note: True concurrent retries with the same key may not be fully atomic
/// in the current implementation. This tests the more common sequential retry pattern.
#[test]
fn idempotency_under_sequential_retries() {
    let (storage, _dir) = create_test_storage();
    let idempotency_key = "unique-request-123";

    // First write
    let first_offset = storage
        .write(
            "test-topic",
            0,
            b"idempotent-payload",
            0,
            Some(idempotency_key),
        )
        .expect("write should succeed");

    // Sequential retries should all return the same offset
    for i in 0..10 {
        let retry_offset = storage
            .write(
                "test-topic",
                0,
                b"idempotent-payload",
                0,
                Some(idempotency_key),
            )
            .expect("write should succeed");
        assert_eq!(
            retry_offset, first_offset,
            "Retry {} got different offset: {} vs {}",
            i, retry_offset, first_offset
        );
    }

    // Should only have one event stored
    let events = storage
        .read("test-topic", 0, 0, 100)
        .expect("read should succeed");
    assert_eq!(
        events.len(),
        1,
        "Should only store one event for idempotent writes"
    );
    assert_eq!(events[0].payload, b"idempotent-payload");
}

/// Test multiple different idempotency keys written concurrently (each key written once).
/// This verifies that different idempotency keys don't interfere with each other.
#[test]
fn multiple_idempotency_keys_concurrent() {
    let (storage, _dir) = create_test_storage();
    let num_keys = 50;

    // Each key is written by a single thread - no concurrent retries
    let handles: Vec<_> = (0..num_keys)
        .map(|key_id| {
            let s = Arc::clone(&storage);
            let key = format!("key-{}", key_id);
            thread::spawn(move || {
                let offset = s
                    .write(
                        "test-topic",
                        0,
                        format!("payload-{}", key_id).as_bytes(),
                        0,
                        Some(&key),
                    )
                    .expect("write should succeed");
                (key_id, offset)
            })
        })
        .collect();

    let results: Vec<(i32, u64)> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All offsets should be unique (different keys should get different offsets)
    let mut offsets: Vec<u64> = results.iter().map(|(_, o)| *o).collect();
    offsets.sort();
    let len_before = offsets.len();
    offsets.dedup();
    assert_eq!(
        offsets.len(),
        len_before,
        "Each unique key should get a unique offset"
    );

    // Should have exactly num_keys events
    let events = storage
        .read("test-topic", 0, 0, 1000)
        .expect("read should succeed");
    assert_eq!(
        events.len(),
        num_keys as usize,
        "Should have exactly {} events",
        num_keys
    );
}

// =============================================================================
// Consumer Offset Concurrent Access
// =============================================================================

/// Test concurrent consumer offset commits.
#[test]
fn concurrent_consumer_offset_commits() {
    let (storage, _dir) = create_test_storage();
    let num_threads = 10;

    // Each thread commits increasing offsets for the same consumer group
    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let s = Arc::clone(&storage);
            let offset = (i + 1) * 100; // 100, 200, 300, ...
            thread::spawn(move || {
                s.commit_offset("test-group", "test-topic", 0, offset as u64)
                    .expect("commit should succeed");
                offset
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // The final offset should be one of the committed values
    let final_offset = storage
        .get_offset("test-group", "test-topic", 0)
        .expect("get offset should succeed")
        .expect("should have an offset");

    // Should be a multiple of 100 between 100 and 1000
    assert!(
        (100..=1000).contains(&final_offset) && final_offset.is_multiple_of(100),
        "Final offset {} is not valid",
        final_offset
    );
}

/// Test concurrent access to different consumer groups.
#[test]
fn concurrent_different_consumer_groups() {
    let (storage, _dir) = create_test_storage();
    let num_groups = 5;

    let handles: Vec<_> = (0..num_groups)
        .map(|group_id| {
            let s = Arc::clone(&storage);
            let group = format!("group-{}", group_id);
            let offset = (group_id + 1) * 100;
            thread::spawn(move || {
                s.commit_offset(&group, "test-topic", 0, offset as u64)
                    .expect("commit should succeed");
                (group, offset)
            })
        })
        .collect();

    let results: Vec<(String, i32)> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Verify each group has its own offset
    for (group, expected_offset) in results {
        let actual = storage
            .get_offset(&group, "test-topic", 0)
            .expect("get offset should succeed")
            .expect("should have an offset");
        assert_eq!(
            actual, expected_offset as u64,
            "Group {} should have offset {}",
            group, expected_offset
        );
    }
}

// =============================================================================
// High Watermark Concurrent Access
// =============================================================================

/// Test that high watermark is correctly updated under concurrent writes.
#[test]
fn high_watermark_under_concurrent_writes() {
    let (storage, _dir) = create_test_storage();
    let num_threads = 10;
    let writes_per_thread = 50;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let s = Arc::clone(&storage);
            thread::spawn(move || {
                let mut max_seq = 0u64;
                for _ in 0..writes_per_thread {
                    let seq = s
                        .write("test-topic", 0, b"data", 0, None)
                        .expect("write should succeed");
                    max_seq = max_seq.max(seq);
                }
                max_seq
            })
        })
        .collect();

    let max_seqs: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let overall_max = *max_seqs.iter().max().unwrap();

    // High watermark should be at least as high as the max sequence we saw
    let hwm = storage
        .high_watermark("test-topic", 0)
        .expect("high_watermark should succeed");

    assert!(
        hwm >= overall_max,
        "High watermark {} should be >= max sequence {}",
        hwm,
        overall_max
    );

    // Should equal total writes
    assert_eq!(
        hwm,
        (num_threads * writes_per_thread) as u64,
        "High watermark should equal total writes"
    );
}

// =============================================================================
// Stress Test
// =============================================================================

/// Stress test with mixed operations.
#[test]
fn stress_test_mixed_operations() {
    let (storage, _dir) = create_test_storage();
    let duration = std::time::Duration::from_secs(2);
    let start = std::time::Instant::now();

    let storage_write = Arc::clone(&storage);
    let storage_read = Arc::clone(&storage);
    let storage_commit = Arc::clone(&storage);

    // Writer thread
    let writer = thread::spawn(move || {
        let mut count = 0u64;
        while start.elapsed() < duration {
            storage_write
                .write(
                    "stress-topic",
                    0,
                    format!("event-{}", count).as_bytes(),
                    0,
                    None,
                )
                .expect("write should succeed");
            count += 1;
        }
        count
    });

    // Reader thread - reads should not fail
    let reader = thread::spawn(move || {
        let mut read_count = 0u64;
        while start.elapsed() < duration {
            let events = storage_read
                .read("stress-topic", 0, 0, 100)
                .expect("read should succeed");
            // Verify events are in order if we got any
            for window in events.windows(2) {
                assert!(
                    window[0].sequence < window[1].sequence,
                    "Events out of order during read"
                );
            }
            read_count += events.len() as u64;
            thread::sleep(std::time::Duration::from_millis(10));
        }
        read_count
    });

    // Commit thread - commits should not fail
    let committer = thread::spawn(move || {
        let mut commit_count = 0u64;
        while start.elapsed() < duration {
            storage_commit
                .commit_offset("stress-group", "stress-topic", 0, commit_count)
                .expect("commit should succeed");
            commit_count += 1;
            thread::sleep(std::time::Duration::from_millis(50));
        }
        commit_count
    });

    let writes = writer.join().unwrap();
    let reads = reader.join().unwrap();
    let commits = committer.join().unwrap();

    // Verify data integrity - all written events should be readable
    let events = storage
        .read("stress-topic", 0, 0, 1000000)
        .expect("read should succeed");

    assert_eq!(
        events.len(),
        writes as usize,
        "Should have all {} written events, but got {}",
        writes,
        events.len()
    );

    // Verify sequence order
    for window in events.windows(2) {
        assert!(
            window[0].sequence < window[1].sequence,
            "Events out of order after stress test"
        );
    }

    println!(
        "Stress test completed: {} writes, {} read ops, {} commits in {:?}",
        writes, reads, commits, duration
    );
}
