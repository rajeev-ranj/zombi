//! Crash recovery tests for Zombi storage system.
//!
//! These tests verify that data survives crashes and restarts.
//! Run with: cargo test --test crash_recovery_tests

use tempfile::TempDir;
use zombi::contracts::HotStorage;
use zombi::storage::RocksDbStorage;

fn create_storage_at(dir: &std::path::Path) -> RocksDbStorage {
    RocksDbStorage::open(dir).expect("Failed to open storage")
}

// =============================================================================
// Basic Recovery Tests
// =============================================================================

/// Test that data survives a clean shutdown and restart.
#[test]
fn data_survives_clean_restart() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Write data
    {
        let storage = create_storage_at(dir.path());
        for i in 0..100 {
            storage
                .write(
                    "test-topic",
                    0,
                    format!("event-{}", i).as_bytes(),
                    i as i64,
                    None,
                )
                .expect("write should succeed");
        }
        // Storage dropped here - clean shutdown
    }

    // Phase 2: Reopen and verify
    {
        let storage = create_storage_at(dir.path());
        let events = storage
            .read("test-topic", 0, 0, 1000)
            .expect("read should succeed");

        assert_eq!(events.len(), 100, "Should recover all 100 events");

        // Verify order and content
        for (i, event) in events.iter().enumerate() {
            let expected_payload = format!("event-{}", i);
            assert_eq!(
                String::from_utf8_lossy(&event.payload),
                expected_payload,
                "Event {} should have correct payload",
                i
            );
            assert_eq!(event.timestamp_ms, i as i64);
        }
    }
}

/// Test that data survives a simulated crash (drop without explicit close).
#[test]
fn recover_after_crash_before_flush() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Write data, simulate crash (drop without explicit flush)
    {
        let storage = create_storage_at(dir.path());
        for i in 0..100 {
            storage
                .write("test-topic", 0, &[i as u8], 0, None)
                .expect("write should succeed");
        }
        // Drop without explicit close - simulates crash
        // RocksDB WAL should have the data
    }

    // Phase 2: Restart and verify recovery
    {
        let storage = create_storage_at(dir.path());
        let events = storage
            .read("test-topic", 0, 0, 1000)
            .expect("read should succeed");

        assert_eq!(events.len(), 100, "Lost events after crash!");
    }
}

// =============================================================================
// Sequence Generator Recovery Tests
// =============================================================================

/// Test that sequence numbers continue monotonically after restart.
#[test]
fn sequence_generator_recovers_after_restart() {
    let dir = TempDir::new().unwrap();
    let mut last_seq_before: u64 = 0;

    // Phase 1: Write data and record last sequence
    {
        let storage = create_storage_at(dir.path());
        for _ in 0..100 {
            last_seq_before = storage
                .write("test-topic", 0, b"data", 0, None)
                .expect("write should succeed");
        }
    }

    // Phase 2: Restart and write more data
    {
        let storage = create_storage_at(dir.path());
        let first_seq_after = storage
            .write("test-topic", 0, b"new-data", 0, None)
            .expect("write should succeed");

        assert!(
            first_seq_after > last_seq_before,
            "Sequence after restart ({}) should be > sequence before ({})",
            first_seq_after,
            last_seq_before
        );
    }
}

/// Test that high watermark is correctly recovered.
#[test]
fn high_watermark_recovers_after_restart() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Write data
    {
        let storage = create_storage_at(dir.path());
        for _ in 0..50 {
            storage
                .write("test-topic", 0, b"data", 0, None)
                .expect("write should succeed");
        }

        let hwm = storage.high_watermark("test-topic", 0).unwrap();
        assert_eq!(hwm, 50, "High watermark should be 50 before restart");
    }

    // Phase 2: Verify high watermark after restart
    {
        let storage = create_storage_at(dir.path());
        let hwm = storage.high_watermark("test-topic", 0).unwrap();
        assert_eq!(hwm, 50, "High watermark should be 50 after restart");

        // New writes should continue from correct sequence
        let new_seq = storage
            .write("test-topic", 0, b"new-data", 0, None)
            .expect("write should succeed");
        assert_eq!(new_seq, 51, "New sequence should be 51");
    }
}

// =============================================================================
// Idempotency Key Recovery Tests
// =============================================================================

/// Test that idempotency keys survive restart.
#[test]
fn idempotency_keys_survive_restart() {
    let dir = TempDir::new().unwrap();
    let idempotency_key = "unique-request-456";
    let original_offset: u64;

    // Phase 1: Write with idempotency key
    {
        let storage = create_storage_at(dir.path());
        original_offset = storage
            .write(
                "test-topic",
                0,
                b"original-payload",
                0,
                Some(idempotency_key),
            )
            .expect("write should succeed");
    }

    // Phase 2: Restart and retry with same key
    {
        let storage = create_storage_at(dir.path());
        let retry_offset = storage
            .write("test-topic", 0, b"retry-payload", 0, Some(idempotency_key))
            .expect("write should succeed");

        assert_eq!(
            retry_offset, original_offset,
            "Retry after restart should return same offset"
        );

        // Should still only have one event
        let events = storage
            .read("test-topic", 0, 0, 100)
            .expect("read should succeed");
        assert_eq!(events.len(), 1, "Should only have one event");
        assert_eq!(events[0].payload, b"original-payload");
    }
}

/// Test multiple idempotency keys across restarts.
#[test]
fn multiple_idempotency_keys_survive_restart() {
    let dir = TempDir::new().unwrap();
    let num_keys = 10;

    // Phase 1: Write with multiple idempotency keys
    let mut original_offsets = Vec::new();
    {
        let storage = create_storage_at(dir.path());
        for i in 0..num_keys {
            let key = format!("key-{}", i);
            let offset = storage
                .write(
                    "test-topic",
                    0,
                    format!("payload-{}", i).as_bytes(),
                    0,
                    Some(&key),
                )
                .expect("write should succeed");
            original_offsets.push((key, offset));
        }
    }

    // Phase 2: Restart and retry all keys
    {
        let storage = create_storage_at(dir.path());
        for (key, original_offset) in &original_offsets {
            let retry_offset = storage
                .write("test-topic", 0, b"new-payload", 0, Some(key))
                .expect("write should succeed");

            assert_eq!(
                retry_offset, *original_offset,
                "Key {} should return same offset after restart",
                key
            );
        }

        // Should still only have num_keys events
        let events = storage
            .read("test-topic", 0, 0, 1000)
            .expect("read should succeed");
        assert_eq!(
            events.len(),
            num_keys,
            "Should only have {} events",
            num_keys
        );
    }
}

// =============================================================================
// Consumer Offset Recovery Tests
// =============================================================================

/// Test that consumer offsets survive restart.
#[test]
fn consumer_offsets_survive_restart() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Commit offsets
    {
        let storage = create_storage_at(dir.path());
        storage
            .commit_offset("group-a", "topic-1", 0, 100)
            .expect("commit should succeed");
        storage
            .commit_offset("group-a", "topic-1", 1, 200)
            .expect("commit should succeed");
        storage
            .commit_offset("group-b", "topic-1", 0, 50)
            .expect("commit should succeed");
    }

    // Phase 2: Verify after restart
    {
        let storage = create_storage_at(dir.path());

        let offset_a_0 = storage
            .get_offset("group-a", "topic-1", 0)
            .expect("get offset should succeed");
        let offset_a_1 = storage
            .get_offset("group-a", "topic-1", 1)
            .expect("get offset should succeed");
        let offset_b_0 = storage
            .get_offset("group-b", "topic-1", 0)
            .expect("get offset should succeed");

        assert_eq!(offset_a_0, Some(100), "group-a partition 0 offset");
        assert_eq!(offset_a_1, Some(200), "group-a partition 1 offset");
        assert_eq!(offset_b_0, Some(50), "group-b partition 0 offset");
    }
}

// =============================================================================
// Multiple Partition Recovery Tests
// =============================================================================

/// Test that data in multiple partitions survives restart.
#[test]
fn multiple_partitions_survive_restart() {
    let dir = TempDir::new().unwrap();
    let num_partitions = 4;
    let events_per_partition = 25;

    // Phase 1: Write to multiple partitions
    {
        let storage = create_storage_at(dir.path());
        for partition in 0..num_partitions {
            for i in 0..events_per_partition {
                storage
                    .write(
                        "multi-partition-topic",
                        partition,
                        format!("p{}-event-{}", partition, i).as_bytes(),
                        0,
                        None,
                    )
                    .expect("write should succeed");
            }
        }
    }

    // Phase 2: Verify all partitions after restart
    {
        let storage = create_storage_at(dir.path());
        for partition in 0..num_partitions {
            let events = storage
                .read("multi-partition-topic", partition, 0, 1000)
                .expect("read should succeed");

            assert_eq!(
                events.len(),
                events_per_partition as usize,
                "Partition {} should have {} events",
                partition,
                events_per_partition
            );

            // Verify content
            for (i, event) in events.iter().enumerate() {
                let expected = format!("p{}-event-{}", partition, i);
                assert_eq!(
                    String::from_utf8_lossy(&event.payload),
                    expected,
                    "Partition {} event {} should have correct payload",
                    partition,
                    i
                );
            }
        }
    }
}

// =============================================================================
// Multiple Topic Recovery Tests
// =============================================================================

/// Test that data in multiple topics survives restart.
#[test]
fn multiple_topics_survive_restart() {
    let dir = TempDir::new().unwrap();
    let topics = vec!["users", "orders", "clickstream", "metrics"];

    // Phase 1: Write to multiple topics
    {
        let storage = create_storage_at(dir.path());
        for topic in &topics {
            for i in 0..20 {
                storage
                    .write(
                        topic,
                        0,
                        format!("{}-event-{}", topic, i).as_bytes(),
                        0,
                        None,
                    )
                    .expect("write should succeed");
            }
        }
    }

    // Phase 2: Verify all topics after restart
    {
        let storage = create_storage_at(dir.path());
        for topic in &topics {
            let events = storage
                .read(topic, 0, 0, 1000)
                .expect("read should succeed");

            assert_eq!(events.len(), 20, "Topic {} should have 20 events", topic);

            // Verify content
            for (i, event) in events.iter().enumerate() {
                let expected = format!("{}-event-{}", topic, i);
                assert_eq!(
                    String::from_utf8_lossy(&event.payload),
                    expected,
                    "Topic {} event {} should have correct payload",
                    topic,
                    i
                );
            }
        }
    }
}

// =============================================================================
// Rapid Restart Test
// =============================================================================

/// Test multiple rapid restarts don't corrupt data.
#[test]
fn rapid_restarts_no_corruption() {
    let dir = TempDir::new().unwrap();
    let restarts = 5;
    let events_per_cycle = 20;
    let mut total_events = 0u64;

    for cycle in 0..restarts {
        let storage = create_storage_at(dir.path());

        // Verify existing data
        let events = storage
            .read("test-topic", 0, 0, 10000)
            .expect("read should succeed");
        assert_eq!(
            events.len(),
            total_events as usize,
            "Cycle {} should have {} events",
            cycle,
            total_events
        );

        // Write more data
        for i in 0..events_per_cycle {
            storage
                .write(
                    "test-topic",
                    0,
                    format!("cycle-{}-event-{}", cycle, i).as_bytes(),
                    0,
                    None,
                )
                .expect("write should succeed");
        }
        total_events += events_per_cycle;

        // Drop storage (simulate restart)
    }

    // Final verification
    {
        let storage = create_storage_at(dir.path());
        let events = storage
            .read("test-topic", 0, 0, 10000)
            .expect("read should succeed");
        assert_eq!(
            events.len(),
            total_events as usize,
            "Final check should have {} events",
            total_events
        );

        // Verify all events are in order
        for window in events.windows(2) {
            assert!(
                window[0].sequence < window[1].sequence,
                "Events should be in order after rapid restarts"
            );
        }
    }
}

// =============================================================================
// Large Data Recovery Test
// =============================================================================

/// Test recovery of larger payloads.
#[test]
fn large_payload_recovery() {
    let dir = TempDir::new().unwrap();
    let payload_sizes = [1024, 10 * 1024, 100 * 1024]; // 1KB, 10KB, 100KB

    // Phase 1: Write large payloads
    {
        let storage = create_storage_at(dir.path());
        for (i, size) in payload_sizes.iter().enumerate() {
            let payload: Vec<u8> = (0..*size).map(|b| (b % 256) as u8).collect();
            storage
                .write("large-topic", 0, &payload, 0, None)
                .expect("write should succeed");
            println!("Wrote {}KB payload (event {})", size / 1024, i);
        }
    }

    // Phase 2: Verify after restart
    {
        let storage = create_storage_at(dir.path());
        let events = storage
            .read("large-topic", 0, 0, 100)
            .expect("read should succeed");

        assert_eq!(events.len(), payload_sizes.len());

        for (i, (event, expected_size)) in events.iter().zip(payload_sizes.iter()).enumerate() {
            assert_eq!(
                event.payload.len(),
                *expected_size,
                "Event {} should have {} bytes",
                i,
                expected_size
            );

            // Verify content
            for (j, byte) in event.payload.iter().enumerate() {
                assert_eq!(
                    *byte,
                    (j % 256) as u8,
                    "Event {} byte {} should be correct",
                    i,
                    j
                );
            }
        }
    }
}
