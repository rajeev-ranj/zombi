#![allow(unused_imports, dead_code)]

use proptest::prelude::*;
use zombi::contracts::{HotStorage, SequenceGenerator, StoredEvent};

// Test harness for running property tests against any HotStorage implementation.
// This module defines the tests; implementations plug in via the `test_storage` function.

// =============================================================================
// INV-1: Sequences are monotonic
// =============================================================================

/// Property: Every sequence number returned by next() is greater than the previous.
pub fn prop_sequences_are_monotonic<S: SequenceGenerator>(seq_gen: &S, iterations: usize) {
    let mut prev = 0u64;
    for _ in 0..iterations {
        let next = seq_gen.next().expect("next() should not fail");
        assert!(
            next > prev,
            "Sequence must be monotonic: got {} after {}",
            next,
            prev
        );
        prev = next;
    }
}

// =============================================================================
// INV-2: No data loss
// =============================================================================

/// Property: Every written event can be read back.
pub fn prop_no_data_loss<S: HotStorage>(
    storage: &S,
    topic: &str,
    partition: u32,
    payloads: &[Vec<u8>],
) {
    let mut offsets = Vec::new();

    // Write all events
    for payload in payloads {
        let offset = storage
            .write(topic, partition, payload, 0, None)
            .expect("write should succeed");
        offsets.push(offset);
    }

    // Read them all back
    let low = *offsets.first().unwrap_or(&0);
    let events = storage
        .read(topic, partition, low, payloads.len())
        .expect("read should succeed");

    assert_eq!(
        events.len(),
        payloads.len(),
        "Should read back same number of events"
    );

    for (i, event) in events.iter().enumerate() {
        assert_eq!(
            event.payload, payloads[i],
            "Event {} payload should match",
            i
        );
    }
}

// =============================================================================
// INV-3: Order preserved
// =============================================================================

/// Property: Events are read back in the order they were written.
pub fn prop_order_preserved<S: HotStorage>(
    storage: &S,
    topic: &str,
    partition: u32,
    payloads: &[Vec<u8>],
) {
    let mut offsets = Vec::new();

    for payload in payloads {
        let offset = storage
            .write(topic, partition, payload, 0, None)
            .expect("write should succeed");
        offsets.push(offset);
    }

    // Offsets should be strictly increasing
    for i in 1..offsets.len() {
        assert!(
            offsets[i] > offsets[i - 1],
            "Offsets must be strictly increasing"
        );
    }

    // Reading should return in offset order
    if !offsets.is_empty() {
        let events = storage
            .read(topic, partition, offsets[0], payloads.len())
            .expect("read should succeed");

        for (i, event) in events.iter().enumerate() {
            assert_eq!(
                event.sequence, offsets[i],
                "Event {} should have correct offset",
                i
            );
        }
    }
}

// =============================================================================
// INV-4: Idempotent writes
// =============================================================================

/// Property: Writing with the same idempotency key returns the same offset.
pub fn prop_idempotent_writes<S: HotStorage>(
    storage: &S,
    topic: &str,
    partition: u32,
    payload: &[u8],
    idempotency_key: &str,
) {
    let offset1 = storage
        .write(topic, partition, payload, 0, Some(idempotency_key))
        .expect("first write should succeed");

    let offset2 = storage
        .write(topic, partition, payload, 0, Some(idempotency_key))
        .expect("second write should succeed");

    assert_eq!(
        offset1, offset2,
        "Idempotent writes must return the same offset"
    );

    // Should only have one event stored
    let events = storage
        .read(topic, partition, offset1, 10)
        .expect("read should succeed");

    let matching_events: Vec<_> = events
        .iter()
        .filter(|e| e.idempotency_key.as_deref() == Some(idempotency_key))
        .collect();

    assert_eq!(
        matching_events.len(),
        1,
        "Should only store one event for idempotent writes"
    );
}

// =============================================================================
// INV-5: Partition isolation
// =============================================================================

/// Property: Events written to one partition don't appear in another.
pub fn prop_partition_isolation<S: HotStorage>(
    storage: &S,
    topic: &str,
    partition_a: u32,
    partition_b: u32,
    payload_a: &[u8],
    payload_b: &[u8],
) {
    if partition_a == partition_b {
        return; // Skip if same partition
    }

    let _offset_a = storage
        .write(topic, partition_a, payload_a, 0, None)
        .expect("write to partition A should succeed");

    let _offset_b = storage
        .write(topic, partition_b, payload_b, 0, None)
        .expect("write to partition B should succeed");

    // Read from partition A - should only see payload_a
    let events_a = storage
        .read(topic, partition_a, 0, 100)
        .expect("read from partition A should succeed");

    for event in &events_a {
        assert_eq!(
            event.partition, partition_a,
            "Event should be in partition A"
        );
        assert_ne!(
            event.payload, payload_b,
            "Partition A should not contain partition B's payload"
        );
    }

    // Read from partition B - should only see payload_b
    let events_b = storage
        .read(topic, partition_b, 0, 100)
        .expect("read from partition B should succeed");

    for event in &events_b {
        assert_eq!(
            event.partition, partition_b,
            "Event should be in partition B"
        );
        assert_ne!(
            event.payload, payload_a,
            "Partition B should not contain partition A's payload"
        );
    }
}

// =============================================================================
// Proptest strategies
// =============================================================================

prop_compose! {
    fn arb_payload()(bytes in prop::collection::vec(any::<u8>(), 1..1000)) -> Vec<u8> {
        bytes
    }
}

prop_compose! {
    fn arb_payloads(max_count: usize)(
        payloads in prop::collection::vec(arb_payload(), 1..max_count)
    ) -> Vec<Vec<u8>> {
        payloads
    }
}

prop_compose! {
    fn arb_topic()(s in "[a-z][a-z0-9_-]{0,49}") -> String {
        s
    }
}

prop_compose! {
    fn arb_partition()(p in 0u32..16) -> u32 {
        p
    }
}

prop_compose! {
    fn arb_idempotency_key()(s in "[a-zA-Z0-9_-]{8,32}") -> String {
        s
    }
}

// =============================================================================
// Test module with RocksDB implementation
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use zombi::storage::{derive_partition_columns, AtomicSequenceGenerator, RocksDbStorage};

    fn create_test_storage() -> (RocksDbStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open(dir.path()).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_sequences_are_monotonic() {
        let seq_gen = AtomicSequenceGenerator::new();
        prop_sequences_are_monotonic(&seq_gen, 1000);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn test_no_data_loss(payloads in arb_payloads(20)) {
            let (storage, _dir) = create_test_storage();
            prop_no_data_loss(&storage, "test-topic", 0, &payloads);
        }

        #[test]
        fn test_order_preserved(payloads in arb_payloads(20)) {
            let (storage, _dir) = create_test_storage();
            prop_order_preserved(&storage, "test-topic", 0, &payloads);
        }

        #[test]
        fn test_idempotent_writes(
            payload in arb_payload(),
            key in arb_idempotency_key()
        ) {
            let (storage, _dir) = create_test_storage();
            prop_idempotent_writes(&storage, "test-topic", 0, &payload, &key);
        }

        #[test]
        fn test_partition_isolation(
            payload_a in arb_payload(),
            payload_b in arb_payload(),
            partition_a in arb_partition(),
            partition_b in arb_partition()
        ) {
            let (storage, _dir) = create_test_storage();
            prop_partition_isolation(
                &storage,
                "test-topic",
                partition_a,
                partition_b,
                &payload_a,
                &payload_b
            );
        }

        /// INV-8 (watermark persistence): Flush watermarks round-trip through RocksDB.
        #[test]
        fn test_flush_watermark_round_trips(
            topic in arb_topic(),
            partition in arb_partition(),
            watermark in any::<u64>()
        ) {
            let (storage, _dir) = create_test_storage();
            storage.save_flush_watermark(&topic, partition, watermark).unwrap();
            let loaded = storage.load_flush_watermark(&topic, partition).unwrap();
            prop_assert_eq!(loaded, watermark);
        }

        /// INV-6 (hour partitioning): Grouping events by derive_partition_columns
        /// produces groups where every event shares the same (date, hour).
        #[test]
        fn test_hour_grouping_produces_same_hour_segments(
            timestamps in prop::collection::vec(
                // Random timestamps across a 48-hour range starting 2024-01-15
                1705276800000i64..1705449600000i64,
                1..50usize
            )
        ) {
            let mut groups: std::collections::BTreeMap<(i32, i32), Vec<i64>> =
                std::collections::BTreeMap::new();
            for ts in &timestamps {
                let (date, hour) = derive_partition_columns(*ts);
                groups.entry((date, hour)).or_default().push(*ts);
            }

            // All timestamps in each group have the same (date, hour)
            for ((date, hour), group_timestamps) in &groups {
                for ts in group_timestamps {
                    let (d, h) = derive_partition_columns(*ts);
                    prop_assert_eq!(d, *date);
                    prop_assert_eq!(h, *hour);
                }
            }

            // Union of all groups equals the original set size
            let total: usize = groups.values().map(|g| g.len()).sum();
            prop_assert_eq!(total, timestamps.len());

            // No empty groups
            for group in groups.values() {
                prop_assert!(!group.is_empty());
            }
        }
    }
}
