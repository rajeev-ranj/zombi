//! Crash recovery tests for Zombi storage system.
//!
//! These tests verify that data survives crashes and restarts.
//! Run with: cargo test --test crash_recovery_tests
//!
//! ## Test Categories
//!
//! ### Hot Storage Recovery (RocksDB WAL)
//! - `data_survives_clean_restart`
//! - `recover_after_crash_before_flush`
//! - `sequence_generator_recovers_after_restart`
//! - `high_watermark_recovers_after_restart`
//! - `idempotency_keys_survive_restart`
//! - `multiple_idempotency_keys_survive_restart`
//! - `consumer_offsets_survive_restart`
//! - `multiple_partitions_survive_restart`
//! - `multiple_topics_survive_restart`
//! - `rapid_restarts_no_corruption`
//! - `large_payload_recovery`
//!
//! ### Cold Storage Recovery (Issue #21)
//! - `unflushed_events_survive_crash` - RocksDB WAL preserves unflushed events
//! - `flushed_events_readable_after_restart` - Cold storage persists across restart
//! - `mixed_hot_cold_recovery` - Partial flush scenario
//! - `flush_watermark_starts_at_zero_after_restart` - Documents volatile watermark behavior
//! - `multi_partition_crash_recovery` - Partition independence in recovery

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use zombi::contracts::{
    ColdStorage, ColdStorageInfo, Flusher, HotStorage, SegmentInfo, StoredEvent,
};
use zombi::flusher::{BackgroundFlusher, FlusherConfig};
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

// =============================================================================
// Cold Storage Recovery Tests (Issue #21)
// =============================================================================

/// File-based cold storage for testing persistence across restarts.
///
/// This mock writes JSON segments to disk, simulating S3 behavior
/// without network I/O. Files persist across storage reopens, allowing
/// true crash recovery testing.
struct FileColdStorage {
    base_path: PathBuf,
}

impl FileColdStorage {
    fn new(base_path: &Path) -> Self {
        Self {
            base_path: base_path.to_path_buf(),
        }
    }

    fn segment_dir(&self, topic: &str, partition: u32) -> PathBuf {
        self.base_path.join(topic).join(partition.to_string())
    }

    fn segment_path(&self, topic: &str, partition: u32, start: u64, end: u64) -> PathBuf {
        self.segment_dir(topic, partition)
            .join(format!("{:016x}-{:016x}.json", start, end))
    }
}

impl ColdStorage for FileColdStorage {
    async fn write_segment(
        &self,
        topic: &str,
        partition: u32,
        events: &[StoredEvent],
    ) -> Result<String, zombi::contracts::StorageError> {
        if events.is_empty() {
            return Err(zombi::contracts::StorageError::S3(
                "Cannot write empty segment".into(),
            ));
        }

        let dir = self.segment_dir(topic, partition);
        fs::create_dir_all(&dir).map_err(|e| {
            zombi::contracts::StorageError::S3(format!("Failed to create dir: {}", e))
        })?;

        let start = events.first().unwrap().sequence;
        let end = events.last().unwrap().sequence;
        let path = self.segment_path(topic, partition, start, end);

        let json = serde_json::to_string_pretty(events)
            .map_err(|e| zombi::contracts::StorageError::S3(format!("JSON encode error: {}", e)))?;

        fs::write(&path, json).map_err(|e| {
            zombi::contracts::StorageError::S3(format!("Failed to write segment: {}", e))
        })?;

        Ok(path.to_string_lossy().to_string())
    }

    async fn read_events(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        limit: usize,
        _since_ms: Option<i64>,
        _until_ms: Option<i64>,
    ) -> Result<Vec<StoredEvent>, zombi::contracts::StorageError> {
        let dir = self.segment_dir(topic, partition);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        // Read all segment files and collect events
        let mut all_events = Vec::new();
        let entries = fs::read_dir(&dir).map_err(|e| {
            zombi::contracts::StorageError::S3(format!("Failed to read dir: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                zombi::contracts::StorageError::S3(format!("Failed to read entry: {}", e))
            })?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "json") {
                let content = fs::read_to_string(&path).map_err(|e| {
                    zombi::contracts::StorageError::S3(format!("Failed to read file: {}", e))
                })?;

                let events: Vec<StoredEvent> = serde_json::from_str(&content).map_err(|e| {
                    zombi::contracts::StorageError::S3(format!("JSON decode error: {}", e))
                })?;

                all_events.extend(events);
            }
        }

        // Sort by sequence and filter
        all_events.sort_by_key(|e| e.sequence);
        Ok(all_events
            .into_iter()
            .filter(|e| e.sequence >= start_offset)
            .take(limit)
            .collect())
    }

    async fn list_segments(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Vec<SegmentInfo>, zombi::contracts::StorageError> {
        let dir = self.segment_dir(topic, partition);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut segments = Vec::new();
        let entries = fs::read_dir(&dir).map_err(|e| {
            zombi::contracts::StorageError::S3(format!("Failed to read dir: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                zombi::contracts::StorageError::S3(format!("Failed to read entry: {}", e))
            })?;
            let path = entry.path();

            if let Some(file_name) = path.file_stem() {
                let name = file_name.to_string_lossy();
                // Parse filename like "0000000000000001-0000000000000050"
                if let Some((start_str, end_str)) = name.split_once('-') {
                    if let (Ok(start), Ok(end)) = (
                        u64::from_str_radix(start_str, 16),
                        u64::from_str_radix(end_str, 16),
                    ) {
                        let metadata = fs::metadata(&path).ok();
                        let size_bytes = metadata.map(|m| m.len()).unwrap_or(0);

                        segments.push(SegmentInfo {
                            segment_id: path.to_string_lossy().to_string(),
                            start_offset: start,
                            end_offset: end,
                            event_count: (end - start + 1) as usize,
                            size_bytes,
                        });
                    }
                }
            }
        }

        segments.sort_by_key(|s| s.start_offset);
        Ok(segments)
    }

    fn storage_info(&self) -> ColdStorageInfo {
        ColdStorageInfo {
            storage_type: "file".into(),
            iceberg_enabled: false,
            bucket: "local".into(),
            base_path: self.base_path.to_string_lossy().to_string(),
        }
    }
}

/// Test that unflushed events survive a crash (RocksDB WAL preserves them).
///
/// This test verifies INV-2: No data loss after ACK.
/// Events written to hot storage but not yet flushed to cold storage
/// should be recoverable from the RocksDB WAL after a crash.
#[test]
fn unflushed_events_survive_crash() {
    let dir = TempDir::new().unwrap();
    let events_count = 50;

    // Phase 1: Write events to hot storage, do NOT flush to cold
    {
        let storage = create_storage_at(dir.path());
        for i in 0..events_count {
            storage
                .write(
                    "test-topic",
                    0,
                    format!("unflushed-event-{}", i).as_bytes(),
                    i as i64 * 1000,
                    None,
                )
                .expect("write should succeed");
        }
        // Drop storage without flushing - simulates crash
    }

    // Phase 2: Reopen and verify all events recovered from WAL
    {
        let storage = create_storage_at(dir.path());
        let events = storage
            .read("test-topic", 0, 0, 1000)
            .expect("read should succeed");

        assert_eq!(
            events.len(),
            events_count,
            "All {} unflushed events should be recovered from WAL",
            events_count
        );

        // Verify content and order
        for (i, event) in events.iter().enumerate() {
            let expected = format!("unflushed-event-{}", i);
            assert_eq!(
                String::from_utf8_lossy(&event.payload),
                expected,
                "Event {} should have correct payload after recovery",
                i
            );
            assert_eq!(
                event.timestamp_ms,
                i as i64 * 1000,
                "Event {} should have correct timestamp",
                i
            );
        }
    }
}

/// Test that events flushed to cold storage are readable after restart.
///
/// This test verifies that cold storage (file-based mock simulating S3)
/// persists data independently of hot storage across restarts.
#[tokio::test]
async fn flushed_events_readable_after_restart() {
    let hot_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();
    let events_count = 100;

    // Phase 1: Write events and flush to cold storage
    {
        let hot_storage = Arc::new(create_storage_at(hot_dir.path()));
        let cold_storage = Arc::new(FileColdStorage::new(cold_dir.path()));

        // Write events
        for i in 0..events_count {
            hot_storage
                .write(
                    "test-topic",
                    0,
                    format!("flushed-event-{}", i).as_bytes(),
                    i as i64 * 1000,
                    None,
                )
                .expect("write should succeed");
        }

        // Flush to cold storage using the flusher
        let config = FlusherConfig::default();
        let flusher =
            BackgroundFlusher::new(Arc::clone(&hot_storage), Arc::clone(&cold_storage), config);

        let result = flusher.flush_now().await.expect("flush should succeed");
        assert_eq!(
            result.events_flushed, events_count,
            "Should flush all events"
        );

        // Drop both storages - simulates crash
    }

    // Phase 2: Reopen cold storage only and verify events are readable
    {
        let cold_storage = FileColdStorage::new(cold_dir.path());
        let events = cold_storage
            .read_events("test-topic", 0, 0, 1000, None, None)
            .await
            .expect("read should succeed");

        assert_eq!(
            events.len(),
            events_count,
            "All {} events should be readable from cold storage after restart",
            events_count
        );

        // Verify content and order
        for (i, event) in events.iter().enumerate() {
            let expected = format!("flushed-event-{}", i);
            assert_eq!(
                String::from_utf8_lossy(&event.payload),
                expected,
                "Event {} should have correct payload in cold storage",
                i
            );
        }

        // Verify segments were created
        let segments = cold_storage
            .list_segments("test-topic", 0)
            .await
            .expect("list should succeed");
        assert!(!segments.is_empty(), "Should have at least one segment");
    }
}

/// Test mixed hot/cold recovery scenario (most realistic crash case).
///
/// This test covers the common case where some events have been flushed
/// to cold storage while newer events remain only in hot storage.
/// After a crash, both tiers should be consistent.
#[tokio::test]
async fn mixed_hot_cold_recovery() {
    let hot_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();
    let flushed_count = 50;
    let unflushed_count = 30;
    let total_count = flushed_count + unflushed_count;

    // Phase 1: Write some events and flush, then write more (not flushed)
    {
        let hot_storage = Arc::new(create_storage_at(hot_dir.path()));
        let cold_storage = Arc::new(FileColdStorage::new(cold_dir.path()));

        // Write first batch
        for i in 0..flushed_count {
            hot_storage
                .write(
                    "test-topic",
                    0,
                    format!("event-{}", i).as_bytes(),
                    i as i64 * 1000,
                    None,
                )
                .expect("write should succeed");
        }

        // Flush first batch to cold storage
        let config = FlusherConfig::default();
        let flusher =
            BackgroundFlusher::new(Arc::clone(&hot_storage), Arc::clone(&cold_storage), config);

        let result = flusher.flush_now().await.expect("flush should succeed");
        assert_eq!(result.events_flushed, flushed_count);

        // Write second batch (not flushed)
        for i in flushed_count..total_count {
            hot_storage
                .write(
                    "test-topic",
                    0,
                    format!("event-{}", i).as_bytes(),
                    i as i64 * 1000,
                    None,
                )
                .expect("write should succeed");
        }

        // Drop storages - simulates crash before second batch is flushed
    }

    // Phase 2: Verify cold storage has first batch, hot storage has all events
    {
        let hot_storage = create_storage_at(hot_dir.path());
        let cold_storage = FileColdStorage::new(cold_dir.path());

        // Cold storage should have only the first batch
        let cold_events = cold_storage
            .read_events("test-topic", 0, 0, 1000, None, None)
            .await
            .expect("cold read should succeed");
        assert_eq!(
            cold_events.len(),
            flushed_count,
            "Cold storage should have {} flushed events",
            flushed_count
        );

        // Hot storage should have ALL events (including unflushed)
        let hot_events = hot_storage
            .read("test-topic", 0, 0, 1000)
            .expect("hot read should succeed");
        assert_eq!(
            hot_events.len(),
            total_count,
            "Hot storage should have all {} events",
            total_count
        );

        // Verify continuity - all events present and in order
        for (i, event) in hot_events.iter().enumerate() {
            let expected = format!("event-{}", i);
            assert_eq!(
                String::from_utf8_lossy(&event.payload),
                expected,
                "Event {} should be correct after mixed recovery",
                i
            );
        }
    }
}

/// Test that flush watermark resets to zero after restart.
///
/// This test documents the EXPECTED behavior that flush watermarks are
/// volatile (in-memory HashMap). On restart, watermarks reset to 0.
/// This is a known limitation, not a bug.
///
/// Implication: After restart, the flusher may re-flush events that were
/// already flushed. This is safe (idempotent) but may cause duplicate
/// segments in cold storage.
#[tokio::test]
async fn flush_watermark_starts_at_zero_after_restart() {
    let hot_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();
    let events_count = 100;

    // Phase 1: Write events, flush all, record watermark
    let watermark_before: u64;
    {
        let hot_storage = Arc::new(create_storage_at(hot_dir.path()));
        let cold_storage = Arc::new(FileColdStorage::new(cold_dir.path()));

        for i in 0..events_count {
            hot_storage
                .write("test-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
                .expect("write should succeed");
        }

        let config = FlusherConfig::default();
        let flusher =
            BackgroundFlusher::new(Arc::clone(&hot_storage), Arc::clone(&cold_storage), config);

        let result = flusher.flush_now().await.expect("flush should succeed");
        assert_eq!(result.events_flushed, events_count);

        watermark_before = flusher
            .flush_watermark("test-topic", 0)
            .await
            .expect("watermark should be readable");
        assert_eq!(
            watermark_before, events_count as u64,
            "Watermark should be {} after flushing all events",
            events_count
        );

        // Drop - simulates crash
    }

    // Phase 2: Create new flusher and verify watermark is 0
    {
        let hot_storage = Arc::new(create_storage_at(hot_dir.path()));
        let cold_storage = Arc::new(FileColdStorage::new(cold_dir.path()));

        let config = FlusherConfig::default();
        let flusher =
            BackgroundFlusher::new(Arc::clone(&hot_storage), Arc::clone(&cold_storage), config);

        let watermark_after = flusher
            .flush_watermark("test-topic", 0)
            .await
            .expect("watermark should be readable");

        // This documents EXPECTED behavior: watermark is volatile
        assert_eq!(
            watermark_after, 0,
            "Flush watermark should reset to 0 after restart (watermark is volatile)"
        );

        // Consequence: flushing again would re-flush all events
        // This is safe due to idempotent segment naming in S3/Iceberg
    }
}

/// Test that multiple partitions recover independently after crash.
///
/// This test verifies INV-5: Partition isolation - each partition's data
/// is recovered independently without affecting other partitions.
#[test]
fn multi_partition_crash_recovery() {
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
                        format!("partition-{}-event-{}", partition, i).as_bytes(),
                        (partition as i64 * 1000) + i as i64,
                        None,
                    )
                    .expect("write should succeed");
            }
        }
        // Drop without explicit close - simulates crash
    }

    // Phase 2: Verify each partition recovered independently
    {
        let storage = create_storage_at(dir.path());

        for partition in 0..num_partitions {
            let events = storage
                .read("multi-partition-topic", partition, 0, 1000)
                .expect("read should succeed");

            assert_eq!(
                events.len(),
                events_per_partition as usize,
                "Partition {} should have {} events after crash recovery",
                partition,
                events_per_partition
            );

            // Verify content
            for (i, event) in events.iter().enumerate() {
                let expected = format!("partition-{}-event-{}", partition, i);
                assert_eq!(
                    String::from_utf8_lossy(&event.payload),
                    expected,
                    "Partition {} event {} should have correct payload",
                    partition,
                    i
                );
            }

            // Verify sequences are monotonic within partition
            for window in events.windows(2) {
                assert!(
                    window[0].sequence < window[1].sequence,
                    "Partition {} sequences should be monotonic after recovery",
                    partition
                );
            }

            // Verify partition isolation
            for event in &events {
                assert_eq!(
                    event.partition, partition,
                    "Event should belong to partition {}",
                    partition
                );
            }
        }

        // Verify high watermarks are correct for each partition
        for partition in 0..num_partitions {
            let hwm = storage
                .high_watermark("multi-partition-topic", partition)
                .expect("high watermark should succeed");
            assert_eq!(
                hwm, events_per_partition as u64,
                "Partition {} high watermark should be {} after recovery",
                partition, events_per_partition
            );
        }
    }
}
