use std::path::Path;
use std::sync::Arc;

use dashmap::DashMap;
use rocksdb::{Options, WriteBatch, DB};

use crate::contracts::{HotStorage, SequenceGenerator, StorageError, StoredEvent};
use crate::storage::AtomicSequenceGenerator;

/// Key prefix for event data
const EVENT_PREFIX: &str = "evt";
/// Key prefix for idempotency tracking
const IDEM_PREFIX: &str = "idem";
/// Key prefix for high watermark tracking
const HWM_PREFIX: &str = "hwm";
/// Key prefix for consumer offsets
const CONSUMER_PREFIX: &str = "consumer";

/// RocksDB-backed hot storage implementation.
pub struct RocksDbStorage {
    db: DB,
    /// Per-partition sequence generators (lock-free concurrent map)
    sequences: DashMap<(String, u32), Arc<AtomicSequenceGenerator>>,
    /// Path for sequence persistence (reserved for future use)
    #[allow(dead_code)]
    data_path: std::path::PathBuf,
}

impl RocksDbStorage {
    /// Opens or creates a RocksDB storage at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // Compression: LZ4 is fast with decent compression
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // Write buffer: larger buffer = fewer flushes to disk
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        opts.set_max_write_buffer_number(3); // Keep 3 buffers before stalling

        // Parallelism: use available CPU cores
        let parallelism = std::thread::available_parallelism()
            .map(|p| p.get() as i32)
            .unwrap_or(4);
        opts.increase_parallelism(parallelism);
        opts.set_max_background_jobs(parallelism.min(4)); // Background compaction/flush

        // Level compaction tuning
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB SST files
        opts.set_level_zero_file_num_compaction_trigger(4);

        // Disable WAL sync on every write for better throughput
        // (data is still durable after process crash, just not OS crash)
        opts.set_wal_dir(path.join("wal"));

        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;

        Ok(Self {
            db,
            sequences: DashMap::new(),
            data_path: path.to_path_buf(),
        })
    }

    /// Gets or creates a sequence generator for a topic/partition.
    fn get_sequence(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Arc<AtomicSequenceGenerator>, StorageError> {
        let key = (topic.to_string(), partition);

        // DashMap handles concurrent access - use entry API for atomic get-or-insert
        let entry = self.sequences.entry(key).or_try_insert_with(|| {
            // Load high watermark from DB if exists
            let hwm = self.load_high_watermark(topic, partition)?;
            Ok::<_, StorageError>(Arc::new(AtomicSequenceGenerator::starting_from(hwm)))
        })?;

        Ok(Arc::clone(entry.value()))
    }

    /// Loads the high watermark from the database.
    fn load_high_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        let key = format!("{}:{}:{}", HWM_PREFIX, topic, partition);
        match self.db.get(key.as_bytes()) {
            Ok(Some(bytes)) => {
                let hwm = u64::from_be_bytes(
                    bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| StorageError::Serialization("Invalid hwm bytes".into()))?,
                );
                Ok(hwm)
            }
            Ok(None) => Ok(0),
            Err(e) => Err(StorageError::RocksDb(e.to_string())),
        }
    }

    /// Saves the high watermark to the database.
    fn save_high_watermark(
        &self,
        topic: &str,
        partition: u32,
        hwm: u64,
    ) -> Result<(), StorageError> {
        let key = format!("{}:{}:{}", HWM_PREFIX, topic, partition);
        self.db
            .put(key.as_bytes(), hwm.to_be_bytes())
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    /// Creates an event key.
    fn event_key(topic: &str, partition: u32, sequence: u64) -> String {
        format!("{}:{}:{}:{:016x}", EVENT_PREFIX, topic, partition, sequence)
    }

    /// Creates an idempotency key.
    fn idempotency_key(topic: &str, partition: u32, idem_key: &str) -> String {
        format!("{}:{}:{}:{}", IDEM_PREFIX, topic, partition, idem_key)
    }

    /// Creates a consumer offset key.
    fn consumer_offset_key(group: &str, topic: &str, partition: u32) -> String {
        format!("{}:{}:{}:{}", CONSUMER_PREFIX, group, topic, partition)
    }

    /// Serializes a stored event to bytes using bincode (fast binary format).
    fn serialize_event(event: &StoredEvent) -> Result<Vec<u8>, StorageError> {
        bincode::serialize(event).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    /// Deserializes bytes to a stored event using bincode.
    fn deserialize_event(bytes: &[u8]) -> Result<StoredEvent, StorageError> {
        bincode::deserialize(bytes).map_err(|e| StorageError::Serialization(e.to_string()))
    }
}

impl HotStorage for RocksDbStorage {
    fn write(
        &self,
        topic: &str,
        partition: u32,
        payload: &[u8],
        timestamp_ms: i64,
        idempotency_key: Option<&str>,
    ) -> Result<u64, StorageError> {
        // Check idempotency first (this read is unavoidable)
        if let Some(idem_key) = idempotency_key {
            if let Some(existing_offset) =
                self.get_idempotency_offset(topic, partition, idem_key)?
            {
                return Ok(existing_offset);
            }
        }

        // Get next sequence
        let seq_gen = self.get_sequence(topic, partition)?;
        let sequence = seq_gen
            .next()
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Create stored event
        let event = StoredEvent {
            sequence,
            topic: topic.to_string(),
            partition,
            payload: payload.to_vec(),
            timestamp_ms,
            idempotency_key: idempotency_key.map(String::from),
        };

        // Serialize
        let event_bytes = Self::serialize_event(&event)?;

        // Use WriteBatch for atomic multi-key write (single disk operation)
        let mut batch = WriteBatch::default();

        // Add event to batch
        let event_key = Self::event_key(topic, partition, sequence);
        batch.put(event_key.as_bytes(), &event_bytes);

        // Add idempotency mapping to batch if present
        if let Some(idem_key) = idempotency_key {
            let idem_db_key = Self::idempotency_key(topic, partition, idem_key);
            batch.put(idem_db_key.as_bytes(), sequence.to_be_bytes());
        }

        // Add high watermark update to batch
        let hwm_key = format!("{}:{}:{}", HWM_PREFIX, topic, partition);
        batch.put(hwm_key.as_bytes(), sequence.to_be_bytes());

        // Single atomic write for all operations
        self.db
            .write(batch)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        Ok(sequence)
    }

    fn read(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        let mut events = Vec::with_capacity(limit);
        let prefix = format!("{}:{}:{}:", EVENT_PREFIX, topic, partition);

        // Create iterator starting at the offset
        let start_key = Self::event_key(topic, partition, offset);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            start_key.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            if events.len() >= limit {
                break;
            }

            let (key, value) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);

            // Stop if we've left our prefix
            if !key_str.starts_with(&prefix) {
                break;
            }

            let event = Self::deserialize_event(&value)?;
            events.push(event);
        }

        Ok(events)
    }

    fn high_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        self.load_high_watermark(topic, partition)
    }

    fn low_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        let prefix = format!("{}:{}:{}:", EVENT_PREFIX, topic, partition);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);

            if !key_str.starts_with(&prefix) {
                break;
            }

            // Parse sequence from key
            if let Some(seq_hex) = key_str.strip_prefix(&prefix) {
                if let Ok(seq) = u64::from_str_radix(seq_hex, 16) {
                    return Ok(seq);
                }
            }
        }

        Ok(0)
    }

    fn get_idempotency_offset(
        &self,
        topic: &str,
        partition: u32,
        idempotency_key: &str,
    ) -> Result<Option<u64>, StorageError> {
        let key = Self::idempotency_key(topic, partition, idempotency_key);
        match self.db.get(key.as_bytes()) {
            Ok(Some(bytes)) => {
                let offset = u64::from_be_bytes(
                    bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| StorageError::Serialization("Invalid offset bytes".into()))?,
                );
                Ok(Some(offset))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::RocksDb(e.to_string())),
        }
    }

    fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), StorageError> {
        let key = Self::consumer_offset_key(group, topic, partition);
        self.db
            .put(key.as_bytes(), offset.to_be_bytes())
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn get_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>, StorageError> {
        let key = Self::consumer_offset_key(group, topic, partition);
        match self.db.get(key.as_bytes()) {
            Ok(Some(bytes)) => {
                let offset = u64::from_be_bytes(
                    bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| StorageError::Serialization("Invalid offset bytes".into()))?,
                );
                Ok(Some(offset))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::RocksDb(e.to_string())),
        }
    }

    fn list_partitions(&self, topic: &str) -> Result<Vec<u32>, StorageError> {
        use std::collections::HashSet;

        let mut partitions = HashSet::new();
        let prefix = format!("{}:{}:", EVENT_PREFIX, topic);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);

            if !key_str.starts_with(&prefix) {
                break;
            }

            // Key format: evt:topic:partition:sequence
            // After prefix we have: partition:sequence
            if let Some(rest) = key_str.strip_prefix(&prefix) {
                if let Some(partition_str) = rest.split(':').next() {
                    if let Ok(partition) = partition_str.parse::<u32>() {
                        partitions.insert(partition);
                    }
                }
            }
        }

        let mut result: Vec<u32> = partitions.into_iter().collect();
        result.sort();
        Ok(result)
    }

    fn list_topics(&self) -> Result<Vec<String>, StorageError> {
        use std::collections::HashSet;

        let mut topics = HashSet::new();
        let prefix = format!("{}:", EVENT_PREFIX);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);

            if !key_str.starts_with(&prefix) {
                break;
            }

            // Key format: evt:topic:partition:sequence
            if let Some(rest) = key_str.strip_prefix(&prefix) {
                if let Some(topic) = rest.split(':').next() {
                    topics.insert(topic.to_string());
                }
            }
        }

        let mut result: Vec<String> = topics.into_iter().collect();
        result.sort();
        Ok(result)
    }

    fn read_all_partitions(
        &self,
        topic: &str,
        start_timestamp_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        let partitions = self.list_partitions(topic)?;

        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        // Read from all partitions (use a reasonable per-partition limit)
        let per_partition_limit = limit.saturating_mul(2).max(1000);
        let mut all_events = Vec::new();
        for partition in partitions {
            let events = self.read(topic, partition, 0, per_partition_limit)?;
            all_events.extend(events);
        }

        // Filter by timestamp if specified
        if let Some(start_ts) = start_timestamp_ms {
            all_events.retain(|e| e.timestamp_ms >= start_ts);
        }

        // Sort by timestamp
        all_events.sort_by_key(|e| e.timestamp_ms);

        // Apply limit
        all_events.truncate(limit);

        Ok(all_events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_storage() -> (RocksDbStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open(dir.path()).unwrap();
        (storage, dir)
    }

    #[test]
    fn write_and_read_single_event() {
        let (storage, _dir) = create_test_storage();

        let offset = storage
            .write("test-topic", 0, b"hello world", 1234567890, None)
            .unwrap();

        assert_eq!(offset, 1);

        let events = storage.read("test-topic", 0, 1, 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"hello world");
        assert_eq!(events[0].timestamp_ms, 1234567890);
        assert_eq!(events[0].sequence, 1);
    }

    #[test]
    fn write_returns_monotonic_sequences() {
        let (storage, _dir) = create_test_storage();

        let mut prev = 0;
        for i in 0..100 {
            let offset = storage
                .write("test-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
                .unwrap();
            assert!(offset > prev, "Expected {} > {}", offset, prev);
            prev = offset;
        }
    }

    #[test]
    fn idempotent_writes_return_same_offset() {
        let (storage, _dir) = create_test_storage();

        let offset1 = storage
            .write("test-topic", 0, b"payload", 0, Some("req-123"))
            .unwrap();

        let offset2 = storage
            .write("test-topic", 0, b"payload", 0, Some("req-123"))
            .unwrap();

        assert_eq!(offset1, offset2);

        // Should only have one event
        let events = storage.read("test-topic", 0, 0, 100).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn partitions_are_isolated() {
        let (storage, _dir) = create_test_storage();

        storage
            .write("test-topic", 0, b"partition-0", 0, None)
            .unwrap();
        storage
            .write("test-topic", 1, b"partition-1", 0, None)
            .unwrap();

        let events_0 = storage.read("test-topic", 0, 0, 100).unwrap();
        let events_1 = storage.read("test-topic", 1, 0, 100).unwrap();

        assert_eq!(events_0.len(), 1);
        assert_eq!(events_1.len(), 1);
        assert_eq!(events_0[0].payload, b"partition-0");
        assert_eq!(events_1[0].payload, b"partition-1");
    }

    #[test]
    fn high_watermark_tracks_latest_offset() {
        let (storage, _dir) = create_test_storage();

        assert_eq!(storage.high_watermark("test-topic", 0).unwrap(), 0);

        storage.write("test-topic", 0, b"event1", 0, None).unwrap();
        assert_eq!(storage.high_watermark("test-topic", 0).unwrap(), 1);

        storage.write("test-topic", 0, b"event2", 0, None).unwrap();
        storage.write("test-topic", 0, b"event3", 0, None).unwrap();
        assert_eq!(storage.high_watermark("test-topic", 0).unwrap(), 3);
    }

    #[test]
    fn read_respects_limit() {
        let (storage, _dir) = create_test_storage();

        for i in 0..10 {
            storage
                .write("test-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
                .unwrap();
        }

        let events = storage.read("test-topic", 0, 1, 3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[2].sequence, 3);
    }

    #[test]
    fn read_from_middle_offset() {
        let (storage, _dir) = create_test_storage();

        for i in 0..10 {
            storage
                .write("test-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
                .unwrap();
        }

        let events = storage.read("test-topic", 0, 5, 100).unwrap();
        assert_eq!(events.len(), 6); // Events 5-10
        assert_eq!(events[0].sequence, 5);
    }

    #[test]
    fn commit_and_get_consumer_offset() {
        let (storage, _dir) = create_test_storage();

        // Initially no offset
        assert_eq!(
            storage.get_offset("my-group", "test-topic", 0).unwrap(),
            None
        );

        // Commit offset
        storage
            .commit_offset("my-group", "test-topic", 0, 100)
            .unwrap();

        // Should be able to read it back
        assert_eq!(
            storage.get_offset("my-group", "test-topic", 0).unwrap(),
            Some(100)
        );

        // Update offset
        storage
            .commit_offset("my-group", "test-topic", 0, 200)
            .unwrap();
        assert_eq!(
            storage.get_offset("my-group", "test-topic", 0).unwrap(),
            Some(200)
        );
    }

    #[test]
    fn consumer_offsets_are_isolated_by_group() {
        let (storage, _dir) = create_test_storage();

        storage
            .commit_offset("group-a", "test-topic", 0, 100)
            .unwrap();
        storage
            .commit_offset("group-b", "test-topic", 0, 200)
            .unwrap();

        assert_eq!(
            storage.get_offset("group-a", "test-topic", 0).unwrap(),
            Some(100)
        );
        assert_eq!(
            storage.get_offset("group-b", "test-topic", 0).unwrap(),
            Some(200)
        );
    }
}
