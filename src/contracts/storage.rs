use serde::{Deserialize, Serialize};

use crate::contracts::error::StorageError;

/// Hot storage for recent events (RocksDB).
///
/// # Invariants
/// - INV-2: No data loss - every ACKed write is readable
/// - INV-3: Order preserved - events read in sequence order
/// - INV-4: Idempotent - same idempotency key = same offset
/// - INV-5: Partition isolation - partition data stays in partition
pub trait HotStorage: Send + Sync {
    /// Writes an event to storage.
    /// Returns the assigned sequence number.
    fn write(
        &self,
        topic: &str,
        partition: u32,
        payload: &[u8],
        timestamp_ms: i64,
        idempotency_key: Option<&str>,
    ) -> Result<u64, StorageError>;

    /// Reads events starting from offset.
    /// Returns up to `limit` events.
    fn read(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError>;

    /// Returns the highest sequence number written to this partition.
    fn high_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError>;

    /// Returns the lowest available sequence number in hot storage.
    fn low_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError>;

    /// Checks if an idempotency key has been seen before.
    /// Returns Some(offset) if the key exists, None otherwise.
    fn get_idempotency_offset(
        &self,
        topic: &str,
        partition: u32,
        idempotency_key: &str,
    ) -> Result<Option<u64>, StorageError>;

    /// Commits a consumer group offset.
    fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), StorageError>;

    /// Gets the committed offset for a consumer group.
    fn get_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>, StorageError>;

    /// Lists all partitions that have data for a topic.
    fn list_partitions(&self, topic: &str) -> Result<Vec<u32>, StorageError>;

    /// Lists all topics that have data.
    fn list_topics(&self) -> Result<Vec<String>, StorageError>;

    /// Reads events from all partitions, merged by timestamp.
    /// Returns up to `limit` events starting from `start_timestamp_ms`.
    fn read_all_partitions(
        &self,
        topic: &str,
        start_timestamp_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError>;
}

/// An event with its storage metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub sequence: u64,
    pub topic: String,
    pub partition: u32,
    pub payload: Vec<u8>,
    pub timestamp_ms: i64,
    pub idempotency_key: Option<String>,
}
