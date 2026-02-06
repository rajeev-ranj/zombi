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

    /// Writes multiple events to storage in a single batch (#1 Bulk Write API).
    /// Returns the assigned sequence numbers for each event.
    /// All events must be for the same topic.
    fn write_batch(&self, topic: &str, events: &[BulkWriteEvent])
        -> Result<Vec<u64>, StorageError>;

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

    /// Lists all known partitions for a topic (including those with only metadata after cleanup).
    fn list_partitions(&self, topic: &str) -> Result<Vec<u32>, StorageError>;

    /// Lists all known topics (including those with only metadata after cleanup).
    fn list_topics(&self) -> Result<Vec<String>, StorageError>;

    /// Reads events from all partitions, merged by timestamp.
    ///
    /// # Arguments
    /// * `topic` - The topic to read from
    /// * `start_offsets` - Optional per-partition starting offsets (avoids reading from 0)
    /// * `start_timestamp_ms` - Optional timestamp filter
    /// * `limit` - Maximum number of events to return
    fn read_all_partitions(
        &self,
        topic: &str,
        start_offsets: Option<&std::collections::HashMap<u32, u64>>,
        start_timestamp_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError>;

    /// Reads events within a timestamp range using the timestamp secondary index.
    ///
    /// This method provides O(log n) lookup for time-based queries when the
    /// timestamp index is enabled (`ZOMBI_TIMESTAMP_INDEX_ENABLED=true`).
    ///
    /// # Arguments
    /// * `topic` - The topic to read from
    /// * `partition` - The partition to read from
    /// * `since_ms` - Start of time range (inclusive), None for no lower bound
    /// * `until_ms` - End of time range (exclusive), None for no upper bound
    /// * `limit` - Maximum number of events to return
    ///
    /// # Returns
    /// Events within the time range, sorted by sequence number.
    /// Returns empty vec if timestamp index is not enabled.
    fn read_by_timestamp(
        &self,
        topic: &str,
        partition: u32,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        // Default implementation falls back to read_all_partitions
        // Implementations with timestamp index should override this
        let mut offsets = std::collections::HashMap::new();
        offsets.insert(partition, 0);
        let events = self.read_all_partitions(topic, Some(&offsets), since_ms, limit)?;
        // Filter by partition and time range
        Ok(events
            .into_iter()
            .filter(|e| {
                e.partition == partition
                    && since_ms.is_none_or(|s| e.timestamp_ms >= s)
                    && until_ms.is_none_or(|u| e.timestamp_ms < u)
            })
            .take(limit)
            .collect())
    }

    /// Saves the flush watermark for a topic/partition.
    /// The flush watermark tracks the last sequence number successfully flushed to cold storage.
    fn save_flush_watermark(
        &self,
        topic: &str,
        partition: u32,
        watermark: u64,
    ) -> Result<(), StorageError>;

    /// Loads the flush watermark for a topic/partition.
    /// Returns 0 if no watermark has been persisted.
    fn load_flush_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError>;

    /// Deletes events from hot storage up to (and including) `up_to_sequence`.
    ///
    /// Also deletes associated idempotency keys and timestamp index entries.
    /// High watermark keys (`hwm:*`), flush watermark keys (`flush_wm:*`), and
    /// cleanup watermark keys (`cleanup_wm:*`) are preserved.
    ///
    /// Returns the count of deleted event keys. Idempotent: calling with a sequence
    /// at or below the last cleanup watermark returns 0.
    fn delete_flushed_events(
        &self,
        topic: &str,
        partition: u32,
        up_to_sequence: u64,
    ) -> Result<u64, StorageError>;
}

/// Specifies which columns to read from cold storage.
/// When `fields` is `None`, all columns are returned (default behavior).
/// When `fields` is `Some(...)`, only the specified columns are projected,
/// enabling Parquet column pruning for I/O and CPU savings.
#[derive(Debug, Clone, Default)]
pub struct ColumnProjection {
    pub fields: Option<Vec<String>>,
}

impl ColumnProjection {
    /// Returns a projection that reads all columns.
    pub fn all() -> Self {
        Self { fields: None }
    }

    /// Returns a projection that reads only the specified columns.
    pub fn select(fields: Vec<String>) -> Self {
        Self {
            fields: Some(fields),
        }
    }

    /// Returns `true` if all columns should be read.
    pub fn is_all(&self) -> bool {
        self.fields.is_none()
    }
}

/// The set of known base column names for validation.
pub const KNOWN_COLUMNS: &[&str] = &[
    "sequence",
    "topic",
    "partition",
    "payload",
    "timestamp_ms",
    "idempotency_key",
];

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_projection_all_returns_none_fields() {
        let proj = ColumnProjection::all();
        assert!(proj.is_all());
        assert!(proj.fields.is_none());
    }

    #[test]
    fn column_projection_select_returns_specified_fields() {
        let proj = ColumnProjection::select(vec!["payload".into(), "timestamp_ms".into()]);
        assert!(!proj.is_all());
        assert_eq!(
            proj.fields.unwrap(),
            vec!["payload".to_string(), "timestamp_ms".to_string()]
        );
    }

    #[test]
    fn column_projection_default_is_all() {
        let proj = ColumnProjection::default();
        assert!(proj.is_all());
    }

    #[test]
    fn known_columns_contains_base_fields() {
        assert!(KNOWN_COLUMNS.contains(&"sequence"));
        assert!(KNOWN_COLUMNS.contains(&"payload"));
        assert!(KNOWN_COLUMNS.contains(&"timestamp_ms"));
        assert!(KNOWN_COLUMNS.contains(&"topic"));
        assert!(KNOWN_COLUMNS.contains(&"partition"));
        assert!(KNOWN_COLUMNS.contains(&"idempotency_key"));
        assert!(!KNOWN_COLUMNS.contains(&"nonexistent"));
    }
}

/// An event for bulk write operations.
#[derive(Debug, Clone)]
pub struct BulkWriteEvent {
    pub partition: u32,
    pub payload: Vec<u8>,
    pub timestamp_ms: i64,
    pub idempotency_key: Option<String>,
}
