use std::future::Future;

use serde::Serialize;

use crate::contracts::error::StorageError;
use crate::contracts::StoredEvent;

/// Information about the cold storage backend.
#[derive(Debug, Clone, Serialize)]
pub struct ColdStorageInfo {
    /// Storage type identifier (e.g., "s3", "iceberg")
    pub storage_type: String,
    /// Whether Iceberg metadata is enabled
    pub iceberg_enabled: bool,
    /// S3 bucket name
    pub bucket: String,
    /// Base path within the bucket
    pub base_path: String,
}

/// Cold storage for archived events (S3).
///
/// Events are written in batches as log segments.
pub trait ColdStorage: Send + Sync {
    /// Writes a batch of events to cold storage.
    /// Returns the segment identifier.
    fn write_segment(
        &self,
        topic: &str,
        partition: u32,
        events: &[StoredEvent],
    ) -> impl Future<Output = Result<String, StorageError>> + Send;

    /// Reads events from cold storage starting at the given offset.
    fn read_events(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredEvent>, StorageError>> + Send;

    /// Lists all segments for a topic/partition.
    fn list_segments(
        &self,
        topic: &str,
        partition: u32,
    ) -> impl Future<Output = Result<Vec<SegmentInfo>, StorageError>> + Send;

    /// Returns information about the storage backend.
    fn storage_info(&self) -> ColdStorageInfo;

    /// Returns the Iceberg metadata location for a table, if Iceberg is enabled.
    fn iceberg_metadata_location(&self, _topic: &str) -> Option<String> {
        None
    }
}

/// Information about a stored segment.
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    /// Segment identifier (S3 key)
    pub segment_id: String,
    /// First offset in the segment
    pub start_offset: u64,
    /// Last offset in the segment
    pub end_offset: u64,
    /// Number of events in the segment
    pub event_count: usize,
    /// Segment size in bytes
    pub size_bytes: u64,
}
