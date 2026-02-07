use std::collections::HashMap;
use std::future::Future;

use serde::Serialize;

use crate::contracts::error::StorageError;
use crate::contracts::storage::ColumnProjection;
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

/// Statistics about pending files for a topic, awaiting snapshot commit.
#[derive(Debug, Clone, Default)]
pub struct PendingSnapshotStats {
    /// Number of pending data files
    pub file_count: usize,
    /// Total bytes across pending files
    pub total_bytes: u64,
}

/// Additional context captured at snapshot commit time.
#[derive(Debug, Clone, Default)]
pub struct SnapshotCommitContext {
    /// Persisted flush watermark per partition at commit time.
    pub watermarks_by_partition: HashMap<u32, u64>,
    /// High watermark (write head) per partition at commit time.
    pub high_watermarks_by_partition: HashMap<u32, u64>,
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
    /// Optional time range parameters enable partition pruning for better performance.
    #[allow(clippy::too_many_arguments)]
    fn read_events(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        limit: usize,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
        projection: &ColumnProjection,
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

    /// Commits an Iceberg snapshot (metadata + manifest) for a table.
    /// Only applicable for Iceberg backends; S3 backends should be a no-op.
    fn commit_snapshot(
        &self,
        _topic: &str,
        _context: SnapshotCommitContext,
    ) -> impl Future<Output = Result<Option<i64>, StorageError>> + Send {
        async move { Ok(None) }
    }

    /// Returns statistics about pending files awaiting snapshot commit.
    /// Used for batched snapshot logic.
    fn pending_snapshot_stats(&self, _topic: &str) -> PendingSnapshotStats {
        PendingSnapshotStats::default()
    }

    /// Returns pending snapshot stats for a specific topic/partition.
    ///
    /// This is used by partition failure cleanup logic to avoid topic-wide
    /// races under concurrent partition flushes.
    fn pending_snapshot_stats_for_partition(
        &self,
        _topic: &str,
        _partition: u32,
    ) -> PendingSnapshotStats {
        PendingSnapshotStats::default()
    }

    /// Returns the current table metadata JSON for catalog registration.
    /// Only Iceberg backends return Some; S3 backends return None.
    fn table_metadata_json(&self, _topic: &str) -> Option<String> {
        None
    }

    /// Clears pending (uncommitted) data files for a topic/partition.
    ///
    /// Called when a flush partition fails partway through writing hour-group
    /// segments. The already-uploaded S3 files become orphans (invisible without
    /// a snapshot), but clearing them from `pending_data_files` prevents
    /// duplicate rows on the next retry cycle.
    fn clear_pending_data_files(&self, _topic: &str, _partition: u32) {}

    /// Returns the highest committed sequence watermark per partition for a topic.
    ///
    /// Implementations should only consider files reachable from the current
    /// committed snapshot (not orphan files). Backends without snapshot semantics
    /// return an empty map.
    fn committed_flush_watermarks(
        &self,
        _topic: &str,
    ) -> impl Future<Output = Result<HashMap<u32, u64>, StorageError>> + Send {
        async move { Ok(HashMap::new()) }
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
