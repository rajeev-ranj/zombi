use std::future::Future;

use crate::contracts::error::StorageError;

/// Background flusher that moves data from hot to cold storage.
///
/// # Behavior
/// - Triggers every N seconds OR every M events (whichever comes first)
/// - Writes events as segments to cold storage
/// - Updates flush watermark atomically
/// - Optionally deletes flushed data from hot storage after grace period
pub trait Flusher: Send + Sync {
    /// Starts the flusher background task.
    fn start(&self) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Stops the flusher gracefully.
    fn stop(&self) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Forces an immediate flush of pending events.
    fn flush_now(&self) -> impl Future<Output = Result<FlushResult, StorageError>> + Send;

    /// Returns the current flush watermark for a topic/partition.
    fn flush_watermark(
        &self,
        topic: &str,
        partition: u32,
    ) -> impl Future<Output = Result<u64, StorageError>> + Send;
}

/// Result of a flush operation.
#[derive(Debug, Clone)]
pub struct FlushResult {
    /// Number of events flushed
    pub events_flushed: usize,
    /// Number of segments written
    pub segments_written: usize,
    /// New flush watermark
    pub new_watermark: u64,
}
