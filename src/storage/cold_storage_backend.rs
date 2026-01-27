//! Unified cold storage backend that supports both S3 and Iceberg modes.

use crate::contracts::{
    ColdStorage, ColdStorageInfo, ColumnProjection, SegmentInfo, StorageError, StoredEvent,
};
use crate::storage::{IcebergStorage, S3Storage};

/// Unified cold storage backend supporting multiple storage modes.
pub enum ColdStorageBackend {
    /// Plain S3 storage (JSON segments)
    S3(S3Storage),
    /// Iceberg-compatible storage (Parquet + metadata)
    Iceberg(IcebergStorage),
}

impl ColdStorageBackend {
    /// Creates an S3 backend.
    pub fn s3(storage: S3Storage) -> Self {
        Self::S3(storage)
    }

    /// Creates an Iceberg backend.
    pub fn iceberg(storage: IcebergStorage) -> Self {
        Self::Iceberg(storage)
    }

    /// Returns whether this is an Iceberg backend.
    pub fn is_iceberg(&self) -> bool {
        matches!(self, Self::Iceberg(_))
    }
}

impl ColdStorage for ColdStorageBackend {
    async fn write_segment(
        &self,
        topic: &str,
        partition: u32,
        events: &[StoredEvent],
    ) -> Result<String, StorageError> {
        match self {
            Self::S3(s) => s.write_segment(topic, partition, events).await,
            Self::Iceberg(s) => s.write_segment(topic, partition, events).await,
        }
    }

    async fn read_events(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        limit: usize,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
        projection: &ColumnProjection,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        match self {
            Self::S3(s) => {
                s.read_events(
                    topic,
                    partition,
                    start_offset,
                    limit,
                    since_ms,
                    until_ms,
                    projection,
                )
                .await
            }
            Self::Iceberg(s) => {
                s.read_events(
                    topic,
                    partition,
                    start_offset,
                    limit,
                    since_ms,
                    until_ms,
                    projection,
                )
                .await
            }
        }
    }

    async fn list_segments(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Vec<SegmentInfo>, StorageError> {
        match self {
            Self::S3(s) => s.list_segments(topic, partition).await,
            Self::Iceberg(s) => s.list_segments(topic, partition).await,
        }
    }

    fn storage_info(&self) -> ColdStorageInfo {
        match self {
            Self::S3(s) => s.storage_info(),
            Self::Iceberg(s) => s.storage_info(),
        }
    }

    fn iceberg_metadata_location(&self, topic: &str) -> Option<String> {
        match self {
            Self::S3(s) => s.iceberg_metadata_location(topic),
            Self::Iceberg(s) => s.iceberg_metadata_location(topic),
        }
    }

    async fn commit_snapshot(&self, topic: &str) -> Result<Option<i64>, StorageError> {
        match self {
            Self::S3(s) => s.commit_snapshot(topic).await,
            Self::Iceberg(s) => s.commit_snapshot(topic).await,
        }
    }
}
