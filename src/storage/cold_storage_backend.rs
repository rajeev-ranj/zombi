//! Unified cold storage backend that supports both S3 and Iceberg modes.

use crate::contracts::{
    ColdStorage, ColdStorageInfo, ColumnProjection, IcebergCatalogTable, PendingSnapshotStats,
    SegmentInfo, SnapshotCommitContext, StorageError, StoredEvent,
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

    /// Returns the current table metadata for a topic (Iceberg only).
    pub fn get_table_metadata(
        &self,
        topic: &str,
    ) -> Result<Option<crate::storage::TableMetadata>, StorageError> {
        match self {
            Self::Iceberg(s) => s.get_table_metadata(topic),
            Self::S3(_) => Ok(None),
        }
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

    async fn commit_snapshot(
        &self,
        topic: &str,
        context: SnapshotCommitContext,
    ) -> Result<Option<i64>, StorageError> {
        match self {
            Self::S3(s) => s.commit_snapshot(topic, context).await,
            Self::Iceberg(s) => s.commit_snapshot(topic, context).await,
        }
    }

    fn table_metadata_json(&self, topic: &str) -> Option<String> {
        match self {
            Self::S3(_) => None,
            Self::Iceberg(s) => s.table_metadata_json(topic),
        }
    }

    async fn list_iceberg_tables(&self) -> Result<Vec<String>, StorageError> {
        match self {
            Self::S3(_) => Ok(Vec::new()),
            Self::Iceberg(s) => s.list_tables().await,
        }
    }

    async fn load_iceberg_table(
        &self,
        topic: &str,
    ) -> Result<Option<IcebergCatalogTable>, StorageError> {
        match self {
            Self::S3(_) => Ok(None),
            Self::Iceberg(s) => s.load_table_for_catalog(topic).await,
        }
    }

    async fn iceberg_table_exists(&self, topic: &str) -> Result<bool, StorageError> {
        match self {
            Self::S3(_) => Ok(false),
            Self::Iceberg(s) => s.iceberg_table_exists(topic).await,
        }
    }

    fn pending_snapshot_stats(&self, topic: &str) -> PendingSnapshotStats {
        match self {
            Self::S3(s) => s.pending_snapshot_stats(topic),
            Self::Iceberg(s) => s.pending_snapshot_stats(topic),
        }
    }

    fn pending_snapshot_stats_for_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> PendingSnapshotStats {
        match self {
            Self::S3(s) => s.pending_snapshot_stats_for_partition(topic, partition),
            Self::Iceberg(s) => s.pending_snapshot_stats_for_partition(topic, partition),
        }
    }

    fn clear_pending_data_files(&self, topic: &str, partition: u32) {
        match self {
            Self::S3(s) => s.clear_pending_data_files(topic, partition),
            Self::Iceberg(s) => s.clear_pending_data_files(topic, partition),
        }
    }

    async fn committed_flush_watermarks(
        &self,
        topic: &str,
    ) -> Result<std::collections::HashMap<u32, u64>, StorageError> {
        match self {
            Self::S3(s) => s.committed_flush_watermarks(topic).await,
            Self::Iceberg(s) => s.committed_flush_watermarks(topic).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{IcebergStorage, RetryConfig};

    #[tokio::test]
    async fn cold_storage_backend_delegates_pending_methods() {
        let retry = RetryConfig {
            max_retries: 1,
            initial_delay_ms: 1,
            max_delay_ms: 1,
        };
        let storage = IcebergStorage::with_endpoint_and_retry(
            "test-bucket",
            "tables",
            "http://127.0.0.1:9",
            "us-east-1",
            retry,
        )
        .await
        .unwrap();

        storage
            .insert_pending_data_files_for_test("events", 0, 1)
            .unwrap();
        storage
            .insert_pending_data_files_for_test("events", 1, 1)
            .unwrap();

        let backend = ColdStorageBackend::iceberg(storage);
        assert_eq!(backend.pending_snapshot_stats("events").file_count, 2);
        assert_eq!(
            backend
                .pending_snapshot_stats_for_partition("events", 0)
                .file_count,
            1
        );
        assert_eq!(
            backend
                .pending_snapshot_stats_for_partition("events", 1)
                .file_count,
            1
        );

        backend.clear_pending_data_files("events", 1);
        assert_eq!(backend.pending_snapshot_stats("events").file_count, 1);
        assert_eq!(
            backend
                .pending_snapshot_stats_for_partition("events", 1)
                .file_count,
            0
        );

        backend.clear_pending_data_files("events", 0);
        assert_eq!(backend.pending_snapshot_stats("events").file_count, 0);
    }
}
