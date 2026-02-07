use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::contracts::{LockResultExt, StorageError, StoredEvent};
use crate::storage::{
    data_file_name, write_parquet_to_bytes_sorted, DataFile, IcebergStorage, ParquetFileMetadata,
};

/// Configuration for compaction.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum file size to consider for compaction (bytes).
    /// Files smaller than this are candidates for merging.
    pub min_file_size_bytes: u64,
    /// Target file size after compaction (bytes).
    /// Default: 512MB (Iceberg best practice for compacted files).
    /// Override with `ZOMBI_COMPACTED_FILE_SIZE_MB` environment variable.
    pub target_file_size_bytes: u64,
    /// Maximum number of files to compact in one operation.
    pub max_files_per_compaction: usize,
    /// Minimum number of small files before triggering compaction.
    pub min_files_to_compact: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_file_size_bytes: 64 * 1024 * 1024,     // 64MB
            target_file_size_bytes: 512 * 1024 * 1024, // 512MB (Iceberg best practice)
            max_files_per_compaction: 10,
            min_files_to_compact: 3,
        }
    }
}

impl CompactionConfig {
    /// Creates a config with environment variable overrides.
    ///
    /// Reads the following environment variables:
    /// - `ZOMBI_COMPACTED_FILE_SIZE_MB`: Target file size after compaction (default: 512MB)
    pub fn from_env() -> Self {
        let default = Self::default();

        let target_file_size_bytes = std::env::var("ZOMBI_COMPACTED_FILE_SIZE_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(default.target_file_size_bytes);

        Self {
            target_file_size_bytes,
            ..default
        }
    }
}

/// Result of a compaction operation.
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// Number of input files compacted.
    pub files_compacted: usize,
    /// Number of output files created.
    pub files_created: usize,
    /// Total rows processed.
    pub rows_processed: i64,
    /// Bytes saved (input - output).
    pub bytes_saved: i64,
    /// New snapshot ID if metadata was updated.
    pub snapshot_id: Option<i64>,
}

#[derive(Debug, Clone)]
struct CompactionCandidate {
    data_file: DataFile,
    s3_key: String,
    size_bytes: u64,
}

#[derive(Debug, Clone)]
struct CompactionPlan {
    base_snapshot_id: i64,
    groups: HashMap<String, Vec<CompactionCandidate>>,
}

#[derive(Debug, Clone)]
struct PartitionCompactionResult {
    files_compacted: usize,
    files_created: usize,
    rows_processed: i64,
    input_bytes: u64,
    output_bytes: u64,
    deleted_file_paths: Vec<String>,
    deleted_s3_keys: Vec<String>,
    added_files: Vec<(DataFile, ParquetFileMetadata)>,
    added_s3_keys: Vec<String>,
}

struct CompactionGuard<'a> {
    topic: String,
    compacting: &'a Mutex<HashSet<String>>,
}

impl Drop for CompactionGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut in_progress) = self.compacting.lock() {
            in_progress.remove(&self.topic);
        }
    }
}

/// Compactor for merging small Parquet files.
pub struct Compactor {
    iceberg_storage: Arc<IcebergStorage>,
    config: CompactionConfig,
    compacting: Mutex<HashSet<String>>,
}

impl Compactor {
    /// Creates a new compactor.
    pub fn new(iceberg_storage: Arc<IcebergStorage>, config: CompactionConfig) -> Self {
        Self {
            iceberg_storage,
            config,
            compacting: Mutex::new(HashSet::new()),
        }
    }

    fn try_start_compaction(&self, topic: &str) -> Result<CompactionGuard<'_>, StorageError> {
        let mut in_progress = self.compacting.lock().map_lock_err()?;
        if !in_progress.insert(topic.to_string()) {
            return Err(StorageError::CompactionInProgress(topic.to_string()));
        }
        Ok(CompactionGuard {
            topic: topic.to_string(),
            compacting: &self.compacting,
        })
    }

    /// Identifies files that are candidates for compaction using Iceberg metadata.
    async fn find_compaction_candidates(
        &self,
        topic: &str,
    ) -> Result<Option<CompactionPlan>, StorageError> {
        let Some(active) = self.iceberg_storage.active_data_files(topic).await? else {
            return Ok(None);
        };

        let mut groups: HashMap<String, Vec<CompactionCandidate>> = HashMap::new();
        for file in active.files {
            let size = file.data_file.file_size_in_bytes.max(0) as u64;
            if size >= self.config.min_file_size_bytes {
                continue;
            }

            groups
                .entry(file.partition_path.clone())
                .or_default()
                .push(CompactionCandidate {
                    data_file: file.data_file,
                    s3_key: file.s3_key,
                    size_bytes: size,
                });
        }

        groups.retain(|_, files| files.len() >= self.config.min_files_to_compact);

        Ok(Some(CompactionPlan {
            base_snapshot_id: active.snapshot_id,
            groups,
        }))
    }

    /// Compacts files for one full partition path.
    async fn compact_partition_group(
        &self,
        partition_path: &str,
        candidates: Vec<CompactionCandidate>,
    ) -> Result<PartitionCompactionResult, StorageError> {
        if candidates.is_empty() {
            return Ok(PartitionCompactionResult {
                files_compacted: 0,
                files_created: 0,
                rows_processed: 0,
                input_bytes: 0,
                output_bytes: 0,
                deleted_file_paths: Vec::new(),
                deleted_s3_keys: Vec::new(),
                added_files: Vec::new(),
                added_s3_keys: Vec::new(),
            });
        }

        let files_to_compact: Vec<_> = candidates
            .into_iter()
            .take(self.config.max_files_per_compaction)
            .collect();
        let input_bytes: u64 = files_to_compact.iter().map(|f| f.size_bytes).sum();

        let mut all_events = Vec::new();
        for file in &files_to_compact {
            let events = self.read_parquet_file(&file.s3_key).await?;
            all_events.extend(events);
        }

        if all_events.is_empty() {
            return Ok(PartitionCompactionResult {
                files_compacted: 0,
                files_created: 0,
                rows_processed: 0,
                input_bytes,
                output_bytes: 0,
                deleted_file_paths: Vec::new(),
                deleted_s3_keys: Vec::new(),
                added_files: Vec::new(),
                added_s3_keys: Vec::new(),
            });
        }

        all_events.sort_by_key(|e| e.sequence);
        let total_rows = all_events.len() as i64;

        let mut current_batch = Vec::new();
        let mut current_size_estimate = 0usize;
        let mut added_files = Vec::new();
        let mut added_s3_keys = Vec::new();

        for event in all_events {
            let event_size = event.payload.len() + 100;
            current_size_estimate += event_size;
            current_batch.push(event);

            if current_size_estimate >= self.config.target_file_size_bytes as usize {
                let (bytes, parquet_metadata) = write_parquet_to_bytes_sorted(&mut current_batch)?;
                let filename = data_file_name();
                let key = format!("{}/{}", partition_path, filename);
                self.upload_bytes(&key, bytes).await?;

                let s3_path = format!("s3://{}/{}", self.iceberg_storage.bucket(), key);
                let data_file = DataFile::from_parquet_metadata(&parquet_metadata, &s3_path);
                added_files.push((data_file, parquet_metadata));
                added_s3_keys.push(key);

                current_batch.clear();
                current_size_estimate = 0;
            }
        }

        if !current_batch.is_empty() {
            let (bytes, parquet_metadata) = write_parquet_to_bytes_sorted(&mut current_batch)?;
            let filename = data_file_name();
            let key = format!("{}/{}", partition_path, filename);
            self.upload_bytes(&key, bytes).await?;

            let s3_path = format!("s3://{}/{}", self.iceberg_storage.bucket(), key);
            let data_file = DataFile::from_parquet_metadata(&parquet_metadata, &s3_path);
            added_files.push((data_file, parquet_metadata));
            added_s3_keys.push(key);
        }

        let output_bytes: u64 = added_files.iter().map(|(_, m)| m.file_size_bytes).sum();

        let deleted_file_paths = files_to_compact
            .iter()
            .map(|f| f.data_file.file_path.clone())
            .collect();
        let deleted_s3_keys = files_to_compact.iter().map(|f| f.s3_key.clone()).collect();

        Ok(PartitionCompactionResult {
            files_compacted: files_to_compact.len(),
            files_created: added_files.len(),
            rows_processed: total_rows,
            input_bytes,
            output_bytes,
            deleted_file_paths,
            deleted_s3_keys,
            added_files,
            added_s3_keys,
        })
    }

    /// Runs compaction for all partitions of a topic.
    pub async fn compact_topic(&self, topic: &str) -> Result<CompactionResult, StorageError> {
        if self.iceberg_storage.is_structured_schema_enabled(topic) {
            return Err(StorageError::InvalidInput(format!(
                "Compaction for table '{}' with structured schema is not yet supported",
                topic
            )));
        }

        let _guard = self.try_start_compaction(topic)?;

        let Some(plan) = self.find_compaction_candidates(topic).await? else {
            return Ok(CompactionResult::default());
        };

        if plan.groups.is_empty() {
            return Ok(CompactionResult::default());
        }

        let mut group_keys: Vec<String> = plan.groups.keys().cloned().collect();
        group_keys.sort();
        let mut groups = plan.groups;

        let mut files_compacted = 0usize;
        let mut files_created = 0usize;
        let mut rows_processed = 0i64;
        let mut input_bytes_total = 0u64;
        let mut output_bytes_total = 0u64;
        let mut deleted_file_paths = Vec::new();
        let mut deleted_s3_keys = Vec::new();
        let mut added_files = Vec::new();
        let mut added_s3_keys = Vec::new();

        for key in group_keys {
            let candidates = groups.remove(&key).unwrap_or_default();
            let result = self.compact_partition_group(&key, candidates).await?;

            files_compacted += result.files_compacted;
            files_created += result.files_created;
            rows_processed += result.rows_processed;
            input_bytes_total += result.input_bytes;
            output_bytes_total += result.output_bytes;
            deleted_file_paths.extend(result.deleted_file_paths);
            deleted_s3_keys.extend(result.deleted_s3_keys);
            added_files.extend(result.added_files);
            added_s3_keys.extend(result.added_s3_keys);
        }

        if files_compacted == 0 || files_created == 0 || added_files.is_empty() {
            return Ok(CompactionResult {
                files_compacted,
                files_created,
                rows_processed,
                bytes_saved: input_bytes_total as i64 - output_bytes_total as i64,
                snapshot_id: None,
            });
        }

        let snapshot_id = match self
            .iceberg_storage
            .commit_compaction_snapshot(
                topic,
                plan.base_snapshot_id,
                deleted_file_paths,
                added_files,
            )
            .await
        {
            Ok(id) => id,
            Err(error) => {
                // Best-effort cleanup of newly uploaded compacted files.
                for key in &added_s3_keys {
                    if let Err(del_err) = self.delete_file(key).await {
                        tracing::warn!(
                            topic = topic,
                            key = key,
                            error = %del_err,
                            "Failed to clean up orphaned compacted file after commit failure"
                        );
                    }
                }
                return Err(error);
            }
        };

        tracing::info!(
            topic = topic,
            snapshot_id = snapshot_id,
            files_compacted = files_compacted,
            files_created = files_created,
            rows_processed = rows_processed,
            input_bytes = input_bytes_total,
            output_bytes = output_bytes_total,
            bytes_saved = input_bytes_total as i64 - output_bytes_total as i64,
            "Compaction committed"
        );

        for key in deleted_s3_keys {
            if let Err(error) = self.delete_file(&key).await {
                tracing::warn!(
                    topic = topic,
                    key = key,
                    error = %error,
                    "Failed to delete compacted source file after metadata commit"
                );
            }
        }

        Ok(CompactionResult {
            files_compacted,
            files_created,
            rows_processed,
            bytes_saved: input_bytes_total as i64 - output_bytes_total as i64,
            snapshot_id: Some(snapshot_id),
        })
    }

    /// Reads events from a Parquet file in S3 (with retry).
    async fn read_parquet_file(&self, key: &str) -> Result<Vec<StoredEvent>, StorageError> {
        let bytes = self
            .iceberg_storage
            .download_object_bytes(self.iceberg_storage.bucket(), key)
            .await?;
        read_parquet_events(&bytes)
    }

    /// Uploads bytes to S3 (with retry).
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.iceberg_storage
            .upload_bytes(key, data, "application/octet-stream")
            .await
    }

    /// Deletes a file from S3.
    async fn delete_file(&self, key: &str) -> Result<(), StorageError> {
        self.iceberg_storage
            .client()
            .delete_object()
            .bucket(self.iceberg_storage.bucket())
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(())
    }
}

/// Reads events from Parquet bytes (shared with iceberg_storage).
fn read_parquet_events(bytes: &[u8]) -> Result<Vec<StoredEvent>, StorageError> {
    use arrow::array::{Array, BinaryArray, Int64Array, StringArray, UInt32Array, UInt64Array};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    let reader = builder
        .build()
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    let mut events = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| StorageError::Serialization(e.to_string()))?;

        let sequences = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| StorageError::Serialization("Invalid sequence column".into()))?;

        let topics = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| StorageError::Serialization("Invalid topic column".into()))?;

        let partitions = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| StorageError::Serialization("Invalid partition column".into()))?;

        let payloads = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| StorageError::Serialization("Invalid payload column".into()))?;

        let timestamps = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StorageError::Serialization("Invalid timestamp column".into()))?;

        let idempotency_keys = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| StorageError::Serialization("Invalid idempotency_key column".into()))?;

        for i in 0..batch.num_rows() {
            events.push(StoredEvent {
                sequence: sequences.value(i),
                topic: topics.value(i).to_string(),
                partition: partitions.value(i),
                payload: payloads.value(i).to_vec(),
                timestamp_ms: timestamps.value(i),
                idempotency_key: if idempotency_keys.is_null(i) {
                    None
                } else {
                    Some(idempotency_keys.value(i).to_string())
                },
            });
        }
    }

    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RetryConfig;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.min_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.target_file_size_bytes, 512 * 1024 * 1024); // 512MB for compacted files
        assert_eq!(config.max_files_per_compaction, 10);
        assert_eq!(config.min_files_to_compact, 3);
    }

    #[test]
    fn test_compaction_config_from_env() {
        // Test that from_env uses defaults when no env vars are set
        let config = CompactionConfig::from_env();
        // Should be 512MB unless ZOMBI_COMPACTED_FILE_SIZE_MB is set
        assert_eq!(config.min_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_files_per_compaction, 10);
        assert_eq!(config.min_files_to_compact, 3);
    }

    #[tokio::test]
    async fn test_compactor_rejects_concurrent_compaction() {
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

        let compactor = Compactor::new(Arc::new(storage), CompactionConfig::default());

        let first = compactor.try_start_compaction("events").unwrap();
        let second = compactor.try_start_compaction("events");
        assert!(matches!(
            second,
            Err(StorageError::CompactionInProgress(topic)) if topic == "events"
        ));

        drop(first);

        assert!(compactor.try_start_compaction("events").is_ok());
    }
}
