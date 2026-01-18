use std::collections::HashMap;

use aws_sdk_s3::Client;

use crate::contracts::{StorageError, StoredEvent};
use crate::storage::{data_file_name, write_parquet_to_bytes};

/// Configuration for compaction.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum file size to consider for compaction (bytes).
    /// Files smaller than this are candidates for merging.
    pub min_file_size_bytes: u64,
    /// Target file size after compaction (bytes).
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
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB
            max_files_per_compaction: 10,
            min_files_to_compact: 3,
        }
    }
}

/// Result of a compaction operation.
#[derive(Debug, Clone)]
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

/// File info for compaction planning.
#[derive(Debug, Clone)]
struct CompactionCandidate {
    key: String,
    size_bytes: u64,
}

/// Compactor for merging small Parquet files.
pub struct Compactor {
    client: Client,
    bucket: String,
    base_path: String,
    config: CompactionConfig,
}

impl Compactor {
    /// Creates a new compactor.
    pub fn new(
        client: Client,
        bucket: impl Into<String>,
        base_path: impl Into<String>,
        config: CompactionConfig,
    ) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            base_path: base_path.into(),
            config,
        }
    }

    /// Identifies files that are candidates for compaction.
    async fn find_compaction_candidates(
        &self,
        topic: &str,
    ) -> Result<HashMap<u32, Vec<CompactionCandidate>>, StorageError> {
        let prefix = format!("{}/{}/data/", self.base_path, topic);

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;

        let mut candidates: HashMap<u32, Vec<CompactionCandidate>> = HashMap::new();

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = object.key() {
                    if !key.ends_with(".parquet") {
                        continue;
                    }

                    let size = object.size().unwrap_or(0) as u64;

                    // Only consider files smaller than threshold
                    if size >= self.config.min_file_size_bytes {
                        continue;
                    }

                    // Extract partition from key: .../partition=N/file.parquet
                    if let Some(partition) = extract_partition_from_key(key) {
                        candidates
                            .entry(partition)
                            .or_default()
                            .push(CompactionCandidate {
                                key: key.to_string(),
                                size_bytes: size,
                            });
                    }
                }
            }
        }

        // Filter to only partitions with enough files
        candidates.retain(|_, files| files.len() >= self.config.min_files_to_compact);

        Ok(candidates)
    }

    /// Compacts files for a single partition.
    async fn compact_partition(
        &self,
        topic: &str,
        partition: u32,
        candidates: Vec<CompactionCandidate>,
    ) -> Result<CompactionResult, StorageError> {
        if candidates.is_empty() {
            return Ok(CompactionResult {
                files_compacted: 0,
                files_created: 0,
                rows_processed: 0,
                bytes_saved: 0,
                snapshot_id: None,
            });
        }

        // Limit number of files per compaction
        let files_to_compact: Vec<_> = candidates
            .into_iter()
            .take(self.config.max_files_per_compaction)
            .collect();

        let input_bytes: u64 = files_to_compact.iter().map(|f| f.size_bytes).sum();

        // Read all events from the files
        let mut all_events = Vec::new();
        for file in &files_to_compact {
            let events = self.read_parquet_file(&file.key).await?;
            all_events.extend(events);
        }

        if all_events.is_empty() {
            return Ok(CompactionResult {
                files_compacted: files_to_compact.len(),
                files_created: 0,
                rows_processed: 0,
                bytes_saved: 0,
                snapshot_id: None,
            });
        }

        // Sort by sequence to maintain order
        all_events.sort_by_key(|e| e.sequence);

        let total_rows = all_events.len() as i64;

        // Write compacted file(s)
        let mut output_files = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_size_estimate: usize = 0;

        for event in all_events {
            let event_size = event.payload.len() + 100; // Rough estimate with overhead
            current_batch.push(event);
            current_size_estimate += event_size;

            // Write batch when it reaches target size
            if current_size_estimate >= self.config.target_file_size_bytes as usize {
                let (bytes, metadata) = write_parquet_to_bytes(&current_batch)?;
                let filename = data_file_name();
                let key = format!(
                    "{}/{}/data/partition={}/{}",
                    self.base_path, topic, partition, filename
                );

                self.upload_bytes(&key, bytes).await?;
                output_files.push((key, metadata));

                current_batch.clear();
                current_size_estimate = 0;
            }
        }

        // Write remaining events
        if !current_batch.is_empty() {
            let (bytes, metadata) = write_parquet_to_bytes(&current_batch)?;
            let filename = data_file_name();
            let key = format!(
                "{}/{}/data/partition={}/{}",
                self.base_path, topic, partition, filename
            );

            self.upload_bytes(&key, bytes).await?;
            output_files.push((key, metadata));
        }

        let output_bytes: u64 = output_files.iter().map(|(_, m)| m.file_size_bytes).sum();

        // Delete old files
        for file in &files_to_compact {
            self.delete_file(&file.key).await?;
        }

        tracing::info!(
            topic = topic,
            partition = partition,
            input_files = files_to_compact.len(),
            output_files = output_files.len(),
            input_bytes = input_bytes,
            output_bytes = output_bytes,
            rows = total_rows,
            "Compaction complete"
        );

        Ok(CompactionResult {
            files_compacted: files_to_compact.len(),
            files_created: output_files.len(),
            rows_processed: total_rows,
            bytes_saved: input_bytes as i64 - output_bytes as i64,
            snapshot_id: None, // Metadata update handled separately
        })
    }

    /// Runs compaction for all partitions of a topic.
    pub async fn compact_topic(&self, topic: &str) -> Result<CompactionResult, StorageError> {
        let candidates = self.find_compaction_candidates(topic).await?;

        let mut total_result = CompactionResult {
            files_compacted: 0,
            files_created: 0,
            rows_processed: 0,
            bytes_saved: 0,
            snapshot_id: None,
        };

        for (partition, files) in candidates {
            let result = self.compact_partition(topic, partition, files).await?;
            total_result.files_compacted += result.files_compacted;
            total_result.files_created += result.files_created;
            total_result.rows_processed += result.rows_processed;
            total_result.bytes_saved += result.bytes_saved;
        }

        Ok(total_result)
    }

    /// Reads events from a Parquet file in S3.
    async fn read_parquet_file(&self, key: &str) -> Result<Vec<StoredEvent>, StorageError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;

        let bytes = response
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?
            .into_bytes();

        read_parquet_events(&bytes)
    }

    /// Uploads bytes to S3.
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        use aws_sdk_s3::primitives::ByteStream;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(())
    }

    /// Deletes a file from S3.
    async fn delete_file(&self, key: &str) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(())
    }
}

/// Extracts partition number from an S3 key.
fn extract_partition_from_key(key: &str) -> Option<u32> {
    // Key format: .../partition=N/file.parquet
    for part in key.split('/') {
        if let Some(num_str) = part.strip_prefix("partition=") {
            return num_str.parse().ok();
        }
    }
    None
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

    #[test]
    fn test_extract_partition_from_key() {
        let key = "tables/events/data/partition=5/abc123.parquet";
        assert_eq!(extract_partition_from_key(key), Some(5));

        let key2 = "tables/events/data/partition=0/file.parquet";
        assert_eq!(extract_partition_from_key(key2), Some(0));

        let invalid = "tables/events/data/file.parquet";
        assert_eq!(extract_partition_from_key(invalid), None);
    }

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.min_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.target_file_size_bytes, 128 * 1024 * 1024);
        assert_eq!(config.max_files_per_compaction, 10);
        assert_eq!(config.min_files_to_compact, 3);
    }
}
