use std::collections::HashMap;
use std::sync::RwLock;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::contracts::{
    ColdStorage, ColdStorageInfo, LockResultExt, SegmentInfo, StorageError, StoredEvent,
};
use crate::storage::{
    data_file_name, derive_partition_columns, format_partition_date, manifest_list_file_name,
    metadata_file_name, write_parquet_to_bytes, DataFile, ManifestListEntry, ParquetFileMetadata,
    SnapshotOperation, TableMetadata,
};

/// Iceberg-compatible cold storage that writes Parquet files with metadata.
pub struct IcebergStorage {
    client: Client,
    bucket: String,
    base_path: String,
    /// Table metadata per topic
    table_metadata: RwLock<HashMap<String, TableMetadata>>,
    /// Metadata version counter per topic
    metadata_versions: RwLock<HashMap<String, i64>>,
    /// Accumulated data files per topic (for manifest generation)
    pending_data_files: RwLock<HashMap<String, Vec<(DataFile, ParquetFileMetadata)>>>,
}

impl IcebergStorage {
    /// Creates a new Iceberg storage with default AWS configuration.
    pub async fn new(
        bucket: impl Into<String>,
        base_path: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket: bucket.into(),
            base_path: base_path.into(),
            table_metadata: RwLock::new(HashMap::new()),
            metadata_versions: RwLock::new(HashMap::new()),
            pending_data_files: RwLock::new(HashMap::new()),
        })
    }

    /// Creates a new Iceberg storage with a custom endpoint (for MinIO/LocalStack).
    pub async fn with_endpoint(
        bucket: impl Into<String>,
        base_path: impl Into<String>,
        endpoint: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let endpoint = endpoint.into();
        let region = region.into();

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.clone()))
            .load()
            .await;

        let s3_config = S3ConfigBuilder::from(&config)
            .endpoint_url(&endpoint)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        Ok(Self {
            client,
            bucket: bucket.into(),
            base_path: base_path.into(),
            table_metadata: RwLock::new(HashMap::new()),
            metadata_versions: RwLock::new(HashMap::new()),
            pending_data_files: RwLock::new(HashMap::new()),
        })
    }

    /// Returns the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the base path.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    /// Gets or creates table metadata for a topic.
    fn get_or_create_metadata(&self, topic: &str) -> Result<TableMetadata, StorageError> {
        let metadata = self.table_metadata.read().map_lock_err()?;
        if let Some(m) = metadata.get(topic) {
            return Ok(m.clone());
        }
        drop(metadata);

        // Create new metadata
        let location = format!("s3://{}/{}/{}", self.bucket, self.base_path, topic);
        let new_metadata = TableMetadata::new(&location);

        let mut metadata = self.table_metadata.write().map_lock_err()?;
        metadata.insert(topic.to_string(), new_metadata.clone());

        Ok(new_metadata)
    }

    /// Returns the S3 key for a data file with time-based partitioning.
    fn data_file_key(
        &self,
        topic: &str,
        partition: u32,
        filename: &str,
        event_date: i32,
        event_hour: i32,
    ) -> String {
        make_data_file_key(
            &self.base_path,
            topic,
            partition,
            filename,
            event_date,
            event_hour,
        )
    }

    /// Returns the S3 key for metadata files.
    fn metadata_key(&self, topic: &str, filename: &str) -> String {
        make_metadata_key(&self.base_path, topic, filename)
    }

    /// Uploads bytes to S3.
    async fn upload_bytes(
        &self,
        key: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> Result<(), StorageError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(())
    }

    /// Commits pending data files by creating a new snapshot.
    /// This should be called after a batch of write_segment calls.
    pub async fn commit_snapshot(&self, topic: &str) -> Result<Option<i64>, StorageError> {
        // Get pending files
        let pending = {
            let mut pending = self.pending_data_files.write().map_lock_err()?;
            pending.remove(topic).unwrap_or_default()
        };

        if pending.is_empty() {
            return Ok(None);
        }

        let mut metadata = self.get_or_create_metadata(topic)?;

        // Calculate totals
        let total_files = pending.len();
        let total_rows: i64 = pending.iter().map(|(_, m)| m.row_count as i64).sum();

        // Create manifest list entry (simplified - in production would write actual Avro manifest)
        let snapshot_id = crate::storage::generate_snapshot_id();
        let manifest_list_filename = manifest_list_file_name(snapshot_id);
        let manifest_list_key = self.metadata_key(topic, &manifest_list_filename);
        let manifest_list_s3_path = format!("s3://{}/{}", self.bucket, manifest_list_key);

        // Create a simple manifest list (JSON for now, Avro in production)
        let manifest_entries: Vec<ManifestListEntry> = vec![ManifestListEntry {
            manifest_path: format!(
                "s3://{}/{}/{}/metadata/manifest.avro",
                self.bucket, self.base_path, topic
            ),
            manifest_length: 0,
            partition_spec_id: 0,
            content: 0,
            sequence_number: metadata.last_sequence_number + 1,
            min_sequence_number: metadata.last_sequence_number + 1,
            added_snapshot_id: snapshot_id,
            added_files_count: total_files as i32,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: total_rows,
            existing_rows_count: 0,
            deleted_rows_count: 0,
        }];

        // Write manifest list as JSON (simplified)
        let manifest_list_json = serde_json::to_vec(&manifest_entries)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.upload_bytes(&manifest_list_key, manifest_list_json, "application/json")
            .await?;

        // Add snapshot to metadata
        metadata.add_snapshot(
            &manifest_list_s3_path,
            total_files,
            total_rows,
            SnapshotOperation::Append,
        );

        // Write new metadata file
        let version = {
            let mut versions = self.metadata_versions.write().map_lock_err()?;
            let v = versions.entry(topic.to_string()).or_insert(0);
            *v += 1;
            *v
        };

        let metadata_filename = metadata_file_name(version);
        let metadata_key = self.metadata_key(topic, &metadata_filename);
        let metadata_json = metadata.to_json()?.into_bytes();
        self.upload_bytes(&metadata_key, metadata_json, "application/json")
            .await?;

        // Update cached metadata
        {
            let mut cached = self.table_metadata.write().map_lock_err()?;
            cached.insert(topic.to_string(), metadata);
        }

        tracing::info!(
            topic = topic,
            snapshot_id = snapshot_id,
            files = total_files,
            rows = total_rows,
            "Created Iceberg snapshot"
        );

        Ok(Some(snapshot_id))
    }

    /// Returns the current table metadata for a topic.
    pub fn get_table_metadata(&self, topic: &str) -> Result<Option<TableMetadata>, StorageError> {
        let metadata = self.table_metadata.read().map_lock_err()?;
        Ok(metadata.get(topic).cloned())
    }
}

impl ColdStorage for IcebergStorage {
    async fn write_segment(
        &self,
        topic: &str,
        partition: u32,
        events: &[StoredEvent],
    ) -> Result<String, StorageError> {
        if events.is_empty() {
            return Err(StorageError::S3("Cannot write empty segment".into()));
        }

        // Validate all events are within the same hour for proper partitioning
        let (event_date, event_hour) = derive_partition_columns(events[0].timestamp_ms);
        for event in &events[1..] {
            let (date, hour) = derive_partition_columns(event.timestamp_ms);
            if date != event_date || hour != event_hour {
                return Err(StorageError::InvalidInput(
                    format!(
                        "Events must be within same hour for partitioned write: first={}-{}, later={}-{}",
                        event_date, event_hour, date, hour
                    )
                ));
            }
        }

        // Convert events to Parquet
        let (parquet_bytes, parquet_metadata) = write_parquet_to_bytes(events)?;

        // Generate data file name and key
        let filename = data_file_name();
        let key = self.data_file_key(topic, partition, &filename, event_date, event_hour);
        let s3_path = format!("s3://{}/{}", self.bucket, key);

        // Upload Parquet file
        self.upload_bytes(&key, parquet_bytes, "application/octet-stream")
            .await?;

        // Create DataFile entry
        let data_file = DataFile::from_parquet_metadata(&parquet_metadata, &s3_path);

        // Add to pending files for this topic
        {
            let mut pending = self.pending_data_files.write().map_lock_err()?;
            pending
                .entry(topic.to_string())
                .or_default()
                .push((data_file, parquet_metadata.clone()));
        }

        tracing::debug!(
            topic = topic,
            partition = partition,
            key = key,
            rows = parquet_metadata.row_count,
            bytes = parquet_metadata.file_size_bytes,
            "Wrote Parquet segment"
        );

        Ok(key)
    }

    async fn read_events(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        limit: usize,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        // Log warning if time range not provided (less efficient)
        if since_ms.is_none() || until_ms.is_none() {
            tracing::warn!(
                topic = topic,
                partition = partition,
                "read_events called without full time range, listing all data (may be slow)"
            );
        }

        // For now, use full data prefix (future improvement: time-based prefix pruning)
        let prefix = format!("{}/{}/data/", self.base_path, topic);

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;

        let mut all_events = Vec::new();

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = object.key() {
                    // Filter for Parquet files in the correct partition directory
                    if !key.ends_with(".parquet") {
                        continue;
                    }

                    // Check if file is in the correct partition directory
                    // Path format: tables/events/data/event_date=YYYY-MM-DD/event_hour=HH/partition=N/
                    let partition_suffix = format!("/partition={}/", partition);
                    if !key.contains(&partition_suffix) {
                        continue;
                    }

                    // Download and read Parquet file
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

                    // Parse Parquet and extract events
                    let events = read_parquet_events(&bytes)?;

                    for event in events {
                        if event.sequence >= start_offset {
                            all_events.push(event);
                            if all_events.len() >= limit {
                                all_events.sort_by_key(|e| e.sequence);
                                return Ok(all_events);
                            }
                        }
                    }
                }
            }
        }

        all_events.sort_by_key(|e| e.sequence);
        Ok(all_events)
    }

    async fn list_segments(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Vec<SegmentInfo>, StorageError> {
        let prefix = format!("{}/{}/data/", self.base_path, topic);

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;

        let mut segments = Vec::new();

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = object.key() {
                    // Filter for Parquet files in the correct partition directory
                    if !key.ends_with(".parquet") {
                        continue;
                    }

                    // Check if file is in the correct partition directory
                    let partition_suffix = format!("/partition={}/", partition);
                    if !key.contains(&partition_suffix) {
                        continue;
                    }

                    segments.push(SegmentInfo {
                        segment_id: key.to_string(),
                        start_offset: 0, // Would need to read Parquet metadata
                        end_offset: 0,
                        event_count: 0,
                        size_bytes: object.size().unwrap_or(0) as u64,
                    });
                }
            }
        }

        Ok(segments)
    }

    fn storage_info(&self) -> ColdStorageInfo {
        ColdStorageInfo {
            storage_type: "iceberg".into(),
            iceberg_enabled: true,
            bucket: self.bucket.clone(),
            base_path: self.base_path.clone(),
        }
    }

    fn iceberg_metadata_location(&self, topic: &str) -> Option<String> {
        Some(format!(
            "s3://{}/{}/{}/metadata/",
            self.bucket, self.base_path, topic
        ))
    }

    async fn commit_snapshot(&self, topic: &str) -> Result<Option<i64>, StorageError> {
        self.commit_snapshot(topic).await
    }
}

/// Reads events from Parquet bytes.
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

/// Helper function to generate data file key with time-based partitioning (for testing).
fn make_data_file_key(
    base_path: &str,
    topic: &str,
    partition: u32,
    filename: &str,
    event_date: i32,
    event_hour: i32,
) -> String {
    let date_str = format_partition_date(event_date);
    format!(
        "{}/{}/data/event_date={}/event_hour={}/partition={}/{}",
        base_path, topic, date_str, event_hour, partition, filename
    )
}

/// Helper function to generate metadata key (for testing).
fn make_metadata_key(base_path: &str, topic: &str, filename: &str) -> String {
    format!("{}/{}/metadata/{}", base_path, topic, filename)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_key() {
        let key = make_data_file_key("tables", "events", 0, "abc123.parquet", 19737, 14);
        assert_eq!(
            key,
            "tables/events/data/event_date=2024-01-15/event_hour=14/partition=0/abc123.parquet"
        );
    }

    #[test]
    fn test_format_partition_date() {
        // 2024-01-15 is day 19737 since epoch
        assert_eq!(format_partition_date(19737), "2024-01-15");
        assert_eq!(format_partition_date(0), "1970-01-01");
        assert_eq!(format_partition_date(1), "1970-01-02");
    }

    #[test]
    fn test_metadata_key() {
        let key = make_metadata_key("tables", "events", "v1.metadata.json");
        assert_eq!(key, "tables/events/metadata/v1.metadata.json");
    }

    #[test]
    fn test_read_parquet_events_roundtrip() {
        let events = vec![
            StoredEvent {
                sequence: 1,
                topic: "test".into(),
                partition: 0,
                payload: b"hello".to_vec(),
                timestamp_ms: 1000,
                idempotency_key: Some("key1".into()),
            },
            StoredEvent {
                sequence: 2,
                topic: "test".into(),
                partition: 0,
                payload: b"world".to_vec(),
                timestamp_ms: 2000,
                idempotency_key: None,
            },
        ];

        let (bytes, _) = write_parquet_to_bytes(&events).unwrap();
        let read_events = read_parquet_events(&bytes).unwrap();

        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].sequence, 1);
        assert_eq!(read_events[0].payload, b"hello");
        assert_eq!(read_events[1].sequence, 2);
        assert_eq!(read_events[1].idempotency_key, None);
    }

    #[test]
    fn test_read_parquet_large_batch() {
        // Test with 1000 events
        let events: Vec<StoredEvent> = (0..1000)
            .map(|i| StoredEvent {
                sequence: i as u64,
                topic: "large-batch".into(),
                partition: (i % 4) as u32,
                payload: format!("payload-{}", i).into_bytes(),
                timestamp_ms: 1000 + i as i64,
                idempotency_key: if i % 2 == 0 {
                    Some(format!("key-{}", i))
                } else {
                    None
                },
            })
            .collect();

        let (bytes, metadata) = write_parquet_to_bytes(&events).unwrap();
        assert_eq!(metadata.row_count, 1000);

        let read_events = read_parquet_events(&bytes).unwrap();
        assert_eq!(read_events.len(), 1000);

        // Verify first and last events
        assert_eq!(read_events[0].sequence, 0);
        assert_eq!(read_events[0].topic, "large-batch");
        assert_eq!(read_events[999].sequence, 999);
        assert_eq!(read_events[999].timestamp_ms, 1999);
    }

    #[test]
    fn test_read_parquet_binary_payloads() {
        // Test with various binary payloads including non-UTF8 data
        let events = vec![
            StoredEvent {
                sequence: 1,
                topic: "binary".into(),
                partition: 0,
                payload: vec![0x00, 0x01, 0xFF, 0xFE], // Non-UTF8 bytes
                timestamp_ms: 1000,
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 2,
                topic: "binary".into(),
                partition: 0,
                payload: vec![0u8; 10000], // Large zero-filled payload
                timestamp_ms: 2000,
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 3,
                topic: "binary".into(),
                partition: 0,
                payload: (0..255).collect(), // All byte values
                timestamp_ms: 3000,
                idempotency_key: Some("binary-key".into()),
            },
        ];

        let (bytes, _) = write_parquet_to_bytes(&events).unwrap();
        let read_events = read_parquet_events(&bytes).unwrap();

        assert_eq!(read_events.len(), 3);
        assert_eq!(read_events[0].payload, vec![0x00, 0x01, 0xFF, 0xFE]);
        assert_eq!(read_events[1].payload.len(), 10000);
        assert_eq!(read_events[2].payload, (0..255).collect::<Vec<u8>>());
    }

    #[test]
    fn test_read_parquet_preserves_all_fields() {
        let event = StoredEvent {
            sequence: 123456789,
            topic: "test-topic-with-dashes".into(),
            partition: 42,
            payload: b"test payload data".to_vec(),
            timestamp_ms: 1705555555555,
            idempotency_key: Some("unique-key-12345".into()),
        };

        let (bytes, _) = write_parquet_to_bytes(&[event.clone()]).unwrap();
        let read_events = read_parquet_events(&bytes).unwrap();

        assert_eq!(read_events.len(), 1);
        let read = &read_events[0];
        assert_eq!(read.sequence, event.sequence);
        assert_eq!(read.topic, event.topic);
        assert_eq!(read.partition, event.partition);
        assert_eq!(read.payload, event.payload);
        assert_eq!(read.timestamp_ms, event.timestamp_ms);
        assert_eq!(read.idempotency_key, event.idempotency_key);
    }

    #[test]
    fn test_read_parquet_multiple_partitions() {
        let events: Vec<StoredEvent> = (0..100)
            .map(|i| StoredEvent {
                sequence: i as u64,
                topic: "multi-partition".into(),
                partition: (i % 8) as u32, // 8 partitions
                payload: format!("p{}-event", i % 8).into_bytes(),
                timestamp_ms: 1000 + i as i64,
                idempotency_key: None,
            })
            .collect();

        let (bytes, _) = write_parquet_to_bytes(&events).unwrap();
        let read_events = read_parquet_events(&bytes).unwrap();

        assert_eq!(read_events.len(), 100);

        // Count events per partition
        let mut partition_counts = [0usize; 8];
        for event in &read_events {
            partition_counts[event.partition as usize] += 1;
        }

        // Each partition should have ~12-13 events
        for count in partition_counts {
            assert!(count >= 12 && count <= 13);
        }
    }
}
