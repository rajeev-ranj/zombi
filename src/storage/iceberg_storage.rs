use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use apache_avro::types::Value as AvroValue;
use apache_avro::Reader as AvroReader;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;

use crate::contracts::{
    ColdStorage, ColdStorageInfo, IcebergCatalogTable, LockResultExt, PayloadFormat,
    PendingSnapshotStats, SegmentInfo, SnapshotCommitContext, StorageError, StoredEvent,
    TableSchemaConfig,
};
use crate::s3_retry;
use crate::storage::parquet::write_parquet_to_bytes_structured_sorted;
use crate::storage::retry::RetryConfig;
use crate::storage::{
    data_file_name, derive_partition_columns, format_partition_date, manifest_file_name,
    manifest_list_file_name, manifest_list_to_avro_bytes, metadata_file_name,
    write_parquet_to_bytes_sorted, DataFile, ManifestFile, ManifestListEntry, ParquetFileMetadata,
    SnapshotOperation, TableMetadata,
};

type PendingDataFiles = HashMap<(String, u32), Vec<(DataFile, ParquetFileMetadata)>>;

/// Maximum S3 object size we will download into memory (10 MB).
const MAX_DOWNLOAD_BYTES: u64 = 10 * 1024 * 1024;

/// Iceberg-compatible cold storage that writes Parquet files with metadata.
pub struct IcebergStorage {
    client: Client,
    bucket: String,
    base_path: String,
    /// Table metadata per topic
    table_metadata: RwLock<HashMap<String, TableMetadata>>,
    /// Metadata version counter per topic
    metadata_versions: RwLock<HashMap<String, i64>>,
    /// Accumulated data files per (topic, partition) for manifest generation.
    pending_data_files: RwLock<PendingDataFiles>,
    /// Retry configuration for S3 operations
    retry_config: RetryConfig,
    /// Schema configs for structured column extraction, keyed by table/topic name.
    /// When present for a topic, payloads are parsed and extracted into typed columns.
    schema_configs: HashMap<String, TableSchemaConfig>,
}

impl IcebergStorage {
    /// Creates a new Iceberg storage with default AWS configuration and retry settings.
    pub async fn new(
        bucket: impl Into<String>,
        base_path: impl Into<String>,
    ) -> Result<Self, StorageError> {
        Self::new_with_retry(bucket, base_path, RetryConfig::from_env()).await
    }

    /// Creates a new Iceberg storage with custom retry configuration.
    pub async fn new_with_retry(
        bucket: impl Into<String>,
        base_path: impl Into<String>,
        retry_config: RetryConfig,
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
            retry_config,
            schema_configs: HashMap::new(),
        })
    }

    /// Creates a new Iceberg storage with a custom endpoint (for MinIO/LocalStack).
    pub async fn with_endpoint(
        bucket: impl Into<String>,
        base_path: impl Into<String>,
        endpoint: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, StorageError> {
        Self::with_endpoint_and_retry(bucket, base_path, endpoint, region, RetryConfig::from_env())
            .await
    }

    /// Creates a new Iceberg storage with a custom endpoint and retry configuration.
    pub async fn with_endpoint_and_retry(
        bucket: impl Into<String>,
        base_path: impl Into<String>,
        endpoint: impl Into<String>,
        region: impl Into<String>,
        retry_config: RetryConfig,
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
            retry_config,
            schema_configs: HashMap::new(),
        })
    }

    /// Sets schema configurations for structured column extraction.
    /// Call this after construction to enable payload extraction for specific topics.
    pub fn set_schema_configs(&mut self, configs: Vec<TableSchemaConfig>) {
        for config in configs {
            self.schema_configs.insert(config.table.clone(), config);
        }
    }

    /// Returns the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the base path.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    #[cfg(test)]
    pub(crate) fn insert_pending_data_files_for_test(
        &self,
        topic: &str,
        partition: u32,
        count: usize,
    ) -> Result<(), StorageError> {
        let mut pending = self.pending_data_files.write().map_lock_err()?;
        let entries = pending.entry((topic.to_string(), partition)).or_default();

        for i in 0..count {
            let mut column_stats = crate::storage::ColumnStatistics {
                sequence_min: (i + 1) as u64,
                sequence_max: (i + 1) as u64,
                ..Default::default()
            };
            column_stats.partition_min = partition;
            column_stats.partition_max = partition;

            let metadata = ParquetFileMetadata {
                path: format!("test-{}-{}-{}.parquet", topic, partition, i),
                row_count: 1,
                min_sequence: (i + 1) as u64,
                max_sequence: (i + 1) as u64,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                file_size_bytes: 1,
                partition_values: Default::default(),
                column_stats,
            };
            let s3_path = format!("s3://{}/{}", self.bucket, metadata.path);
            let data_file = DataFile::from_parquet_metadata(&metadata, &s3_path);
            entries.push((data_file, metadata));
        }

        Ok(())
    }

    /// Gets or creates table metadata for a topic.
    /// If a `TableSchemaConfig` is registered for this topic, the metadata
    /// will use the structured schema with extracted columns.
    fn get_or_create_metadata(&self, topic: &str) -> Result<TableMetadata, StorageError> {
        let metadata = self.table_metadata.read().map_lock_err()?;
        if let Some(m) = metadata.get(topic) {
            return Ok(m.clone());
        }
        drop(metadata);

        // Create new metadata — use structured schema if config is available
        let location = format!("s3://{}/{}/{}", self.bucket, self.base_path, topic);
        let new_metadata = if let Some(config) = self.schema_configs.get(topic) {
            TableMetadata::with_schema_config(&location, config)
        } else {
            TableMetadata::new(&location)
        };

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

    /// Parses a metadata file version from a key like `.../v12.metadata.json`.
    fn parse_metadata_version_from_key(key: &str) -> Option<i64> {
        let filename = key.rsplit('/').next()?;
        let version = filename.strip_prefix('v')?.strip_suffix(".metadata.json")?;
        version.parse::<i64>().ok()
    }

    /// Parses an S3 URI (`s3://bucket/key`) into `(bucket, key)`.
    fn parse_s3_uri(uri: &str) -> Result<(String, String), StorageError> {
        let without_scheme = uri
            .strip_prefix("s3://")
            .ok_or_else(|| StorageError::InvalidInput(format!("Invalid S3 URI: {}", uri)))?;
        let (bucket, key) = without_scheme.split_once('/').ok_or_else(|| {
            StorageError::InvalidInput(format!("Invalid S3 URI (missing key): {}", uri))
        })?;
        if bucket.is_empty() || key.is_empty() {
            return Err(StorageError::InvalidInput(format!(
                "Invalid S3 URI (empty bucket/key): {}",
                uri
            )));
        }
        Ok((bucket.to_string(), key.to_string()))
    }

    /// Builds additional snapshot summary entries from commit context.
    fn snapshot_summary_from_context(context: &SnapshotCommitContext) -> HashMap<String, String> {
        let mut summary = HashMap::new();
        for (partition, watermark) in &context.watermarks_by_partition {
            summary.insert(
                format!("zombi.watermark.{}", partition),
                watermark.to_string(),
            );
        }
        for (partition, high_watermark) in &context.high_watermarks_by_partition {
            summary.insert(
                format!("zombi.high_watermark.{}", partition),
                high_watermark.to_string(),
            );
        }
        summary
    }

    /// Helper: fetches a named field from an Avro record.
    fn avro_record_field<'a>(
        fields: &'a [(String, AvroValue)],
        name: &str,
    ) -> Option<&'a AvroValue> {
        fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }

    /// Helper: unwraps Avro union values.
    fn avro_unwrap_union(value: &AvroValue) -> &AvroValue {
        match value {
            AvroValue::Union(_, inner) => inner.as_ref(),
            other => other,
        }
    }

    /// Helper: extracts an i32/int field from an Avro record.
    fn avro_record_i32(fields: &[(String, AvroValue)], name: &str) -> Option<i32> {
        match Self::avro_unwrap_union(Self::avro_record_field(fields, name)?) {
            AvroValue::Int(v) => Some(*v),
            AvroValue::Long(v) => i32::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Helper: extracts a string field from an Avro record.
    fn avro_record_string<'a>(fields: &'a [(String, AvroValue)], name: &str) -> Option<&'a str> {
        match Self::avro_unwrap_union(Self::avro_record_field(fields, name)?) {
            AvroValue::String(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Helper: extracts a bytes value from an Avro map keyed by field-id.
    fn avro_map_bytes_by_field_id(
        map: &HashMap<String, AvroValue>,
        field_id: i32,
    ) -> Option<&[u8]> {
        let key = field_id.to_string();
        match Self::avro_unwrap_union(map.get(&key)?) {
            AvroValue::Bytes(v) => Some(v.as_slice()),
            AvroValue::Fixed(_, v) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// Decodes a big-endian i64 from bytes.
    fn decode_i64_be(bytes: &[u8]) -> Option<i64> {
        let arr: [u8; 8] = bytes.try_into().ok()?;
        Some(i64::from_be_bytes(arr))
    }

    /// Decodes a big-endian i32 from bytes.
    fn decode_i32_be(bytes: &[u8]) -> Option<i32> {
        let arr: [u8; 4] = bytes.try_into().ok()?;
        Some(i32::from_be_bytes(arr))
    }

    /// Extracts data-manifest paths from a manifest-list Avro file.
    fn extract_manifest_paths_from_manifest_list_avro(
        bytes: &[u8],
    ) -> Result<Vec<String>, StorageError> {
        let reader =
            AvroReader::new(bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let mut manifest_paths = Vec::new();

        for value in reader {
            let value = value.map_err(|e| StorageError::Serialization(e.to_string()))?;
            let fields = match value {
                AvroValue::Record(fields) => fields,
                _ => continue,
            };

            // Only data manifests are relevant for flush watermark reconciliation.
            let content = Self::avro_record_i32(&fields, "content").unwrap_or(0);
            if content != 0 {
                continue;
            }

            if let Some(path) = Self::avro_record_string(&fields, "manifest_path") {
                manifest_paths.push(path.to_string());
            }
        }

        Ok(manifest_paths)
    }

    /// Extracts max committed sequence watermark per partition from one manifest file.
    fn extract_partition_watermarks_from_manifest_avro(
        bytes: &[u8],
    ) -> Result<HashMap<u32, u64>, StorageError> {
        let reader =
            AvroReader::new(bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let mut per_partition = HashMap::new();

        for value in reader {
            let value = value.map_err(|e| StorageError::Serialization(e.to_string()))?;
            let entry_fields = match value {
                AvroValue::Record(fields) => fields,
                _ => continue,
            };

            // Skip deleted entries.
            if Self::avro_record_i32(&entry_fields, "status") == Some(2) {
                continue;
            }

            let data_file_fields = match Self::avro_unwrap_union(
                match Self::avro_record_field(&entry_fields, "data_file") {
                    Some(v) => v,
                    None => continue,
                },
            ) {
                AvroValue::Record(fields) => fields,
                _ => continue,
            };

            let upper_bounds = match Self::avro_unwrap_union(
                match Self::avro_record_field(data_file_fields, "upper_bounds") {
                    Some(v) => v,
                    None => continue,
                },
            ) {
                AvroValue::Map(map) => map,
                _ => continue,
            };

            let sequence = match Self::avro_map_bytes_by_field_id(
                upper_bounds,
                crate::storage::iceberg::field_ids::SEQUENCE,
            )
            .and_then(Self::decode_i64_be)
            .and_then(|v| u64::try_from(v).ok())
            {
                Some(v) => v,
                None => continue,
            };

            let partition = match Self::avro_map_bytes_by_field_id(
                upper_bounds,
                crate::storage::iceberg::field_ids::PARTITION,
            )
            .and_then(Self::decode_i32_be)
            .and_then(|v| u32::try_from(v).ok())
            {
                Some(v) => v,
                None => continue,
            };

            per_partition
                .entry(partition)
                .and_modify(|current: &mut u64| *current = (*current).max(sequence))
                .or_insert(sequence);
        }

        Ok(per_partition)
    }

    /// Lists S3 keys or directory prefixes under a given prefix with pagination.
    ///
    /// When `delimiter` is `None`, returns all object keys (flat listing).
    /// When `delimiter` is `Some("/")`, returns only common prefixes (directory listing).
    async fn list_keys_with_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<Vec<String>, StorageError> {
        let client = &self.client;
        let mut continuation_token: Option<String> = None;
        let mut results = Vec::new();

        loop {
            let continuation = continuation_token.clone();
            let response = s3_retry!(
                operation = {
                    let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);
                    if let Some(d) = delimiter {
                        request = request.delimiter(d);
                    }
                    if let Some(token) = continuation.as_deref() {
                        request = request.continuation_token(token);
                    }
                    request.send().await
                },
                retry_config = self.retry_config,
                context = format!("LIST s3://{}/{}", bucket, prefix),
            )?;

            let is_truncated = response.is_truncated().unwrap_or(false);
            continuation_token = response.next_continuation_token().map(|t| t.to_string());

            if delimiter.is_some() {
                if let Some(common_prefixes) = response.common_prefixes {
                    for cp in common_prefixes {
                        if let Some(p) = cp.prefix {
                            results.push(p);
                        }
                    }
                }
            } else if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        results.push(key);
                    }
                }
            }

            if !is_truncated {
                break;
            }
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(results)
    }

    /// Checks if the given metadata prefix contains at least one `v*.metadata.json` file.
    async fn has_metadata_files(
        &self,
        bucket: &str,
        metadata_prefix: &str,
    ) -> Result<bool, StorageError> {
        let client = &self.client;
        let response = s3_retry!(
            operation = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(metadata_prefix)
                .max_keys(20)
                .send()
                .await,
            retry_config = self.retry_config,
            context = format!("LIST-EXISTS s3://{}/{}", bucket, metadata_prefix),
        )?;

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = &object.key {
                    if Self::parse_metadata_version_from_key(key).is_some() {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Downloads an S3 object as bytes, rejecting objects larger than [`MAX_DOWNLOAD_BYTES`].
    async fn download_object_bytes(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let client = &self.client;
        let response = s3_retry!(
            operation = client.get_object().bucket(bucket).key(key).send().await,
            retry_config = self.retry_config,
            context = format!("GET s3://{}/{}", bucket, key),
        )?;
        if let Some(len) = response
            .content_length()
            .and_then(|l| u64::try_from(l).ok())
        {
            if len > MAX_DOWNLOAD_BYTES {
                return Err(StorageError::S3(format!(
                    "Object too large ({} bytes, max {}): s3://{}/{}",
                    len, MAX_DOWNLOAD_BYTES, bucket, key
                )));
            }
        }
        let bytes = response
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?
            .into_bytes();
        Ok(bytes.to_vec())
    }

    /// Loads the latest committed metadata file version and parsed payload for a topic.
    ///
    /// Delegates to `load_latest_table_metadata_raw` for the S3 work, then parses.
    /// The raw method already caches version and parsed metadata on miss.
    async fn load_latest_table_metadata_with_version(
        &self,
        topic: &str,
    ) -> Result<Option<(i64, TableMetadata)>, StorageError> {
        // Fast path: both caches hit
        if let Some(cached) = self.get_table_metadata(topic)? {
            let version = {
                let versions = self.metadata_versions.read().map_lock_err()?;
                versions.get(topic).copied()
            };
            if let Some(version) = version {
                return Ok(Some((version, cached)));
            }
        }

        // Slow path: _raw fetches from S3 and caches version + parsed metadata
        let Some((version, raw_json)) = self.load_latest_table_metadata_raw(topic).await? else {
            return Ok(None);
        };

        // _raw already cached the parsed metadata; read it back to avoid re-parsing
        if let Some(cached) = self.get_table_metadata(topic)? {
            return Ok(Some((version, cached)));
        }

        // Fallback: parse directly (shouldn't happen unless cache was evicted)
        let metadata = TableMetadata::from_json(&raw_json)?;
        Ok(Some((version, metadata)))
    }

    /// Loads the latest metadata file for a topic from S3, populating local caches.
    async fn load_latest_table_metadata(
        &self,
        topic: &str,
    ) -> Result<Option<TableMetadata>, StorageError> {
        Ok(self
            .load_latest_table_metadata_with_version(topic)
            .await?
            .map(|(_version, metadata)| metadata))
    }

    /// Lists table names under the configured Iceberg base path.
    ///
    /// Uses delimiter-based S3 listing to enumerate table directories (O(tables))
    /// instead of listing every key under the base path (O(all-keys)).
    pub async fn list_tables(&self) -> Result<Vec<String>, StorageError> {
        let root_prefix = format!("{}/", self.base_path.trim_end_matches('/'));

        let dir_prefixes = self
            .list_keys_with_prefix(&self.bucket, &root_prefix, Some("/"))
            .await?;

        let mut table_names = Vec::new();
        for dir_prefix in dir_prefixes {
            let Some(suffix) = dir_prefix.strip_prefix(&root_prefix) else {
                continue;
            };
            let table = suffix.trim_end_matches('/');
            if table.is_empty() || table.contains('/') {
                continue;
            }

            let metadata_prefix = format!("{}metadata/", dir_prefix);
            if self
                .has_metadata_files(&self.bucket, &metadata_prefix)
                .await?
            {
                table_names.push(table.to_string());
            }
        }

        table_names.sort();
        Ok(table_names)
    }

    /// Loads the latest metadata file as raw JSON plus its version number.
    ///
    /// Unlike `load_latest_table_metadata_with_version`, this returns the original
    /// JSON bytes from S3 without deserializing and re-serializing, preserving
    /// unknown fields and formatting.
    async fn load_latest_table_metadata_raw(
        &self,
        topic: &str,
    ) -> Result<Option<(i64, String)>, StorageError> {
        // Check version cache for a direct download path
        let cached_version = {
            let versions = self.metadata_versions.read().map_lock_err()?;
            versions.get(topic).copied()
        };

        if let Some(version) = cached_version {
            let key = format!(
                "{}/{}/metadata/{}",
                self.base_path.trim_end_matches('/'),
                topic,
                metadata_file_name(version)
            );
            let bytes = self.download_object_bytes(&self.bucket, &key).await?;
            let json =
                String::from_utf8(bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;
            return Ok(Some((version, json)));
        }

        // No cached version: list metadata files, find latest, download raw
        let metadata_prefix = format!("{}/{}/metadata/", self.base_path, topic);
        let keys = self
            .list_keys_with_prefix(&self.bucket, &metadata_prefix, None)
            .await?;

        let latest = keys
            .into_iter()
            .filter_map(|key| {
                Self::parse_metadata_version_from_key(&key).map(|version| (version, key))
            })
            .max_by_key(|(version, _)| *version);

        let Some((version, key)) = latest else {
            return Ok(None);
        };

        let bytes = self.download_object_bytes(&self.bucket, &key).await?;
        let json =
            String::from_utf8(bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Cache the version and parsed metadata for other callers
        {
            let mut versions = self.metadata_versions.write().map_lock_err()?;
            versions.insert(topic.to_string(), version);
        }
        if let Ok(metadata) = TableMetadata::from_json(&json) {
            let mut cached = self.table_metadata.write().map_lock_err()?;
            cached.insert(topic.to_string(), metadata);
        }

        Ok(Some((version, json)))
    }

    /// Loads committed metadata payload and exact metadata file location for REST catalog load-table.
    pub async fn load_table_for_catalog(
        &self,
        topic: &str,
    ) -> Result<Option<IcebergCatalogTable>, StorageError> {
        let Some((version, raw_json)) = self.load_latest_table_metadata_raw(topic).await? else {
            return Ok(None);
        };

        let metadata_location = format!(
            "s3://{}/{}/{}/metadata/{}",
            self.bucket,
            self.base_path.trim_end_matches('/'),
            topic,
            metadata_file_name(version)
        );

        Ok(Some(IcebergCatalogTable {
            metadata_location,
            metadata_json: raw_json,
        }))
    }

    /// Checks if an S3 key belongs to the specified partition.
    /// Path format: .../partition=N/...
    #[inline]
    fn is_partition_file(key: &str, partition: u32) -> bool {
        let partition_suffix = format!("/partition={}/", partition);
        key.contains(&partition_suffix)
    }

    /// Uploads bytes to S3 with retry.
    async fn upload_bytes(
        &self,
        key: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> Result<(), StorageError> {
        let client = &self.client;
        let bucket = &self.bucket;
        let data = Bytes::from(data); // Convert once; Bytes::clone is cheap (ref-counted)
        s3_retry!(
            operation = {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(data.clone()))
                    .content_type(content_type)
                    .send()
                    .await
            },
            retry_config = self.retry_config,
            context = format!("PUT {}", key),
        )?;
        Ok(())
    }

    /// Commits pending data files by creating a new snapshot.
    /// This should be called after a batch of write_segment calls.
    pub async fn commit_snapshot(
        &self,
        topic: &str,
        context: SnapshotCommitContext,
    ) -> Result<Option<i64>, StorageError> {
        // Drain pending files for this topic under a write lock. This prevents
        // races where new files are added between a read snapshot and later clear.
        // New writes after this point remain in the map for the next snapshot.
        let drained_by_key = {
            let mut pending = self.pending_data_files.write().map_lock_err()?;
            let keys: Vec<(String, u32)> = pending
                .keys()
                .filter(|(pending_topic, _partition)| pending_topic == topic)
                .cloned()
                .collect();
            let mut drained = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(files) = pending.remove(&key) {
                    drained.push((key, files));
                }
            }
            drained
        };

        if drained_by_key.is_empty() {
            return Ok(None);
        }

        let pending: Vec<(DataFile, ParquetFileMetadata)> = drained_by_key
            .iter()
            .flat_map(|(_key, files)| files.iter().cloned())
            .collect();

        let extra_summary = Self::snapshot_summary_from_context(&context);

        let commit_result: Result<(i64, usize, i64), StorageError> = async move {
            let mut metadata = self.get_or_create_metadata(topic)?;

            // Calculate totals
            let total_files = pending.len();
            let total_rows: i64 = pending.iter().map(|(_, m)| m.row_count as i64).sum();

            let snapshot_id = crate::storage::generate_snapshot_id();

            // Build manifest file with all data file entries
            let mut manifest = ManifestFile::new(snapshot_id, metadata.last_sequence_number + 1);
            for (data_file, _) in &pending {
                manifest.add_data_file(data_file.clone());
            }

            // Serialize manifest to Avro and upload
            let manifest_filename = manifest_file_name();
            let manifest_key = self.metadata_key(topic, &manifest_filename);
            let manifest_s3_path = format!("s3://{}/{}", self.bucket, manifest_key);
            let manifest_avro_bytes = manifest.to_avro_bytes(&metadata)?;
            let manifest_length = manifest_avro_bytes.len() as i64;
            self.upload_bytes(&manifest_key, manifest_avro_bytes, "application/avro")
                .await?;

            // Build manifest list entry pointing to the real manifest
            let manifest_list_entries = vec![ManifestListEntry {
                manifest_path: manifest_s3_path,
                manifest_length,
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

            // Serialize manifest list to Avro and upload
            let manifest_list_filename = manifest_list_file_name(snapshot_id);
            let manifest_list_key = self.metadata_key(topic, &manifest_list_filename);
            let manifest_list_s3_path = format!("s3://{}/{}", self.bucket, manifest_list_key);
            let manifest_list_avro =
                manifest_list_to_avro_bytes(&manifest_list_entries, &metadata)?;
            self.upload_bytes(&manifest_list_key, manifest_list_avro, "application/avro")
                .await?;

            // Add snapshot to metadata
            metadata.add_snapshot(
                &manifest_list_s3_path,
                total_files,
                total_rows,
                SnapshotOperation::Append,
                extra_summary,
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

            Ok((snapshot_id, total_files, total_rows))
        }
        .await;

        match commit_result {
            Ok((snapshot_id, total_files, total_rows)) => {
                tracing::info!(
                    topic = topic,
                    snapshot_id = snapshot_id,
                    files = total_files,
                    rows = total_rows,
                    "Created Iceberg snapshot"
                );
                Ok(Some(snapshot_id))
            }
            Err(error) => {
                // Restore drained pending files so they can be retried on the next commit.
                match self.pending_data_files.write() {
                    Ok(mut pending_map) => {
                        for (key, mut files) in drained_by_key {
                            pending_map.entry(key).or_default().append(&mut files);
                        }
                    }
                    Err(lock_error) => {
                        tracing::error!(
                            topic = topic,
                            error = %lock_error,
                            "Failed to restore pending snapshot files after commit failure"
                        );
                    }
                }
                Err(error)
            }
        }
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

        // Convert events to Parquet with sorting for optimized queries
        // Clone events since we need to sort them
        let mut events_to_write = events.to_vec();
        let (parquet_bytes, parquet_metadata) = if let Some(config) = self.schema_configs.get(topic)
        {
            if config.payload_format == PayloadFormat::Json && !config.fields.is_empty() {
                write_parquet_to_bytes_structured_sorted(&mut events_to_write, config)?
            } else {
                write_parquet_to_bytes_sorted(&mut events_to_write)?
            }
        } else {
            write_parquet_to_bytes_sorted(&mut events_to_write)?
        };

        // Generate data file name and key
        let filename = data_file_name();
        let key = self.data_file_key(topic, partition, &filename, event_date, event_hour);
        let s3_path = format!("s3://{}/{}", self.bucket, key);

        // Upload Parquet file
        self.upload_bytes(&key, parquet_bytes, "application/octet-stream")
            .await?;

        // Create DataFile entry
        let data_file = DataFile::from_parquet_metadata(&parquet_metadata, &s3_path);

        // Add to pending files for this topic/partition
        {
            let mut pending = self.pending_data_files.write().map_lock_err()?;
            pending
                .entry((topic.to_string(), partition))
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
        _projection: &crate::contracts::ColumnProjection,
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

        let client = &self.client;
        let bucket = &self.bucket;
        let response = s3_retry!(
            operation = {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(&prefix)
                    .send()
                    .await
            },
            retry_config = self.retry_config,
            context = format!("LIST {}", prefix),
        )?;

        let mut all_events = Vec::new();

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = object.key() {
                    // Filter for Parquet files in the correct partition directory
                    if !key.ends_with(".parquet") || !Self::is_partition_file(key, partition) {
                        continue;
                    }

                    // Download and read Parquet file with retry
                    let key_owned = key.to_string();
                    let response = s3_retry!(
                        operation = {
                            client
                                .get_object()
                                .bucket(bucket)
                                .key(&key_owned)
                                .send()
                                .await
                        },
                        retry_config = self.retry_config,
                        context = format!("GET {}", key_owned),
                    )?;

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

        let client = &self.client;
        let bucket = &self.bucket;
        let response = s3_retry!(
            operation = {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(&prefix)
                    .send()
                    .await
            },
            retry_config = self.retry_config,
            context = format!("LIST {}", prefix),
        )?;

        let mut segments = Vec::new();

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = object.key() {
                    // Filter for Parquet files in the correct partition directory
                    if !key.ends_with(".parquet") || !Self::is_partition_file(key, partition) {
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

    async fn commit_snapshot(
        &self,
        topic: &str,
        context: SnapshotCommitContext,
    ) -> Result<Option<i64>, StorageError> {
        self.commit_snapshot(topic, context).await
    }

    fn table_metadata_json(&self, topic: &str) -> Option<String> {
        self.get_table_metadata(topic)
            .ok()
            .flatten()
            .and_then(|m| m.to_json().ok())
    }

    async fn list_iceberg_tables(&self) -> Result<Vec<String>, StorageError> {
        self.list_tables().await
    }

    async fn load_iceberg_table(
        &self,
        topic: &str,
    ) -> Result<Option<IcebergCatalogTable>, StorageError> {
        self.load_table_for_catalog(topic).await
    }

    async fn iceberg_table_exists(&self, topic: &str) -> Result<bool, StorageError> {
        // Fast path: check caches
        {
            let metadata = self.table_metadata.read().map_lock_err()?;
            if metadata.contains_key(topic) {
                return Ok(true);
            }
        }
        {
            let versions = self.metadata_versions.read().map_lock_err()?;
            if versions.contains_key(topic) {
                return Ok(true);
            }
        }

        // Slow path: check S3 for at least one metadata file
        let metadata_prefix = format!("{}/{}/metadata/", self.base_path, topic);
        self.has_metadata_files(&self.bucket, &metadata_prefix)
            .await
    }

    fn clear_pending_data_files(&self, topic: &str, partition: u32) {
        if let Ok(mut pending) = self.pending_data_files.write() {
            if let Some(removed) = pending.remove(&(topic.to_string(), partition)) {
                if !removed.is_empty() {
                    tracing::warn!(
                        topic = topic,
                        partition = partition,
                        orphaned_files = removed.len(),
                        "Cleared pending data files after flush failure (orphaned S3 files remain)"
                    );
                }
            }
        }
    }

    async fn committed_flush_watermarks(
        &self,
        topic: &str,
    ) -> Result<HashMap<u32, u64>, StorageError> {
        let metadata = match self.load_latest_table_metadata(topic).await? {
            Some(metadata) => metadata,
            None => return Ok(HashMap::new()),
        };

        let current_snapshot_id = match metadata.current_snapshot_id {
            Some(id) => id,
            None => return Ok(HashMap::new()),
        };

        let manifest_list_uri = match metadata
            .snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == current_snapshot_id)
            .map(|snapshot| snapshot.manifest_list.clone())
        {
            Some(uri) => uri,
            None => return Ok(HashMap::new()),
        };

        let (manifest_list_bucket, manifest_list_key) = Self::parse_s3_uri(&manifest_list_uri)?;
        let manifest_list_bytes = self
            .download_object_bytes(&manifest_list_bucket, &manifest_list_key)
            .await?;
        let manifest_paths =
            Self::extract_manifest_paths_from_manifest_list_avro(&manifest_list_bytes)?;

        let mut seen_paths = HashSet::new();
        let mut per_partition = HashMap::new();
        for manifest_uri in manifest_paths {
            if !seen_paths.insert(manifest_uri.clone()) {
                continue;
            }
            let (bucket, key) = Self::parse_s3_uri(&manifest_uri)?;
            let manifest_bytes = self.download_object_bytes(&bucket, &key).await?;
            let manifest_watermarks =
                Self::extract_partition_watermarks_from_manifest_avro(&manifest_bytes)?;
            for (partition, watermark) in manifest_watermarks {
                per_partition
                    .entry(partition)
                    .and_modify(|current: &mut u64| *current = (*current).max(watermark))
                    .or_insert(watermark);
            }
        }

        Ok(per_partition)
    }

    fn pending_snapshot_stats_for_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> PendingSnapshotStats {
        let pending = match self.pending_data_files.read() {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    topic = topic,
                    partition = partition,
                    error = %e,
                    "Failed to acquire lock for partition pending_snapshot_stats, returning default"
                );
                return PendingSnapshotStats::default();
            }
        };

        if let Some(files) = pending.get(&(topic.to_string(), partition)) {
            PendingSnapshotStats {
                file_count: files.len(),
                total_bytes: files.iter().map(|(_, m)| m.file_size_bytes).sum::<u64>(),
            }
        } else {
            PendingSnapshotStats::default()
        }
    }

    fn pending_snapshot_stats(&self, topic: &str) -> PendingSnapshotStats {
        let pending = match self.pending_data_files.read() {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    topic = topic,
                    error = %e,
                    "Failed to acquire lock for pending_snapshot_stats, returning default"
                );
                return PendingSnapshotStats::default();
            }
        };

        let mut file_count = 0usize;
        let mut total_bytes = 0u64;
        for ((pending_topic, _partition), files) in pending.iter() {
            if pending_topic == topic {
                file_count += files.len();
                total_bytes += files.iter().map(|(_, m)| m.file_size_bytes).sum::<u64>();
            }
        }

        if file_count > 0 {
            PendingSnapshotStats {
                file_count,
                total_bytes,
            }
        } else {
            PendingSnapshotStats::default()
        }
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
    use crate::storage::{write_parquet_to_bytes, RetryConfig};

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

        let (bytes, _) = write_parquet_to_bytes(std::slice::from_ref(&event)).unwrap();
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
            assert!((12..=13).contains(&count));
        }
    }

    #[test]
    fn parse_metadata_version_from_key_extracts_numeric_version() {
        assert_eq!(
            IcebergStorage::parse_metadata_version_from_key(
                "tables/events/metadata/v42.metadata.json"
            ),
            Some(42)
        );
        assert_eq!(
            IcebergStorage::parse_metadata_version_from_key("tables/events/metadata/snap-1.avro"),
            None
        );
    }

    #[test]
    fn extract_manifest_paths_filters_non_data_manifests() {
        let metadata = TableMetadata::new("s3://test-bucket/tables/events");
        let entries = vec![
            ManifestListEntry {
                manifest_path: "s3://test-bucket/tables/events/metadata/data-m0.avro".into(),
                manifest_length: 100,
                partition_spec_id: 0,
                content: 0,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 1,
                added_files_count: 1,
                existing_files_count: 0,
                deleted_files_count: 0,
                added_rows_count: 10,
                existing_rows_count: 0,
                deleted_rows_count: 0,
            },
            ManifestListEntry {
                manifest_path: "s3://test-bucket/tables/events/metadata/delete-m0.avro".into(),
                manifest_length: 100,
                partition_spec_id: 0,
                content: 1,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 1,
                added_files_count: 0,
                existing_files_count: 0,
                deleted_files_count: 1,
                added_rows_count: 0,
                existing_rows_count: 0,
                deleted_rows_count: 10,
            },
        ];

        let avro = manifest_list_to_avro_bytes(&entries, &metadata).unwrap();
        let paths = IcebergStorage::extract_manifest_paths_from_manifest_list_avro(&avro).unwrap();
        assert_eq!(
            paths,
            vec!["s3://test-bucket/tables/events/metadata/data-m0.avro".to_string()]
        );
    }

    #[test]
    fn extract_partition_watermarks_reads_upper_bounds() {
        use crate::storage::{ColumnStatistics, PartitionValues};

        fn make_data_file(partition: u32, sequence_max: u64) -> DataFile {
            let parquet_meta = ParquetFileMetadata {
                path: format!("test-{}-{}.parquet", partition, sequence_max),
                row_count: 1,
                min_sequence: sequence_max,
                max_sequence: sequence_max,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                file_size_bytes: 1,
                partition_values: PartitionValues::default(),
                column_stats: ColumnStatistics {
                    sequence_min: sequence_max,
                    sequence_max,
                    partition_min: partition,
                    partition_max: partition,
                    timestamp_min: 0,
                    timestamp_max: 0,
                    event_date_min: 0,
                    event_date_max: 0,
                    event_hour_min: 0,
                    event_hour_max: 0,
                },
            };
            DataFile::from_parquet_metadata(
                &parquet_meta,
                "s3://test-bucket/tables/events/data/file.parquet",
            )
        }

        let metadata = TableMetadata::new("s3://test-bucket/tables/events");
        let mut manifest = ManifestFile::new(123, 1);
        manifest.add_data_file(make_data_file(0, 3));
        manifest.add_data_file(make_data_file(0, 7));
        manifest.add_data_file(make_data_file(1, 4));
        manifest.entries.push(crate::storage::ManifestEntry {
            status: 2,
            snapshot_id: 123,
            data_file: make_data_file(0, 99),
        });

        let avro = manifest.to_avro_bytes(&metadata).unwrap();
        let per_partition =
            IcebergStorage::extract_partition_watermarks_from_manifest_avro(&avro).unwrap();
        assert_eq!(per_partition.get(&0), Some(&7));
        assert_eq!(per_partition.get(&1), Some(&4));
    }

    #[tokio::test]
    async fn clear_pending_data_files_is_partition_scoped() {
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
        assert_eq!(storage.pending_snapshot_stats("events").file_count, 2);
        assert_eq!(
            storage
                .pending_snapshot_stats_for_partition("events", 0)
                .file_count,
            1
        );
        assert_eq!(
            storage
                .pending_snapshot_stats_for_partition("events", 1)
                .file_count,
            1
        );

        storage.clear_pending_data_files("events", 1);
        assert_eq!(storage.pending_snapshot_stats("events").file_count, 1);
        assert_eq!(
            storage
                .pending_snapshot_stats_for_partition("events", 1)
                .file_count,
            0
        );
        storage.clear_pending_data_files("events", 0);
        assert_eq!(storage.pending_snapshot_stats("events").file_count, 0);
    }

    #[tokio::test]
    async fn commit_snapshot_failure_preserves_pending_entries() {
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
        assert_eq!(storage.pending_snapshot_stats("events").file_count, 1);

        let result = storage
            .commit_snapshot("events", SnapshotCommitContext::default())
            .await;
        assert!(result.is_err());

        // Pending files should still be present after a failed commit attempt.
        assert_eq!(storage.pending_snapshot_stats("events").file_count, 1);
    }

    #[test]
    fn snapshot_summary_from_context_maps_watermarks() {
        let context = SnapshotCommitContext {
            watermarks_by_partition: HashMap::from([(0, 42), (1, 99)]),
            high_watermarks_by_partition: HashMap::from([(0, 100), (1, 200)]),
        };

        let summary = IcebergStorage::snapshot_summary_from_context(&context);

        assert_eq!(summary.get("zombi.watermark.0").unwrap(), "42");
        assert_eq!(summary.get("zombi.watermark.1").unwrap(), "99");
        assert_eq!(summary.get("zombi.high_watermark.0").unwrap(), "100");
        assert_eq!(summary.get("zombi.high_watermark.1").unwrap(), "200");
        assert_eq!(summary.len(), 4);
    }

    #[test]
    fn snapshot_summary_from_empty_context_is_empty() {
        let context = SnapshotCommitContext::default();
        let summary = IcebergStorage::snapshot_summary_from_context(&context);
        assert!(summary.is_empty());
    }
}
