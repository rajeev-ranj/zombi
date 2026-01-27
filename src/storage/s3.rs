use std::collections::HashMap;
use std::sync::RwLock;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;

use crate::contracts::{
    ColdStorage, ColdStorageInfo, LockResultExt, SegmentInfo, StorageError, StoredEvent,
};
use crate::s3_retry;
use crate::storage::retry::RetryConfig;

/// S3-backed cold storage implementation.
pub struct S3Storage {
    client: Client,
    bucket: String,
    /// Cache of segment info per topic/partition
    segment_cache: RwLock<HashMap<(String, u32), Vec<SegmentInfo>>>,
    /// Retry configuration for S3 operations
    retry_config: RetryConfig,
}

impl S3Storage {
    /// Creates a new S3 storage with default AWS configuration and retry settings.
    pub async fn new(bucket: impl Into<String>) -> Result<Self, StorageError> {
        Self::new_with_retry(bucket, RetryConfig::from_env()).await
    }

    /// Creates a new S3 storage with custom retry configuration.
    pub async fn new_with_retry(
        bucket: impl Into<String>,
        retry_config: RetryConfig,
    ) -> Result<Self, StorageError> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket: bucket.into(),
            segment_cache: RwLock::new(HashMap::new()),
            retry_config,
        })
    }

    /// Creates a new S3 storage with a custom endpoint (for MinIO/LocalStack).
    pub async fn with_endpoint(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, StorageError> {
        Self::with_endpoint_and_retry(bucket, endpoint, region, RetryConfig::from_env()).await
    }

    /// Creates a new S3 storage with a custom endpoint and retry configuration.
    pub async fn with_endpoint_and_retry(
        bucket: impl Into<String>,
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
            segment_cache: RwLock::new(HashMap::new()),
            retry_config,
        })
    }

    /// Returns the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Generates an S3 key for a segment.
    fn segment_key(topic: &str, partition: u32, start_offset: u64, end_offset: u64) -> String {
        format!(
            "segments/{}/{}/{:016x}-{:016x}.json",
            topic, partition, start_offset, end_offset
        )
    }

    /// Parses segment info from an S3 key.
    fn parse_segment_key(key: &str) -> Option<(String, u32, u64, u64)> {
        // Format: segments/{topic}/{partition}/{start}-{end}.json
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 4 || parts[0] != "segments" {
            return None;
        }

        let topic = parts[1].to_string();
        let partition: u32 = parts[2].parse().ok()?;

        let filename = parts[3].strip_suffix(".json")?;
        let offsets: Vec<&str> = filename.split('-').collect();
        if offsets.len() != 2 {
            return None;
        }

        let start = u64::from_str_radix(offsets[0], 16).ok()?;
        let end = u64::from_str_radix(offsets[1], 16).ok()?;

        Some((topic, partition, start, end))
    }

    /// Serializes events to JSON bytes.
    fn serialize_events(events: &[StoredEvent]) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(events).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    /// Deserializes events from JSON bytes.
    fn deserialize_events(bytes: &[u8]) -> Result<Vec<StoredEvent>, StorageError> {
        serde_json::from_slice(bytes).map_err(|e| StorageError::Serialization(e.to_string()))
    }
}

impl ColdStorage for S3Storage {
    async fn write_segment(
        &self,
        topic: &str,
        partition: u32,
        events: &[StoredEvent],
    ) -> Result<String, StorageError> {
        if events.is_empty() {
            return Err(StorageError::S3("Cannot write empty segment".into()));
        }

        let start_offset = events.first().map(|e| e.sequence).unwrap_or(0);
        let end_offset = events.last().map(|e| e.sequence).unwrap_or(0);

        let key = Self::segment_key(topic, partition, start_offset, end_offset);
        let body = Bytes::from(Self::serialize_events(events)?);
        let size = body.len() as u64;

        let client = &self.client;
        let bucket = &self.bucket;
        s3_retry!(
            operation = {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(ByteStream::from(body.clone())) // Bytes::clone is cheap (ref-counted)
                    .content_type("application/json")
                    .send()
                    .await
            },
            retry_config = self.retry_config,
            context = format!("PUT {}", key),
        )?;

        // Update cache
        {
            let mut cache = self.segment_cache.write().map_lock_err()?;
            let segments = cache.entry((topic.to_string(), partition)).or_default();
            segments.push(SegmentInfo {
                segment_id: key.clone(),
                start_offset,
                end_offset,
                event_count: events.len(),
                size_bytes: size,
            });
            segments.sort_by_key(|s| s.start_offset);
        }

        Ok(key)
    }

    async fn read_events(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        limit: usize,
        _since_ms: Option<i64>,
        _until_ms: Option<i64>,
        _projection: &crate::contracts::ColumnProjection,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        // List segments that might contain our offset
        let segments = self.list_segments(topic, partition).await?;

        let mut events = Vec::new();

        for segment in segments {
            if segment.end_offset < start_offset {
                continue; // Skip segments entirely before our offset
            }

            // Read segment with retry
            let client = &self.client;
            let bucket = &self.bucket;
            let segment_key = &segment.segment_id;
            let response = s3_retry!(
                operation = {
                    client
                        .get_object()
                        .bucket(bucket)
                        .key(segment_key)
                        .send()
                        .await
                },
                retry_config = self.retry_config,
                context = format!("GET {}", segment_key),
            )?;

            let bytes = response
                .body
                .collect()
                .await
                .map_err(|e| StorageError::S3(e.to_string()))?
                .into_bytes();

            let segment_events = Self::deserialize_events(&bytes)?;

            for event in segment_events {
                if event.sequence >= start_offset {
                    events.push(event);
                    if events.len() >= limit {
                        return Ok(events);
                    }
                }
            }
        }

        Ok(events)
    }

    async fn list_segments(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Vec<SegmentInfo>, StorageError> {
        let prefix = format!("segments/{}/{}/", topic, partition);

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
                    if let Some((_, _, start, end)) = Self::parse_segment_key(key) {
                        segments.push(SegmentInfo {
                            segment_id: key.to_string(),
                            start_offset: start,
                            end_offset: end,
                            event_count: 0, // Unknown without reading
                            size_bytes: object.size().unwrap_or(0) as u64,
                        });
                    }
                }
            }
        }

        segments.sort_by_key(|s| s.start_offset);
        Ok(segments)
    }

    fn storage_info(&self) -> ColdStorageInfo {
        ColdStorageInfo {
            storage_type: "s3".into(),
            iceberg_enabled: false,
            bucket: self.bucket.clone(),
            base_path: "segments".into(),
        }
    }

    fn iceberg_metadata_location(&self, _topic: &str) -> Option<String> {
        None
    }

    async fn commit_snapshot(&self, _topic: &str) -> Result<Option<i64>, StorageError> {
        // S3 storage doesn't create Iceberg metadata
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_key_generation() {
        let key = S3Storage::segment_key("my-topic", 5, 100, 200);
        assert_eq!(
            key,
            "segments/my-topic/5/0000000000000064-00000000000000c8.json"
        );
    }

    #[test]
    fn test_parse_segment_key() {
        let key = "segments/my-topic/5/0000000000000064-00000000000000c8.json";
        let parsed = S3Storage::parse_segment_key(key);
        assert_eq!(parsed, Some(("my-topic".to_string(), 5, 100, 200)));
    }

    #[test]
    fn test_parse_invalid_segment_key() {
        assert!(S3Storage::parse_segment_key("invalid").is_none());
        assert!(S3Storage::parse_segment_key("segments/topic").is_none());
        assert!(S3Storage::parse_segment_key("other/topic/0/1-2.json").is_none());
    }
}
