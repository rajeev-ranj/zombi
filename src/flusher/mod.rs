use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::contracts::{
    ColdStorage, FlushResult, Flusher, HotStorage, LockResultExt, StorageError,
};
use crate::metrics::{FlushMetrics, IcebergMetrics};

/// Configuration for the background flusher.
#[derive(Debug, Clone)]
pub struct FlusherConfig {
    /// Interval between flush checks
    pub interval: Duration,
    /// Target batch size for optimal compression.
    /// Note: Currently advisory - all available events are flushed each cycle.
    /// Actual batching is controlled by target_file_size_bytes for Iceberg.
    pub batch_size: usize,
    /// Maximum events per segment
    pub max_segment_size: usize,
    /// Target size in bytes for each Parquet file (for Iceberg)
    /// Default: 128MB (Iceberg recommended)
    pub target_file_size_bytes: usize,
    /// Enable Iceberg metadata management
    pub iceberg_enabled: bool,
    /// Minimum number of files accumulated before committing a snapshot.
    /// Default: 10 files. Set to 1 to snapshot every flush (old behavior).
    pub snapshot_threshold_files: usize,
    /// Minimum bytes accumulated before committing a snapshot.
    /// Default: 1GB. Snapshot commits when this OR threshold_files is exceeded.
    pub snapshot_threshold_bytes: usize,
    /// Maximum concurrent S3 uploads during flush.
    /// Default: 4. Higher values can improve throughput but increase memory usage.
    pub max_concurrent_s3_uploads: usize,
}

impl Default for FlusherConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            batch_size: 1000,
            max_segment_size: 10000,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB
            iceberg_enabled: false,
            snapshot_threshold_files: 10,
            snapshot_threshold_bytes: 1024 * 1024 * 1024, // 1GB
            max_concurrent_s3_uploads: 4,
        }
    }
}

impl FlusherConfig {
    /// Creates a config optimized for Iceberg with size-based flushing.
    ///
    /// Tuned for Iceberg best practices:
    /// - Target 64-256MB Parquet files for optimal query performance
    /// - 5-minute flush interval for low-volume tables to accumulate data
    /// - Size-based batching via target_file_size_bytes
    /// - Batched snapshots: commit only when 10 files or 1GB accumulated
    /// - Pipelined S3 uploads: 4 concurrent uploads for better throughput
    ///
    /// # Durability
    ///
    /// The 5-minute flush interval means up to 5 minutes of data may be lost
    /// on crash (WAL is disabled for throughput). For tighter durability
    /// guarantees, override with `ZOMBI_FLUSH_INTERVAL_SECS` environment variable.
    pub fn iceberg_defaults() -> Self {
        Self {
            interval: Duration::from_secs(300), // 5 minutes for low-volume tables
            batch_size: 10000,                  // Advisory target batch size
            max_segment_size: 100000,           // Max events per segment
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB target (Iceberg best practice)
            iceberg_enabled: true,
            snapshot_threshold_files: 10, // Batch snapshots for reduced metadata churn
            snapshot_threshold_bytes: 1024 * 1024 * 1024, // 1GB
            max_concurrent_s3_uploads: 4, // Pipeline S3 writes
        }
    }
}

/// Background flusher that moves events from hot to cold storage.
pub struct BackgroundFlusher<H, C>
where
    H: HotStorage + 'static,
    C: ColdStorage + 'static,
{
    hot_storage: Arc<H>,
    cold_storage: Arc<C>,
    config: FlusherConfig,
    /// Flush watermarks per topic/partition
    watermarks: Arc<RwLock<HashMap<(String, u32), u64>>>,
    /// Flag to signal shutdown
    shutdown: Arc<AtomicBool>,
    /// Notify for immediate flush requests
    flush_notify: Arc<Notify>,
    /// Handle to the background task
    task_handle: RwLock<Option<JoinHandle<()>>>,
    /// Topics/partitions to monitor
    topics: Arc<RwLock<Vec<(String, u32)>>>,
    /// Flush metrics
    flush_metrics: Arc<FlushMetrics>,
    /// Iceberg metrics
    iceberg_metrics: Arc<IcebergMetrics>,
}

impl<H, C> BackgroundFlusher<H, C>
where
    H: HotStorage + 'static,
    C: ColdStorage + 'static,
{
    /// Creates a new background flusher.
    pub fn new(
        hot_storage: Arc<H>,
        cold_storage: Arc<C>,
        config: FlusherConfig,
        flush_metrics: Arc<FlushMetrics>,
        iceberg_metrics: Arc<IcebergMetrics>,
    ) -> Self {
        Self {
            hot_storage,
            cold_storage,
            config,
            watermarks: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
            task_handle: RwLock::new(None),
            topics: Arc::new(RwLock::new(Vec::new())),
            flush_metrics,
            iceberg_metrics,
        }
    }

    /// Registers a topic/partition for flushing.
    pub fn register_topic(&self, topic: String, partition: u32) -> Result<(), StorageError> {
        let mut topics = self.topics.write().map_lock_err()?;
        let key = (topic, partition);
        if !topics.contains(&key) {
            topics.push(key);
        }
        Ok(())
    }

    fn list_topic_partitions(hot_storage: &H) -> Result<Vec<(String, u32)>, StorageError> {
        let topic_names = hot_storage.list_topics()?;
        let mut all = Vec::new();

        for topic in topic_names {
            match hot_storage.list_partitions(&topic) {
                Ok(partitions) => {
                    for partition in partitions {
                        all.push((topic.clone(), partition));
                    }
                }
                Err(e) => {
                    tracing::error!(topic = %topic, error = %e, "Failed to list partitions");
                }
            }
        }

        Ok(all)
    }

    /// Flushes a single topic/partition.
    ///
    /// When `iceberg_enabled` is true, uses `target_file_size_bytes` to limit
    /// the segment size instead of event count alone.
    ///
    /// Returns (events_flushed, new_watermark, bytes_flushed) on success.
    #[allow(clippy::too_many_arguments)]
    async fn flush_partition(
        hot_storage: &H,
        cold_storage: &C,
        topic: &str,
        partition: u32,
        watermark: u64,
        max_segment_size: usize,
        iceberg_enabled: bool,
        target_file_size_bytes: usize,
    ) -> Result<(usize, u64, usize), StorageError> {
        let high_watermark = hot_storage.high_watermark(topic, partition)?;

        if high_watermark <= watermark {
            return Ok((0, watermark, 0)); // Nothing to flush
        }

        // Read events from hot storage
        let all_events = hot_storage.read(topic, partition, watermark + 1, max_segment_size)?;

        if all_events.is_empty() {
            return Ok((0, watermark, 0));
        }

        // When Iceberg is enabled, use size-based batching
        let events = if iceberg_enabled {
            let mut total_size: usize = 0;
            let mut count = 0;

            for event in &all_events {
                // Estimate event size: payload + overhead for metadata (timestamp, sequence, etc.)
                // Parquet typically has ~10-20% overhead, so estimate conservatively
                let event_size = event.payload.len() + 64; // 64 bytes for metadata overhead
                if total_size + event_size > target_file_size_bytes && count > 0 {
                    break;
                }
                total_size += event_size;
                count += 1;
            }

            tracing::debug!(
                topic = topic,
                partition = partition,
                total_events = all_events.len(),
                size_limited_events = count,
                estimated_size = total_size,
                target_size = target_file_size_bytes,
                "Using size-based batching for Iceberg"
            );

            all_events.into_iter().take(count).collect::<Vec<_>>()
        } else {
            all_events
        };

        if events.is_empty() {
            return Ok((0, watermark, 0));
        }

        let events_count = events.len();
        let new_watermark = events.last().map(|e| e.sequence).unwrap_or(watermark);

        // Calculate bytes flushed (payload + metadata overhead)
        let bytes_flushed: usize = events.iter().map(|e| e.payload.len() + 64).sum();

        // Write to cold storage
        cold_storage
            .write_segment(topic, partition, &events)
            .await?;

        tracing::info!(
            topic = topic,
            partition = partition,
            events = events_count,
            bytes = bytes_flushed,
            new_watermark = new_watermark,
            iceberg_enabled = iceberg_enabled,
            "Flushed events to cold storage"
        );

        Ok((events_count, new_watermark, bytes_flushed))
    }
}

impl<H, C> Flusher for BackgroundFlusher<H, C>
where
    H: HotStorage + 'static,
    C: ColdStorage + 'static,
{
    async fn start(&self) -> Result<(), StorageError> {
        self.shutdown.store(false, Ordering::SeqCst);

        let hot_storage = Arc::clone(&self.hot_storage);
        let cold_storage = Arc::clone(&self.cold_storage);
        let watermarks = Arc::clone(&self.watermarks);
        let shutdown = Arc::clone(&self.shutdown);
        let flush_notify = Arc::clone(&self.flush_notify);
        let _topics = Arc::clone(&self.topics); // Reserved for manual registration
        let interval = self.config.interval;
        let max_segment_size = self.config.max_segment_size;
        let iceberg_enabled = self.config.iceberg_enabled;
        let target_file_size_bytes = self.config.target_file_size_bytes;
        let snapshot_threshold_files = self.config.snapshot_threshold_files;
        let snapshot_threshold_bytes = self.config.snapshot_threshold_bytes;
        let max_concurrent_uploads = self.config.max_concurrent_s3_uploads;
        let flush_metrics = Arc::clone(&self.flush_metrics);
        let iceberg_metrics = Arc::clone(&self.iceberg_metrics);

        let handle = tokio::spawn(async move {
            tracing::info!(
                max_concurrent_uploads = max_concurrent_uploads,
                "Flusher background task started with pipelined S3 uploads"
            );

            loop {
                // Wait for either timeout or explicit flush request
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {},
                    _ = flush_notify.notified() => {},
                }

                if shutdown.load(Ordering::SeqCst) {
                    tracing::info!("Flusher shutdown requested");
                    break;
                }

                // Discover topics from hot storage
                let topics_to_flush = match Self::list_topic_partitions(hot_storage.as_ref()) {
                    Ok(topics) => topics,
                    Err(e) => {
                        tracing::error!("Failed to list topics: {}", e);
                        continue;
                    }
                };

                // Collect unique topics for snapshot commit
                let mut flushed_topics = std::collections::HashSet::new();

                // Use FuturesUnordered for pipelined S3 uploads
                let mut flush_futures: FuturesUnordered<_> = FuturesUnordered::new();

                // Queue up flush futures for all partitions
                for (topic, partition) in topics_to_flush {
                    let current_watermark = {
                        match watermarks.read() {
                            Ok(w) => *w.get(&(topic.clone(), partition)).unwrap_or(&0),
                            Err(e) => {
                                tracing::error!("Failed to read watermarks: {}", e);
                                continue;
                            }
                        }
                    };

                    let hot = Arc::clone(&hot_storage);
                    let cold = Arc::clone(&cold_storage);
                    let topic_clone = topic.clone();

                    // Create the flush future with timing
                    flush_futures.push(async move {
                        let start = Instant::now();
                        let result = Self::flush_partition(
                            &hot,
                            &cold,
                            &topic_clone,
                            partition,
                            current_watermark,
                            max_segment_size,
                            iceberg_enabled,
                            target_file_size_bytes,
                        )
                        .await;
                        let duration_us = start.elapsed().as_micros() as u64;
                        (topic_clone, partition, result, duration_us)
                    });

                    // If we've reached max concurrency, wait for one to complete
                    if flush_futures.len() >= max_concurrent_uploads {
                        if let Some((topic, partition, result, duration_us)) =
                            flush_futures.next().await
                        {
                            match result {
                                Ok((count, new_watermark, bytes)) => {
                                    if count > 0 {
                                        // Record flush metrics
                                        flush_metrics.record_flush(
                                            count as u64,
                                            bytes as u64,
                                            duration_us,
                                        );

                                        // Record Iceberg metrics if enabled
                                        if iceberg_enabled {
                                            iceberg_metrics
                                                .record_parquet_write(&topic, bytes as u64);
                                        }

                                        if let Ok(mut w) = watermarks.write() {
                                            w.insert((topic.clone(), partition), new_watermark);
                                        }
                                        flushed_topics.insert(topic);
                                    }
                                }
                                Err(e) => {
                                    // Record S3 error
                                    iceberg_metrics.record_s3_error();
                                    tracing::error!(
                                        topic = %topic,
                                        partition = partition,
                                        error = %e,
                                        "Failed to flush partition"
                                    );
                                }
                            }
                        }
                    }
                }

                // Drain remaining futures
                while let Some((topic, partition, result, duration_us)) = flush_futures.next().await
                {
                    match result {
                        Ok((count, new_watermark, bytes)) => {
                            if count > 0 {
                                // Record flush metrics
                                flush_metrics.record_flush(count as u64, bytes as u64, duration_us);

                                // Record Iceberg metrics if enabled
                                if iceberg_enabled {
                                    iceberg_metrics.record_parquet_write(&topic, bytes as u64);
                                }

                                if let Ok(mut w) = watermarks.write() {
                                    w.insert((topic.clone(), partition), new_watermark);
                                }
                                flushed_topics.insert(topic);
                            }
                        }
                        Err(e) => {
                            // Record S3 error
                            iceberg_metrics.record_s3_error();
                            tracing::error!(
                                topic = %topic,
                                partition = partition,
                                error = %e,
                                "Failed to flush partition"
                            );
                        }
                    }
                }

                // Check and commit snapshots for topics that were flushed
                if iceberg_enabled {
                    for topic in flushed_topics {
                        let stats = cold_storage.pending_snapshot_stats(&topic);

                        // Check if we've exceeded either threshold
                        let should_commit = stats.file_count >= snapshot_threshold_files
                            || stats.total_bytes >= snapshot_threshold_bytes as u64;

                        if should_commit {
                            tracing::debug!(
                                topic = %topic,
                                pending_files = stats.file_count,
                                pending_bytes = stats.total_bytes,
                                threshold_files = snapshot_threshold_files,
                                threshold_bytes = snapshot_threshold_bytes,
                                "Snapshot threshold exceeded, committing"
                            );

                            match cold_storage.commit_snapshot(&topic).await {
                                Ok(Some(snapshot_id)) => {
                                    // Record snapshot commit metric
                                    iceberg_metrics.record_snapshot_commit(&topic);
                                    tracing::info!(
                                        topic = %topic,
                                        snapshot_id = snapshot_id,
                                        files = stats.file_count,
                                        bytes = stats.total_bytes,
                                        "Committed Iceberg snapshot (batched)"
                                    );
                                }
                                Ok(None) => {
                                    tracing::debug!(
                                        topic = %topic,
                                        "No Iceberg snapshot to commit (no pending files)"
                                    );
                                }
                                Err(e) => {
                                    iceberg_metrics.record_s3_error();
                                    tracing::error!(
                                        topic = %topic,
                                        error = %e,
                                        "Failed to commit Iceberg snapshot"
                                    );
                                }
                            }
                        } else {
                            tracing::debug!(
                                topic = %topic,
                                pending_files = stats.file_count,
                                pending_bytes = stats.total_bytes,
                                threshold_files = snapshot_threshold_files,
                                threshold_bytes = snapshot_threshold_bytes,
                                "Deferring snapshot commit (thresholds not met)"
                            );
                        }
                    }
                }
            }

            tracing::info!("Flusher background task stopped");
        });

        let mut task_handle = self.task_handle.write().map_lock_err()?;
        *task_handle = Some(handle);

        Ok(())
    }

    async fn stop(&self) -> Result<(), StorageError> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.flush_notify.notify_one(); // Wake up the task

        let handle = {
            let mut task_handle = self.task_handle.write().map_lock_err()?;
            task_handle.take()
        };

        if let Some(handle) = handle {
            handle
                .await
                .map_err(|e| StorageError::S3(format!("Task join error: {}", e)))?;
        }

        Ok(())
    }

    async fn flush_now(&self) -> Result<FlushResult, StorageError> {
        let topics_to_flush = Self::list_topic_partitions(self.hot_storage.as_ref())?;

        let mut total_events = 0;
        let mut total_segments = 0;
        let mut max_watermark = 0u64;
        let mut flushed_topics = std::collections::HashSet::new();

        for (topic, partition) in topics_to_flush {
            let current_watermark = self
                .watermarks
                .read()
                .map_lock_err()?
                .get(&(topic.clone(), partition))
                .copied()
                .unwrap_or(0);

            let start = Instant::now();
            let (count, new_watermark, bytes) = Self::flush_partition(
                &self.hot_storage,
                &self.cold_storage,
                &topic,
                partition,
                current_watermark,
                self.config.max_segment_size,
                self.config.iceberg_enabled,
                self.config.target_file_size_bytes,
            )
            .await?;
            let duration_us = start.elapsed().as_micros() as u64;

            if count > 0 {
                // Record flush metrics
                self.flush_metrics
                    .record_flush(count as u64, bytes as u64, duration_us);

                // Record Iceberg metrics if enabled
                if self.config.iceberg_enabled {
                    self.iceberg_metrics
                        .record_parquet_write(&topic, bytes as u64);
                }

                total_events += count;
                total_segments += 1;
                flushed_topics.insert(topic.clone());

                self.watermarks
                    .write()
                    .map_lock_err()?
                    .insert((topic, partition), new_watermark);

                if new_watermark > max_watermark {
                    max_watermark = new_watermark;
                }
            }
        }

        // Force commit all pending snapshots on flush_now (typically used for shutdown)
        if self.config.iceberg_enabled {
            for topic in flushed_topics {
                let stats = self.cold_storage.pending_snapshot_stats(&topic);
                if stats.file_count > 0 {
                    match self.cold_storage.commit_snapshot(&topic).await {
                        Ok(Some(snapshot_id)) => {
                            // Record snapshot commit metric
                            self.iceberg_metrics.record_snapshot_commit(&topic);
                            tracing::info!(
                                topic = %topic,
                                snapshot_id = snapshot_id,
                                files = stats.file_count,
                                bytes = stats.total_bytes,
                                "Force-committed Iceberg snapshot on flush_now"
                            );
                        }
                        Ok(None) => {}
                        Err(e) => {
                            self.iceberg_metrics.record_s3_error();
                            tracing::error!(
                                topic = %topic,
                                error = %e,
                                "Failed to commit Iceberg snapshot on flush_now"
                            );
                        }
                    }
                }
            }
        }

        Ok(FlushResult {
            events_flushed: total_events,
            segments_written: total_segments,
            new_watermark: max_watermark,
        })
    }

    async fn flush_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        let watermarks = self.watermarks.read().map_lock_err()?;
        Ok(*watermarks
            .get(&(topic.to_string(), partition))
            .unwrap_or(&0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{BulkWriteEvent, ColdStorageInfo, SegmentInfo, StoredEvent};
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[test]
    fn test_flusher_config_default() {
        let config = FlusherConfig::default();
        assert_eq!(config.interval, Duration::from_secs(5));
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_segment_size, 10000);
        assert_eq!(config.target_file_size_bytes, 128 * 1024 * 1024);
        assert!(!config.iceberg_enabled);
        assert_eq!(config.snapshot_threshold_files, 10);
        assert_eq!(config.snapshot_threshold_bytes, 1024 * 1024 * 1024); // 1GB
        assert_eq!(config.max_concurrent_s3_uploads, 4);
    }

    #[test]
    fn test_flusher_config_iceberg_defaults() {
        let config = FlusherConfig::iceberg_defaults();
        assert_eq!(config.interval, Duration::from_secs(300)); // 5 minutes
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.max_segment_size, 100000);
        assert_eq!(config.target_file_size_bytes, 128 * 1024 * 1024);
        assert!(config.iceberg_enabled);
        assert_eq!(config.snapshot_threshold_files, 10);
        assert_eq!(config.snapshot_threshold_bytes, 1024 * 1024 * 1024); // 1GB
        assert_eq!(config.max_concurrent_s3_uploads, 4);
    }

    #[derive(Default)]
    struct TestHotStorage {
        events: HashMap<(String, u32), Vec<StoredEvent>>,
    }

    impl TestHotStorage {
        fn insert(&mut self, topic: &str, partition: u32, events: Vec<StoredEvent>) {
            self.events.insert((topic.to_string(), partition), events);
        }
    }

    impl HotStorage for TestHotStorage {
        fn write(
            &self,
            _topic: &str,
            _partition: u32,
            _payload: &[u8],
            _timestamp_ms: i64,
            _idempotency_key: Option<&str>,
        ) -> Result<u64, StorageError> {
            Err(StorageError::S3("not implemented".into()))
        }

        fn write_batch(
            &self,
            _topic: &str,
            _events: &[BulkWriteEvent],
        ) -> Result<Vec<u64>, StorageError> {
            Err(StorageError::S3("not implemented".into()))
        }

        fn read(
            &self,
            topic: &str,
            partition: u32,
            offset: u64,
            limit: usize,
        ) -> Result<Vec<StoredEvent>, StorageError> {
            let key = (topic.to_string(), partition);
            let events = self
                .events
                .get(&key)
                .ok_or_else(|| StorageError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })?;

            Ok(events
                .iter()
                .filter(|event| event.sequence >= offset)
                .take(limit)
                .cloned()
                .collect())
        }

        fn high_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
            let key = (topic.to_string(), partition);
            let events = self
                .events
                .get(&key)
                .ok_or_else(|| StorageError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })?;
            Ok(events.last().map(|event| event.sequence).unwrap_or(0))
        }

        fn low_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
            let key = (topic.to_string(), partition);
            let events = self
                .events
                .get(&key)
                .ok_or_else(|| StorageError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })?;
            Ok(events.first().map(|event| event.sequence).unwrap_or(0))
        }

        fn get_idempotency_offset(
            &self,
            _topic: &str,
            _partition: u32,
            _idempotency_key: &str,
        ) -> Result<Option<u64>, StorageError> {
            Ok(None)
        }

        fn commit_offset(
            &self,
            _group: &str,
            _topic: &str,
            _partition: u32,
            _offset: u64,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        fn get_offset(
            &self,
            _group: &str,
            _topic: &str,
            _partition: u32,
        ) -> Result<Option<u64>, StorageError> {
            Ok(None)
        }

        fn list_partitions(&self, topic: &str) -> Result<Vec<u32>, StorageError> {
            let mut partitions = std::collections::HashSet::new();
            for key in self.events.keys() {
                if key.0 == topic {
                    partitions.insert(key.1);
                }
            }
            let mut result: Vec<u32> = partitions.into_iter().collect();
            result.sort_unstable();
            Ok(result)
        }

        fn list_topics(&self) -> Result<Vec<String>, StorageError> {
            let mut topics = std::collections::HashSet::new();
            for key in self.events.keys() {
                topics.insert(key.0.clone());
            }
            let mut result: Vec<String> = topics.into_iter().collect();
            result.sort();
            Ok(result)
        }

        fn read_all_partitions(
            &self,
            _topic: &str,
            _start_offsets: Option<&std::collections::HashMap<u32, u64>>,
            _start_timestamp_ms: Option<i64>,
            _limit: usize,
        ) -> Result<Vec<StoredEvent>, StorageError> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct TestColdStorage {
        writes: Mutex<Vec<(String, u32, usize)>>,
    }

    impl ColdStorage for TestColdStorage {
        async fn write_segment(
            &self,
            topic: &str,
            partition: u32,
            events: &[StoredEvent],
        ) -> Result<String, StorageError> {
            let mut writes = self
                .writes
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            writes.push((topic.to_string(), partition, events.len()));
            Ok("segment-1".to_string())
        }

        async fn read_events(
            &self,
            _topic: &str,
            _partition: u32,
            _start_offset: u64,
            _limit: usize,
            _since_ms: Option<i64>,
            _until_ms: Option<i64>,
        ) -> Result<Vec<StoredEvent>, StorageError> {
            Ok(Vec::new())
        }

        async fn list_segments(
            &self,
            _topic: &str,
            _partition: u32,
        ) -> Result<Vec<SegmentInfo>, StorageError> {
            Ok(Vec::new())
        }

        fn storage_info(&self) -> ColdStorageInfo {
            ColdStorageInfo {
                storage_type: "test".into(),
                iceberg_enabled: false,
                bucket: String::new(),
                base_path: String::new(),
            }
        }

        async fn commit_snapshot(&self, _topic: &str) -> Result<Option<i64>, StorageError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn flush_now_discovers_topics_and_flushes() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                StoredEvent {
                    sequence: 1,
                    topic: "events".into(),
                    partition: 0,
                    payload: vec![1],
                    timestamp_ms: 0,
                    idempotency_key: None,
                },
                StoredEvent {
                    sequence: 2,
                    topic: "events".into(),
                    partition: 0,
                    payload: vec![2],
                    timestamp_ms: 0,
                    idempotency_key: None,
                },
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        let flush_metrics = Arc::new(FlushMetrics::default());
        let iceberg_metrics = Arc::new(IcebergMetrics::default());
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            FlusherConfig::default(),
            flush_metrics,
            iceberg_metrics,
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 2);
        assert_eq!(result.segments_written, 1);
        assert_eq!(result.new_watermark, 2);

        let writes = cold.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, "events");
        assert_eq!(writes[0].1, 0);
        assert_eq!(writes[0].2, 2);
    }
}
