use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::contracts::{
    ColdStorage, FlushResult, Flusher, HotStorage, LockResultExt, StorageError, StoredEvent,
};
use crate::metrics::{FlushMetrics, IcebergMetrics};
use crate::storage::{derive_partition_columns, CatalogClient};

type DeferredWatermarks = HashMap<String, HashMap<u32, u64>>;

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
    /// Optional catalog client for auto-registration after snapshot commits.
    catalog_client: Option<Arc<CatalogClient>>,
    /// Topics that have been successfully registered with the catalog.
    registered_topics: Arc<RwLock<std::collections::HashSet<String>>>,
    /// Deferred per-topic/partition watermarks (Iceberg mode only), persisted
    /// only after snapshot commit succeeds.
    pending_watermark_persists: Arc<RwLock<DeferredWatermarks>>,
}

impl<H, C> BackgroundFlusher<H, C>
where
    H: HotStorage + 'static,
    C: ColdStorage + 'static,
{
    /// Creates a new background flusher.
    ///
    /// Loads persisted flush watermarks from hot storage so the flusher
    /// resumes from the last committed position after a restart.
    pub fn new(
        hot_storage: Arc<H>,
        cold_storage: Arc<C>,
        config: FlusherConfig,
        flush_metrics: Arc<FlushMetrics>,
        iceberg_metrics: Arc<IcebergMetrics>,
    ) -> Self {
        // Load persisted flush watermarks from hot storage
        let watermarks = {
            let mut wm = HashMap::new();
            match Self::list_topic_partitions(hot_storage.as_ref()) {
                Ok(topics) => {
                    for (topic, partition) in topics {
                        match hot_storage.load_flush_watermark(&topic, partition) {
                            Ok(watermark) if watermark > 0 => {
                                tracing::info!(
                                    topic = %topic,
                                    partition = partition,
                                    watermark = watermark,
                                    "Restored flush watermark from storage"
                                );
                                wm.insert((topic, partition), watermark);
                            }
                            Ok(_) => {} // No persisted watermark (new partition)
                            Err(e) => {
                                tracing::warn!(
                                    topic = %topic,
                                    partition = partition,
                                    error = %e,
                                    "Failed to load flush watermark, starting from 0"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to list topics for watermark restoration, starting fresh"
                    );
                }
            }
            Arc::new(RwLock::new(wm))
        };

        Self {
            hot_storage,
            cold_storage,
            config,
            watermarks,
            shutdown: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
            task_handle: RwLock::new(None),
            topics: Arc::new(RwLock::new(Vec::new())),
            flush_metrics,
            iceberg_metrics,
            catalog_client: None,
            registered_topics: Arc::new(RwLock::new(std::collections::HashSet::new())),
            pending_watermark_persists: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Sets the catalog client for auto-registration after snapshot commits.
    pub fn set_catalog_client(&mut self, client: Arc<CatalogClient>) {
        self.catalog_client = Some(client);
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
    /// Returns (events_flushed, new_watermark, bytes_flushed, segments_written) on success.
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
    ) -> Result<(usize, u64, usize, usize), StorageError> {
        let high_watermark = hot_storage.high_watermark(topic, partition)?;

        if high_watermark <= watermark {
            return Ok((0, watermark, 0, 0)); // Nothing to flush
        }

        // Read events from hot storage
        let all_events = hot_storage.read(topic, partition, watermark + 1, max_segment_size)?;

        if all_events.is_empty() {
            return Ok((0, watermark, 0, 0));
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
            return Ok((0, watermark, 0, 0));
        }

        // Note: size truncation above caps the batch before hour grouping, so per-hour segments may
        // be uneven.
        // Group events by (event_date, event_hour) for correct Iceberg partitioning.
        // BTreeMap iterates in (date, hour) order, ensuring chronological segment writes.
        let mut hour_groups: std::collections::BTreeMap<(i32, i32), Vec<StoredEvent>> =
            std::collections::BTreeMap::new();

        // Note on partial failures: earlier hour groups may be written before a later write fails.
        // For Iceberg, those files remain invisible until a snapshot commit because snapshots
        // explicitly reference data files. For non-Iceberg, segments are keyed by offset range,
        // so retries overwrite the same keys.
        for event in events {
            let key = if iceberg_enabled {
                derive_partition_columns(event.timestamp_ms)
            } else {
                (0, 0) // Non-Iceberg: single group, no splitting
            };
            hour_groups.entry(key).or_default().push(event);
        }

        let mut total_events_count = 0usize;
        let mut total_bytes_flushed = 0usize;
        let mut max_watermark = watermark;
        let num_segments = hour_groups.len();

        for ((_date, _hour), group_events) in &hour_groups {
            let group_count = group_events.len();
            let group_bytes: usize = group_events.iter().map(|e| e.payload.len() + 64).sum();
            // Ordering invariant: hot_storage.read() returns sequence-ordered events, and
            // push() preserves insertion order within each hour group, so last() is max seq.
            let group_max_seq = group_events.last().map(|e| e.sequence).unwrap_or(watermark);

            cold_storage
                .write_segment(topic, partition, group_events)
                .await?;

            total_events_count += group_count;
            total_bytes_flushed += group_bytes;
            if group_max_seq > max_watermark {
                max_watermark = group_max_seq;
            }
        }

        tracing::info!(
            topic = topic,
            partition = partition,
            events = total_events_count,
            bytes = total_bytes_flushed,
            segments = num_segments,
            new_watermark = max_watermark,
            iceberg_enabled = iceberg_enabled,
            "Flushed events to cold storage"
        );

        Ok((
            total_events_count,
            max_watermark,
            total_bytes_flushed,
            num_segments,
        ))
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
        let catalog_client = self.catalog_client.clone();
        let registered_topics = Arc::clone(&self.registered_topics);
        let pending_watermark_persists = Arc::clone(&self.pending_watermark_persists);

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

                // Collect topics that may require snapshot commit checks. We check
                // all discovered topics each cycle so transient commit failures on
                // quiet topics are retried without requiring fresh data.
                let mut topics_to_check = std::collections::HashSet::new();

                // Use FuturesUnordered for pipelined S3 uploads
                let mut flush_futures: FuturesUnordered<_> = FuturesUnordered::new();

                // Queue up flush futures for all partitions
                for (topic, partition) in topics_to_flush {
                    topics_to_check.insert(topic.clone());
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
                        let files_before = cold
                            .pending_snapshot_stats_for_partition(&topic_clone, partition)
                            .file_count;
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
                        (topic_clone, partition, result, duration_us, files_before)
                    });

                    // If we've reached max concurrency, wait for one to complete
                    if flush_futures.len() >= max_concurrent_uploads {
                        if let Some((topic, partition, result, duration_us, files_before)) =
                            flush_futures.next().await
                        {
                            match result {
                                Ok((count, new_watermark, bytes, num_segments)) => {
                                    if count > 0 {
                                        // Record flush metrics
                                        flush_metrics.record_flush(
                                            count as u64,
                                            bytes as u64,
                                            duration_us,
                                        );

                                        // Record Iceberg metrics if enabled
                                        if iceberg_enabled {
                                            iceberg_metrics.record_parquet_write(
                                                &topic,
                                                bytes as u64,
                                                num_segments as u64,
                                            );
                                        }

                                        if let Ok(mut w) = watermarks.write() {
                                            w.insert((topic.clone(), partition), new_watermark);
                                        }

                                        persist_or_defer_watermark(
                                            hot_storage.as_ref(),
                                            &topic,
                                            partition,
                                            new_watermark,
                                            iceberg_enabled,
                                            pending_watermark_persists.as_ref(),
                                            flush_metrics.as_ref(),
                                        );
                                    }
                                }
                                Err(e) => {
                                    // Record S3 error
                                    iceberg_metrics.record_s3_error();
                                    clear_failed_partition_state(
                                        hot_storage.as_ref(),
                                        cold_storage.as_ref(),
                                        &topic,
                                        partition,
                                        files_before,
                                        watermarks.as_ref(),
                                        pending_watermark_persists.as_ref(),
                                        flush_metrics.as_ref(),
                                        iceberg_metrics.as_ref(),
                                    );
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
                while let Some((topic, partition, result, duration_us, files_before)) =
                    flush_futures.next().await
                {
                    match result {
                        Ok((count, new_watermark, bytes, num_segments)) => {
                            if count > 0 {
                                // Record flush metrics
                                flush_metrics.record_flush(count as u64, bytes as u64, duration_us);

                                // Record Iceberg metrics if enabled
                                if iceberg_enabled {
                                    iceberg_metrics.record_parquet_write(
                                        &topic,
                                        bytes as u64,
                                        num_segments as u64,
                                    );
                                }

                                if let Ok(mut w) = watermarks.write() {
                                    w.insert((topic.clone(), partition), new_watermark);
                                }

                                persist_or_defer_watermark(
                                    hot_storage.as_ref(),
                                    &topic,
                                    partition,
                                    new_watermark,
                                    iceberg_enabled,
                                    pending_watermark_persists.as_ref(),
                                    flush_metrics.as_ref(),
                                );
                            }
                        }
                        Err(e) => {
                            // Record S3 error
                            iceberg_metrics.record_s3_error();
                            clear_failed_partition_state(
                                hot_storage.as_ref(),
                                cold_storage.as_ref(),
                                &topic,
                                partition,
                                files_before,
                                watermarks.as_ref(),
                                pending_watermark_persists.as_ref(),
                                flush_metrics.as_ref(),
                                iceberg_metrics.as_ref(),
                            );
                            tracing::error!(
                                topic = %topic,
                                partition = partition,
                                error = %e,
                                "Failed to flush partition"
                            );
                        }
                    }
                }

                // Check and commit snapshots for relevant topics.
                if iceberg_enabled {
                    // Include topics with deferred watermark state so commit retries
                    // are attempted even if no new records were flushed this cycle.
                    if let Ok(pending) = pending_watermark_persists.read() {
                        topics_to_check.extend(pending.keys().cloned());
                    } else {
                        tracing::error!("Failed to read deferred watermark state");
                    }

                    for topic in topics_to_check {
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

                                    // Catalog auto-registration (non-fatal)
                                    if let Some(ref catalog) = catalog_client {
                                        if let Some(metadata_json) =
                                            cold_storage.table_metadata_json(&topic)
                                        {
                                            match serde_json::from_str::<
                                                crate::storage::TableMetadata,
                                            >(
                                                &metadata_json
                                            ) {
                                                Ok(metadata) => {
                                                    let is_new = registered_topics
                                                        .read()
                                                        .map(|r| !r.contains(&topic))
                                                        .unwrap_or(true);
                                                    let result = if is_new {
                                                        catalog
                                                            .register_table(&topic, &metadata)
                                                            .await
                                                    } else {
                                                        catalog
                                                            .update_table(&topic, &metadata)
                                                            .await
                                                    };
                                                    match result {
                                                        Ok(()) => {
                                                            if let Ok(mut r) =
                                                                registered_topics.write()
                                                            {
                                                                r.insert(topic.clone());
                                                            }
                                                        }
                                                        Err(e) => {
                                                            tracing::warn!(
                                                                error = %e,
                                                                topic = %topic,
                                                                "Catalog update failed (non-fatal)"
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        error = %e,
                                                        topic = %topic,
                                                        "Failed to parse table metadata for catalog"
                                                    );
                                                }
                                            }
                                        }
                                    }
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
                                    // Don't persist watermarks if snapshot commit
                                    // failed — the data isn't durable yet.
                                    continue;
                                }
                            }

                            // Persist deferred watermarks after successful snapshot commit.
                            flush_pending_watermarks(
                                hot_storage.as_ref(),
                                &topic,
                                pending_watermark_persists.as_ref(),
                                flush_metrics.as_ref(),
                            );
                        } else if stats.file_count == 0 {
                            // No pending files and no commit needed: the data was
                            // already committed in a prior cycle but the watermark
                            // persist failed and was re-queued. Safe to persist now.
                            flush_pending_watermarks(
                                hot_storage.as_ref(),
                                &topic,
                                pending_watermark_persists.as_ref(),
                                flush_metrics.as_ref(),
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
        let mut topics_to_commit = std::collections::HashSet::new();
        let mut first_error: Option<StorageError> = None;

        for (topic, partition) in topics_to_flush {
            // Always add to topics_to_commit so pending snapshots from prior
            // cycles get committed even if this partition has no new data.
            topics_to_commit.insert(topic.clone());
            let current_watermark = self
                .watermarks
                .read()
                .map_lock_err()?
                .get(&(topic.clone(), partition))
                .copied()
                .unwrap_or(0);

            let files_before = self
                .cold_storage
                .pending_snapshot_stats_for_partition(&topic, partition)
                .file_count;
            let start = Instant::now();
            let result = Self::flush_partition(
                &self.hot_storage,
                &self.cold_storage,
                &topic,
                partition,
                current_watermark,
                self.config.max_segment_size,
                self.config.iceberg_enabled,
                self.config.target_file_size_bytes,
            )
            .await;
            let duration_us = start.elapsed().as_micros() as u64;

            match result {
                Ok((count, new_watermark, bytes, num_segments)) => {
                    if count > 0 {
                        // Record flush metrics
                        self.flush_metrics
                            .record_flush(count as u64, bytes as u64, duration_us);

                        // Record Iceberg metrics if enabled
                        if self.config.iceberg_enabled {
                            self.iceberg_metrics.record_parquet_write(
                                &topic,
                                bytes as u64,
                                num_segments as u64,
                            );
                        }

                        total_events += count;
                        total_segments += num_segments;

                        self.watermarks
                            .write()
                            .map_lock_err()?
                            .insert((topic.clone(), partition), new_watermark);

                        persist_or_defer_watermark(
                            self.hot_storage.as_ref(),
                            &topic,
                            partition,
                            new_watermark,
                            self.config.iceberg_enabled,
                            self.pending_watermark_persists.as_ref(),
                            self.flush_metrics.as_ref(),
                        );

                        if new_watermark > max_watermark {
                            max_watermark = new_watermark;
                        }
                    }
                }
                Err(e) => {
                    self.iceberg_metrics.record_s3_error();
                    clear_failed_partition_state(
                        self.hot_storage.as_ref(),
                        self.cold_storage.as_ref(),
                        &topic,
                        partition,
                        files_before,
                        self.watermarks.as_ref(),
                        self.pending_watermark_persists.as_ref(),
                        self.flush_metrics.as_ref(),
                        self.iceberg_metrics.as_ref(),
                    );
                    tracing::error!(
                        topic = %topic,
                        partition = partition,
                        error = %e,
                        "Failed to flush partition in flush_now"
                    );
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // Force commit all pending snapshots on flush_now (typically used for shutdown)
        if self.config.iceberg_enabled {
            if let Ok(pending) = self.pending_watermark_persists.read() {
                topics_to_commit.extend(pending.keys().cloned());
            } else {
                tracing::error!("Failed to read deferred watermark state before flush_now commit");
            }

            let mut commit_error: Option<StorageError> = None;
            for topic in topics_to_commit {
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

                            // Persist flush watermarks after successful snapshot
                            flush_pending_watermarks(
                                self.hot_storage.as_ref(),
                                &topic,
                                self.pending_watermark_persists.as_ref(),
                                self.flush_metrics.as_ref(),
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
                            if commit_error.is_none() {
                                commit_error = Some(e);
                            }
                        }
                    }
                } else {
                    // No pending files: the data was already committed in a
                    // prior cycle but watermark persist failed. Safe to retry.
                    flush_pending_watermarks(
                        self.hot_storage.as_ref(),
                        &topic,
                        self.pending_watermark_persists.as_ref(),
                        self.flush_metrics.as_ref(),
                    );
                }
            }
            if let Some(err) = commit_error {
                if let Some(ref flush_err) = first_error {
                    tracing::error!(
                        flush_error = %flush_err,
                        commit_error = %err,
                        "flush_now encountered both flush and snapshot commit errors; returning commit error"
                    );
                }
                return Err(err);
            }
        }

        // Return the first flush-partition error if any occurred
        if let Some(err) = first_error {
            return Err(err);
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

fn persist_or_defer_watermark<H: HotStorage>(
    hot_storage: &H,
    topic: &str,
    partition: u32,
    watermark: u64,
    iceberg_enabled: bool,
    pending: &RwLock<DeferredWatermarks>,
    flush_metrics: &FlushMetrics,
) {
    if iceberg_enabled {
        if let Ok(mut deferred) = pending.write() {
            deferred
                .entry(topic.to_string())
                .or_default()
                .insert(partition, watermark);
        } else {
            flush_metrics.record_watermark_persist_error();
            tracing::error!(
                topic = %topic,
                partition = partition,
                "Failed to defer flush watermark due to lock error"
            );
        }
    } else if let Err(e) = hot_storage.save_flush_watermark(topic, partition, watermark) {
        flush_metrics.record_watermark_persist_error();
        tracing::error!(
            topic = %topic,
            partition = partition,
            error = %e,
            "Failed to persist flush watermark"
        );
    }
}

fn flush_pending_watermarks<H: HotStorage>(
    hot_storage: &H,
    topic: &str,
    pending: &RwLock<DeferredWatermarks>,
    flush_metrics: &FlushMetrics,
) {
    let partitions = match pending.write() {
        Ok(mut deferred) => deferred.remove(topic),
        Err(e) => {
            flush_metrics.record_watermark_persist_error();
            tracing::error!(
                topic = %topic,
                error = %e,
                "Failed to drain deferred flush watermarks due to lock error"
            );
            None
        }
    };

    if let Some(partitions) = partitions {
        let mut failed = HashMap::new();
        for (partition, wm) in partitions {
            match hot_storage.save_flush_watermark(topic, partition, wm) {
                Ok(()) => {}
                Err(first_err) => {
                    tracing::warn!(
                        topic = %topic,
                        partition = partition,
                        error = %first_err,
                        "Failed to persist flush watermark, retrying once"
                    );
                    // Retry once
                    if let Err(e) = hot_storage.save_flush_watermark(topic, partition, wm) {
                        flush_metrics.record_watermark_persist_error();
                        tracing::error!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Failed to persist flush watermark after retry, re-queuing"
                        );
                        // Re-queue so the next cycle retries
                        failed.insert(partition, wm);
                    }
                }
            }
        }
        if !failed.is_empty() {
            if let Ok(mut deferred) = pending.write() {
                deferred
                    .entry(topic.to_string())
                    .or_default()
                    .extend(failed);
            } else {
                flush_metrics.record_watermark_persist_error();
                tracing::error!(
                    topic = %topic,
                    "Failed to re-queue deferred flush watermarks due to lock error"
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn clear_failed_partition_state<H: HotStorage, C: ColdStorage>(
    hot_storage: &H,
    cold_storage: &C,
    topic: &str,
    partition: u32,
    files_before: usize,
    watermarks: &RwLock<HashMap<(String, u32), u64>>,
    pending_watermarks: &RwLock<DeferredWatermarks>,
    flush_metrics: &FlushMetrics,
    iceberg_metrics: &IcebergMetrics,
) {
    // Load durable watermark for rollback decision.
    let durable_watermark = match hot_storage.load_flush_watermark(topic, partition) {
        Ok(wm) => wm,
        Err(e) => {
            flush_metrics.record_watermark_persist_error();
            tracing::error!(
                topic = %topic,
                partition = partition,
                error = %e,
                "Failed to load durable watermark during failure recovery, resetting to 0"
            );
            0
        }
    };

    let in_memory_watermark = watermarks
        .read()
        .ok()
        .and_then(|w| w.get(&(topic.to_string(), partition)).copied())
        .unwrap_or(0);

    let files_after = cold_storage
        .pending_snapshot_stats_for_partition(topic, partition)
        .file_count;
    let new_files_written = files_after > files_before;
    let watermark_advanced = in_memory_watermark > durable_watermark;

    // Clear pending files if:
    // - New segments were written this cycle (partial write), OR
    // - The in-memory watermark advanced past durable, meaning prior pending
    //   files will be re-flushed after rollback and would otherwise duplicate.
    //
    // Skip clearing if neither condition is true (pre-write failure with no
    // watermark advancement), which preserves files from prior successful cycles.
    if new_files_written || watermark_advanced {
        cold_storage.clear_pending_data_files(topic, partition);
    }

    // Keep gauges in sync with cold-storage truth after cleanup.
    let pending_stats = cold_storage.pending_snapshot_stats(topic);
    iceberg_metrics.update_pending_stats(
        topic,
        pending_stats.file_count as u64,
        pending_stats.total_bytes,
    );

    // Roll back in-memory watermark to last durable value so dropped pending files are re-flushed.
    match watermarks.write() {
        Ok(mut in_memory) => {
            let key = (topic.to_string(), partition);
            let previous = in_memory.get(&key).copied().unwrap_or(0);
            if durable_watermark == 0 {
                in_memory.remove(&key);
            } else {
                in_memory.insert(key, durable_watermark);
            }
            if previous != durable_watermark {
                tracing::warn!(
                    topic = %topic,
                    partition = partition,
                    previous_watermark = previous,
                    rolled_back_to = durable_watermark,
                    "Rolled back in-memory flush watermark after partition failure"
                );
            }
        }
        Err(e) => {
            flush_metrics.record_watermark_persist_error();
            tracing::error!(
                topic = %topic,
                partition = partition,
                error = %e,
                "Failed to roll back in-memory watermark due to lock error"
            );
        }
    }

    if let Ok(mut deferred) = pending_watermarks.write() {
        let remove_topic = if let Some(partitions) = deferred.get_mut(topic) {
            partitions.remove(&partition);
            partitions.is_empty()
        } else {
            false
        };
        if remove_topic {
            deferred.remove(topic);
        }
    } else {
        flush_metrics.record_watermark_persist_error();
        tracing::error!(
            topic = %topic,
            partition = partition,
            "Failed to clear deferred watermark state for failed partition due to lock error"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        BulkWriteEvent, ColdStorageInfo, PendingSnapshotStats, SegmentInfo, StoredEvent,
    };
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
        flush_watermarks: Mutex<HashMap<(String, u32), u64>>,
        save_failures_remaining: Mutex<u32>,
        /// When > 0, high_watermark() will fail and decrement.
        high_watermark_failures: Mutex<u32>,
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
            if let Ok(mut remaining) = self.high_watermark_failures.lock() {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(StorageError::S3("injected high_watermark failure".into()));
                }
            }
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

        fn save_flush_watermark(
            &self,
            topic: &str,
            partition: u32,
            watermark: u64,
        ) -> Result<(), StorageError> {
            {
                let mut remaining = self
                    .save_failures_remaining
                    .lock()
                    .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(StorageError::S3("injected watermark failure".into()));
                }
            }
            let mut wm = self
                .flush_watermarks
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            wm.insert((topic.to_string(), partition), watermark);
            Ok(())
        }

        fn load_flush_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
            let wm = self
                .flush_watermarks
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            Ok(wm
                .get(&(topic.to_string(), partition))
                .copied()
                .unwrap_or(0))
        }
    }

    #[derive(Default)]
    struct TestColdStorage {
        writes: Mutex<Vec<(String, u32, Vec<StoredEvent>)>>,
        /// If set, the Nth write_segment call (0-indexed) will fail.
        fail_on_call: Mutex<Option<usize>>,
        /// If set, the Nth commit_snapshot call (0-indexed) will fail.
        fail_commit_on_call: Mutex<Option<usize>>,
        commit_attempts: Mutex<usize>,
        pending_stats: Mutex<HashMap<(String, u32), PendingSnapshotStats>>,
        commit_counter: Mutex<i64>,
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
            let call_index = writes.len();
            let fail_on = self
                .fail_on_call
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            if *fail_on == Some(call_index) {
                return Err(StorageError::S3("injected failure".into()));
            }
            writes.push((topic.to_string(), partition, events.to_vec()));
            let mut pending = self
                .pending_stats
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            let stats = pending.entry((topic.to_string(), partition)).or_default();
            stats.file_count += 1;
            stats.total_bytes += events.iter().map(|e| e.payload.len() as u64).sum::<u64>();
            Ok(format!("segment-{}", call_index))
        }

        async fn read_events(
            &self,
            _topic: &str,
            _partition: u32,
            _start_offset: u64,
            _limit: usize,
            _since_ms: Option<i64>,
            _until_ms: Option<i64>,
            _projection: &crate::contracts::ColumnProjection,
        ) -> Result<Vec<StoredEvent>, StorageError> {
            Ok(Vec::new())
        }

        fn pending_snapshot_stats(&self, topic: &str) -> PendingSnapshotStats {
            let pending = match self.pending_stats.lock() {
                Ok(p) => p,
                Err(_) => return PendingSnapshotStats::default(),
            };

            let mut stats = PendingSnapshotStats::default();
            for ((pending_topic, _partition), per_partition_stats) in pending.iter() {
                if pending_topic == topic {
                    stats.file_count += per_partition_stats.file_count;
                    stats.total_bytes += per_partition_stats.total_bytes;
                }
            }
            stats
        }

        fn pending_snapshot_stats_for_partition(
            &self,
            topic: &str,
            partition: u32,
        ) -> PendingSnapshotStats {
            let pending = match self.pending_stats.lock() {
                Ok(p) => p,
                Err(_) => return PendingSnapshotStats::default(),
            };
            pending
                .get(&(topic.to_string(), partition))
                .cloned()
                .unwrap_or_default()
        }

        async fn commit_snapshot(&self, topic: &str) -> Result<Option<i64>, StorageError> {
            let commit_call_index = {
                let mut attempts = self
                    .commit_attempts
                    .lock()
                    .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
                let call_index = *attempts;
                *attempts += 1;
                call_index
            };
            let fail_on_commit = *self
                .fail_commit_on_call
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            if fail_on_commit == Some(commit_call_index) {
                return Err(StorageError::S3("injected commit failure".into()));
            }

            let mut pending = self
                .pending_stats
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            let mut had_pending = false;
            for ((pending_topic, _partition), stats) in pending.iter_mut() {
                if pending_topic == topic && stats.file_count > 0 {
                    had_pending = true;
                    stats.file_count = 0;
                    stats.total_bytes = 0;
                }
            }
            if had_pending {
                let mut counter = self
                    .commit_counter
                    .lock()
                    .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
                *counter += 1;
                return Ok(Some(*counter));
            }
            Ok(None)
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

        fn clear_pending_data_files(&self, topic: &str, partition: u32) {
            if let Ok(mut pending) = self.pending_stats.lock() {
                pending.remove(&(topic.to_string(), partition));
            }
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
        assert_eq!(writes[0].2.len(), 2);
    }

    // --- Hour-boundary splitting tests (Issue #98) ---

    // 2024-01-15 00:00:00 UTC
    const TS_HOUR_0_START: i64 = 1705276800000;
    // 2024-01-15 00:30:00 UTC
    const TS_HOUR_0_MID: i64 = 1705278600000;
    // 2024-01-15 00:59:59.999 UTC
    const TS_HOUR_0_END: i64 = 1705280399999;
    // 2024-01-15 01:00:00 UTC
    const TS_HOUR_1_START: i64 = 1705280400000;
    // 2024-01-15 01:30:00 UTC
    const TS_HOUR_1_MID: i64 = 1705282200000;
    // 2024-01-15 02:00:00 UTC
    const TS_HOUR_2_START: i64 = 1705284000000;

    fn make_event(seq: u64, ts: i64) -> StoredEvent {
        StoredEvent {
            sequence: seq,
            topic: "events".into(),
            partition: 0,
            payload: vec![seq as u8],
            timestamp_ms: ts,
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn flush_splits_events_at_hour_boundary() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_0_MID),
                make_event(3, TS_HOUR_1_START),
                make_event(4, TS_HOUR_1_MID),
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 4);
        assert_eq!(result.new_watermark, 4);

        let writes = cold.writes.lock().unwrap();
        assert_eq!(writes.len(), 2, "Expected 2 segments (one per hour)");
        assert_eq!(writes[0].2.len(), 2, "Hour 0 should have 2 events");
        assert_eq!(writes[1].2.len(), 2, "Hour 1 should have 2 events");
    }

    #[tokio::test]
    async fn flush_single_hour_no_split() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_0_MID),
                make_event(3, TS_HOUR_0_END),
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 3);

        let writes = cold.writes.lock().unwrap();
        assert_eq!(writes.len(), 1, "All events in same hour, no split");
        assert_eq!(writes[0].2.len(), 3);
    }

    #[tokio::test]
    async fn flush_exact_boundary_goes_to_new_hour() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_END),   // 00:59:59.999 → hour 0
                make_event(2, TS_HOUR_1_START), // 01:00:00.000 → hour 1
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 2);

        let writes = cold.writes.lock().unwrap();
        assert_eq!(writes.len(), 2, "Exact boundary splits into 2 segments");
        assert_eq!(writes[0].2[0].timestamp_ms, TS_HOUR_0_END);
        assert_eq!(writes[1].2[0].timestamp_ms, TS_HOUR_1_START);
    }

    #[tokio::test]
    async fn flush_preserves_sequence_order_within_hour() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_0_MID),
                make_event(3, TS_HOUR_0_END),
                make_event(4, TS_HOUR_1_START),
                make_event(5, TS_HOUR_1_MID),
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        flusher.flush_now().await.unwrap();

        let writes = cold.writes.lock().unwrap();
        // Verify sequence order within each hour group
        for write in writes.iter() {
            for window in write.2.windows(2) {
                assert!(
                    window[0].sequence < window[1].sequence,
                    "Sequences must be strictly ascending within an hour group"
                );
            }
        }
    }

    #[tokio::test]
    async fn flush_hour_split_fails_fast_on_segment_error() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_1_START),
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        *cold.fail_on_call.lock().unwrap() = Some(1); // Fail on second write
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await;
        assert!(result.is_err(), "Should fail when a segment write fails");

        let writes = cold.writes.lock().unwrap();
        assert_eq!(
            writes.len(),
            1,
            "Only the first segment should have been written"
        );
    }

    /// Verifies that a partial flush failure followed by a retry does not
    /// produce duplicate data files in cold storage. This is the regression
    /// test for the [HIGH] finding: without `clear_pending_data_files`,
    /// the hour-0 segment from the first (failed) attempt would accumulate
    /// alongside the hour-0 segment from the second (successful) attempt,
    /// causing duplicate rows after snapshot commit.
    #[tokio::test]
    async fn flush_retry_after_partial_failure_no_duplicates() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_1_START),
            ],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        // Fail on the 2nd write_segment (hour-1), so hour-0 succeeds
        *cold.fail_on_call.lock().unwrap() = Some(1);
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        // First attempt: hour-0 write succeeds, hour-1 fails.
        // flush_now should return an error and clear pending data files.
        let result = flusher.flush_now().await;
        assert!(result.is_err(), "Should fail when a segment write fails");

        // Pending stats should be cleared by clear_pending_data_files
        let pending = cold.pending_snapshot_stats("events");
        assert_eq!(
            pending.file_count, 0,
            "Pending files should be cleared after partial failure"
        );

        // Watermark should NOT have advanced (no successful flush)
        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            0,
            "Watermark must not advance after failed flush"
        );

        // Clear the failure injection for the retry
        *cold.fail_on_call.lock().unwrap() = None;

        // Second attempt: both hour-0 and hour-1 should succeed cleanly
        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 2);

        let writes = cold.writes.lock().unwrap();
        // Total writes: 1 (hour-0 from first attempt) + 2 (hour-0 + hour-1 from retry)
        // = 3 total write_segment calls. But only 2 of them (the retry pair) should
        // be in pending_data_files when the snapshot commits, because the first
        // attempt's hour-0 was cleared.
        assert_eq!(
            writes.len(),
            3,
            "3 total write_segment calls: 1 orphaned + 2 from successful retry"
        );

        // The pending stats after successful flush+commit should be zero
        // (snapshot was committed in flush_now for iceberg mode)
        let pending = cold.pending_snapshot_stats("events");
        assert_eq!(
            pending.file_count, 0,
            "All pending files should be committed after successful retry"
        );
    }

    #[tokio::test]
    async fn flush_multi_partition_partial_failure_preserves_successful_partition() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![StoredEvent {
                sequence: 1,
                topic: "events".into(),
                partition: 0,
                payload: vec![1],
                timestamp_ms: TS_HOUR_0_START,
                idempotency_key: None,
            }],
        );
        hot.insert(
            "events",
            1,
            vec![StoredEvent {
                sequence: 1,
                topic: "events".into(),
                partition: 1,
                payload: vec![2],
                timestamp_ms: TS_HOUR_0_START,
                idempotency_key: None,
            }],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        // Partition 0 write succeeds first, then partition 1 write fails.
        *cold.fail_on_call.lock().unwrap() = Some(1);
        let config = FlusherConfig {
            iceberg_enabled: true,
            snapshot_threshold_files: 1,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await;
        assert!(
            result.is_err(),
            "flush_now should surface the failed partition"
        );

        // Successful partition should still be committed and watermark persisted.
        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            1,
            "Partition 0 watermark should persist after its successful commit"
        );
        // Failed partition should not be persisted.
        assert_eq!(
            hot.load_flush_watermark("events", 1).unwrap(),
            0,
            "Partition 1 watermark must remain unpersisted after failure"
        );

        // flush_now force-commits surviving pending files, so no pending files remain.
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 0);

        *cold.fail_on_call.lock().unwrap() = None;
        let retry = flusher.flush_now().await.unwrap();
        assert_eq!(
            retry.events_flushed, 1,
            "Retry should flush only the previously failed partition"
        );
        assert_eq!(hot.load_flush_watermark("events", 1).unwrap(), 1);
    }

    #[tokio::test]
    async fn flush_non_iceberg_does_not_split() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_1_START),
                make_event(3, TS_HOUR_2_START),
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig::default(); // iceberg_enabled = false
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 3);

        let writes = cold.writes.lock().unwrap();
        assert_eq!(writes.len(), 1, "Non-Iceberg mode should not split by hour");
        assert_eq!(writes[0].2.len(), 3);
    }

    #[tokio::test]
    async fn flush_three_hours_produces_three_segments() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_1_START),
                make_event(3, TS_HOUR_2_START),
            ],
        );

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::new(hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 3);

        let writes = cold.writes.lock().unwrap();
        assert_eq!(writes.len(), 3, "3 hours should produce 3 segments");
        for write in writes.iter() {
            assert_eq!(write.2.len(), 1, "Each hour has 1 event");
        }
    }

    // --- Watermark persistence tests (Issue #99) ---

    #[tokio::test]
    async fn flush_watermark_persisted_in_non_iceberg_mode() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![make_event(1, TS_HOUR_0_START), make_event(2, TS_HOUR_0_MID)],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig::default(); // iceberg_enabled = false
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 2);
        assert_eq!(result.new_watermark, 2);

        // Watermark should be persisted in hot storage
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 2);
    }

    #[tokio::test]
    async fn flush_watermark_restored_on_new_flusher() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_0_MID),
                make_event(3, TS_HOUR_0_END),
            ],
        );
        // Simulate a previously persisted watermark at sequence 1
        hot.save_flush_watermark("events", 0, 1).unwrap();

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig::default();
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        // Flusher should resume from watermark 1, only flushing events 2 and 3
        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 2);
        assert_eq!(result.new_watermark, 3);

        let writes = cold.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].2.len(),
            2,
            "Should only flush events after watermark"
        );
        assert_eq!(writes[0].2[0].sequence, 2);
        assert_eq!(writes[0].2[1].sequence, 3);
    }

    #[tokio::test]
    async fn flush_watermark_not_advanced_on_error() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![make_event(1, TS_HOUR_0_START), make_event(2, TS_HOUR_0_MID)],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        *cold.fail_on_call.lock().unwrap() = Some(0); // Fail on first write
        let config = FlusherConfig::default();
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await;
        assert!(result.is_err());

        // Watermark should NOT be persisted (remained at 0)
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn flush_deferred_watermarks_survive_across_cycles() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![make_event(1, TS_HOUR_0_START), make_event(2, TS_HOUR_0_MID)],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            interval: Duration::from_millis(10),
            max_segment_size: 1,
            snapshot_threshold_files: 2,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        flusher.start().await.unwrap();

        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            0,
            "Watermark should remain deferred until snapshot commit"
        );

        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if hot.load_flush_watermark("events", 0).unwrap() == 2 {
                break;
            }
        }

        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            2,
            "Watermark should persist after snapshot commit"
        );

        flusher.stop().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stop_then_flush_now_persists_deferred_watermarks() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![make_event(1, TS_HOUR_0_START), make_event(2, TS_HOUR_0_MID)],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            interval: Duration::from_millis(10),
            snapshot_threshold_files: 2,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        flusher.start().await.unwrap();

        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if cold.pending_snapshot_stats("events").file_count == 1 {
                break;
            }
        }

        // Before snapshot commit, watermark remains deferred.
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 0);
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 1);

        // Simulate production shutdown order: stop background loop, then flush_now.
        flusher.stop().await.unwrap();
        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 0);
        assert_eq!(result.segments_written, 0);

        // Deferred watermark from the background cycle must still persist.
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 2);
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn background_retries_snapshot_commit_on_quiet_topic() {
        let mut hot = TestHotStorage::default();
        hot.insert("events", 0, vec![make_event(1, TS_HOUR_0_START)]);

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        *cold.fail_commit_on_call.lock().unwrap() = Some(0); // First commit attempt fails

        let config = FlusherConfig {
            iceberg_enabled: true,
            interval: Duration::from_millis(10),
            snapshot_threshold_files: 1, // Commit every cycle once there are pending files
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        flusher.start().await.unwrap();

        // First cycle flushes data and fails commit.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if *cold.commit_attempts.lock().unwrap() >= 1 {
                break;
            }
        }
        assert_eq!(*cold.commit_attempts.lock().unwrap(), 1);
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 0);
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 1);

        // No new data arrives. Commit should still retry and eventually succeed.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if hot.load_flush_watermark("events", 0).unwrap() == 1 {
                break;
            }
        }

        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 1);
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 0);
        assert!(*cold.commit_attempts.lock().unwrap() >= 2);
        assert_eq!(cold.writes.lock().unwrap().len(), 1); // No re-flush on quiet topic

        flusher.stop().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn background_failure_rolls_back_in_memory_watermark_to_durable() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_1_START),
            ],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        // First write succeeds; second write attempt fails, triggering cleanup.
        *cold.fail_on_call.lock().unwrap() = Some(1);

        let config = FlusherConfig {
            iceberg_enabled: true,
            interval: Duration::from_millis(10),
            max_segment_size: 1,
            snapshot_threshold_files: 2,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        flusher.start().await.unwrap();

        // Cycle 1: flush sequence 1, watermark advances in-memory, pending files = 1.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if cold.writes.lock().unwrap().len() == 1 {
                break;
            }
        }
        assert_eq!(cold.writes.lock().unwrap().len(), 1);
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 1);

        // Cycle 2: write fails and cleanup clears pending files. In-memory watermark
        // must roll back to the durable watermark (still 0 because no snapshot committed yet).
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if cold.pending_snapshot_stats("events").file_count == 0 {
                break;
            }
        }
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 0);
        assert_eq!(
            flusher.flush_watermark("events", 0).await.unwrap(),
            0,
            "In-memory watermark must roll back after cleanup"
        );

        // Remove failure injection and allow replay from durable watermark.
        *cold.fail_on_call.lock().unwrap() = None;
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if hot.load_flush_watermark("events", 0).unwrap() == 2 {
                break;
            }
        }

        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            2,
            "Both events should be eventually persisted after replay"
        );
        let first_sequences: Vec<u64> = {
            let writes = cold.writes.lock().unwrap();
            writes
                .iter()
                .filter_map(|(_, _, events)| events.first().map(|e| e.sequence))
                .collect()
        };
        assert_eq!(
            first_sequences,
            vec![1, 1, 2],
            "Sequence 1 should be replayed after cleanup"
        );

        flusher.stop().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn background_failure_cleanup_resyncs_pending_metrics() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_1_START),
            ],
        );

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        *cold.fail_on_call.lock().unwrap() = Some(1);

        let flush_metrics = Arc::new(FlushMetrics::default());
        let iceberg_metrics = Arc::new(IcebergMetrics::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            interval: Duration::from_millis(10),
            max_segment_size: 1,
            snapshot_threshold_files: 10,
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            flush_metrics,
            Arc::clone(&iceberg_metrics),
        );

        flusher.start().await.unwrap();

        // First cycle writes one pending file and increments pending metrics.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if cold.writes.lock().unwrap().len() == 1 {
                break;
            }
        }
        assert_eq!(
            *iceberg_metrics
                .pending_snapshot_files
                .get("events")
                .unwrap(),
            1
        );

        // Next cycle fails and clears pending files; metrics must resync to 0.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if iceberg_metrics.s3_errors_total.load(Ordering::Relaxed) >= 1 {
                break;
            }
        }
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 0);
        assert_eq!(
            *iceberg_metrics
                .pending_snapshot_files
                .get("events")
                .unwrap(),
            0,
            "Pending file gauge should reflect cleanup state"
        );
        assert_eq!(
            *iceberg_metrics
                .pending_snapshot_bytes
                .get("events")
                .unwrap(),
            0
        );

        flusher.stop().await.unwrap();
    }

    #[tokio::test]
    async fn flush_watermark_persist_failure_retries_once() {
        let mut hot = TestHotStorage::default();
        hot.insert("events", 0, vec![make_event(1, TS_HOUR_0_START)]);

        let hot = Arc::new(hot);
        *hot.save_failures_remaining.lock().unwrap() = 1;

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            ..Default::default()
        };
        let flush_metrics = Arc::new(FlushMetrics::default());
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::clone(&flush_metrics),
            Arc::new(IcebergMetrics::default()),
        );

        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 1);
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 1);
        assert_eq!(
            flush_metrics
                .watermark_persist_errors_total
                .load(Ordering::Relaxed),
            0
        );
    }

    /// P1 regression: deferred watermarks must be persisted even when no
    /// pending data files exist (quiet topic after prior persist failure).
    #[tokio::test]
    async fn flush_deferred_watermark_persisted_without_pending_files() {
        let mut hot = TestHotStorage::default();
        hot.insert("events", 0, vec![make_event(1, TS_HOUR_0_START)]);

        let hot = Arc::new(hot);
        // Fail the first two save_flush_watermark calls (initial attempt + retry)
        // so the watermark gets re-queued to the deferred map.
        *hot.save_failures_remaining.lock().unwrap() = 2;

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            snapshot_threshold_files: 1, // commit snapshot immediately
            ..Default::default()
        };
        let flush_metrics = Arc::new(FlushMetrics::default());
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::clone(&flush_metrics),
            Arc::new(IcebergMetrics::default()),
        );

        // First flush: writes data, commits snapshot, but watermark persist
        // fails twice → re-queued to deferred map.
        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 1);
        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            0,
            "Watermark should NOT be persisted after both attempts failed"
        );
        assert_eq!(
            flush_metrics
                .watermark_persist_errors_total
                .load(Ordering::Relaxed),
            1,
            "One error recorded (retry also failed)"
        );

        // Second flush: no new data to flush, but the deferred watermark
        // should now be persisted (save_failures exhausted).
        let result = flusher.flush_now().await.unwrap();
        assert_eq!(result.events_flushed, 0);
        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            1,
            "Deferred watermark must be persisted on quiet topic"
        );
    }

    /// P2 regression: a pre-write failure (e.g., high_watermark read error)
    /// must NOT call clear_pending_data_files.
    #[tokio::test]
    async fn pre_write_failure_preserves_prior_pending_files() {
        let mut hot = TestHotStorage::default();
        hot.insert(
            "events",
            0,
            vec![make_event(1, TS_HOUR_0_START), make_event(2, TS_HOUR_0_MID)],
        );

        let cold = TestColdStorage::default();

        // Simulate 3 pending files from a prior successful cycle
        {
            let mut pending = cold.pending_stats.lock().unwrap();
            pending.insert(
                ("events".to_string(), 0),
                PendingSnapshotStats {
                    file_count: 3,
                    total_bytes: 1024,
                },
            );
        }

        // Inject a high_watermark failure so flush_partition fails before
        // any cold storage writes.
        *hot.high_watermark_failures.lock().unwrap() = 1;

        let files_before = cold
            .pending_snapshot_stats_for_partition("events", 0)
            .file_count;
        assert_eq!(files_before, 3);

        // Call flush_partition directly — it will fail at high_watermark()
        let result = BackgroundFlusher::<TestHotStorage, TestColdStorage>::flush_partition(
            &hot,
            &cold,
            "events",
            0,
            0,     // watermark
            10000, // max_segment_size
            true,  // iceberg_enabled
            128 * 1024 * 1024,
        )
        .await;
        assert!(
            result.is_err(),
            "Should fail on injected high_watermark error"
        );

        let files_after = cold
            .pending_snapshot_stats_for_partition("events", 0)
            .file_count;
        assert_eq!(
            files_before, files_after,
            "No new files should have been added"
        );

        let watermarks = RwLock::new(HashMap::new());
        let deferred = RwLock::new(HashMap::new());
        let flush_metrics = FlushMetrics::default();
        let iceberg_metrics = IcebergMetrics::default();
        clear_failed_partition_state(
            &hot,
            &cold,
            "events",
            0,
            files_before,
            &watermarks,
            &deferred,
            &flush_metrics,
            &iceberg_metrics,
        );

        // Verify pending files are preserved
        assert_eq!(
            cold.pending_snapshot_stats_for_partition("events", 0)
                .file_count,
            3,
            "Pre-write failure must NOT clear prior cycles' pending files"
        );
    }

    /// Verifies that a partial write failure (where some segments were written
    /// before the error) DOES trigger cleanup via clear_failed_partition_state.
    #[tokio::test]
    async fn partial_write_failure_clears_pending_files() {
        let mut hot = TestHotStorage::default();
        // Two events in different hours → two segment writes
        hot.insert(
            "events",
            0,
            vec![
                make_event(1, TS_HOUR_0_START),
                make_event(2, TS_HOUR_1_START),
            ],
        );

        let cold = TestColdStorage::default();
        // Fail on second write_segment call (hour-1)
        *cold.fail_on_call.lock().unwrap() = Some(1);

        let files_before = cold
            .pending_snapshot_stats_for_partition("events", 0)
            .file_count;
        assert_eq!(files_before, 0);

        let result = BackgroundFlusher::<TestHotStorage, TestColdStorage>::flush_partition(
            &hot,
            &cold,
            "events",
            0,
            0,     // watermark
            10000, // max_segment_size
            true,  // iceberg_enabled
            128 * 1024 * 1024,
        )
        .await;
        assert!(result.is_err(), "Should fail on second segment write");

        let files_after = cold
            .pending_snapshot_stats_for_partition("events", 0)
            .file_count;
        assert!(
            files_after > files_before,
            "Partial write should have added files"
        );

        let watermarks = RwLock::new(HashMap::new());
        let deferred = RwLock::new(HashMap::new());
        let flush_metrics = FlushMetrics::default();
        let iceberg_metrics = IcebergMetrics::default();
        clear_failed_partition_state(
            &hot,
            &cold,
            "events",
            0,
            files_before,
            &watermarks,
            &deferred,
            &flush_metrics,
            &iceberg_metrics,
        );

        assert_eq!(
            cold.pending_snapshot_stats_for_partition("events", 0)
                .file_count,
            0,
            "Partial write failure should clear pending files to prevent duplicates"
        );
    }

    /// P1 regression: cleanup decisions must use partition-scoped pending stats,
    /// not topic-wide aggregates that can be skewed by concurrent partitions.
    #[tokio::test]
    async fn concurrent_partition_cleanup_uses_partition_scoped_stats() {
        let hot = TestHotStorage::default();
        let cold = TestColdStorage::default();

        {
            let mut pending = cold.pending_stats.lock().unwrap();
            pending.insert(
                ("events".to_string(), 0),
                PendingSnapshotStats {
                    file_count: 3,
                    total_bytes: 300,
                },
            );
        }

        let files_before = cold
            .pending_snapshot_stats_for_partition("events", 1)
            .file_count;
        assert_eq!(files_before, 0);

        // Simulate concurrent partition changes:
        // - partition 0 cleaned up by another worker
        // - partition 1 wrote files before failing
        {
            let mut pending = cold.pending_stats.lock().unwrap();
            pending.remove(&("events".to_string(), 0));
            pending.insert(
                ("events".to_string(), 1),
                PendingSnapshotStats {
                    file_count: 2,
                    total_bytes: 200,
                },
            );
        }

        let watermarks = RwLock::new(HashMap::new());
        let deferred = RwLock::new(HashMap::new());
        let flush_metrics = FlushMetrics::default();
        let iceberg_metrics = IcebergMetrics::default();
        clear_failed_partition_state(
            &hot,
            &cold,
            "events",
            1,
            files_before,
            &watermarks,
            &deferred,
            &flush_metrics,
            &iceberg_metrics,
        );

        assert_eq!(
            cold.pending_snapshot_stats_for_partition("events", 1)
                .file_count,
            0,
            "Partition 1 pending files should be cleaned up based on its own stats"
        );
    }
}
