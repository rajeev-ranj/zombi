use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::contracts::{
    ColdStorage, FlushResult, Flusher, HotStorage, LockResultExt, PendingSnapshotStats,
    StorageError, StoredEvent,
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
    /// Maximum age for pending snapshot data before forcing a commit.
    /// Prevents low-volume topics from holding deferred watermarks indefinitely.
    pub snapshot_max_age: Duration,
    /// Maximum concurrent S3 uploads during flush.
    /// Default: 4. Higher values can improve throughput but increase memory usage.
    pub max_concurrent_s3_uploads: usize,
    /// Retention window in seconds for flushed events in hot storage.
    /// After events are flushed to cold storage and the retention window elapses,
    /// they are deleted from RocksDB. Default: 0 (delete immediately after flush).
    pub hot_retention_secs: u64,
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
            snapshot_max_age: Duration::from_secs(1800),  // 30 minutes
            max_concurrent_s3_uploads: 4,
            hot_retention_secs: 0,
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
            snapshot_max_age: Duration::from_secs(1800), // 30 minutes
            max_concurrent_s3_uploads: 4, // Pipeline S3 writes
            hot_retention_secs: 0,
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

    /// On startup (Iceberg mode), reconcile durable flush watermarks with the
    /// highest sequence already committed in Iceberg metadata.
    ///
    /// This prevents re-flushing already committed data when a crash happens
    /// after snapshot commit but before durable watermark persistence.
    async fn reconcile_committed_watermarks_on_startup(&self) {
        let topic_partitions = match Self::list_topic_partitions(self.hot_storage.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to list topics for startup watermark reconciliation"
                );
                return;
            }
        };

        let mut topics = std::collections::HashSet::new();
        topics.extend(topic_partitions.into_iter().map(|(topic, _)| topic));

        for topic in topics {
            let committed = match self.cold_storage.committed_flush_watermarks(&topic).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        topic = %topic,
                        error = %e,
                        "Failed to load committed Iceberg watermarks during startup reconciliation"
                    );
                    continue;
                }
            };

            if committed.is_empty() {
                continue;
            }

            for (partition, committed_watermark) in committed {
                let durable = match self.hot_storage.load_flush_watermark(&topic, partition) {
                    Ok(wm) => wm,
                    Err(e) => {
                        self.flush_metrics.record_watermark_persist_error();
                        tracing::warn!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Failed to load durable watermark during startup reconciliation, using 0"
                        );
                        0
                    }
                };

                if committed_watermark <= durable {
                    continue;
                }

                match self.watermarks.write() {
                    Ok(mut in_memory) => {
                        in_memory.insert((topic.clone(), partition), committed_watermark);
                    }
                    Err(e) => {
                        self.flush_metrics.record_watermark_persist_error();
                        tracing::error!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Failed to update in-memory watermark during startup reconciliation"
                        );
                        continue;
                    }
                }

                persist_committed_watermark_or_defer(
                    self.hot_storage.as_ref(),
                    &topic,
                    partition,
                    committed_watermark,
                    self.pending_watermark_persists.as_ref(),
                    self.flush_metrics.as_ref(),
                );

                tracing::info!(
                    topic = %topic,
                    partition = partition,
                    durable_watermark = durable,
                    reconciled_watermark = committed_watermark,
                    "Reconciled startup watermark from committed Iceberg metadata"
                );
            }
        }
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

        if self.config.iceberg_enabled {
            self.reconcile_committed_watermarks_on_startup().await;
        }

        // Cleanup any events that were flushed but not cleaned up before the last shutdown.
        // Empty persist_times map means retention is skipped (no in-flight reads after restart).
        {
            let mut empty_times = HashMap::new();
            if let Ok(topics) = self.hot_storage.list_topics() {
                for topic in topics {
                    cleanup_flushed_events(
                        self.hot_storage.as_ref(),
                        &topic,
                        self.config.hot_retention_secs,
                        &mut empty_times,
                    );
                }
            }
        }

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
        let snapshot_max_age = self.config.snapshot_max_age;
        let max_concurrent_uploads = self.config.max_concurrent_s3_uploads;
        let hot_retention_secs = self.config.hot_retention_secs;
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
            let mut pending_snapshot_since: HashMap<String, tokio::time::Instant> = HashMap::new();
            // Maps (topic, partition) → (persist_time, flush_wm_at_recording).
            // flush_wm_at_recording caps cleanup so recently-flushed events aren't
            // deleted before their own retention window.
            let mut watermark_persist_times: HashMap<(String, u32), (Instant, u64)> =
                HashMap::new();

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
                            if let Err(e) = apply_partition_flush_result(
                                hot_storage.as_ref(),
                                cold_storage.as_ref(),
                                &topic,
                                partition,
                                files_before,
                                result,
                                duration_us,
                                iceberg_enabled,
                                watermarks.as_ref(),
                                pending_watermark_persists.as_ref(),
                                flush_metrics.as_ref(),
                                iceberg_metrics.as_ref(),
                                "Failed to flush partition",
                            ) {
                                tracing::error!(
                                    topic = %topic,
                                    partition = partition,
                                    error = %e,
                                    "Background flush error handled"
                                );
                            }
                        }
                    }
                }

                // Drain remaining futures
                while let Some((topic, partition, result, duration_us, files_before)) =
                    flush_futures.next().await
                {
                    if let Err(e) = apply_partition_flush_result(
                        hot_storage.as_ref(),
                        cold_storage.as_ref(),
                        &topic,
                        partition,
                        files_before,
                        result,
                        duration_us,
                        iceberg_enabled,
                        watermarks.as_ref(),
                        pending_watermark_persists.as_ref(),
                        flush_metrics.as_ref(),
                        iceberg_metrics.as_ref(),
                        "Failed to flush partition",
                    ) {
                        tracing::error!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Background flush error handled"
                        );
                    }
                }

                // Non-Iceberg mode: watermarks persisted immediately, cleanup now
                if !iceberg_enabled {
                    // Record watermark persist times for retention tracking
                    let now = Instant::now();
                    for topic in &topics_to_check {
                        if let Ok(partitions) = hot_storage.list_partitions(topic) {
                            for p in partitions {
                                let wm = hot_storage.load_flush_watermark(topic, p).unwrap_or(0);
                                watermark_persist_times
                                    .entry((topic.clone(), p))
                                    .or_insert((now, wm));
                            }
                        }
                        cleanup_flushed_events(
                            hot_storage.as_ref(),
                            topic,
                            hot_retention_secs,
                            &mut watermark_persist_times,
                        );
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

                        if stats.file_count > 0 {
                            let pending_age = pending_snapshot_since
                                .entry(topic.clone())
                                .or_insert_with(tokio::time::Instant::now)
                                .elapsed();

                            // Check if we've exceeded thresholds or max age.
                            let threshold_exceeded = stats.file_count >= snapshot_threshold_files
                                || stats.total_bytes >= snapshot_threshold_bytes as u64;
                            let age_exceeded = pending_age >= snapshot_max_age;
                            let should_commit = threshold_exceeded || age_exceeded;

                            if should_commit {
                                tracing::debug!(
                                    topic = %topic,
                                    pending_files = stats.file_count,
                                    pending_bytes = stats.total_bytes,
                                    pending_age_secs = pending_age.as_secs_f64(),
                                    threshold_files = snapshot_threshold_files,
                                    threshold_bytes = snapshot_threshold_bytes,
                                    snapshot_max_age_secs = snapshot_max_age.as_secs(),
                                    reason = if age_exceeded && !threshold_exceeded {
                                        "age"
                                    } else {
                                        "threshold"
                                    },
                                    "Committing Iceberg snapshot"
                                );

                                let committed_snapshot = match commit_snapshot_for_topic(
                                    hot_storage.as_ref(),
                                    cold_storage.as_ref(),
                                    &topic,
                                    stats,
                                    pending_watermark_persists.as_ref(),
                                    flush_metrics.as_ref(),
                                    iceberg_metrics.as_ref(),
                                    "Committed Iceberg snapshot (batched)",
                                    "No Iceberg snapshot to commit (no pending files)",
                                    "Failed to commit Iceberg snapshot",
                                )
                                .await
                                {
                                    Ok(v) => v,
                                    Err(_) => {
                                        // Don't persist watermarks if snapshot commit
                                        // failed — the data isn't durable yet.
                                        continue;
                                    }
                                };

                                pending_snapshot_since.remove(&topic);
                                if committed_snapshot.is_some() {
                                    if let Some(ref catalog) = catalog_client {
                                        update_catalog_registration(
                                            catalog,
                                            cold_storage.as_ref(),
                                            &topic,
                                            registered_topics.as_ref(),
                                        )
                                        .await;
                                    }
                                }

                                // Record watermark persist times for retention tracking.
                                // or_insert so the clock isn't reset every cycle; the
                                // flush_wm snapshot caps cleanup to avoid deleting
                                // events flushed after the timer was set.
                                if let Ok(partitions) = hot_storage.list_partitions(&topic) {
                                    let now = Instant::now();
                                    for p in partitions {
                                        let wm = hot_storage
                                            .load_flush_watermark(&topic, p)
                                            .unwrap_or(0);
                                        watermark_persist_times
                                            .entry((topic.clone(), p))
                                            .or_insert((now, wm));
                                    }
                                }

                                // Cleanup flushed events from hot storage
                                cleanup_flushed_events(
                                    hot_storage.as_ref(),
                                    &topic,
                                    hot_retention_secs,
                                    &mut watermark_persist_times,
                                );
                            } else {
                                tracing::debug!(
                                    topic = %topic,
                                    pending_files = stats.file_count,
                                    pending_bytes = stats.total_bytes,
                                    pending_age_secs = pending_age.as_secs_f64(),
                                    threshold_files = snapshot_threshold_files,
                                    threshold_bytes = snapshot_threshold_bytes,
                                    snapshot_max_age_secs = snapshot_max_age.as_secs(),
                                    "Deferring snapshot commit (thresholds and age not met)"
                                );
                            }
                        } else {
                            pending_snapshot_since.remove(&topic);
                            // No pending files and no commit needed: the data was
                            // already committed in a prior cycle but the watermark
                            // persist failed and was re-queued. Safe to persist now.
                            flush_pending_watermarks(
                                hot_storage.as_ref(),
                                &topic,
                                pending_watermark_persists.as_ref(),
                                flush_metrics.as_ref(),
                            );

                            // Record watermark persist times for retention tracking
                            if let Ok(partitions) = hot_storage.list_partitions(&topic) {
                                let now = Instant::now();
                                for p in partitions {
                                    let wm =
                                        hot_storage.load_flush_watermark(&topic, p).unwrap_or(0);
                                    watermark_persist_times
                                        .entry((topic.clone(), p))
                                        .or_insert((now, wm));
                                }
                            }

                            // Cleanup flushed events from hot storage
                            cleanup_flushed_events(
                                hot_storage.as_ref(),
                                &topic,
                                hot_retention_secs,
                                &mut watermark_persist_times,
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

            match apply_partition_flush_result(
                self.hot_storage.as_ref(),
                self.cold_storage.as_ref(),
                &topic,
                partition,
                files_before,
                result,
                duration_us,
                self.config.iceberg_enabled,
                self.watermarks.as_ref(),
                self.pending_watermark_persists.as_ref(),
                self.flush_metrics.as_ref(),
                self.iceberg_metrics.as_ref(),
                "Failed to flush partition in flush_now",
            ) {
                Ok(applied) => {
                    total_events += applied.events_flushed;
                    total_segments += applied.segments_written;
                    if let Some(new_watermark) = applied.new_watermark {
                        if new_watermark > max_watermark {
                            max_watermark = new_watermark;
                        }
                    }
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // Non-Iceberg mode: watermarks are persisted immediately, cleanup now
        if !self.config.iceberg_enabled {
            let mut empty_times = HashMap::new();
            for topic in &topics_to_commit {
                cleanup_flushed_events(
                    self.hot_storage.as_ref(),
                    topic,
                    self.config.hot_retention_secs,
                    &mut empty_times,
                );
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
                    if let Err(e) = commit_snapshot_for_topic(
                        self.hot_storage.as_ref(),
                        self.cold_storage.as_ref(),
                        &topic,
                        stats,
                        self.pending_watermark_persists.as_ref(),
                        self.flush_metrics.as_ref(),
                        self.iceberg_metrics.as_ref(),
                        "Force-committed Iceberg snapshot on flush_now",
                        "No Iceberg snapshot to commit (no pending files)",
                        "Failed to commit Iceberg snapshot on flush_now",
                    )
                    .await
                    {
                        if commit_error.is_none() {
                            commit_error = Some(e);
                        }
                    } else {
                        // Cleanup flushed events (flush_now = shutdown, no retention)
                        let mut empty_times = HashMap::new();
                        cleanup_flushed_events(
                            self.hot_storage.as_ref(),
                            &topic,
                            self.config.hot_retention_secs,
                            &mut empty_times,
                        );
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

                    // Cleanup flushed events
                    let mut empty_times = HashMap::new();
                    cleanup_flushed_events(
                        self.hot_storage.as_ref(),
                        &topic,
                        self.config.hot_retention_secs,
                        &mut empty_times,
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

struct AppliedPartitionFlush {
    events_flushed: usize,
    segments_written: usize,
    new_watermark: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
fn apply_partition_flush_result<H: HotStorage, C: ColdStorage>(
    hot_storage: &H,
    cold_storage: &C,
    topic: &str,
    partition: u32,
    files_before: usize,
    result: Result<(usize, u64, usize, usize), StorageError>,
    duration_us: u64,
    iceberg_enabled: bool,
    watermarks: &RwLock<HashMap<(String, u32), u64>>,
    pending_watermark_persists: &RwLock<DeferredWatermarks>,
    flush_metrics: &FlushMetrics,
    iceberg_metrics: &IcebergMetrics,
    error_log: &'static str,
) -> Result<AppliedPartitionFlush, StorageError> {
    match result {
        Ok((count, new_watermark, bytes, num_segments)) => {
            if count == 0 {
                return Ok(AppliedPartitionFlush {
                    events_flushed: 0,
                    segments_written: 0,
                    new_watermark: None,
                });
            }

            flush_metrics.record_flush(count as u64, bytes as u64, duration_us);

            if iceberg_enabled {
                iceberg_metrics.record_parquet_write(topic, bytes as u64, num_segments as u64);
            }

            match watermarks.write() {
                Ok(mut w) => {
                    w.insert((topic.to_string(), partition), new_watermark);
                }
                Err(e) => {
                    flush_metrics.record_watermark_persist_error();
                    return Err(StorageError::LockPoisoned(e.to_string()));
                }
            }

            persist_or_defer_watermark(
                hot_storage,
                topic,
                partition,
                new_watermark,
                iceberg_enabled,
                pending_watermark_persists,
                flush_metrics,
            );

            Ok(AppliedPartitionFlush {
                events_flushed: count,
                segments_written: num_segments,
                new_watermark: Some(new_watermark),
            })
        }
        Err(e) => {
            iceberg_metrics.record_s3_error();
            clear_failed_partition_state(
                hot_storage,
                cold_storage,
                topic,
                partition,
                files_before,
                watermarks,
                pending_watermark_persists,
                flush_metrics,
                iceberg_metrics,
            );
            tracing::error!(
                topic = %topic,
                partition = partition,
                error = %e,
                "{}",
                error_log
            );
            Err(e)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_snapshot_for_topic<H: HotStorage, C: ColdStorage>(
    hot_storage: &H,
    cold_storage: &C,
    topic: &str,
    stats: PendingSnapshotStats,
    pending_watermark_persists: &RwLock<DeferredWatermarks>,
    flush_metrics: &FlushMetrics,
    iceberg_metrics: &IcebergMetrics,
    success_log: &'static str,
    no_snapshot_log: &'static str,
    error_log: &'static str,
) -> Result<Option<i64>, StorageError> {
    let snapshot_id = match cold_storage.commit_snapshot(topic).await {
        Ok(snapshot_id) => snapshot_id,
        Err(e) => {
            iceberg_metrics.record_s3_error();
            tracing::error!(
                topic = %topic,
                error = %e,
                "{}",
                error_log
            );
            return Err(e);
        }
    };

    if let Some(snapshot_id) = snapshot_id {
        iceberg_metrics.record_snapshot_commit(topic);
        tracing::info!(
            topic = %topic,
            snapshot_id = snapshot_id,
            files = stats.file_count,
            bytes = stats.total_bytes,
            "{}",
            success_log
        );
    } else {
        tracing::debug!(topic = %topic, "{}", no_snapshot_log);
    }

    // Persist deferred watermark writes now that snapshot state is durable.
    flush_pending_watermarks(
        hot_storage,
        topic,
        pending_watermark_persists,
        flush_metrics,
    );

    Ok(snapshot_id)
}

async fn update_catalog_registration<C: ColdStorage>(
    catalog: &CatalogClient,
    cold_storage: &C,
    topic: &str,
    registered_topics: &RwLock<std::collections::HashSet<String>>,
) {
    let Some(metadata_json) = cold_storage.table_metadata_json(topic) else {
        return;
    };

    let metadata = match serde_json::from_str::<crate::storage::TableMetadata>(&metadata_json) {
        Ok(metadata) => metadata,
        Err(e) => {
            tracing::warn!(
                error = %e,
                topic = %topic,
                "Failed to parse table metadata for catalog"
            );
            return;
        }
    };

    let is_new = registered_topics
        .read()
        .map(|r| !r.contains(topic))
        .unwrap_or(true);
    let result = if is_new {
        catalog.register_table(topic, &metadata).await
    } else {
        catalog.update_table(topic, &metadata).await
    };

    match result {
        Ok(()) => {
            if let Ok(mut r) = registered_topics.write() {
                r.insert(topic.to_string());
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

fn persist_committed_watermark_or_defer<H: HotStorage>(
    hot_storage: &H,
    topic: &str,
    partition: u32,
    watermark: u64,
    pending: &RwLock<DeferredWatermarks>,
    flush_metrics: &FlushMetrics,
) {
    if let Err(e) = hot_storage.save_flush_watermark(topic, partition, watermark) {
        flush_metrics.record_watermark_persist_error();
        tracing::warn!(
            topic = %topic,
            partition = partition,
            watermark = watermark,
            error = %e,
            "Failed to persist reconciled startup watermark, deferring retry"
        );
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
                "Failed to defer reconciled startup watermark due to lock error"
            );
        }
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

/// Deletes flushed events from hot storage for all partitions of a topic.
///
/// For each partition, deletes events up to the flush watermark (subject to
/// the retention window). When retention is enabled, cleanup is capped to the
/// flush_wm recorded when the timer was set, so events flushed after that
/// point are not deleted prematurely. Failures are non-fatal and logged.
fn cleanup_flushed_events<H: HotStorage>(
    hot_storage: &H,
    topic: &str,
    hot_retention_secs: u64,
    watermark_persist_times: &mut HashMap<(String, u32), (Instant, u64)>,
) {
    let partitions = match hot_storage.list_partitions(topic) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                topic = %topic,
                error = %e,
                "Failed to list partitions for cleanup"
            );
            return;
        }
    };

    for partition in partitions {
        let flush_wm = match hot_storage.load_flush_watermark(topic, partition) {
            Ok(wm) => wm,
            Err(e) => {
                tracing::warn!(
                    topic = %topic,
                    partition = partition,
                    error = %e,
                    "Failed to load flush watermark for cleanup"
                );
                continue;
            }
        };

        if flush_wm == 0 {
            continue;
        }

        // Determine the cleanup target watermark.
        // When retention is enabled and we have a recorded entry, use the
        // flush_wm snapshot from recording time so recently-flushed events
        // (with higher sequences) are not deleted before their own window.
        // When retention=0 or no entry (restart/flush_now), use current flush_wm.
        let key = (topic.to_string(), partition);
        let cleanup_target = if hot_retention_secs > 0 {
            if let Some(&(persist_time, recorded_wm)) = watermark_persist_times.get(&key) {
                if persist_time.elapsed() < Duration::from_secs(hot_retention_secs) {
                    continue; // Retention window not elapsed
                }
                recorded_wm
            } else {
                // No entry (e.g., after restart) — proceed with current flush_wm
                flush_wm
            }
        } else {
            flush_wm
        };

        if cleanup_target == 0 {
            // Remove stale entry so or_insert can refresh with a non-zero
            // flush_wm on the next cycle (partition may have been flushed since).
            watermark_persist_times.remove(&key);
            continue;
        }

        match hot_storage.delete_flushed_events(topic, partition, cleanup_target) {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(
                        topic = %topic,
                        partition = partition,
                        deleted = count,
                        up_to = cleanup_target,
                        "Cleaned up flushed events from hot storage"
                    );
                }
                // Remove persist time so the next flush records a fresh retention window
                watermark_persist_times.remove(&key);
            }
            Err(e) => {
                tracing::warn!(
                    topic = %topic,
                    partition = partition,
                    error = %e,
                    "Failed to cleanup flushed events (will retry next cycle)"
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
    let files_after = cold_storage
        .pending_snapshot_stats_for_partition(topic, partition)
        .file_count;
    let new_files_written = files_after > files_before;

    // Clear pending files only when this cycle wrote new files before failing.
    // For pre-write failures, preserve existing pending files and deferred
    // watermark state so a later commit/persist retry can succeed.
    if new_files_written {
        cold_storage.clear_pending_data_files(topic, partition);

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
    } else {
        tracing::debug!(
            topic = %topic,
            partition = partition,
            "Flush failed before writing new files; preserving deferred watermark state"
        );
    }

    // Keep gauges in sync with cold-storage truth after cleanup.
    let pending_stats = cold_storage.pending_snapshot_stats(topic);
    iceberg_metrics.update_pending_stats(
        topic,
        pending_stats.file_count as u64,
        pending_stats.total_bytes,
    );
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
        assert_eq!(config.snapshot_max_age, Duration::from_secs(1800));
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
        assert_eq!(config.snapshot_max_age, Duration::from_secs(1800));
        assert_eq!(config.max_concurrent_s3_uploads, 4);
    }

    #[derive(Default)]
    struct TestHotStorage {
        events: HashMap<(String, u32), Vec<StoredEvent>>,
        flush_watermarks: Mutex<HashMap<(String, u32), u64>>,
        save_failures_remaining: Mutex<u32>,
        /// When > 0, high_watermark() will fail and decrement.
        high_watermark_failures: Mutex<u32>,
        /// Tracks calls to delete_flushed_events: (topic, partition, up_to_sequence).
        delete_calls: Mutex<Vec<(String, u32, u64)>>,
        /// Cleanup watermarks (tracks last cleanup position per partition).
        cleanup_watermarks: Mutex<HashMap<(String, u32), u64>>,
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

        fn delete_flushed_events(
            &self,
            topic: &str,
            partition: u32,
            up_to_sequence: u64,
        ) -> Result<u64, StorageError> {
            let key = (topic.to_string(), partition);
            let cleanup_wm = {
                let wm = self
                    .cleanup_watermarks
                    .lock()
                    .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
                wm.get(&key).copied().unwrap_or(0)
            };
            if up_to_sequence <= cleanup_wm {
                return Ok(0);
            }
            if let Ok(mut calls) = self.delete_calls.lock() {
                calls.push((topic.to_string(), partition, up_to_sequence));
            }
            let mut wm = self
                .cleanup_watermarks
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            wm.insert(key, up_to_sequence);
            Ok(up_to_sequence - cleanup_wm)
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
        committed_watermarks: Mutex<HashMap<String, HashMap<u32, u64>>>,
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

        async fn committed_flush_watermarks(
            &self,
            topic: &str,
        ) -> Result<HashMap<u32, u64>, StorageError> {
            let committed = self
                .committed_watermarks
                .lock()
                .map_err(|e| StorageError::S3(format!("Lock error: {}", e)))?;
            Ok(committed.get(topic).cloned().unwrap_or_default())
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

    #[tokio::test]
    async fn startup_reconciles_committed_iceberg_watermark() {
        let mut hot = TestHotStorage::default();
        hot.insert("events", 0, vec![make_event(1, TS_HOUR_0_START)]);
        let hot = Arc::new(hot);

        let cold = Arc::new(TestColdStorage::default());
        cold.committed_watermarks
            .lock()
            .unwrap()
            .entry("events".to_string())
            .or_default()
            .insert(0, 7);

        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            FlusherConfig {
                iceberg_enabled: true,
                ..Default::default()
            },
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        flusher.start().await.unwrap();

        assert_eq!(flusher.flush_watermark("events", 0).await.unwrap(), 7);
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 7);

        flusher.stop().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn startup_reconciliation_defers_watermark_persist_on_failure() {
        let mut hot = TestHotStorage::default();
        hot.insert("events", 0, vec![make_event(1, TS_HOUR_0_START)]);
        let hot = Arc::new(hot);
        *hot.save_failures_remaining.lock().unwrap() = 1;

        let cold = Arc::new(TestColdStorage::default());
        cold.committed_watermarks
            .lock()
            .unwrap()
            .entry("events".to_string())
            .or_default()
            .insert(0, 1);

        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            FlusherConfig {
                iceberg_enabled: true,
                interval: Duration::from_millis(10),
                ..Default::default()
            },
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        flusher.start().await.unwrap();

        // Startup reconciliation should update in-memory watermark even if
        // durable persistence fails on the first attempt.
        assert_eq!(flusher.flush_watermark("events", 0).await.unwrap(), 1);
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 0);

        // Background loop should retry deferred persist when no pending files exist.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if hot.load_flush_watermark("events", 0).unwrap() == 1 {
                break;
            }
        }
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 1);

        flusher.stop().await.unwrap();
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

        // First cycle: sequence 1 write succeeds, sequence 2 write fails in the
        // same flush pass (partial failure), so cleanup clears pending files and
        // rolls back to durable watermark.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if cold.writes.lock().unwrap().len() == 1
                && cold.pending_snapshot_stats("events").file_count == 0
            {
                break;
            }
        }
        assert_eq!(cold.writes.lock().unwrap().len(), 1);
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
    async fn background_pre_write_failure_preserves_pending_metrics() {
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

        // Next cycle fails before writing new files. Pending files and metrics
        // should stay intact so deferred state can be retried.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if iceberg_metrics.s3_errors_total.load(Ordering::Relaxed) >= 1 {
                break;
            }
        }
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 1);
        assert_eq!(
            *iceberg_metrics
                .pending_snapshot_files
                .get("events")
                .unwrap(),
            1,
            "Pending file gauge should remain unchanged on pre-write failure"
        );
        assert_eq!(
            *iceberg_metrics
                .pending_snapshot_bytes
                .get("events")
                .unwrap(),
            1
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

    /// P1 regression: pre-write failures must not discard deferred watermarks.
    /// If commit already succeeded but watermark persist previously failed,
    /// the deferred watermark should survive transient flush errors and retry.
    #[tokio::test]
    async fn pre_write_failure_preserves_deferred_watermark_state() {
        let mut hot = TestHotStorage::default();
        hot.insert("events", 0, vec![make_event(1, TS_HOUR_0_START)]);

        let hot = Arc::new(hot);
        // First persist attempt + retry both fail, leaving deferred state queued.
        *hot.save_failures_remaining.lock().unwrap() = 2;

        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            snapshot_threshold_files: 1, // commit snapshot immediately
            ..Default::default()
        };
        let flusher = BackgroundFlusher::new(
            Arc::clone(&hot),
            Arc::clone(&cold),
            config,
            Arc::new(FlushMetrics::default()),
            Arc::new(IcebergMetrics::default()),
        );

        let first = flusher.flush_now().await.unwrap();
        assert_eq!(first.events_flushed, 1);
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 0);

        // Inject a pre-write failure (high_watermark read) on next cycle.
        *hot.high_watermark_failures.lock().unwrap() = 1;
        let second = flusher.flush_now().await;
        assert!(second.is_err(), "Expected injected pre-write failure");

        // In-memory watermark should remain advanced so retry can persist
        // without re-flushing data.
        assert_eq!(flusher.flush_watermark("events", 0).await.unwrap(), 1);

        // Next cycle should persist deferred watermark without re-flushing.
        let third = flusher.flush_now().await.unwrap();
        assert_eq!(third.events_flushed, 0);
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 1);
        assert_eq!(
            cold.writes.lock().unwrap().len(),
            1,
            "Should not re-flush after pre-write failure"
        );
    }

    /// Under-threshold pending files should still commit after max age to
    /// prevent deferred watermarks from staying in memory indefinitely.
    #[tokio::test(start_paused = true)]
    async fn background_commits_under_threshold_after_snapshot_max_age() {
        let mut hot = TestHotStorage::default();
        hot.insert("events", 0, vec![make_event(1, TS_HOUR_0_START)]);

        let hot = Arc::new(hot);
        let cold = Arc::new(TestColdStorage::default());
        let config = FlusherConfig {
            iceberg_enabled: true,
            interval: Duration::from_millis(10),
            snapshot_threshold_files: 10,
            snapshot_threshold_bytes: usize::MAX,
            snapshot_max_age: Duration::from_millis(35),
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

        // First cycles flush data but stay under commit thresholds.
        for _ in 0..2 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 1);
        assert_eq!(hot.load_flush_watermark("events", 0).unwrap(), 0);

        // Eventually age threshold should force a snapshot commit.
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if hot.load_flush_watermark("events", 0).unwrap() == 1 {
                break;
            }
        }
        assert_eq!(
            hot.load_flush_watermark("events", 0).unwrap(),
            1,
            "Watermark should persist after age-triggered commit"
        );
        assert_eq!(cold.pending_snapshot_stats("events").file_count, 0);

        flusher.stop().await.unwrap();
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

    // ---- Retention-window tests for cleanup_flushed_events ----

    /// Helper: create a TestHotStorage with a single partition and a set flush watermark.
    fn hot_with_flush_wm(topic: &str, partition: u32, flush_wm: u64) -> TestHotStorage {
        let mut hot = TestHotStorage::default();
        // Insert a dummy event so list_partitions discovers this partition
        hot.insert(
            topic,
            partition,
            vec![StoredEvent {
                sequence: 1,
                topic: topic.to_string(),
                partition,
                payload: vec![],
                timestamp_ms: 0,
                idempotency_key: None,
            }],
        );
        hot.save_flush_watermark(topic, partition, flush_wm)
            .unwrap();
        hot
    }

    #[test]
    fn retention_defers_cleanup_until_window_expires() {
        let hot = hot_with_flush_wm("t", 0, 100);
        let mut persist_times: HashMap<(String, u32), (Instant, u64)> = HashMap::new();
        // Record persist time just now — window has NOT elapsed
        persist_times.insert(("t".to_string(), 0), (Instant::now(), 100));

        cleanup_flushed_events(&hot, "t", 3600, &mut persist_times);

        // Should NOT have deleted anything (window not elapsed)
        let calls = hot.delete_calls.lock().unwrap();
        assert!(
            calls.is_empty(),
            "Cleanup should be deferred during retention window"
        );
        // Entry should still be present
        assert!(persist_times.contains_key(&("t".to_string(), 0)));
    }

    #[test]
    fn retention_allows_cleanup_after_window_expires() {
        let hot = hot_with_flush_wm("t", 0, 100);
        let mut persist_times: HashMap<(String, u32), (Instant, u64)> = HashMap::new();
        // Record persist time in the past (window already elapsed)
        let past = Instant::now() - Duration::from_secs(200);
        persist_times.insert(("t".to_string(), 0), (past, 100));

        cleanup_flushed_events(&hot, "t", 60, &mut persist_times);

        let calls = hot.delete_calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "Cleanup should proceed after retention window"
        );
        assert_eq!(calls[0], ("t".to_string(), 0, 100));
        // Entry should be removed after successful cleanup
        assert!(!persist_times.contains_key(&("t".to_string(), 0)));
    }

    #[test]
    fn retention_caps_cleanup_to_recorded_wm_not_current() {
        // Simulate: recorded_wm=50, but flush_wm has since advanced to 200
        let hot = hot_with_flush_wm("t", 0, 200);
        let mut persist_times: HashMap<(String, u32), (Instant, u64)> = HashMap::new();
        let past = Instant::now() - Duration::from_secs(200);
        persist_times.insert(("t".to_string(), 0), (past, 50));

        cleanup_flushed_events(&hot, "t", 60, &mut persist_times);

        let calls = hot.delete_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        // Should delete up to recorded_wm=50, NOT current flush_wm=200
        assert_eq!(
            calls[0],
            ("t".to_string(), 0, 50),
            "Cleanup must be capped to recorded watermark, not current flush watermark"
        );
    }

    #[test]
    fn retention_recorded_wm_zero_does_not_block_future_cleanup() {
        // Scenario: entry was recorded when flush_wm=0 (partition hadn't been
        // flushed yet). Later flush_wm advances to 100. The stale entry with
        // recorded_wm=0 should be removed so cleanup can proceed.

        // Phase 1: flush_wm has now advanced to 100 but the stale entry still
        // has recorded_wm=0 (recorded before any flush happened).
        let hot = hot_with_flush_wm("t", 0, 100);
        let mut persist_times: HashMap<(String, u32), (Instant, u64)> = HashMap::new();
        let past = Instant::now() - Duration::from_secs(200);
        persist_times.insert(("t".to_string(), 0), (past, 0));

        cleanup_flushed_events(&hot, "t", 60, &mut persist_times);

        // cleanup_target = recorded_wm = 0, so stale entry should be removed
        // but no actual deletion occurs
        let calls = hot.delete_calls.lock().unwrap();
        assert!(
            calls.is_empty(),
            "Should not delete when cleanup_target is 0"
        );
        drop(calls);
        assert!(
            !persist_times.contains_key(&("t".to_string(), 0)),
            "Stale wm=0 entry should be removed to unblock future cleanup"
        );

        // Phase 2: next cycle — no entry, so cleanup uses current flush_wm=100
        cleanup_flushed_events(&hot, "t", 60, &mut persist_times);

        let calls = hot.delete_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            ("t".to_string(), 0, 100),
            "Partition should be cleanable after stale entry removal"
        );
    }

    #[test]
    fn retention_zero_means_immediate_cleanup() {
        let hot = hot_with_flush_wm("t", 0, 100);
        let mut persist_times: HashMap<(String, u32), (Instant, u64)> = HashMap::new();
        // Even with a recent entry, retention=0 should bypass the window
        persist_times.insert(("t".to_string(), 0), (Instant::now(), 50));

        cleanup_flushed_events(&hot, "t", 0, &mut persist_times);

        let calls = hot.delete_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        // With retention=0, uses current flush_wm directly
        assert_eq!(calls[0], ("t".to_string(), 0, 100));
    }

    #[test]
    fn retention_entry_removed_after_cleanup_enables_fresh_window() {
        let hot = hot_with_flush_wm("t", 0, 100);
        let mut persist_times: HashMap<(String, u32), (Instant, u64)> = HashMap::new();
        let past = Instant::now() - Duration::from_secs(200);
        persist_times.insert(("t".to_string(), 0), (past, 100));

        // First cleanup succeeds and removes the entry
        cleanup_flushed_events(&hot, "t", 60, &mut persist_times);
        assert!(!persist_times.contains_key(&("t".to_string(), 0)));

        // Simulate: new flush advances watermark
        hot.save_flush_watermark("t", 0, 250).unwrap();

        // Re-insert entry (as the background loop would via or_insert)
        let now = Instant::now();
        persist_times.insert(("t".to_string(), 0), (now, 250));

        // This should NOT cleanup yet (fresh window just started)
        cleanup_flushed_events(&hot, "t", 60, &mut persist_times);
        let calls = hot.delete_calls.lock().unwrap();
        // Only the first cleanup call should be present
        assert_eq!(
            calls.len(),
            1,
            "Second call should be deferred (fresh window)"
        );
        assert_eq!(calls[0], ("t".to_string(), 0, 100));
    }
}
