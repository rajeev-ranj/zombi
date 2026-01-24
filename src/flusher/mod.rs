use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::contracts::{
    ColdStorage, FlushResult, Flusher, HotStorage, LockResultExt, StorageError,
};

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
}

impl Default for FlusherConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            batch_size: 1000,
            max_segment_size: 10000,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB
            iceberg_enabled: false,
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
    pub fn iceberg_defaults() -> Self {
        Self {
            interval: Duration::from_secs(300),        // 5 minutes for low-volume tables
            batch_size: 10000,                         // Advisory target batch size
            max_segment_size: 100000,                  // Max events per segment
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB target (Iceberg best practice)
            iceberg_enabled: true,
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
}

impl<H, C> BackgroundFlusher<H, C>
where
    H: HotStorage + 'static,
    C: ColdStorage + 'static,
{
    /// Creates a new background flusher.
    pub fn new(hot_storage: Arc<H>, cold_storage: Arc<C>, config: FlusherConfig) -> Self {
        Self {
            hot_storage,
            cold_storage,
            config,
            watermarks: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
            task_handle: RwLock::new(None),
            topics: Arc::new(RwLock::new(Vec::new())),
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

    /// Flushes a single topic/partition.
    ///
    /// When `iceberg_enabled` is true, uses `target_file_size_bytes` to limit
    /// the segment size instead of event count alone.
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
    ) -> Result<(usize, u64), StorageError> {
        let high_watermark = hot_storage.high_watermark(topic, partition)?;

        if high_watermark <= watermark {
            return Ok((0, watermark)); // Nothing to flush
        }

        // Read events from hot storage
        let all_events = hot_storage.read(topic, partition, watermark + 1, max_segment_size)?;

        if all_events.is_empty() {
            return Ok((0, watermark));
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
            return Ok((0, watermark));
        }

        let events_count = events.len();
        let new_watermark = events.last().map(|e| e.sequence).unwrap_or(watermark);

        // Write to cold storage
        cold_storage
            .write_segment(topic, partition, &events)
            .await?;

        // Commit Iceberg snapshot if enabled
        if iceberg_enabled && events_count > 0 {
            match cold_storage.commit_snapshot(topic).await {
                Ok(Some(snapshot_id)) => {
                    tracing::info!(
                        topic = topic,
                        partition = partition,
                        snapshot_id = snapshot_id,
                        "Committed Iceberg snapshot"
                    );
                }
                Ok(None) => {
                    tracing::debug!(
                        topic = topic,
                        partition = partition,
                        "No Iceberg snapshot to commit (no pending files)"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        topic = topic,
                        partition = partition,
                        error = %e,
                        "Failed to commit Iceberg snapshot"
                    );
                }
            }
        }

        tracing::info!(
            topic = topic,
            partition = partition,
            events = events_count,
            new_watermark = new_watermark,
            iceberg_enabled = iceberg_enabled,
            "Flushed events to cold storage"
        );

        Ok((events_count, new_watermark))
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

        let handle = tokio::spawn(async move {
            tracing::info!("Flusher background task started");

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
                let topics_to_flush: Vec<(String, u32)> = match hot_storage.list_topics() {
                    Ok(topic_names) => {
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
                        all
                    }
                    Err(e) => {
                        tracing::error!("Failed to list topics: {}", e);
                        continue;
                    }
                };

                // Flush each topic/partition
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

                    match Self::flush_partition(
                        &hot_storage,
                        &cold_storage,
                        &topic,
                        partition,
                        current_watermark,
                        max_segment_size,
                        iceberg_enabled,
                        target_file_size_bytes,
                    )
                    .await
                    {
                        Ok((count, new_watermark)) => {
                            if count > 0 {
                                if let Ok(mut w) = watermarks.write() {
                                    w.insert((topic.clone(), partition), new_watermark);
                                }
                            }
                        }
                        Err(e) => {
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
        let topics_to_flush = self.topics.read().map_lock_err()?.clone();

        let mut total_events = 0;
        let mut total_segments = 0;
        let mut max_watermark = 0u64;

        for (topic, partition) in topics_to_flush {
            let current_watermark = self
                .watermarks
                .read()
                .map_lock_err()?
                .get(&(topic.clone(), partition))
                .copied()
                .unwrap_or(0);

            let (count, new_watermark) = Self::flush_partition(
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

            if count > 0 {
                total_events += count;
                total_segments += 1;

                self.watermarks
                    .write()
                    .map_lock_err()?
                    .insert((topic, partition), new_watermark);

                if new_watermark > max_watermark {
                    max_watermark = new_watermark;
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

    #[test]
    fn test_flusher_config_default() {
        let config = FlusherConfig::default();
        assert_eq!(config.interval, Duration::from_secs(5));
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_segment_size, 10000);
        assert_eq!(config.target_file_size_bytes, 128 * 1024 * 1024);
        assert!(!config.iceberg_enabled);
    }

    #[test]
    fn test_flusher_config_iceberg_defaults() {
        let config = FlusherConfig::iceberg_defaults();
        assert_eq!(config.interval, Duration::from_secs(300)); // 5 minutes
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.max_segment_size, 100000);
        assert_eq!(config.target_file_size_bytes, 128 * 1024 * 1024);
        assert!(config.iceberg_enabled);
    }
}
