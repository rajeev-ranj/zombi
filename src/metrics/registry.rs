//! Metrics registry containing all observability metrics for Zombi.
//!
//! This module provides structured metrics collection using lock-free atomics
//! and concurrent hashmaps for per-topic/partition metrics.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use super::Histogram;

/// Central registry for all Zombi observability metrics.
///
/// Provides structured access to different metric categories:
/// - Flush pipeline metrics
/// - Iceberg/cold storage metrics
/// - Consumer lag metrics
/// - Hot storage metrics
/// - Enhanced API metrics with histograms
#[derive(Default)]
pub struct MetricsRegistry {
    /// Flush pipeline metrics
    pub flush: Arc<FlushMetrics>,
    /// Iceberg/cold storage metrics
    pub iceberg: Arc<IcebergMetrics>,
    /// Consumer offset and lag metrics
    pub consumer: Arc<ConsumerMetrics>,
    /// Hot storage metrics
    pub hot: Arc<HotStorageMetrics>,
    /// Enhanced API metrics with histograms and per-topic breakdowns
    pub enhanced_api: Arc<EnhancedApiMetrics>,
    /// Write combiner queue depth metrics
    pub combiner: Arc<WriteCombinerMetrics>,
}

impl MetricsRegistry {
    /// Creates a new metrics registry with all metric categories initialized.
    pub fn new() -> Self {
        Self {
            flush: Arc::new(FlushMetrics::default()),
            iceberg: Arc::new(IcebergMetrics::default()),
            consumer: Arc::new(ConsumerMetrics::default()),
            hot: Arc::new(HotStorageMetrics::default()),
            enhanced_api: Arc::new(EnhancedApiMetrics::default()),
            combiner: Arc::new(WriteCombinerMetrics::default()),
        }
    }

    /// Formats all metrics in Prometheus exposition format.
    pub fn format_prometheus(&self) -> String {
        let mut output = String::with_capacity(8192);

        // Flush metrics
        output.push_str(&self.flush.format_prometheus());

        // Iceberg metrics
        output.push_str(&self.iceberg.format_prometheus());

        // Consumer metrics
        output.push_str(&self.consumer.format_prometheus());

        // Hot storage metrics
        output.push_str(&self.hot.format_prometheus());

        // Enhanced API metrics
        output.push_str(&self.enhanced_api.format_prometheus());

        // Write combiner metrics
        output.push_str(&self.combiner.format_prometheus());

        output
    }
}

/// Metrics for the flush pipeline (hot → cold storage).
#[derive(Default)]
pub struct FlushMetrics {
    /// Total number of flush operations
    pub flush_total: AtomicU64,
    /// Total events flushed to cold storage
    pub flush_events_total: AtomicU64,
    /// Total bytes flushed to cold storage
    pub flush_bytes_total: AtomicU64,
    /// Total watermark persistence errors
    pub watermark_persist_errors_total: AtomicU64,
    /// Histogram of flush durations in microseconds
    pub flush_duration_us: Histogram,
}

impl FlushMetrics {
    /// Records a flush operation.
    #[inline]
    pub fn record_flush(&self, events: u64, bytes: u64, duration_us: u64) {
        self.flush_total.fetch_add(1, Ordering::Relaxed);
        self.flush_events_total.fetch_add(events, Ordering::Relaxed);
        self.flush_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.flush_duration_us.observe(duration_us);
    }

    /// Records a flush watermark persistence error.
    #[inline]
    pub fn record_watermark_persist_error(&self) {
        self.watermark_persist_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Formats flush metrics in Prometheus exposition format.
    pub fn format_prometheus(&self) -> String {
        let mut output = String::with_capacity(1024);

        let _ = writeln!(
            output,
            "# HELP zombi_flush_total Total number of flush operations"
        );
        let _ = writeln!(output, "# TYPE zombi_flush_total counter");
        let _ = writeln!(
            output,
            "zombi_flush_total {}",
            self.flush_total.load(Ordering::Relaxed)
        );
        output.push('\n');

        let _ = writeln!(
            output,
            "# HELP zombi_flush_events_total Total events flushed to cold storage"
        );
        let _ = writeln!(output, "# TYPE zombi_flush_events_total counter");
        let _ = writeln!(
            output,
            "zombi_flush_events_total {}",
            self.flush_events_total.load(Ordering::Relaxed)
        );
        output.push('\n');

        let _ = writeln!(
            output,
            "# HELP zombi_flush_bytes_total Total bytes flushed to cold storage"
        );
        let _ = writeln!(output, "# TYPE zombi_flush_bytes_total counter");
        let _ = writeln!(
            output,
            "zombi_flush_bytes_total {}",
            self.flush_bytes_total.load(Ordering::Relaxed)
        );
        output.push('\n');

        let _ = writeln!(
            output,
            "# HELP zombi_watermark_persist_errors_total Total flush watermark persistence errors"
        );
        let _ = writeln!(
            output,
            "# TYPE zombi_watermark_persist_errors_total counter"
        );
        let _ = writeln!(
            output,
            "zombi_watermark_persist_errors_total {}",
            self.watermark_persist_errors_total.load(Ordering::Relaxed)
        );
        output.push('\n');

        output.push_str(&self.flush_duration_us.format_prometheus(
            "zombi_flush_duration_us",
            "Histogram of flush durations in microseconds",
        ));
        output.push('\n');

        output
    }
}

/// Metrics for Iceberg/cold storage operations.
#[derive(Default)]
pub struct IcebergMetrics {
    /// Total Parquet files written
    pub parquet_files_written_total: AtomicU64,
    /// Total Iceberg snapshots committed
    pub iceberg_snapshots_committed_total: AtomicU64,
    /// Total S3 errors encountered
    pub s3_errors_total: AtomicU64,
    /// Pending files per topic (not yet committed to snapshot)
    pub pending_snapshot_files: DashMap<String, u64>,
    /// Pending bytes per topic (not yet committed to snapshot)
    pub pending_snapshot_bytes: DashMap<String, u64>,
}

impl IcebergMetrics {
    /// Records a Parquet file write.
    #[inline]
    pub fn record_parquet_write(&self, topic: &str, bytes: u64) {
        self.parquet_files_written_total
            .fetch_add(1, Ordering::Relaxed);

        // Update pending files and bytes for the topic
        self.pending_snapshot_files
            .entry(topic.to_string())
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.pending_snapshot_bytes
            .entry(topic.to_string())
            .and_modify(|v| *v += bytes)
            .or_insert(bytes);
    }

    /// Records an Iceberg snapshot commit.
    #[inline]
    pub fn record_snapshot_commit(&self, topic: &str) {
        self.iceberg_snapshots_committed_total
            .fetch_add(1, Ordering::Relaxed);

        // Clear pending counts for the topic
        self.pending_snapshot_files.insert(topic.to_string(), 0);
        self.pending_snapshot_bytes.insert(topic.to_string(), 0);
    }

    /// Records an S3 error.
    #[inline]
    pub fn record_s3_error(&self) {
        self.s3_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Updates pending snapshot stats from external source.
    pub fn update_pending_stats(&self, topic: &str, files: u64, bytes: u64) {
        self.pending_snapshot_files.insert(topic.to_string(), files);
        self.pending_snapshot_bytes.insert(topic.to_string(), bytes);
    }

    /// Formats Iceberg metrics in Prometheus exposition format.
    pub fn format_prometheus(&self) -> String {
        let mut output = String::with_capacity(2048);

        let _ = writeln!(
            output,
            "# HELP zombi_parquet_files_written_total Total Parquet files written"
        );
        let _ = writeln!(output, "# TYPE zombi_parquet_files_written_total counter");
        let _ = writeln!(
            output,
            "zombi_parquet_files_written_total {}",
            self.parquet_files_written_total.load(Ordering::Relaxed)
        );
        output.push('\n');

        let _ = writeln!(
            output,
            "# HELP zombi_iceberg_snapshots_committed_total Total Iceberg snapshots committed"
        );
        let _ = writeln!(
            output,
            "# TYPE zombi_iceberg_snapshots_committed_total counter"
        );
        let _ = writeln!(
            output,
            "zombi_iceberg_snapshots_committed_total {}",
            self.iceberg_snapshots_committed_total
                .load(Ordering::Relaxed)
        );
        output.push('\n');

        let _ = writeln!(
            output,
            "# HELP zombi_s3_errors_total Total S3 errors encountered"
        );
        let _ = writeln!(output, "# TYPE zombi_s3_errors_total counter");
        let _ = writeln!(
            output,
            "zombi_s3_errors_total {}",
            self.s3_errors_total.load(Ordering::Relaxed)
        );
        output.push('\n');

        // Per-topic pending snapshot files
        let _ = writeln!(
            output,
            "# HELP zombi_pending_snapshot_files Pending files awaiting Iceberg snapshot commit"
        );
        let _ = writeln!(output, "# TYPE zombi_pending_snapshot_files gauge");
        for entry in self.pending_snapshot_files.iter() {
            let _ = writeln!(
                output,
                "zombi_pending_snapshot_files{{topic=\"{}\"}} {}",
                entry.key(),
                entry.value()
            );
        }
        output.push('\n');

        // Per-topic pending snapshot bytes
        let _ = writeln!(
            output,
            "# HELP zombi_pending_snapshot_bytes Pending bytes awaiting Iceberg snapshot commit"
        );
        let _ = writeln!(output, "# TYPE zombi_pending_snapshot_bytes gauge");
        for entry in self.pending_snapshot_bytes.iter() {
            let _ = writeln!(
                output,
                "zombi_pending_snapshot_bytes{{topic=\"{}\"}} {}",
                entry.key(),
                entry.value()
            );
        }
        output.push('\n');

        output
    }
}

/// Metrics for consumer groups and offset tracking.
#[derive(Default)]
pub struct ConsumerMetrics {
    /// High watermark per topic/partition (latest available offset)
    pub high_watermarks: DashMap<(String, u32), u64>,
    /// Committed offset per consumer group/topic/partition
    pub committed_offsets: DashMap<(String, String, u32), u64>,
}

impl ConsumerMetrics {
    /// Updates the high watermark for a topic/partition.
    #[inline]
    pub fn update_high_watermark(&self, topic: &str, partition: u32, watermark: u64) {
        self.high_watermarks
            .insert((topic.to_string(), partition), watermark);
    }

    /// Updates the committed offset for a consumer group.
    #[inline]
    pub fn update_committed_offset(&self, group: &str, topic: &str, partition: u32, offset: u64) {
        self.committed_offsets
            .insert((group.to_string(), topic.to_string(), partition), offset);
    }

    /// Calculates consumer lag for a specific group/topic/partition.
    pub fn calculate_lag(&self, group: &str, topic: &str, partition: u32) -> Option<u64> {
        let hwm = self
            .high_watermarks
            .get(&(topic.to_string(), partition))
            .map(|v| *v)?;
        let committed = self
            .committed_offsets
            .get(&(group.to_string(), topic.to_string(), partition))
            .map(|v| *v)
            .unwrap_or(0);

        Some(hwm.saturating_sub(committed))
    }

    /// Formats consumer metrics in Prometheus exposition format.
    pub fn format_prometheus(&self) -> String {
        let mut output = String::with_capacity(4096);

        // High watermarks
        let _ = writeln!(
            output,
            "# HELP zombi_high_watermark Latest available offset (high watermark) per partition"
        );
        let _ = writeln!(output, "# TYPE zombi_high_watermark gauge");
        for entry in self.high_watermarks.iter() {
            let (topic, partition) = entry.key();
            let _ = writeln!(
                output,
                "zombi_high_watermark{{topic=\"{}\",partition=\"{}\"}} {}",
                topic,
                partition,
                entry.value()
            );
        }
        output.push('\n');

        // Committed offsets
        let _ = writeln!(
            output,
            "# HELP zombi_committed_offset Committed offset per consumer group"
        );
        let _ = writeln!(output, "# TYPE zombi_committed_offset gauge");
        for entry in self.committed_offsets.iter() {
            let (group, topic, partition) = entry.key();
            let _ = writeln!(
                output,
                "zombi_committed_offset{{group=\"{}\",topic=\"{}\",partition=\"{}\"}} {}",
                group,
                topic,
                partition,
                entry.value()
            );
        }
        output.push('\n');

        // Consumer lag (derived metric)
        let _ = writeln!(
            output,
            "# HELP zombi_consumer_lag Consumer lag (high watermark - committed offset)"
        );
        let _ = writeln!(output, "# TYPE zombi_consumer_lag gauge");
        for entry in self.committed_offsets.iter() {
            let (group, topic, partition) = entry.key();
            if let Some(lag) = self.calculate_lag(group, topic, *partition) {
                let _ = writeln!(
                    output,
                    "zombi_consumer_lag{{group=\"{}\",topic=\"{}\",partition=\"{}\"}} {}",
                    group, topic, partition, lag
                );
            }
        }
        output.push('\n');

        output
    }
}

/// Metrics for hot storage (RocksDB).
#[derive(Default)]
pub struct HotStorageMetrics {
    /// Events currently in hot storage per topic/partition
    pub hot_events: DashMap<(String, u32), u64>,
    /// High watermark per topic/partition (duplicated from consumer for easy access)
    pub high_watermarks: DashMap<(String, u32), u64>,
    /// Low watermark per topic/partition
    pub low_watermarks: DashMap<(String, u32), u64>,
}

impl HotStorageMetrics {
    /// Updates hot storage metrics for a topic/partition.
    pub fn update(&self, topic: &str, partition: u32, low: u64, high: u64) {
        let events = if high >= low { high - low + 1 } else { 0 };
        self.hot_events
            .insert((topic.to_string(), partition), events);
        self.high_watermarks
            .insert((topic.to_string(), partition), high);
        self.low_watermarks
            .insert((topic.to_string(), partition), low);
    }

    /// Formats hot storage metrics in Prometheus exposition format.
    pub fn format_prometheus(&self) -> String {
        let mut output = String::with_capacity(2048);

        let _ = writeln!(
            output,
            "# HELP zombi_hot_storage_events Events currently in hot storage per partition"
        );
        let _ = writeln!(output, "# TYPE zombi_hot_storage_events gauge");
        for entry in self.hot_events.iter() {
            let (topic, partition) = entry.key();
            let _ = writeln!(
                output,
                "zombi_hot_storage_events{{topic=\"{}\",partition=\"{}\"}} {}",
                topic,
                partition,
                entry.value()
            );
        }
        output.push('\n');

        output
    }
}

/// Metrics for write combiner queue depth per shard.
#[derive(Default)]
pub struct WriteCombinerMetrics {
    /// Queue depth per shard (events buffered in channel).
    queue_depth_by_shard: DashMap<u32, AtomicU64>,
}

impl WriteCombinerMetrics {
    /// Register a shard for tracking. Called once per shard at combiner startup.
    pub fn register_shard(&self, shard: u32) {
        self.queue_depth_by_shard.insert(shard, AtomicU64::new(0));
    }

    /// Increment queue depth for a shard (called after successful enqueue).
    #[inline]
    pub fn increment_depth(&self, shard: u32) {
        if let Some(depth) = self.queue_depth_by_shard.get(&shard) {
            depth.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Decrement queue depth for a shard (called after successful dequeue).
    #[inline]
    pub fn decrement_depth(&self, shard: u32) {
        if let Some(depth) = self.queue_depth_by_shard.get(&shard) {
            depth.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Formats write combiner metrics in Prometheus exposition format.
    pub fn format_prometheus(&self) -> String {
        if self.queue_depth_by_shard.is_empty() {
            return String::new();
        }

        let mut output = String::with_capacity(512);
        let mut total: u64 = 0;

        let _ = writeln!(
            output,
            "# HELP zombi_write_combiner_queue_depth Queued events per combiner shard"
        );
        let _ = writeln!(output, "# TYPE zombi_write_combiner_queue_depth gauge");

        // Collect and sort by shard ID for deterministic output
        let mut shards: Vec<(u32, u64)> = self
            .queue_depth_by_shard
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::Relaxed)))
            .collect();
        shards.sort_by_key(|(shard, _)| *shard);

        for (shard, depth) in &shards {
            let _ = writeln!(
                output,
                "zombi_write_combiner_queue_depth{{shard=\"{}\"}} {}",
                shard, depth
            );
            total += depth;
        }
        output.push('\n');

        let _ = writeln!(
            output,
            "# HELP zombi_write_combiner_queue_depth_total Total queued events across all shards"
        );
        let _ = writeln!(
            output,
            "# TYPE zombi_write_combiner_queue_depth_total gauge"
        );
        let _ = writeln!(output, "zombi_write_combiner_queue_depth_total {}", total);
        output.push('\n');

        output
    }
}

/// Enhanced API metrics with histograms and per-topic breakdowns.
#[derive(Default)]
pub struct EnhancedApiMetrics {
    /// Write latency histogram
    pub write_latency_us: Histogram,
    /// Read latency histogram
    pub read_latency_us: Histogram,
    /// Writes per topic
    pub writes_by_topic: DashMap<String, AtomicU64>,
    /// Reads per topic
    pub reads_by_topic: DashMap<String, AtomicU64>,
    /// Total backpressure rejections
    pub backpressure_rejections_total: AtomicU64,
}

impl EnhancedApiMetrics {
    /// Records a write operation.
    #[inline]
    pub fn record_write(&self, topic: &str, latency_us: u64) {
        self.write_latency_us.observe(latency_us);
        self.writes_by_topic
            .entry(topic.to_string())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a read operation.
    #[inline]
    pub fn record_read(&self, topic: &str, latency_us: u64) {
        self.read_latency_us.observe(latency_us);
        self.reads_by_topic
            .entry(topic.to_string())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a backpressure rejection.
    #[inline]
    pub fn record_backpressure_rejection(&self) {
        self.backpressure_rejections_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Formats enhanced API metrics in Prometheus exposition format.
    pub fn format_prometheus(&self) -> String {
        let mut output = String::with_capacity(4096);

        // Write latency histogram
        output.push_str(&self.write_latency_us.format_prometheus(
            "zombi_write_latency_us",
            "Write operation latency histogram in microseconds",
        ));
        output.push('\n');

        // Read latency histogram
        output.push_str(&self.read_latency_us.format_prometheus(
            "zombi_read_latency_us",
            "Read operation latency histogram in microseconds",
        ));
        output.push('\n');

        // Writes by topic
        let _ = writeln!(
            output,
            "# HELP zombi_writes_by_topic_total Write operations per topic"
        );
        let _ = writeln!(output, "# TYPE zombi_writes_by_topic_total counter");
        for entry in self.writes_by_topic.iter() {
            let _ = writeln!(
                output,
                "zombi_writes_by_topic_total{{topic=\"{}\"}} {}",
                entry.key(),
                entry.value().load(Ordering::Relaxed)
            );
        }
        output.push('\n');

        // Reads by topic
        let _ = writeln!(
            output,
            "# HELP zombi_reads_by_topic_total Read operations per topic"
        );
        let _ = writeln!(output, "# TYPE zombi_reads_by_topic_total counter");
        for entry in self.reads_by_topic.iter() {
            let _ = writeln!(
                output,
                "zombi_reads_by_topic_total{{topic=\"{}\"}} {}",
                entry.key(),
                entry.value().load(Ordering::Relaxed)
            );
        }
        output.push('\n');

        // Backpressure rejections
        let _ = writeln!(output, "# HELP zombi_backpressure_rejections_total Total requests rejected due to backpressure");
        let _ = writeln!(output, "# TYPE zombi_backpressure_rejections_total counter");
        let _ = writeln!(
            output,
            "zombi_backpressure_rejections_total {}",
            self.backpressure_rejections_total.load(Ordering::Relaxed)
        );
        output.push('\n');

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flush_metrics() {
        let metrics = FlushMetrics::default();
        assert_eq!(
            metrics
                .watermark_persist_errors_total
                .load(Ordering::Relaxed),
            0
        );
        metrics.record_watermark_persist_error();
        metrics.record_watermark_persist_error();
        metrics.record_flush(100, 10_000, 5_000);
        metrics.record_flush(200, 20_000, 10_000);

        assert_eq!(metrics.flush_total.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.flush_events_total.load(Ordering::Relaxed), 300);
        assert_eq!(metrics.flush_bytes_total.load(Ordering::Relaxed), 30_000);
        assert_eq!(metrics.flush_duration_us.count(), 2);
        assert_eq!(
            metrics
                .watermark_persist_errors_total
                .load(Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn test_iceberg_metrics() {
        let metrics = IcebergMetrics::default();
        metrics.record_parquet_write("events", 1_000_000);
        metrics.record_parquet_write("events", 2_000_000);

        assert_eq!(
            metrics.parquet_files_written_total.load(Ordering::Relaxed),
            2
        );
        assert_eq!(*metrics.pending_snapshot_files.get("events").unwrap(), 2);
        assert_eq!(
            *metrics.pending_snapshot_bytes.get("events").unwrap(),
            3_000_000
        );

        metrics.record_snapshot_commit("events");
        assert_eq!(
            metrics
                .iceberg_snapshots_committed_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(*metrics.pending_snapshot_files.get("events").unwrap(), 0);
    }

    #[test]
    fn test_consumer_metrics() {
        let metrics = ConsumerMetrics::default();
        metrics.update_high_watermark("events", 0, 1000);
        metrics.update_committed_offset("my-group", "events", 0, 500);

        let lag = metrics.calculate_lag("my-group", "events", 0);
        assert_eq!(lag, Some(500));
    }

    #[test]
    fn test_hot_storage_metrics() {
        let metrics = HotStorageMetrics::default();
        metrics.update("events", 0, 100, 500);

        assert_eq!(
            *metrics.hot_events.get(&("events".to_string(), 0)).unwrap(),
            401
        );
    }

    #[test]
    fn test_enhanced_api_metrics() {
        let metrics = EnhancedApiMetrics::default();
        metrics.record_write("events", 100);
        metrics.record_write("events", 200);
        metrics.record_read("events", 50);
        metrics.record_backpressure_rejection();

        assert_eq!(metrics.write_latency_us.count(), 2);
        assert_eq!(metrics.read_latency_us.count(), 1);
        assert_eq!(
            metrics
                .writes_by_topic
                .get("events")
                .unwrap()
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            metrics
                .backpressure_rejections_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_write_combiner_metrics() {
        let metrics = WriteCombinerMetrics::default();
        metrics.register_shard(0);
        metrics.register_shard(1);
        metrics.increment_depth(0);
        metrics.increment_depth(0);
        metrics.increment_depth(1);
        metrics.decrement_depth(0);

        let output = metrics.format_prometheus();
        assert!(output.contains("zombi_write_combiner_queue_depth{shard=\"0\"} 1"));
        assert!(output.contains("zombi_write_combiner_queue_depth{shard=\"1\"} 1"));
        assert!(output.contains("zombi_write_combiner_queue_depth_total 2"));
    }

    #[test]
    fn test_write_combiner_metrics_empty_when_no_shards() {
        let metrics = WriteCombinerMetrics::default();
        let output = metrics.format_prometheus();
        assert!(output.is_empty());
    }

    #[test]
    fn test_metrics_registry_prometheus_format() {
        let registry = MetricsRegistry::new();
        registry.flush.record_flush(100, 10_000, 5_000);
        registry.enhanced_api.record_write("test-topic", 150);

        let output = registry.format_prometheus();
        assert!(output.contains("zombi_flush_total 1"));
        assert!(output.contains("zombi_watermark_persist_errors_total 0"));
        assert!(output.contains("zombi_write_latency_us"));
        assert!(output.contains("zombi_writes_by_topic_total{topic=\"test-topic\"}"));
    }
}
