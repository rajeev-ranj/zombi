use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant};

use crate::contracts::{BulkWriteEvent, HotStorage, StorageError};
use crate::metrics::WriteCombinerMetrics;

/// Configuration for the write combiner.
#[derive(Debug, Clone)]
pub struct WriteCombinerConfig {
    /// Whether the write combiner is enabled.
    pub enabled: bool,
    /// Maximum number of events per batch.
    pub batch_size: usize,
    /// Maximum time to wait before flushing a batch.
    pub window: Duration,
    /// Maximum number of queued events per shard.
    pub queue_capacity: usize,
    /// Number of combiner worker shards.
    pub shards: usize,
    /// Maximum total queued bytes across all shards (secondary defense).
    pub max_queue_bytes: usize,
}

impl Default for WriteCombinerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 10,
            window: Duration::from_micros(100),
            queue_capacity: 10_000,
            shards: 4,
            max_queue_bytes: 256 * 1024 * 1024, // 256 MB
        }
    }
}

impl WriteCombinerConfig {
    /// Creates a config from environment variables.
    ///
    /// Environment variables:
    /// - `ZOMBI_WRITE_COMBINER_ENABLED`: Enable combiner (default: false)
    /// - `ZOMBI_WRITE_COMBINER_SIZE`: Batch size (default: 10)
    /// - `ZOMBI_WRITE_COMBINER_WINDOW_US`: Flush window in microseconds (default: 100)
    /// - `ZOMBI_WRITE_COMBINER_QUEUE_CAPACITY`: Queue capacity per shard (default: 10000)
    /// - `ZOMBI_WRITE_COMBINER_SHARDS`: Number of worker shards (default: 4)
    /// - `ZOMBI_WRITE_COMBINER_QUEUE_BYTES_MB`: Max queued bytes in MB (default: 256)
    pub fn from_env() -> Self {
        let default = Self::default();
        let enabled = std::env::var("ZOMBI_WRITE_COMBINER_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(default.enabled);
        let batch_size = std::env::var("ZOMBI_WRITE_COMBINER_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default.batch_size);
        let window = std::env::var("ZOMBI_WRITE_COMBINER_WINDOW_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .map(Duration::from_micros)
            .unwrap_or(default.window);
        let queue_capacity = std::env::var("ZOMBI_WRITE_COMBINER_QUEUE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default.queue_capacity);
        let shards = std::env::var("ZOMBI_WRITE_COMBINER_SHARDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default.shards);
        let max_queue_bytes = std::env::var("ZOMBI_WRITE_COMBINER_QUEUE_BYTES_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(default.max_queue_bytes);

        Self {
            enabled,
            batch_size,
            window,
            queue_capacity,
            shards,
            max_queue_bytes,
        }
    }
}

pub struct WriteCombiner<H: HotStorage> {
    senders: Vec<mpsc::Sender<WriteRequest>>,
    inflight_bytes: Arc<AtomicU64>,
    _storage: Arc<H>,
    config: WriteCombinerConfig,
    metrics: Arc<WriteCombinerMetrics>,
}

struct WriteRequest {
    table: String,
    event: BulkWriteEvent,
    payload_size: u64,
    response: oneshot::Sender<Result<u64, StorageError>>,
}

fn shard_for_table(table: &str, num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    table.hash(&mut hasher);
    hasher.finish() as usize % num_shards
}

impl<H: HotStorage> WriteCombiner<H> {
    pub fn new(
        storage: Arc<H>,
        config: WriteCombinerConfig,
        metrics: Arc<WriteCombinerMetrics>,
    ) -> Self
    where
        H: 'static,
    {
        let inflight_bytes = Arc::new(AtomicU64::new(0));
        let mut senders = Vec::with_capacity(config.shards);

        for i in 0..config.shards {
            let (sender, receiver) = mpsc::channel(config.queue_capacity);
            metrics.register_shard(i as u32);
            let worker_storage = Arc::clone(&storage);
            let worker_config = config.clone();
            let worker_bytes = Arc::clone(&inflight_bytes);
            let worker_metrics = Arc::clone(&metrics);
            let shard_id = i as u32;
            tokio::spawn(async move {
                run_combiner(
                    worker_storage,
                    receiver,
                    worker_config,
                    worker_bytes,
                    worker_metrics,
                    shard_id,
                )
                .await;
            });
            senders.push(sender);
        }

        Self {
            senders,
            inflight_bytes,
            _storage: storage,
            config,
            metrics,
        }
    }

    pub fn config(&self) -> &WriteCombinerConfig {
        &self.config
    }

    pub async fn write(
        &self,
        table: String,
        partition: u32,
        payload: Vec<u8>,
        timestamp_ms: i64,
        idempotency_key: Option<String>,
    ) -> Result<u64, StorageError> {
        let payload_size = payload.len() as u64;

        // Byte-based backpressure: fetch_add then rollback if over limit (TOCTOU-safe)
        let prev = self
            .inflight_bytes
            .fetch_add(payload_size, Ordering::AcqRel);
        if prev + payload_size > self.config.max_queue_bytes as u64 {
            self.inflight_bytes
                .fetch_sub(payload_size, Ordering::Release);
            return Err(StorageError::Overloaded(
                "Write combiner byte limit exceeded; try bulk writes or reduce payload size".into(),
            ));
        }

        let shard = shard_for_table(&table, self.senders.len());

        let (response_tx, response_rx) = oneshot::channel();
        let request = WriteRequest {
            table,
            event: BulkWriteEvent {
                partition,
                payload,
                timestamp_ms,
                idempotency_key,
            },
            payload_size,
            response: response_tx,
        };

        if let Err(err) = self.senders[shard].try_send(request) {
            // Rollback bytes on channel send failure
            self.inflight_bytes
                .fetch_sub(payload_size, Ordering::Release);
            return Err(match err {
                mpsc::error::TrySendError::Full(_) => StorageError::Overloaded(
                    "Write combiner queue full; try bulk writes or reduce load".into(),
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    StorageError::Overloaded("Write combiner unavailable".into())
                }
            });
        }

        self.metrics.increment_depth(shard as u32);

        response_rx
            .await
            .map_err(|_| StorageError::Overloaded("Write combiner dropped response".into()))?
    }
}

async fn run_combiner<H: HotStorage>(
    storage: Arc<H>,
    mut receiver: mpsc::Receiver<WriteRequest>,
    config: WriteCombinerConfig,
    inflight_bytes: Arc<AtomicU64>,
    metrics: Arc<WriteCombinerMetrics>,
    shard_id: u32,
) {
    let mut batches: HashMap<String, PendingBatch> = HashMap::new();

    loop {
        let next_deadline = batches.values().map(|batch| batch.deadline).min();

        match next_deadline {
            Some(deadline) => {
                tokio::select! {
                    maybe_req = receiver.recv() => {
                        match maybe_req {
                            Some(req) => {
                                metrics.decrement_depth(shard_id);
                                handle_request(&mut batches, req, &config, &storage, &inflight_bytes);
                            }
                            None => {
                                flush_all_batches(&mut batches, &storage, &inflight_bytes);
                                break;
                            }
                        }
                    }
                    _ = time::sleep_until(deadline) => {
                        flush_expired_batches(&mut batches, &storage, &inflight_bytes);
                    }
                }
            }
            None => match receiver.recv().await {
                Some(req) => {
                    metrics.decrement_depth(shard_id);
                    handle_request(&mut batches, req, &config, &storage, &inflight_bytes);
                }
                None => break,
            },
        }
    }
}

struct PendingBatch {
    events: Vec<BulkWriteEvent>,
    responders: Vec<oneshot::Sender<Result<u64, StorageError>>>,
    batch_bytes: u64,
    deadline: Instant,
}

fn handle_request<H: HotStorage>(
    batches: &mut HashMap<String, PendingBatch>,
    req: WriteRequest,
    config: &WriteCombinerConfig,
    storage: &Arc<H>,
    inflight_bytes: &Arc<AtomicU64>,
) {
    let now = Instant::now();
    let payload_size = req.payload_size;
    let batch = batches
        .entry(req.table.clone())
        .or_insert_with(|| PendingBatch {
            events: Vec::with_capacity(config.batch_size),
            responders: Vec::with_capacity(config.batch_size),
            batch_bytes: 0,
            deadline: now + config.window,
        });

    batch.events.push(req.event);
    batch.responders.push(req.response);
    batch.batch_bytes += payload_size;

    if batch.events.len() >= config.batch_size {
        flush_batch(storage, req.table, batches, inflight_bytes);
    }
}

fn flush_expired_batches<H: HotStorage>(
    batches: &mut HashMap<String, PendingBatch>,
    storage: &Arc<H>,
    inflight_bytes: &Arc<AtomicU64>,
) {
    let now = Instant::now();
    let expired: Vec<String> = batches
        .iter()
        .filter(|(_, batch)| batch.deadline <= now)
        .map(|(table, _)| table.clone())
        .collect();
    for table in expired {
        flush_batch(storage, table, batches, inflight_bytes);
    }
}

fn flush_all_batches<H: HotStorage>(
    batches: &mut HashMap<String, PendingBatch>,
    storage: &Arc<H>,
    inflight_bytes: &Arc<AtomicU64>,
) {
    let tables: Vec<String> = batches.keys().cloned().collect();
    for table in tables {
        flush_batch(storage, table, batches, inflight_bytes);
    }
}

fn flush_batch<H: HotStorage>(
    storage: &Arc<H>,
    table: String,
    batches: &mut HashMap<String, PendingBatch>,
    inflight_bytes: &Arc<AtomicU64>,
) {
    let Some(batch) = batches.remove(&table) else {
        return;
    };

    let batch_bytes = batch.batch_bytes;
    let result = storage.write_batch(&table, &batch.events);

    // Decrement inflight bytes after the write completes
    inflight_bytes.fetch_sub(batch_bytes, Ordering::Release);

    match result {
        Ok(offsets) => {
            if offsets.len() == batch.responders.len() {
                for (offset, responder) in offsets.into_iter().zip(batch.responders) {
                    let _ = responder.send(Ok(offset));
                }
            } else {
                let error = StorageError::Serialization(format!(
                    "Combiner offset mismatch: {} responses for {} offsets",
                    batch.responders.len(),
                    offsets.len()
                ));
                for responder in batch.responders {
                    let _ = responder.send(Err(clone_storage_error(&error)));
                }
            }
        }
        Err(err) => {
            for responder in batch.responders {
                let _ = responder.send(Err(clone_storage_error(&err)));
            }
        }
    }
}

fn clone_storage_error(err: &StorageError) -> StorageError {
    match err {
        StorageError::RocksDb(msg) => StorageError::RocksDb(msg.clone()),
        StorageError::S3(msg) => StorageError::S3(msg.clone()),
        StorageError::InvalidInput(msg) => StorageError::InvalidInput(msg.clone()),
        StorageError::TopicNotFound(msg) => StorageError::TopicNotFound(msg.clone()),
        StorageError::PartitionNotFound { topic, partition } => StorageError::PartitionNotFound {
            topic: topic.clone(),
            partition: *partition,
        },
        StorageError::OffsetOutOfRange {
            requested,
            low,
            high,
        } => StorageError::OffsetOutOfRange {
            requested: *requested,
            low: *low,
            high: *high,
        },
        StorageError::Serialization(msg) => StorageError::Serialization(msg.clone()),
        StorageError::Io(msg) => StorageError::Io(msg.clone()),
        StorageError::Overloaded(msg) => StorageError::Overloaded(msg.clone()),
        StorageError::CompactionInProgress(msg) => StorageError::CompactionInProgress(msg.clone()),
        StorageError::CompactionConflict(msg) => StorageError::CompactionConflict(msg.clone()),
        StorageError::LockPoisoned(msg) => StorageError::LockPoisoned(msg.clone()),
        StorageError::InvariantViolation(msg) => StorageError::InvariantViolation(msg.clone()),
    }
}
