use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant};

use crate::contracts::{BulkWriteEvent, HotStorage, StorageError};

/// Configuration for the write combiner.
#[derive(Debug, Clone)]
pub struct WriteCombinerConfig {
    /// Whether the write combiner is enabled.
    pub enabled: bool,
    /// Maximum number of events per batch.
    pub batch_size: usize,
    /// Maximum time to wait before flushing a batch.
    pub window: Duration,
    /// Maximum number of queued events.
    pub queue_capacity: usize,
}

impl Default for WriteCombinerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 10,
            window: Duration::from_micros(100),
            queue_capacity: 100,
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

        let queue_capacity = (batch_size * 10).max(1);

        Self {
            enabled,
            batch_size,
            window,
            queue_capacity,
        }
    }
}

pub struct WriteCombiner<H: HotStorage> {
    sender: mpsc::Sender<WriteRequest>,
    _storage: Arc<H>,
    config: WriteCombinerConfig,
}

struct WriteRequest {
    table: String,
    event: BulkWriteEvent,
    response: oneshot::Sender<Result<u64, StorageError>>,
}

impl<H: HotStorage> WriteCombiner<H> {
    pub fn new(storage: Arc<H>, config: WriteCombinerConfig) -> Self
    where
        H: 'static,
    {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let worker_storage = Arc::clone(&storage);
        let worker_config = config.clone();
        tokio::spawn(async move {
            run_combiner(worker_storage, receiver, worker_config).await;
        });

        Self {
            sender,
            _storage: storage,
            config,
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
        let (response_tx, response_rx) = oneshot::channel();
        let request = WriteRequest {
            table,
            event: BulkWriteEvent {
                partition,
                payload,
                timestamp_ms,
                idempotency_key,
            },
            response: response_tx,
        };

        self.sender.try_send(request).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => StorageError::Overloaded(
                "Write combiner queue full; try bulk writes or reduce load".into(),
            ),
            mpsc::error::TrySendError::Closed(_) => {
                StorageError::Overloaded("Write combiner unavailable".into())
            }
        })?;

        response_rx
            .await
            .map_err(|_| StorageError::Overloaded("Write combiner dropped response".into()))?
    }
}

async fn run_combiner<H: HotStorage>(
    storage: Arc<H>,
    mut receiver: mpsc::Receiver<WriteRequest>,
    config: WriteCombinerConfig,
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
                                handle_request(&mut batches, req, &config, &storage);
                                flush_expired_batches(&mut batches, &storage);
                            }
                            None => {
                                flush_all_batches(&mut batches, &storage);
                                break;
                            }
                        }
                    }
                    _ = time::sleep_until(deadline) => {
                        flush_expired_batches(&mut batches, &storage);
                    }
                }
            }
            None => match receiver.recv().await {
                Some(req) => {
                    handle_request(&mut batches, req, &config, &storage);
                }
                None => break,
            },
        }
    }
}

struct PendingBatch {
    events: Vec<BulkWriteEvent>,
    responders: Vec<oneshot::Sender<Result<u64, StorageError>>>,
    deadline: Instant,
}

fn handle_request<H: HotStorage>(
    batches: &mut HashMap<String, PendingBatch>,
    req: WriteRequest,
    config: &WriteCombinerConfig,
    storage: &Arc<H>,
) {
    let now = Instant::now();
    let batch = batches
        .entry(req.table.clone())
        .or_insert_with(|| PendingBatch {
            events: Vec::with_capacity(config.batch_size),
            responders: Vec::with_capacity(config.batch_size),
            deadline: now + config.window,
        });

    batch.events.push(req.event);
    batch.responders.push(req.response);

    if batch.events.len() >= config.batch_size {
        flush_batch(storage, req.table, batches);
    }
}

fn flush_expired_batches<H: HotStorage>(
    batches: &mut HashMap<String, PendingBatch>,
    storage: &Arc<H>,
) {
    let now = Instant::now();
    let expired_tables: Vec<String> = batches
        .iter()
        .filter_map(|(table, batch)| {
            if batch.deadline <= now {
                Some(table.clone())
            } else {
                None
            }
        })
        .collect();

    for table in expired_tables {
        flush_batch(storage, table, batches);
    }
}

fn flush_all_batches<H: HotStorage>(batches: &mut HashMap<String, PendingBatch>, storage: &Arc<H>) {
    let tables: Vec<String> = batches.keys().cloned().collect();
    for table in tables {
        flush_batch(storage, table, batches);
    }
}

fn flush_batch<H: HotStorage>(
    storage: &Arc<H>,
    table: String,
    batches: &mut HashMap<String, PendingBatch>,
) {
    let Some(batch) = batches.remove(&table) else {
        return;
    };

    let result = storage.write_batch(&table, &batch.events);

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
        StorageError::LockPoisoned(msg) => StorageError::LockPoisoned(msg.clone()),
    }
}
