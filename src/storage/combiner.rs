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
    let mut pending: Option<WriteRequest> = None;

    loop {
        let first = match pending.take().or_else(|| receiver.try_recv().ok()) {
            Some(req) => req,
            None => match receiver.recv().await {
                Some(req) => req,
                None => break,
            },
        };

        let table = first.table.clone();
        let mut events = Vec::with_capacity(config.batch_size);
        let mut responders = Vec::with_capacity(config.batch_size);

        events.push(first.event);
        responders.push(first.response);

        let deadline = Instant::now() + config.window;

        while events.len() < config.batch_size {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match time::timeout(remaining, receiver.recv()).await {
                Ok(Some(req)) => {
                    if req.table != table {
                        pending = Some(req);
                        break;
                    }
                    events.push(req.event);
                    responders.push(req.response);
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let result = storage.write_batch(&table, &events);

        match result {
            Ok(offsets) => {
                if offsets.len() == responders.len() {
                    for (offset, responder) in offsets.into_iter().zip(responders) {
                        let _ = responder.send(Ok(offset));
                    }
                } else {
                    let error = StorageError::Serialization(format!(
                        "Combiner offset mismatch: {} responses for {} offsets",
                        responders.len(),
                        offsets.len()
                    ));
                    for responder in responders {
                        let _ = responder.send(Err(clone_storage_error(&error)));
                    }
                }
            }
            Err(err) => {
                for responder in responders {
                    let _ = responder.send(Err(clone_storage_error(&err)));
                }
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
