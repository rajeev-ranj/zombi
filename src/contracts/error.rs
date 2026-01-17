use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZombiError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Sequence error: {0}")]
    Sequence(#[from] SequenceError),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("RocksDB error: {0}")]
    RocksDb(String),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: {partition} for topic {topic}")]
    PartitionNotFound { topic: String, partition: u32 },

    #[error("Offset out of range: requested {requested}, available {low}..{high}")]
    OffsetOutOfRange { requested: u64, low: u64, high: u64 },

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(String),
}

#[derive(Error, Debug)]
pub enum SequenceError {
    #[error("Failed to persist sequence: {0}")]
    PersistFailed(String),

    #[error("Sequence overflow")]
    Overflow,
}
