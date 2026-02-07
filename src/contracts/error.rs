use std::sync::{PoisonError, RwLockReadGuard, RwLockWriteGuard};

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

/// Extension trait for converting lock errors to StorageError.
pub trait LockResultExt<T> {
    /// Converts a lock error to a StorageError.
    fn map_lock_err(self) -> Result<T, StorageError>;
}

impl<'a, T> LockResultExt<RwLockReadGuard<'a, T>>
    for Result<RwLockReadGuard<'a, T>, PoisonError<RwLockReadGuard<'a, T>>>
{
    #[inline]
    fn map_lock_err(self) -> Result<RwLockReadGuard<'a, T>, StorageError> {
        self.map_err(|e| StorageError::LockPoisoned(e.to_string()))
    }
}

impl<'a, T> LockResultExt<RwLockWriteGuard<'a, T>>
    for Result<RwLockWriteGuard<'a, T>, PoisonError<RwLockWriteGuard<'a, T>>>
{
    #[inline]
    fn map_lock_err(self) -> Result<RwLockWriteGuard<'a, T>, StorageError> {
        self.map_err(|e| StorageError::LockPoisoned(e.to_string()))
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("RocksDB error: {0}")]
    RocksDb(String),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Invariant violation: {0}")]
    InvariantViolation(String),

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

    #[error("Server overloaded: {0}")]
    Overloaded(String),

    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),
}

#[derive(Error, Debug)]
pub enum SequenceError {
    #[error("Failed to persist sequence: {0}")]
    PersistFailed(String),

    #[error("Sequence overflow")]
    Overflow,
}
