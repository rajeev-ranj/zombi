use crate::contracts::error::SequenceError;

/// Generates monotonically increasing sequence numbers.
///
/// # Invariants
/// - `seq[n+1] > seq[n]` always (INV-1: Monotonic)
/// - Survives process restart (INV-6: Recovery)
/// - Lock-free in hot path
pub trait SequenceGenerator: Send + Sync {
    /// Returns the next sequence number.
    /// Each call MUST return a value greater than any previous call.
    fn next(&self) -> Result<u64, SequenceError>;

    /// Returns the current sequence number without incrementing.
    fn current(&self) -> Result<u64, SequenceError>;

    /// Persists the current sequence to durable storage.
    /// Called periodically to ensure crash recovery.
    fn persist(&self) -> Result<(), SequenceError>;
}
