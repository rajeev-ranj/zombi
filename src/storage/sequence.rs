use std::sync::atomic::{AtomicU64, Ordering};

use crate::contracts::{SequenceError, SequenceGenerator};

/// Atomic sequence generator that maintains monotonically increasing sequences.
///
/// Uses atomic operations for lock-free performance in the hot path.
/// Persistence is handled via periodic calls to `persist()`.
pub struct AtomicSequenceGenerator {
    counter: AtomicU64,
    persistence_path: Option<std::path::PathBuf>,
}

impl AtomicSequenceGenerator {
    /// Creates a new sequence generator starting from 0.
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
            persistence_path: None,
        }
    }

    /// Creates a sequence generator with persistence to disk.
    /// Recovers the last sequence from disk if the file exists.
    pub fn with_persistence(path: impl Into<std::path::PathBuf>) -> Result<Self, SequenceError> {
        let path = path.into();
        let initial = if path.exists() {
            let contents = std::fs::read_to_string(&path)
                .map_err(|e| SequenceError::PersistFailed(e.to_string()))?;
            contents
                .trim()
                .parse::<u64>()
                .map_err(|e| SequenceError::PersistFailed(e.to_string()))?
        } else {
            0
        };

        Ok(Self {
            counter: AtomicU64::new(initial),
            persistence_path: Some(path),
        })
    }

    /// Creates a sequence generator starting from a specific value.
    pub fn starting_from(value: u64) -> Self {
        Self {
            counter: AtomicU64::new(value),
            persistence_path: None,
        }
    }
}

impl Default for AtomicSequenceGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceGenerator for AtomicSequenceGenerator {
    fn next(&self) -> Result<u64, SequenceError> {
        // fetch_add returns the previous value, so we add 1 to get the new value
        // Relaxed ordering is sufficient - we only need atomicity, not memory ordering (#3)
        let prev = self.counter.fetch_add(1, Ordering::Relaxed);
        let next = prev.checked_add(1).ok_or(SequenceError::Overflow)?;
        Ok(next)
    }

    fn current(&self) -> Result<u64, SequenceError> {
        // Relaxed is fine for reading current value
        Ok(self.counter.load(Ordering::Relaxed))
    }

    fn persist(&self) -> Result<(), SequenceError> {
        if let Some(ref path) = self.persistence_path {
            // Acquire ensures we see all increments before persisting
            let value = self.counter.load(Ordering::Acquire);
            std::fs::write(path, value.to_string())
                .map_err(|e| SequenceError::PersistFailed(e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_returns_monotonically_increasing_values() {
        let gen = AtomicSequenceGenerator::new();
        let mut prev = 0;
        for _ in 0..1000 {
            let next = gen.next().unwrap();
            assert!(next > prev, "Expected {} > {}", next, prev);
            prev = next;
        }
    }

    #[test]
    fn current_returns_latest_value() {
        let gen = AtomicSequenceGenerator::new();
        assert_eq!(gen.current().unwrap(), 0);

        gen.next().unwrap();
        assert_eq!(gen.current().unwrap(), 1);

        gen.next().unwrap();
        gen.next().unwrap();
        assert_eq!(gen.current().unwrap(), 3);
    }

    #[test]
    fn starting_from_respects_initial_value() {
        let gen = AtomicSequenceGenerator::starting_from(100);
        assert_eq!(gen.current().unwrap(), 100);
        assert_eq!(gen.next().unwrap(), 101);
    }

    #[test]
    fn persistence_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sequence");

        // Create generator and advance it
        {
            let gen = AtomicSequenceGenerator::with_persistence(&path).unwrap();
            for _ in 0..10 {
                gen.next().unwrap();
            }
            gen.persist().unwrap();
        }

        // Create new generator from same path - should recover
        {
            let gen = AtomicSequenceGenerator::with_persistence(&path).unwrap();
            assert_eq!(gen.current().unwrap(), 10);
            assert_eq!(gen.next().unwrap(), 11);
        }
    }

    #[test]
    fn concurrent_access_is_safe() {
        use std::sync::Arc;
        use std::thread;

        let gen = Arc::new(AtomicSequenceGenerator::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let gen = Arc::clone(&gen);
            handles.push(thread::spawn(move || {
                let mut values = vec![];
                for _ in 0..100 {
                    values.push(gen.next().unwrap());
                }
                values
            }));
        }

        let mut all_values: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All values should be unique (no duplicates)
        all_values.sort();
        let len_before = all_values.len();
        all_values.dedup();
        assert_eq!(all_values.len(), len_before, "Found duplicate sequences");

        // Should have exactly 1000 values (10 threads * 100 each)
        assert_eq!(all_values.len(), 1000);
    }
}
