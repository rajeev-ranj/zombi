//! Shared test utilities for Zombi tests.
//!
//! This module provides common helpers and mocks used across test files.
//! Import with: `mod common;` in your test file.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use zombi::contracts::{ColdStorage, ColdStorageInfo, SegmentInfo, StorageError, StoredEvent};
use zombi::storage::RocksDbStorage;

// =============================================================================
// Storage Creation Helpers
// =============================================================================

/// Creates a RocksDB storage in a temporary directory.
/// Returns the storage and TempDir (keep TempDir alive to prevent cleanup).
pub fn create_test_storage() -> (RocksDbStorage, TempDir) {
    let dir = TempDir::new().expect("Failed to create temp directory");
    let storage = RocksDbStorage::open(dir.path()).expect("Failed to open RocksDB");
    (storage, dir)
}

/// Creates an Arc-wrapped RocksDB storage in a temporary directory.
/// Useful for concurrent tests.
pub fn create_arc_storage() -> (Arc<RocksDbStorage>, TempDir) {
    let (storage, dir) = create_test_storage();
    (Arc::new(storage), dir)
}

/// Opens RocksDB at the specified path.
/// Useful for crash recovery tests where you reopen the same directory.
pub fn open_storage_at(path: &Path) -> RocksDbStorage {
    RocksDbStorage::open(path).expect("Failed to open RocksDB")
}

// =============================================================================
// File-Based Cold Storage Mock
// =============================================================================

/// File-based cold storage for testing persistence across restarts.
///
/// This mock writes JSON segments to disk, simulating S3 behavior
/// without network I/O. Files persist across storage reopens, allowing
/// true crash recovery testing.
pub struct FileColdStorage {
    base_path: PathBuf,
}

impl FileColdStorage {
    pub fn new(base_path: &Path) -> Self {
        Self {
            base_path: base_path.to_path_buf(),
        }
    }

    fn segment_dir(&self, topic: &str, partition: u32) -> PathBuf {
        self.base_path.join(topic).join(partition.to_string())
    }

    fn segment_path(&self, topic: &str, partition: u32, start: u64, end: u64) -> PathBuf {
        self.segment_dir(topic, partition)
            .join(format!("{:016x}-{:016x}.json", start, end))
    }
}

impl ColdStorage for FileColdStorage {
    async fn write_segment(
        &self,
        topic: &str,
        partition: u32,
        events: &[StoredEvent],
    ) -> Result<String, StorageError> {
        if events.is_empty() {
            return Err(StorageError::S3("Cannot write empty segment".into()));
        }

        let dir = self.segment_dir(topic, partition);
        fs::create_dir_all(&dir)
            .map_err(|e| StorageError::S3(format!("Failed to create dir: {}", e)))?;

        let start = events.first().unwrap().sequence;
        let end = events.last().unwrap().sequence;
        let path = self.segment_path(topic, partition, start, end);

        let json = serde_json::to_string_pretty(events)
            .map_err(|e| StorageError::S3(format!("JSON encode error: {}", e)))?;

        fs::write(&path, json)
            .map_err(|e| StorageError::S3(format!("Failed to write segment: {}", e)))?;

        Ok(path.to_string_lossy().to_string())
    }

    async fn read_events(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        limit: usize,
        _since_ms: Option<i64>,
        _until_ms: Option<i64>,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        let dir = self.segment_dir(topic, partition);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut all_events = Vec::new();
        let entries = fs::read_dir(&dir)
            .map_err(|e| StorageError::S3(format!("Failed to read dir: {}", e)))?;

        for entry in entries {
            let entry = entry
                .map_err(|e| StorageError::S3(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "json") {
                let content = fs::read_to_string(&path)
                    .map_err(|e| StorageError::S3(format!("Failed to read file: {}", e)))?;

                let events: Vec<StoredEvent> = serde_json::from_str(&content)
                    .map_err(|e| StorageError::S3(format!("JSON decode error: {}", e)))?;

                all_events.extend(events);
            }
        }

        all_events.sort_by_key(|e| e.sequence);
        Ok(all_events
            .into_iter()
            .filter(|e| e.sequence >= start_offset)
            .take(limit)
            .collect())
    }

    async fn list_segments(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Vec<SegmentInfo>, StorageError> {
        let dir = self.segment_dir(topic, partition);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut segments = Vec::new();
        let entries = fs::read_dir(&dir)
            .map_err(|e| StorageError::S3(format!("Failed to read dir: {}", e)))?;

        for entry in entries {
            let entry =
                entry.map_err(|e| StorageError::S3(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();

            if let Some(file_name) = path.file_stem() {
                let name = file_name.to_string_lossy();
                if let Some((start_str, end_str)) = name.split_once('-') {
                    if let (Ok(start), Ok(end)) = (
                        u64::from_str_radix(start_str, 16),
                        u64::from_str_radix(end_str, 16),
                    ) {
                        let metadata = fs::metadata(&path).ok();
                        let size_bytes = metadata.map(|m| m.len()).unwrap_or(0);

                        segments.push(SegmentInfo {
                            segment_id: path.to_string_lossy().to_string(),
                            start_offset: start,
                            end_offset: end,
                            event_count: (end - start + 1) as usize,
                            size_bytes,
                        });
                    }
                }
            }
        }

        segments.sort_by_key(|s| s.start_offset);
        Ok(segments)
    }

    fn storage_info(&self) -> ColdStorageInfo {
        ColdStorageInfo {
            storage_type: "file".into(),
            iceberg_enabled: false,
            bucket: "local".into(),
            base_path: self.base_path.to_string_lossy().to_string(),
        }
    }
}
