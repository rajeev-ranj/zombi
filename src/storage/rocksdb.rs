use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};

use bloomfilter::Bloom;
use dashmap::DashMap;
use rayon::prelude::*;
use rocksdb::{BlockBasedOptions, Options, ReadOptions, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};

use crate::contracts::{HotStorage, SequenceGenerator, StorageError, StoredEvent};
use crate::storage::AtomicSequenceGenerator;

/// Key prefix for event data
const EVENT_PREFIX: &str = "evt";
/// Key prefix for idempotency tracking
const IDEM_PREFIX: &str = "idem";
/// Key prefix for high watermark tracking
const HWM_PREFIX: &str = "hwm";
/// Key prefix for consumer offsets
const CONSUMER_PREFIX: &str = "consumer";
/// Key prefix for timestamp index (for O(1) time-based queries)
const TIMESTAMP_INDEX_PREFIX: &str = "ts";
/// Key prefix for flush watermarks (flusher progress tracking)
const FLUSH_WM_PREFIX: &str = "flush_wm";
const EVENT_VALUE_MAGIC: &[u8; 4] = b"ZEV1";
const KB: usize = 1024;
const MB: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEventValueV1 {
    payload: Vec<u8>,
    timestamp_ms: i64,
    idempotency_key: Option<String>,
}

/// Configuration for Bloom filter-based idempotency key lookups.
#[derive(Debug, Clone)]
pub struct BloomConfig {
    /// Whether Bloom filters are enabled.
    pub enabled: bool,
    /// Expected number of unique idempotency keys per (topic, partition).
    pub expected_items: usize,
    /// Target false positive rate (e.g., 0.01 = 1%).
    pub fp_rate: f64,
    /// Whether to rebuild Bloom filters from RocksDB on startup.
    pub rebuild_on_startup: bool,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            expected_items: 1_000_000,
            fp_rate: 0.01,
            rebuild_on_startup: true,
        }
    }
}

impl BloomConfig {
    /// Creates a BloomConfig from environment variables.
    pub fn from_env() -> Self {
        let enabled = std::env::var("ZOMBI_BLOOM_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let fp_rate = std::env::var("ZOMBI_BLOOM_FP_RATE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.01);
        let expected_items = std::env::var("ZOMBI_BLOOM_EXPECTED_ITEMS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000);
        let rebuild_on_startup = std::env::var("ZOMBI_BLOOM_REBUILD_ON_STARTUP")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);

        Self {
            enabled,
            fp_rate,
            expected_items,
            rebuild_on_startup,
        }
    }
}

/// Configuration for RocksDB tuning parameters.
#[derive(Debug, Clone)]
pub struct RocksDbConfig {
    /// Memtable size in bytes.
    pub write_buffer_size_bytes: usize,
    /// Number of write buffers before stalling.
    pub max_write_buffers: i32,
    /// L0 compaction trigger.
    pub l0_compaction_trigger: i32,
    /// Target SST file size in bytes.
    pub target_file_size_base_bytes: u64,
    /// Block cache size in bytes.
    pub block_cache_size_bytes: usize,
    /// Block size in bytes.
    pub block_size_bytes: usize,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            write_buffer_size_bytes: 64 * MB,
            max_write_buffers: 3,
            l0_compaction_trigger: 4,
            target_file_size_base_bytes: (64 * MB) as u64,
            block_cache_size_bytes: 128 * MB,
            block_size_bytes: 16 * KB,
        }
    }
}

impl RocksDbConfig {
    /// Creates a RocksDbConfig from environment variables.
    ///
    /// Environment variables:
    /// - `ZOMBI_ROCKSDB_WRITE_BUFFER_MB`: Memtable size in MB (default: 64)
    /// - `ZOMBI_ROCKSDB_MAX_WRITE_BUFFERS`: Number of write buffers (default: 3)
    /// - `ZOMBI_ROCKSDB_L0_COMPACTION_TRIGGER`: L0 compaction trigger (default: 4)
    /// - `ZOMBI_ROCKSDB_TARGET_FILE_SIZE_MB`: Target SST file size in MB (default: 64)
    /// - `ZOMBI_ROCKSDB_BLOCK_CACHE_MB`: Block cache size in MB (default: 128)
    /// - `ZOMBI_ROCKSDB_BLOCK_SIZE_KB`: Block size in KB (default: 16)
    pub fn from_env() -> Self {
        let default = Self::default();
        let write_buffer_size_bytes = std::env::var("ZOMBI_ROCKSDB_WRITE_BUFFER_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .map(|mb| mb * MB)
            .unwrap_or(default.write_buffer_size_bytes);
        let max_write_buffers = std::env::var("ZOMBI_ROCKSDB_MAX_WRITE_BUFFERS")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default.max_write_buffers);
        let l0_compaction_trigger = std::env::var("ZOMBI_ROCKSDB_L0_COMPACTION_TRIGGER")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default.l0_compaction_trigger);
        let target_file_size_base_bytes = std::env::var("ZOMBI_ROCKSDB_TARGET_FILE_SIZE_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .map(|mb| mb * MB as u64)
            .unwrap_or(default.target_file_size_base_bytes);
        let block_cache_size_bytes = std::env::var("ZOMBI_ROCKSDB_BLOCK_CACHE_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .map(|mb| mb * MB)
            .unwrap_or(default.block_cache_size_bytes);
        let block_size_bytes = std::env::var("ZOMBI_ROCKSDB_BLOCK_SIZE_KB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .map(|kb| kb * KB)
            .unwrap_or(default.block_size_bytes);

        Self {
            write_buffer_size_bytes,
            max_write_buffers,
            l0_compaction_trigger,
            target_file_size_base_bytes,
            block_cache_size_bytes,
            block_size_bytes,
        }
    }
}

/// RocksDB-backed hot storage implementation.
pub struct RocksDbStorage {
    db: DB,
    /// Per-partition sequence generators (lock-free concurrent map)
    sequences: DashMap<(String, u32), Arc<AtomicSequenceGenerator>>,
    /// Cached partitions per topic (avoids full scan on list_partitions)
    partitions_cache: DashMap<String, HashSet<u32>>,
    /// Cached topics (avoids full scan on list_topics)
    topics_cache: DashMap<(), HashSet<String>>,
    /// Path for sequence persistence (reserved for future use)
    #[allow(dead_code)]
    data_path: std::path::PathBuf,
    /// Enable secondary timestamp index for O(1) time-based queries.
    /// Slightly increases write overhead but enables efficient time-range scans.
    timestamp_index_enabled: bool,
    /// Per-(topic, partition) Bloom filters for idempotency key lookups.
    bloom_filters: DashMap<(String, u32), RwLock<Bloom<String>>>,
    /// Bloom filter configuration.
    bloom_config: BloomConfig,
    /// Whether WAL is enabled for write operations.
    wal_enabled: bool,
}

impl RocksDbStorage {
    /// Opens or creates a RocksDB storage at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let rocksdb_config = RocksDbConfig::from_env();
        tracing::info!(
            write_buffer_mb = rocksdb_config.write_buffer_size_bytes / MB,
            max_write_buffers = rocksdb_config.max_write_buffers,
            l0_compaction_trigger = rocksdb_config.l0_compaction_trigger,
            target_file_size_mb = rocksdb_config.target_file_size_base_bytes / MB as u64,
            block_cache_mb = rocksdb_config.block_cache_size_bytes / MB,
            block_size_kb = rocksdb_config.block_size_bytes / KB,
            "RocksDB tuning parameters configured"
        );

        // Compression: LZ4 is fast with decent compression
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // Write buffer: larger buffer = fewer flushes to disk
        opts.set_write_buffer_size(rocksdb_config.write_buffer_size_bytes);
        opts.set_max_write_buffer_number(rocksdb_config.max_write_buffers);

        // Parallelism: use available CPU cores
        let parallelism = std::thread::available_parallelism()
            .map(|p| p.get() as i32)
            .unwrap_or(4);
        opts.increase_parallelism(parallelism);
        opts.set_max_background_jobs(parallelism.min(4)); // Background compaction/flush

        // Level compaction tuning
        opts.set_target_file_size_base(rocksdb_config.target_file_size_base_bytes);
        opts.set_level_zero_file_num_compaction_trigger(rocksdb_config.l0_compaction_trigger);

        // Block cache: explicitly configure for read performance (#6)
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(
            rocksdb_config.block_cache_size_bytes,
        ));
        block_opts.set_block_size(rocksdb_config.block_size_bytes);
        block_opts.set_cache_index_and_filter_blocks(true); // Cache index blocks too
        opts.set_block_based_table_factory(&block_opts);

        // Disable WAL sync on every write for better throughput
        // (data is still durable after process crash, just not OS crash)
        opts.set_wal_dir(path.join("wal"));

        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Check if timestamp index is enabled via environment variable
        let timestamp_index_enabled = std::env::var("ZOMBI_TIMESTAMP_INDEX_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if timestamp_index_enabled {
            tracing::info!("Timestamp secondary index enabled for time-based queries");
        }

        let bloom_config = BloomConfig::from_env();
        if bloom_config.enabled {
            tracing::info!(
                expected_items = bloom_config.expected_items,
                fp_rate = bloom_config.fp_rate,
                rebuild_on_startup = bloom_config.rebuild_on_startup,
                "Bloom filter enabled for idempotency key lookups"
            );
        }

        let bloom_filters = DashMap::new();

        let wal_enabled = match std::env::var("ZOMBI_ROCKSDB_WAL_ENABLED").as_deref() {
            Ok("0") | Ok("false") | Ok("FALSE") => false,
            Ok("1") | Ok("true") | Ok("TRUE") => true,
            Ok(other) => {
                tracing::warn!(
                    value = other,
                    "Unrecognized ZOMBI_ROCKSDB_WAL_ENABLED value, defaulting to enabled"
                );
                true
            }
            Err(_) => true,
        };
        tracing::info!(wal_enabled = wal_enabled, "RocksDB WAL policy configured");
        if !wal_enabled {
            tracing::warn!("WAL disabled — acknowledged writes may be lost on crash");
        }

        let storage = Self {
            db,
            sequences: DashMap::new(),
            partitions_cache: DashMap::new(),
            topics_cache: DashMap::new(),
            data_path: path.to_path_buf(),
            timestamp_index_enabled,
            bloom_filters,
            bloom_config,
            wal_enabled,
        };

        // Rebuild bloom filters from existing idempotency keys if configured
        if storage.bloom_config.enabled && storage.bloom_config.rebuild_on_startup {
            storage.rebuild_bloom_filters();
        }

        Ok(storage)
    }

    /// Creates storage with explicit timestamp index setting (for testing).
    #[cfg(test)]
    pub fn open_with_timestamp_index(
        path: impl AsRef<Path>,
        timestamp_index_enabled: bool,
    ) -> Result<Self, StorageError> {
        let mut storage = Self::open(path)?;
        storage.timestamp_index_enabled = timestamp_index_enabled;
        Ok(storage)
    }

    /// Creates storage with Bloom filters enabled (for testing).
    #[cfg(test)]
    pub fn open_with_bloom(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut storage = Self::open(path)?;
        storage.bloom_config = BloomConfig {
            enabled: true,
            expected_items: 10_000,
            fp_rate: 0.01,
            rebuild_on_startup: false,
        };
        Ok(storage)
    }

    /// Returns true if the timestamp secondary index is enabled.
    pub fn timestamp_index_enabled(&self) -> bool {
        self.timestamp_index_enabled
    }

    /// Generates a timestamp index key.
    /// Format: ts:{topic}:{partition}:{timestamp_hex}:{sequence_hex}
    fn timestamp_index_key(
        topic: &str,
        partition: u32,
        timestamp_ms: i64,
        sequence: u64,
    ) -> String {
        format!(
            "{}:{}:{}:{:016x}:{:016x}",
            TIMESTAMP_INDEX_PREFIX, topic, partition, timestamp_ms as u64, sequence
        )
    }

    /// Parses a timestamp index key to extract sequence.
    fn parse_timestamp_index_key(key: &str) -> Option<u64> {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() != 5 || parts[0] != TIMESTAMP_INDEX_PREFIX {
            return None;
        }
        u64::from_str_radix(parts[4], 16).ok()
    }

    /// Gets or creates a sequence generator for a topic/partition.
    fn get_sequence(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Arc<AtomicSequenceGenerator>, StorageError> {
        let key = (topic.to_string(), partition);

        // DashMap handles concurrent access - use entry API for atomic get-or-insert
        let entry = self.sequences.entry(key).or_try_insert_with(|| {
            // Load high watermark from DB if exists
            let hwm = self.load_high_watermark(topic, partition)?;
            Ok::<_, StorageError>(Arc::new(AtomicSequenceGenerator::starting_from(hwm)))
        })?;

        Ok(Arc::clone(entry.value()))
    }

    /// Loads the high watermark from the database.
    fn load_high_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        let key = format!("{}:{}:{}", HWM_PREFIX, topic, partition);
        Ok(self.get_u64(&key)?.unwrap_or(0))
    }

    /// Creates an event key.
    fn event_key(topic: &str, partition: u32, sequence: u64) -> String {
        format!("{}:{}:{}:{:016x}", EVENT_PREFIX, topic, partition, sequence)
    }

    /// Creates an idempotency key.
    fn idempotency_key(topic: &str, partition: u32, idem_key: &str) -> String {
        format!("{}:{}:{}:{}", IDEM_PREFIX, topic, partition, idem_key)
    }

    /// Creates a consumer offset key.
    fn consumer_offset_key(group: &str, topic: &str, partition: u32) -> String {
        format!("{}:{}:{}:{}", CONSUMER_PREFIX, group, topic, partition)
    }

    /// Parses partition number from event key suffix (after "evt:topic:" prefix).
    /// Key format: evt:topic:partition:sequence
    #[inline]
    fn parse_partition_from_key_suffix(key_suffix: &str) -> Option<u32> {
        key_suffix.split(':').next().and_then(|s| s.parse().ok())
    }

    /// Parses topic name from event key suffix (after "evt:" prefix).
    /// Key format: evt:topic:partition:sequence
    #[inline]
    fn parse_topic_from_key_suffix(key_suffix: &str) -> Option<&str> {
        key_suffix.split(':').next()
    }

    /// Parses the sequence from a full event key using the known prefix length.
    #[inline]
    fn parse_sequence_from_key_bytes(prefix_len: usize, key: &[u8]) -> Option<u64> {
        let seq_bytes = key.get(prefix_len..)?;
        let seq_str = std::str::from_utf8(seq_bytes).ok()?;
        u64::from_str_radix(seq_str, 16).ok()
    }

    /// Serializes a stored event value to bytes using the compact V1 format.
    fn serialize_event_value(event: &StoredEvent) -> Result<Vec<u8>, StorageError> {
        let value = StoredEventValueV1 {
            payload: event.payload.clone(),
            timestamp_ms: event.timestamp_ms,
            idempotency_key: event.idempotency_key.clone(),
        };
        let encoded =
            bincode::serialize(&value).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let mut bytes = Vec::with_capacity(EVENT_VALUE_MAGIC.len() + encoded.len());
        bytes.extend_from_slice(EVENT_VALUE_MAGIC);
        bytes.extend_from_slice(&encoded);
        Ok(bytes)
    }

    /// Decodes a stored event value from either the compact V1 format or legacy StoredEvent.
    fn decode_event_value(bytes: &[u8]) -> Result<StoredEventValueV1, StorageError> {
        if bytes.len() >= EVENT_VALUE_MAGIC.len() && bytes.starts_with(EVENT_VALUE_MAGIC) {
            let payload = &bytes[EVENT_VALUE_MAGIC.len()..];
            if let Ok(value) = bincode::deserialize::<StoredEventValueV1>(payload) {
                return Ok(value);
            }
        }

        let legacy: StoredEvent =
            bincode::deserialize(bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(StoredEventValueV1 {
            payload: legacy.payload,
            timestamp_ms: legacy.timestamp_ms,
            idempotency_key: legacy.idempotency_key,
        })
    }

    /// Reconstructs a StoredEvent using the key context plus decoded value.
    fn build_stored_event(
        topic: &str,
        partition: u32,
        sequence: u64,
        value: StoredEventValueV1,
    ) -> StoredEvent {
        StoredEvent {
            sequence,
            topic: topic.to_string(),
            partition,
            payload: value.payload,
            timestamp_ms: value.timestamp_ms,
            idempotency_key: value.idempotency_key,
        }
    }

    /// Registers a partition in the cache (called on write).
    /// Checks if already cached to avoid unnecessary writes (#6).
    fn register_partition(&self, topic: &str, partition: u32) {
        // Fast path: check if already cached
        if let Some(partitions) = self.partitions_cache.get(topic) {
            if partitions.contains(&partition) {
                return;
            }
        }
        // Slow path: insert into cache
        self.partitions_cache
            .entry(topic.to_string())
            .or_default()
            .insert(partition);
    }

    /// Registers a topic in the cache (called on write).
    /// Checks if already cached to avoid unnecessary writes (#6).
    fn register_topic(&self, topic: &str) {
        // Fast path: check if already cached
        if let Some(topics) = self.topics_cache.get(&()) {
            if topics.contains(topic) {
                return;
            }
        }
        // Slow path: insert into cache
        self.topics_cache
            .entry(())
            .or_default()
            .insert(topic.to_string());
    }

    /// Checks the Bloom filter for an idempotency key.
    /// Returns `true` if the key is *possibly* present (requires RocksDB confirmation).
    /// Returns `false` if the key is *definitely* absent (skip RocksDB read).
    fn bloom_maybe_contains(&self, topic: &str, partition: u32, idem_key: &str) -> bool {
        if !self.bloom_config.enabled {
            return true; // Conservative: assume present when bloom is disabled
        }
        let key = (topic.to_string(), partition);
        if let Some(entry) = self.bloom_filters.get(&key) {
            if let Ok(bloom) = entry.value().read() {
                return bloom.check(&idem_key.to_string());
            }
        }
        // No bloom filter for this partition yet — conservatively return true
        true
    }

    /// Inserts an idempotency key into the Bloom filter for the given (topic, partition).
    fn bloom_insert(&self, topic: &str, partition: u32, idem_key: &str) {
        if !self.bloom_config.enabled {
            return;
        }
        let key = (topic.to_string(), partition);
        let entry = self.bloom_filters.entry(key).or_insert_with(|| {
            RwLock::new(Bloom::new_for_fp_rate(
                self.bloom_config.expected_items,
                self.bloom_config.fp_rate,
            ))
        });
        if let Ok(mut bloom) = entry.value().write() {
            bloom.set(&idem_key.to_string());
        };
    }

    /// Rebuilds Bloom filters by scanning all `idem:` prefix keys in RocksDB.
    fn rebuild_bloom_filters(&self) {
        let prefix = format!("{}:", IDEM_PREFIX);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        let mut count: usize = 0;
        for item in iter {
            let (key, _) = match item {
                Ok(kv) => kv,
                Err(e) => {
                    tracing::warn!(error = %e, "Error during bloom rebuild scan");
                    break;
                }
            };
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(&prefix) {
                break;
            }
            // Key format: idem:{topic}:{partition}:{idempotency_key}
            let parts: Vec<&str> = key_str.splitn(4, ':').collect();
            if parts.len() == 4 {
                let topic = parts[1];
                if let Ok(partition) = parts[2].parse::<u32>() {
                    self.bloom_insert(topic, partition, parts[3]);
                    count += 1;
                }
            }
        }

        if count > 0 {
            tracing::info!(
                keys = count,
                "Rebuilt Bloom filters from existing idempotency keys"
            );
        }
    }

    /// Creates optimized write options with configurable WAL policy (#4).
    fn write_options(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        // WAL enabled by default for durability; set ZOMBI_ROCKSDB_WAL_ENABLED=false to disable
        opts.disable_wal(!self.wal_enabled);
        opts
    }

    /// Creates durable write options for small critical metadata writes.
    fn write_options_durable(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.disable_wal(false);
        opts.set_sync(true);
        opts
    }

    /// Creates optimized read options for scanning with optional upper bound (#7).
    fn read_options_with_bound(upper_bound: Option<&[u8]>) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_verify_checksums(false); // Skip checksum for speed
        opts.fill_cache(true); // Populate block cache
        if let Some(bound) = upper_bound {
            opts.set_iterate_upper_bound(bound.to_vec());
        }
        opts
    }

    /// Creates an upper bound key for a topic/partition (next partition prefix).
    fn event_upper_bound(topic: &str, partition: u32) -> Vec<u8> {
        // Upper bound is the next partition (partition + 1)
        format!("{}:{}:{}:", EVENT_PREFIX, topic, partition + 1).into_bytes()
    }

    /// Creates a key prefix for a topic/partition as bytes.
    fn event_prefix_bytes(topic: &str, partition: u32) -> Vec<u8> {
        format!("{}:{}:{}:", EVENT_PREFIX, topic, partition).into_bytes()
    }

    /// Parses a u64 from big-endian bytes.
    #[inline]
    fn parse_u64_be(bytes: &[u8]) -> Result<u64, StorageError> {
        bytes
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| StorageError::Serialization("Invalid u64 bytes".into()))
    }

    /// Gets a u64 value from the database by key.
    #[inline]
    fn get_u64(&self, key: &str) -> Result<Option<u64>, StorageError> {
        match self.db.get(key.as_bytes()) {
            Ok(Some(bytes)) => Ok(Some(Self::parse_u64_be(&bytes)?)),
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::RocksDb(e.to_string())),
        }
    }
}

impl HotStorage for RocksDbStorage {
    fn write(
        &self,
        topic: &str,
        partition: u32,
        payload: &[u8],
        timestamp_ms: i64,
        idempotency_key: Option<&str>,
    ) -> Result<u64, StorageError> {
        // Check idempotency: Bloom filter short-circuits the RocksDB read when key is absent
        if let Some(idem_key) = idempotency_key {
            if self.bloom_maybe_contains(topic, partition, idem_key) {
                if let Some(existing_offset) =
                    self.get_idempotency_offset(topic, partition, idem_key)?
                {
                    return Ok(existing_offset);
                }
            }
        }

        // Get next sequence
        let seq_gen = self.get_sequence(topic, partition)?;
        let sequence = seq_gen
            .next()
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Create stored event
        let event = StoredEvent {
            sequence,
            topic: topic.to_string(),
            partition,
            payload: payload.to_vec(),
            timestamp_ms,
            idempotency_key: idempotency_key.map(String::from),
        };

        // Serialize
        let event_bytes = Self::serialize_event_value(&event)?;

        // Use WriteBatch for atomic multi-key write (single disk operation)
        let mut batch = WriteBatch::default();

        // Add event to batch
        let event_key = Self::event_key(topic, partition, sequence);
        batch.put(event_key.as_bytes(), &event_bytes);

        // Add idempotency mapping to batch if present
        if let Some(idem_key) = idempotency_key {
            let idem_db_key = Self::idempotency_key(topic, partition, idem_key);
            batch.put(idem_db_key.as_bytes(), sequence.to_be_bytes());
            // Insert into bloom filter so future lookups can short-circuit
            self.bloom_insert(topic, partition, idem_key);
        }

        // Add high watermark update to batch
        let hwm_key = format!("{}:{}:{}", HWM_PREFIX, topic, partition);
        batch.put(hwm_key.as_bytes(), sequence.to_be_bytes());

        // Add timestamp index if enabled (for O(1) time-based queries)
        if self.timestamp_index_enabled {
            let ts_key = Self::timestamp_index_key(topic, partition, timestamp_ms, sequence);
            batch.put(ts_key.as_bytes(), sequence.to_be_bytes());
        }

        // Single atomic write for all operations (with WAL disabled for throughput #4)
        self.db
            .write_opt(batch, &self.write_options())
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Register partition and topic in cache for fast lookups
        self.register_partition(topic, partition);
        self.register_topic(topic);

        Ok(sequence)
    }

    fn write_batch(
        &self,
        topic: &str,
        events: &[crate::contracts::BulkWriteEvent],
    ) -> Result<Vec<u64>, StorageError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut sequences = Vec::with_capacity(events.len());
        let mut batch = WriteBatch::default();

        // Track high watermarks per partition
        let mut partition_hwms: std::collections::HashMap<u32, u64> =
            std::collections::HashMap::new();
        // Track partitions to register
        let mut partitions_seen: std::collections::HashSet<u32> = std::collections::HashSet::new();

        for event in events {
            // Check idempotency: Bloom filter short-circuits the RocksDB read
            if let Some(ref idem_key) = event.idempotency_key {
                if self.bloom_maybe_contains(topic, event.partition, idem_key) {
                    if let Some(existing_offset) =
                        self.get_idempotency_offset(topic, event.partition, idem_key)?
                    {
                        sequences.push(existing_offset);
                        continue;
                    }
                }
            }

            // Get next sequence for this partition
            let seq_gen = self.get_sequence(topic, event.partition)?;
            let sequence = seq_gen
                .next()
                .map_err(|e| StorageError::RocksDb(e.to_string()))?;

            // Create stored event
            let stored_event = StoredEvent {
                sequence,
                topic: topic.to_string(),
                partition: event.partition,
                payload: event.payload.clone(),
                timestamp_ms: event.timestamp_ms,
                idempotency_key: event.idempotency_key.clone(),
            };

            // Serialize and add to batch
            let event_bytes = Self::serialize_event_value(&stored_event)?;
            let event_key = Self::event_key(topic, event.partition, sequence);
            batch.put(event_key.as_bytes(), &event_bytes);

            // Add idempotency mapping if present
            if let Some(ref idem_key) = event.idempotency_key {
                let idem_db_key = Self::idempotency_key(topic, event.partition, idem_key);
                batch.put(idem_db_key.as_bytes(), sequence.to_be_bytes());
                self.bloom_insert(topic, event.partition, idem_key);
            }

            // Add timestamp index if enabled
            if self.timestamp_index_enabled {
                let ts_key =
                    Self::timestamp_index_key(topic, event.partition, event.timestamp_ms, sequence);
                batch.put(ts_key.as_bytes(), sequence.to_be_bytes());
            }

            // Track high watermark for this partition
            partition_hwms
                .entry(event.partition)
                .and_modify(|hwm| *hwm = (*hwm).max(sequence))
                .or_insert(sequence);

            partitions_seen.insert(event.partition);
            sequences.push(sequence);
        }

        // Add high watermark updates for all partitions
        for (partition, hwm) in &partition_hwms {
            let hwm_key = format!("{}:{}:{}", HWM_PREFIX, topic, partition);
            batch.put(hwm_key.as_bytes(), hwm.to_be_bytes());
        }

        // Single atomic write for ALL events (with WAL disabled for throughput)
        self.db
            .write_opt(batch, &self.write_options())
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Register partitions and topic in cache
        for partition in partitions_seen {
            self.register_partition(topic, partition);
        }
        self.register_topic(topic);

        Ok(sequences)
    }

    fn read(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        let mut events = Vec::with_capacity(limit);
        let prefix_bytes = Self::event_prefix_bytes(topic, partition);

        // Create iterator with optimized read options and upper bound (#7)
        let start_key = Self::event_key(topic, partition, offset);
        let upper_bound = Self::event_upper_bound(topic, partition);
        let read_opts = Self::read_options_with_bound(Some(&upper_bound));
        let iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward),
            read_opts,
        );
        let prefix_len = prefix_bytes.len();

        for item in iter {
            if events.len() >= limit {
                break;
            }

            let (key, value) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;

            // Byte-based prefix check (no String allocation)
            if !key.starts_with(&prefix_bytes) {
                break;
            }

            let sequence = Self::parse_sequence_from_key_bytes(prefix_len, &key)
                .ok_or_else(|| StorageError::Serialization("Invalid event key sequence".into()))?;
            let decoded = Self::decode_event_value(&value)?;
            events.push(Self::build_stored_event(
                topic, partition, sequence, decoded,
            ));
        }

        Ok(events)
    }

    fn high_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        self.load_high_watermark(topic, partition)
    }

    fn low_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        let prefix_bytes = Self::event_prefix_bytes(topic, partition);
        let upper_bound = Self::event_upper_bound(topic, partition);
        let read_opts = Self::read_options_with_bound(Some(&upper_bound));

        let iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(&prefix_bytes, rocksdb::Direction::Forward),
            read_opts,
        );

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;

            // Byte-based prefix check (no String allocation) (#3)
            if !key.starts_with(&prefix_bytes) {
                break;
            }

            // Parse sequence from key bytes (after prefix)
            // Key format: evt:topic:partition:SEQUENCE_HEX
            let seq_bytes = &key[prefix_bytes.len()..];
            if let Ok(seq_str) = std::str::from_utf8(seq_bytes) {
                if let Ok(seq) = u64::from_str_radix(seq_str, 16) {
                    return Ok(seq);
                }
            }
        }

        Ok(0)
    }

    fn get_idempotency_offset(
        &self,
        topic: &str,
        partition: u32,
        idempotency_key: &str,
    ) -> Result<Option<u64>, StorageError> {
        let key = Self::idempotency_key(topic, partition, idempotency_key);
        self.get_u64(&key)
    }

    fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), StorageError> {
        let key = Self::consumer_offset_key(group, topic, partition);
        self.db
            .put_opt(key.as_bytes(), offset.to_be_bytes(), &self.write_options())
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn get_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>, StorageError> {
        let key = Self::consumer_offset_key(group, topic, partition);
        self.get_u64(&key)
    }

    fn list_partitions(&self, topic: &str) -> Result<Vec<u32>, StorageError> {
        // Check cache first - O(1) lookup vs full table scan
        if let Some(partitions) = self.partitions_cache.get(topic) {
            let mut result: Vec<u32> = partitions.iter().copied().collect();
            result.sort();
            return Ok(result);
        }

        // Cache miss - fall back to scan (populates cache for next time)
        let mut partitions = HashSet::new();
        let prefix = format!("{}:{}:", EVENT_PREFIX, topic);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);

            if !key_str.starts_with(&prefix) {
                break;
            }

            if let Some(rest) = key_str.strip_prefix(&prefix) {
                if let Some(partition) = Self::parse_partition_from_key_suffix(rest) {
                    partitions.insert(partition);
                }
            }
        }

        // Populate cache for future calls
        if !partitions.is_empty() {
            self.partitions_cache
                .entry(topic.to_string())
                .or_default()
                .extend(partitions.iter().copied());
        }

        let mut result: Vec<u32> = partitions.into_iter().collect();
        result.sort();
        Ok(result)
    }

    fn list_topics(&self) -> Result<Vec<String>, StorageError> {
        // Check cache first - O(1) lookup vs full table scan (#4)
        if let Some(topics) = self.topics_cache.get(&()) {
            let mut result: Vec<String> = topics.iter().cloned().collect();
            result.sort();
            return Ok(result);
        }

        // Cache miss - fall back to scan (populates cache for next time)
        let mut topics = HashSet::new();
        let prefix = format!("{}:", EVENT_PREFIX);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);

            if !key_str.starts_with(&prefix) {
                break;
            }

            if let Some(rest) = key_str.strip_prefix(&prefix) {
                if let Some(topic) = Self::parse_topic_from_key_suffix(rest) {
                    topics.insert(topic.to_string());
                }
            }
        }

        // Populate cache for future calls
        if !topics.is_empty() {
            self.topics_cache
                .entry(())
                .or_default()
                .extend(topics.iter().cloned());
        }

        let mut result: Vec<String> = topics.into_iter().collect();
        result.sort();
        Ok(result)
    }

    fn read_all_partitions(
        &self,
        topic: &str,
        start_offsets: Option<&HashMap<u32, u64>>,
        start_timestamp_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        let partitions = self.list_partitions(topic)?;

        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        // Read from all partitions in parallel using rayon
        let per_partition_limit = limit.saturating_mul(2).max(1000);

        let results: Vec<Result<Vec<StoredEvent>, StorageError>> = partitions
            .par_iter()
            .map(|&partition| {
                // Use start_offset if provided, otherwise start from 0
                let offset = start_offsets
                    .and_then(|offsets| offsets.get(&partition).copied())
                    .unwrap_or(0);
                self.read(topic, partition, offset, per_partition_limit)
            })
            .collect();

        // Collect results, propagating any errors
        let mut all_events = Vec::new();
        for result in results {
            all_events.extend(result?);
        }

        // Filter by timestamp if specified
        if let Some(start_ts) = start_timestamp_ms {
            all_events.retain(|e| e.timestamp_ms >= start_ts);
        }

        // Sort by timestamp
        all_events.sort_by_key(|e| e.timestamp_ms);

        // Apply limit
        all_events.truncate(limit);

        Ok(all_events)
    }

    fn read_by_timestamp(
        &self,
        topic: &str,
        partition: u32,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        // If timestamp index is not enabled, fall back to full scan
        if !self.timestamp_index_enabled {
            return self.read_by_timestamp_fallback(topic, partition, since_ms, until_ms, limit);
        }

        // Build the key prefix for the timestamp range
        let prefix = format!("{}:{}:{}:", TIMESTAMP_INDEX_PREFIX, topic, partition);

        // Build start and end keys based on time range
        let start_key = match since_ms {
            Some(ts) => format!("{}{:016x}:", prefix, ts as u64),
            None => prefix.clone(),
        };

        let end_key = match until_ms {
            Some(ts) => format!("{}{:016x}:", prefix, ts as u64),
            None => format!("{}g", prefix), // 'g' > any hex char
        };

        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_lower_bound(start_key.as_bytes());
        read_opts.set_iterate_upper_bound(end_key.as_bytes());

        let iter = self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts);

        let mut sequences = Vec::new();
        for item in iter {
            let (key, _value) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);

            if let Some(seq) = Self::parse_timestamp_index_key(&key_str) {
                sequences.push(seq);
                if sequences.len() >= limit {
                    break;
                }
            }
        }

        // Now read the actual events by sequence
        let mut events = Vec::with_capacity(sequences.len());
        for seq in sequences {
            let event_key = Self::event_key(topic, partition, seq);
            if let Some(bytes) = self
                .db
                .get(event_key.as_bytes())
                .map_err(|e| StorageError::RocksDb(e.to_string()))?
            {
                let decoded = Self::decode_event_value(&bytes)?;
                events.push(Self::build_stored_event(topic, partition, seq, decoded));
            }
        }

        Ok(events)
    }

    fn save_flush_watermark(
        &self,
        topic: &str,
        partition: u32,
        watermark: u64,
    ) -> Result<(), StorageError> {
        let key = format!("{}:{}:{}", FLUSH_WM_PREFIX, topic, partition);
        self.db
            .put_opt(
                key.as_bytes(),
                watermark.to_be_bytes(),
                &self.write_options_durable(),
            )
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn load_flush_watermark(&self, topic: &str, partition: u32) -> Result<u64, StorageError> {
        let key = format!("{}:{}:{}", FLUSH_WM_PREFIX, topic, partition);
        Ok(self.get_u64(&key)?.unwrap_or(0))
    }
}

impl RocksDbStorage {
    /// Fallback for read_by_timestamp when index is not enabled.
    fn read_by_timestamp_fallback(
        &self,
        topic: &str,
        partition: u32,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        // Fall back to scanning events and filtering
        let events = self.read(topic, partition, 0, limit * 10)?;
        Ok(events
            .into_iter()
            .filter(|e| {
                since_ms.is_none_or(|s| e.timestamp_ms >= s)
                    && until_ms.is_none_or(|u| e.timestamp_ms < u)
            })
            .take(limit)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_rocksdb_env() {
        std::env::remove_var("ZOMBI_ROCKSDB_WRITE_BUFFER_MB");
        std::env::remove_var("ZOMBI_ROCKSDB_MAX_WRITE_BUFFERS");
        std::env::remove_var("ZOMBI_ROCKSDB_L0_COMPACTION_TRIGGER");
        std::env::remove_var("ZOMBI_ROCKSDB_TARGET_FILE_SIZE_MB");
        std::env::remove_var("ZOMBI_ROCKSDB_BLOCK_CACHE_MB");
        std::env::remove_var("ZOMBI_ROCKSDB_BLOCK_SIZE_KB");
        std::env::remove_var("ZOMBI_ROCKSDB_WAL_ENABLED");
    }

    fn create_test_storage() -> (RocksDbStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open(dir.path()).unwrap();
        (storage, dir)
    }

    #[test]
    fn rocksdb_config_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_rocksdb_env();
        let config = RocksDbConfig::from_env();
        assert_eq!(config.write_buffer_size_bytes, 64 * MB);
        assert_eq!(config.max_write_buffers, 3);
        assert_eq!(config.l0_compaction_trigger, 4);
        assert_eq!(config.target_file_size_base_bytes, (64 * MB) as u64);
        assert_eq!(config.block_cache_size_bytes, 128 * MB);
        assert_eq!(config.block_size_bytes, 16 * KB);
    }

    #[test]
    fn rocksdb_config_from_env_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_rocksdb_env();
        std::env::set_var("ZOMBI_ROCKSDB_WRITE_BUFFER_MB", "128");
        std::env::set_var("ZOMBI_ROCKSDB_MAX_WRITE_BUFFERS", "6");
        std::env::set_var("ZOMBI_ROCKSDB_L0_COMPACTION_TRIGGER", "8");
        std::env::set_var("ZOMBI_ROCKSDB_TARGET_FILE_SIZE_MB", "96");
        std::env::set_var("ZOMBI_ROCKSDB_BLOCK_CACHE_MB", "256");
        std::env::set_var("ZOMBI_ROCKSDB_BLOCK_SIZE_KB", "32");

        let config = RocksDbConfig::from_env();
        assert_eq!(config.write_buffer_size_bytes, 128 * MB);
        assert_eq!(config.max_write_buffers, 6);
        assert_eq!(config.l0_compaction_trigger, 8);
        assert_eq!(config.target_file_size_base_bytes, (96 * MB) as u64);
        assert_eq!(config.block_cache_size_bytes, 256 * MB);
        assert_eq!(config.block_size_bytes, 32 * KB);

        clear_rocksdb_env();
    }

    #[test]
    fn rocksdb_config_ignores_invalid_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_rocksdb_env();
        std::env::set_var("ZOMBI_ROCKSDB_WRITE_BUFFER_MB", "0");
        std::env::set_var("ZOMBI_ROCKSDB_MAX_WRITE_BUFFERS", "-1");
        std::env::set_var("ZOMBI_ROCKSDB_L0_COMPACTION_TRIGGER", "nope");
        std::env::set_var("ZOMBI_ROCKSDB_TARGET_FILE_SIZE_MB", "");
        std::env::set_var("ZOMBI_ROCKSDB_BLOCK_CACHE_MB", "0");
        std::env::set_var("ZOMBI_ROCKSDB_BLOCK_SIZE_KB", "0");

        let config = RocksDbConfig::from_env();
        assert_eq!(config.write_buffer_size_bytes, 64 * MB);
        assert_eq!(config.max_write_buffers, 3);
        assert_eq!(config.l0_compaction_trigger, 4);
        assert_eq!(config.target_file_size_base_bytes, (64 * MB) as u64);
        assert_eq!(config.block_cache_size_bytes, 128 * MB);
        assert_eq!(config.block_size_bytes, 16 * KB);

        clear_rocksdb_env();
    }

    #[test]
    fn event_value_v1_round_trip() {
        let event = StoredEvent {
            sequence: 42,
            topic: "events".into(),
            partition: 1,
            payload: vec![1, 2, 3],
            timestamp_ms: 123456789,
            idempotency_key: Some("idem-1".into()),
        };

        let bytes = RocksDbStorage::serialize_event_value(&event).unwrap();
        assert!(bytes.starts_with(EVENT_VALUE_MAGIC));

        let decoded = RocksDbStorage::decode_event_value(&bytes).unwrap();
        let rebuilt = RocksDbStorage::build_stored_event("events", 1, event.sequence, decoded);

        assert_eq!(rebuilt.sequence, event.sequence);
        assert_eq!(rebuilt.topic, event.topic);
        assert_eq!(rebuilt.partition, event.partition);
        assert_eq!(rebuilt.payload, event.payload);
        assert_eq!(rebuilt.timestamp_ms, event.timestamp_ms);
        assert_eq!(rebuilt.idempotency_key, event.idempotency_key);
    }

    #[test]
    fn event_value_decodes_legacy_payload() {
        let legacy = StoredEvent {
            sequence: 7,
            topic: "legacy".into(),
            partition: 9,
            payload: vec![4, 5],
            timestamp_ms: 55,
            idempotency_key: None,
        };

        let bytes = bincode::serialize(&legacy).unwrap();
        let decoded = RocksDbStorage::decode_event_value(&bytes).unwrap();
        let rebuilt = RocksDbStorage::build_stored_event("override", 2, 99, decoded);

        assert_eq!(rebuilt.sequence, 99);
        assert_eq!(rebuilt.topic, "override");
        assert_eq!(rebuilt.partition, 2);
        assert_eq!(rebuilt.payload, legacy.payload);
        assert_eq!(rebuilt.timestamp_ms, legacy.timestamp_ms);
        assert_eq!(rebuilt.idempotency_key, legacy.idempotency_key);
    }

    #[test]
    fn parse_sequence_from_key_bytes() {
        let key = RocksDbStorage::event_key("topic", 3, 0x1a2b);
        let prefix = RocksDbStorage::event_prefix_bytes("topic", 3);
        let seq = RocksDbStorage::parse_sequence_from_key_bytes(prefix.len(), key.as_bytes())
            .expect("sequence should parse");
        assert_eq!(seq, 0x1a2b);
    }

    #[test]
    fn write_and_read_single_event() {
        let (storage, _dir) = create_test_storage();

        let offset = storage
            .write("test-topic", 0, b"hello world", 1234567890, None)
            .unwrap();

        assert_eq!(offset, 1);

        let events = storage.read("test-topic", 0, 1, 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"hello world");
        assert_eq!(events[0].timestamp_ms, 1234567890);
        assert_eq!(events[0].sequence, 1);
    }

    #[test]
    fn write_returns_monotonic_sequences() {
        let (storage, _dir) = create_test_storage();

        let mut prev = 0;
        for i in 0..100 {
            let offset = storage
                .write("test-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
                .unwrap();
            assert!(offset > prev, "Expected {} > {}", offset, prev);
            prev = offset;
        }
    }

    #[test]
    fn idempotent_writes_return_same_offset() {
        let (storage, _dir) = create_test_storage();

        let offset1 = storage
            .write("test-topic", 0, b"payload", 0, Some("req-123"))
            .unwrap();

        let offset2 = storage
            .write("test-topic", 0, b"payload", 0, Some("req-123"))
            .unwrap();

        assert_eq!(offset1, offset2);

        // Should only have one event
        let events = storage.read("test-topic", 0, 0, 100).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn partitions_are_isolated() {
        let (storage, _dir) = create_test_storage();

        storage
            .write("test-topic", 0, b"partition-0", 0, None)
            .unwrap();
        storage
            .write("test-topic", 1, b"partition-1", 0, None)
            .unwrap();

        let events_0 = storage.read("test-topic", 0, 0, 100).unwrap();
        let events_1 = storage.read("test-topic", 1, 0, 100).unwrap();

        assert_eq!(events_0.len(), 1);
        assert_eq!(events_1.len(), 1);
        assert_eq!(events_0[0].payload, b"partition-0");
        assert_eq!(events_1[0].payload, b"partition-1");
    }

    #[test]
    fn high_watermark_tracks_latest_offset() {
        let (storage, _dir) = create_test_storage();

        assert_eq!(storage.high_watermark("test-topic", 0).unwrap(), 0);

        storage.write("test-topic", 0, b"event1", 0, None).unwrap();
        assert_eq!(storage.high_watermark("test-topic", 0).unwrap(), 1);

        storage.write("test-topic", 0, b"event2", 0, None).unwrap();
        storage.write("test-topic", 0, b"event3", 0, None).unwrap();
        assert_eq!(storage.high_watermark("test-topic", 0).unwrap(), 3);
    }

    #[test]
    fn read_respects_limit() {
        let (storage, _dir) = create_test_storage();

        for i in 0..10 {
            storage
                .write("test-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
                .unwrap();
        }

        let events = storage.read("test-topic", 0, 1, 3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[2].sequence, 3);
    }

    #[test]
    fn read_from_middle_offset() {
        let (storage, _dir) = create_test_storage();

        for i in 0..10 {
            storage
                .write("test-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
                .unwrap();
        }

        let events = storage.read("test-topic", 0, 5, 100).unwrap();
        assert_eq!(events.len(), 6); // Events 5-10
        assert_eq!(events[0].sequence, 5);
    }

    #[test]
    fn commit_and_get_consumer_offset() {
        let (storage, _dir) = create_test_storage();

        // Initially no offset
        assert_eq!(
            storage.get_offset("my-group", "test-topic", 0).unwrap(),
            None
        );

        // Commit offset
        storage
            .commit_offset("my-group", "test-topic", 0, 100)
            .unwrap();

        // Should be able to read it back
        assert_eq!(
            storage.get_offset("my-group", "test-topic", 0).unwrap(),
            Some(100)
        );

        // Update offset
        storage
            .commit_offset("my-group", "test-topic", 0, 200)
            .unwrap();
        assert_eq!(
            storage.get_offset("my-group", "test-topic", 0).unwrap(),
            Some(200)
        );
    }

    #[test]
    fn consumer_offsets_are_isolated_by_group() {
        let (storage, _dir) = create_test_storage();

        storage
            .commit_offset("group-a", "test-topic", 0, 100)
            .unwrap();
        storage
            .commit_offset("group-b", "test-topic", 0, 200)
            .unwrap();

        assert_eq!(
            storage.get_offset("group-a", "test-topic", 0).unwrap(),
            Some(100)
        );
        assert_eq!(
            storage.get_offset("group-b", "test-topic", 0).unwrap(),
            Some(200)
        );
    }

    // =========================================================================
    // Edge Case Tests
    // =========================================================================

    #[test]
    fn empty_payload_is_valid() {
        let (storage, _dir) = create_test_storage();
        let offset = storage.write("test-topic", 0, b"", 0, None).unwrap();
        let events = storage.read("test-topic", 0, offset, 1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"");
    }

    #[test]
    fn large_payload_1mb() {
        let (storage, _dir) = create_test_storage();
        let payload = vec![0u8; 1_000_000]; // 1MB
        let offset = storage.write("test-topic", 0, &payload, 0, None).unwrap();
        let events = storage.read("test-topic", 0, offset, 1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload.len(), 1_000_000);
    }

    #[test]
    fn special_characters_in_topic_name() {
        let (storage, _dir) = create_test_storage();
        let offset = storage.write("topic-with_special.chars", 0, b"data", 0, None);
        assert!(offset.is_ok());

        let events = storage.read("topic-with_special.chars", 0, 0, 100).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn read_empty_topic_returns_empty() {
        let (storage, _dir) = create_test_storage();
        let events = storage.read("nonexistent", 0, 0, 100).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn read_empty_partition_returns_empty() {
        let (storage, _dir) = create_test_storage();
        // Write to partition 0
        storage.write("test-topic", 0, b"data", 0, None).unwrap();
        // Read from partition 1 (empty)
        let events = storage.read("test-topic", 1, 0, 100).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn read_beyond_high_watermark_returns_empty() {
        let (storage, _dir) = create_test_storage();
        for _ in 0..10 {
            storage.write("test-topic", 0, b"data", 0, None).unwrap();
        }
        // Read from offset 100 (beyond high watermark of 10)
        let events = storage.read("test-topic", 0, 100, 100).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn zero_limit_returns_empty() {
        let (storage, _dir) = create_test_storage();
        storage.write("test-topic", 0, b"data", 0, None).unwrap();
        let events = storage.read("test-topic", 0, 0, 0).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn binary_payload_preserved() {
        let (storage, _dir) = create_test_storage();
        // Binary data with null bytes and all byte values
        let payload: Vec<u8> = (0..=255).collect();
        let offset = storage.write("test-topic", 0, &payload, 0, None).unwrap();
        let events = storage.read("test-topic", 0, offset, 1).unwrap();
        assert_eq!(events[0].payload, payload);
    }

    #[test]
    fn unicode_topic_name() {
        let (storage, _dir) = create_test_storage();
        let offset = storage.write("日本語-topic-émoji-🎉", 0, b"data", 0, None);
        assert!(offset.is_ok());

        let events = storage.read("日本語-topic-émoji-🎉", 0, 0, 100).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn very_long_topic_name() {
        let (storage, _dir) = create_test_storage();
        let long_topic = "a".repeat(1000);
        let offset = storage.write(&long_topic, 0, b"data", 0, None);
        assert!(offset.is_ok());

        let events = storage.read(&long_topic, 0, 0, 100).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn very_long_idempotency_key() {
        let (storage, _dir) = create_test_storage();
        let long_key = "k".repeat(1000);
        let offset1 = storage
            .write("test-topic", 0, b"data", 0, Some(&long_key))
            .unwrap();
        let offset2 = storage
            .write("test-topic", 0, b"data", 0, Some(&long_key))
            .unwrap();
        assert_eq!(offset1, offset2);
    }

    #[test]
    fn max_timestamp() {
        let (storage, _dir) = create_test_storage();
        let offset = storage
            .write("test-topic", 0, b"data", i64::MAX, None)
            .unwrap();
        let events = storage.read("test-topic", 0, offset, 1).unwrap();
        assert_eq!(events[0].timestamp_ms, i64::MAX);
    }

    #[test]
    fn high_partition_number() {
        let (storage, _dir) = create_test_storage();
        let offset = storage.write("test-topic", 1000, b"data", 0, None).unwrap();
        let events = storage.read("test-topic", 1000, offset, 1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].partition, 1000);
    }

    #[test]
    fn consumer_offset_zero_is_valid() {
        let (storage, _dir) = create_test_storage();
        storage
            .commit_offset("test-group", "test-topic", 0, 0)
            .unwrap();
        let offset = storage.get_offset("test-group", "test-topic", 0).unwrap();
        assert_eq!(offset, Some(0));
    }

    #[test]
    fn consumer_offset_max_value() {
        let (storage, _dir) = create_test_storage();
        storage
            .commit_offset("test-group", "test-topic", 0, u64::MAX)
            .unwrap();
        let offset = storage.get_offset("test-group", "test-topic", 0).unwrap();
        assert_eq!(offset, Some(u64::MAX));
    }

    #[test]
    fn timestamp_index_key_format() {
        let key = RocksDbStorage::timestamp_index_key("events", 0, 1234567890000, 42);
        assert!(key.starts_with("ts:events:0:"));
        assert!(key.ends_with(":000000000000002a")); // 42 in hex
    }

    #[test]
    fn timestamp_index_key_parsing() {
        let seq = RocksDbStorage::parse_timestamp_index_key(
            "ts:events:0:0000011f71fb0470:000000000000002a",
        );
        assert_eq!(seq, Some(42));
    }

    #[test]
    fn read_by_timestamp_fallback() {
        // Test the fallback path when timestamp index is disabled
        let (storage, _dir) = create_test_storage();
        assert!(!storage.timestamp_index_enabled());

        // Write events with different timestamps
        storage.write("test", 0, b"a", 1000, None).unwrap();
        storage.write("test", 0, b"b", 2000, None).unwrap();
        storage.write("test", 0, b"c", 3000, None).unwrap();

        // Read by timestamp range
        let events = storage
            .read_by_timestamp("test", 0, Some(1500), Some(2500), 10)
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"b");
    }

    #[test]
    fn read_by_timestamp_with_index() {
        // Test with timestamp index enabled
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open_with_timestamp_index(dir.path(), true).unwrap();
        assert!(storage.timestamp_index_enabled());

        // Write events with different timestamps
        storage.write("test", 0, b"a", 1000, None).unwrap();
        storage.write("test", 0, b"b", 2000, None).unwrap();
        storage.write("test", 0, b"c", 3000, None).unwrap();

        // Read by timestamp range using index
        let events = storage
            .read_by_timestamp("test", 0, Some(1500), Some(2500), 10)
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"b");
    }

    // =========================================================================
    // Bloom Filter Tests
    // =========================================================================

    fn create_bloom_storage() -> (RocksDbStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open_with_bloom(dir.path()).unwrap();
        (storage, dir)
    }

    #[test]
    fn bloom_unseen_key_returns_not_present() {
        let (storage, _dir) = create_bloom_storage();
        // Insert a key to create the bloom filter for this partition
        storage.bloom_insert("test", 0, "existing-key");
        // A different key should not be found
        assert!(!storage.bloom_maybe_contains("test", 0, "absent-key"));
    }

    #[test]
    fn bloom_inserted_key_returns_maybe_present() {
        let (storage, _dir) = create_bloom_storage();
        storage.bloom_insert("test", 0, "my-key");
        assert!(storage.bloom_maybe_contains("test", 0, "my-key"));
    }

    #[test]
    fn bloom_idempotent_writes_still_correct() {
        let (storage, _dir) = create_bloom_storage();

        let offset1 = storage
            .write("test-topic", 0, b"payload", 0, Some("req-123"))
            .unwrap();

        let offset2 = storage
            .write("test-topic", 0, b"payload", 0, Some("req-123"))
            .unwrap();

        assert_eq!(offset1, offset2);

        // Should only have one event
        let events = storage.read("test-topic", 0, 0, 100).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn bloom_unique_keys_skip_rocksdb() {
        let (storage, _dir) = create_bloom_storage();

        // Write 100 unique keys — bloom should allow all without RocksDB reads
        for i in 0..100 {
            let key = format!("unique-{}", i);
            let offset = storage
                .write("test-topic", 0, b"data", 0, Some(&key))
                .unwrap();
            assert_eq!(offset, (i + 1) as u64);
        }
    }

    #[test]
    fn bloom_rebuild_on_reopen() {
        let dir = TempDir::new().unwrap();

        // First: open, write with idempotency keys
        {
            let storage = RocksDbStorage::open_with_bloom(dir.path()).unwrap();
            storage
                .write("test-topic", 0, b"data1", 0, Some("key-1"))
                .unwrap();
            storage
                .write("test-topic", 0, b"data2", 0, Some("key-2"))
                .unwrap();
        }

        // Second: reopen with rebuild — bloom should have existing keys
        {
            let mut storage = RocksDbStorage::open(dir.path()).unwrap();
            storage.bloom_config = BloomConfig {
                enabled: true,
                expected_items: 10_000,
                fp_rate: 0.01,
                rebuild_on_startup: true,
            };
            storage.rebuild_bloom_filters();

            assert!(storage.bloom_maybe_contains("test-topic", 0, "key-1"));
            assert!(storage.bloom_maybe_contains("test-topic", 0, "key-2"));

            // Idempotent write should return same offset
            let offset = storage
                .write("test-topic", 0, b"data1", 0, Some("key-1"))
                .unwrap();
            assert_eq!(offset, 1);
        }
    }

    #[test]
    fn wal_enabled_by_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_rocksdb_env();
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open(dir.path()).unwrap();
        assert!(storage.wal_enabled);
    }

    #[test]
    fn wal_can_be_disabled_via_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_rocksdb_env();
        std::env::set_var("ZOMBI_ROCKSDB_WAL_ENABLED", "false");
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open(dir.path()).unwrap();
        assert!(!storage.wal_enabled);
    }

    #[test]
    fn wal_disabled_with_zero() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_rocksdb_env();
        std::env::set_var("ZOMBI_ROCKSDB_WAL_ENABLED", "0");
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open(dir.path()).unwrap();
        assert!(!storage.wal_enabled);
    }

    #[test]
    fn wal_treats_unknown_value_as_enabled() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_rocksdb_env();
        std::env::set_var("ZOMBI_ROCKSDB_WAL_ENABLED", "yes");
        let dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::open(dir.path()).unwrap();
        assert!(storage.wal_enabled);
    }

    mod flush_watermark_tests {
        use super::*;

        #[test]
        fn save_and_load_flush_watermark() {
            let (storage, _dir) = create_test_storage();
            // Initially 0
            assert_eq!(storage.load_flush_watermark("events", 0).unwrap(), 0);
            // Save
            storage.save_flush_watermark("events", 0, 42).unwrap();
            assert_eq!(storage.load_flush_watermark("events", 0).unwrap(), 42);
            // Update
            storage.save_flush_watermark("events", 0, 100).unwrap();
            assert_eq!(storage.load_flush_watermark("events", 0).unwrap(), 100);
            // Different partition is isolated
            assert_eq!(storage.load_flush_watermark("events", 1).unwrap(), 0);
            // Different topic is isolated
            assert_eq!(storage.load_flush_watermark("other", 0).unwrap(), 0);
        }

        #[test]
        fn flush_watermark_survives_reopen() {
            let dir = TempDir::new().unwrap();
            {
                let storage = RocksDbStorage::open(dir.path()).unwrap();
                storage.save_flush_watermark("events", 0, 55).unwrap();
                storage.save_flush_watermark("events", 1, 77).unwrap();
            }
            {
                let storage = RocksDbStorage::open(dir.path()).unwrap();
                assert_eq!(storage.load_flush_watermark("events", 0).unwrap(), 55);
                assert_eq!(storage.load_flush_watermark("events", 1).unwrap(), 77);
            }
        }
    }
    }
}
