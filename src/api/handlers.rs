use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use futures::future::join_all;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::contracts::{ColdStorage, ColumnProjection, HotStorage, StorageError, KNOWN_COLUMNS};
use crate::metrics::MetricsRegistry;
use crate::proto;

/// Server metrics for monitoring.
#[derive(Default)]
pub struct Metrics {
    pub writes_total: AtomicU64,
    pub writes_bytes: AtomicU64,
    pub reads_total: AtomicU64,
    pub read_records_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub write_latency_sum_us: AtomicU64,
    pub read_latency_sum_us: AtomicU64,
    pub start_time: std::sync::OnceLock<Instant>,
}

impl Metrics {
    pub fn new() -> Self {
        let m = Self::default();
        let _ = m.start_time.set(Instant::now());
        m
    }

    pub fn record_write(&self, bytes: u64, latency_us: u64) {
        self.writes_total.fetch_add(1, Ordering::Relaxed);
        self.writes_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.write_latency_sum_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_read(&self, records: u64, latency_us: u64) {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        self.read_records_total
            .fetch_add(records, Ordering::Relaxed);
        self.read_latency_sum_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Configuration for write backpressure.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum number of concurrent inflight writes.
    /// When exceeded, new writes return 503.
    /// Default: 10,000
    pub max_inflight_writes: usize,
    /// Maximum bytes of inflight write payloads.
    /// When exceeded, new writes return 503.
    /// Default: 64MB
    pub max_inflight_bytes: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_inflight_writes: 10_000,
            max_inflight_bytes: 64 * 1024 * 1024, // 64MB
        }
    }
}

impl BackpressureConfig {
    /// Creates a config from environment variables.
    ///
    /// Reads:
    /// - `ZOMBI_MAX_INFLIGHT_WRITES`: Max concurrent writes (default: 10000)
    /// - `ZOMBI_MAX_INFLIGHT_BYTES_MB`: Max inflight bytes in MB (default: 64)
    pub fn from_env() -> Self {
        let default = Self::default();

        let max_inflight_writes = std::env::var("ZOMBI_MAX_INFLIGHT_WRITES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default.max_inflight_writes);

        let max_inflight_bytes = std::env::var("ZOMBI_MAX_INFLIGHT_BYTES_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(default.max_inflight_bytes);

        Self {
            max_inflight_writes,
            max_inflight_bytes,
        }
    }
}

/// Application state shared across handlers.
pub struct AppState<H: HotStorage, C: ColdStorage = NoopColdStorage> {
    pub storage: Arc<H>,
    pub cold_storage: Option<Arc<C>>,
    pub metrics: Arc<Metrics>,
    pub metrics_registry: Arc<MetricsRegistry>,
    /// Semaphore for limiting concurrent writes
    pub write_semaphore: Arc<Semaphore>,
    /// Current inflight bytes (for byte-based backpressure)
    pub inflight_bytes: AtomicU64,
    /// Maximum inflight bytes before rejecting writes
    pub max_inflight_bytes: u64,
}

impl<H: HotStorage, C: ColdStorage> AppState<H, C> {
    /// Creates a new AppState with backpressure configuration.
    pub fn new(
        storage: Arc<H>,
        cold_storage: Option<Arc<C>>,
        metrics: Arc<Metrics>,
        metrics_registry: Arc<MetricsRegistry>,
        backpressure: BackpressureConfig,
    ) -> Self {
        Self {
            storage,
            cold_storage,
            metrics,
            metrics_registry,
            write_semaphore: Arc::new(Semaphore::new(backpressure.max_inflight_writes)),
            inflight_bytes: AtomicU64::new(0),
            max_inflight_bytes: backpressure.max_inflight_bytes as u64,
        }
    }

    /// Tries to acquire backpressure permits for a write of the given size.
    /// Returns an error if the server is overloaded.
    fn try_acquire_write_permit(&self, bytes: u64) -> Result<WritePermit<'_>, StorageError> {
        // Try to acquire the count-based semaphore permit
        let permit = self
            .write_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                StorageError::Overloaded(format!(
                    "Too many concurrent writes (limit: {})",
                    self.write_semaphore.available_permits() + 1 // Add 1 because we just tried to acquire
                ))
            })?;

        // Check byte-based limit
        let current_bytes = self.inflight_bytes.fetch_add(bytes, Ordering::SeqCst);
        if current_bytes + bytes > self.max_inflight_bytes {
            // Rollback the byte count
            self.inflight_bytes.fetch_sub(bytes, Ordering::SeqCst);
            // Permit will be dropped automatically
            drop(permit);
            return Err(StorageError::Overloaded(format!(
                "Too many inflight bytes ({} + {} > {} limit)",
                current_bytes, bytes, self.max_inflight_bytes
            )));
        }

        Ok(WritePermit {
            _permit: permit,
            bytes,
            inflight_bytes: &self.inflight_bytes,
        })
    }
}

/// RAII guard for write permit - releases bytes when dropped.
struct WritePermit<'a> {
    _permit: tokio::sync::OwnedSemaphorePermit,
    bytes: u64,
    inflight_bytes: &'a AtomicU64,
}

impl Drop for WritePermit<'_> {
    fn drop(&mut self) {
        self.inflight_bytes.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// No-op cold storage for when S3 is not configured.
pub struct NoopColdStorage;

impl ColdStorage for NoopColdStorage {
    async fn write_segment(
        &self,
        _topic: &str,
        _partition: u32,
        _events: &[crate::contracts::StoredEvent],
    ) -> Result<String, StorageError> {
        Err(StorageError::S3("Cold storage not configured".into()))
    }

    async fn read_events(
        &self,
        _topic: &str,
        _partition: u32,
        _start_offset: u64,
        _limit: usize,
        _since_ms: Option<i64>,
        _until_ms: Option<i64>,
        _projection: &crate::contracts::ColumnProjection,
    ) -> Result<Vec<crate::contracts::StoredEvent>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_segments(
        &self,
        _topic: &str,
        _partition: u32,
    ) -> Result<Vec<crate::contracts::SegmentInfo>, StorageError> {
        Ok(Vec::new())
    }

    fn storage_info(&self) -> crate::contracts::ColdStorageInfo {
        crate::contracts::ColdStorageInfo {
            storage_type: "none".into(),
            iceberg_enabled: false,
            bucket: String::new(),
            base_path: String::new(),
        }
    }
}

/// Request body for writing records.
#[derive(Debug, Deserialize)]
pub struct WriteRecordRequest {
    pub payload: String,
    #[serde(default)]
    pub partition: u32,
    pub timestamp_ms: Option<i64>,
    pub idempotency_key: Option<String>,
}

/// Response for write operations.
#[derive(Debug, Serialize)]
pub struct WriteRecordResponse {
    pub offset: u64,
    pub partition: u32,
    pub table: String,
}

/// Request body for bulk write operations (#1).
#[derive(Debug, Deserialize)]
pub struct BulkWriteRequest {
    pub records: Vec<BulkWriteRecord>,
}

/// Single record in bulk write request.
#[derive(Debug, Deserialize)]
pub struct BulkWriteRecord {
    pub payload: String,
    #[serde(default)]
    pub partition: u32,
    pub timestamp_ms: Option<i64>,
    pub idempotency_key: Option<String>,
}

/// Response for bulk write operations.
#[derive(Debug, Serialize)]
pub struct BulkWriteResponse {
    pub offsets: Vec<u64>,
    pub count: usize,
    pub table: String,
}

/// Query parameters for reading records.
#[derive(Debug, Deserialize)]
pub struct ReadRecordsQuery {
    /// Starting timestamp in milliseconds (for time-based reads)
    pub since: Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Comma-separated list of columns to project (e.g. "payload,timestamp_ms").
    /// When omitted, all columns are returned.
    pub fields: Option<String>,
}

fn default_limit() -> usize {
    100
}

/// Response for read operations.
#[derive(Debug, Serialize)]
pub struct ReadRecordsResponse {
    pub records: Vec<serde_json::Value>,
    pub count: usize,
    pub has_more: bool,
}

/// Error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// API error type.
#[allow(dead_code)]
pub enum ApiError {
    Storage(StorageError),
    BadRequest(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_response) = match self {
            ApiError::Storage(StorageError::TopicNotFound(topic)) => (
                StatusCode::NOT_FOUND,
                ErrorResponse {
                    error: format!("Topic not found: {}", topic),
                    code: "TOPIC_NOT_FOUND".into(),
                },
            ),
            ApiError::Storage(StorageError::OffsetOutOfRange {
                requested,
                low,
                high,
            }) => (
                StatusCode::NOT_FOUND,
                ErrorResponse {
                    error: format!(
                        "Offset {} out of range. Available: {}..{}",
                        requested, low, high
                    ),
                    code: "OFFSET_OUT_OF_RANGE".into(),
                },
            ),
            ApiError::Storage(StorageError::Overloaded(msg)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    error: msg,
                    code: "SERVER_OVERLOADED".into(),
                },
            ),
            ApiError::Storage(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse {
                    error: e.to_string(),
                    code: "STORAGE_ERROR".into(),
                },
            ),
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    error: msg,
                    code: "BAD_REQUEST".into(),
                },
            ),
        };

        (status, Json(error_response)).into_response()
    }
}

impl From<StorageError> for ApiError {
    fn from(e: StorageError) -> Self {
        ApiError::Storage(e)
    }
}

/// POST /tables/{table}
/// Write a record to a table.
/// Accepts either:
/// - Content-Type: application/x-protobuf (protobuf Event)
/// - Content-Type: application/json (JSON WriteRecordRequest)
pub async fn write_record<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<WriteRecordResponse>), ApiError> {
    let start = Instant::now();
    let body_len = body.len() as u64;

    // Check backpressure against raw body size (not parsed payload size) so we
    // reject oversized requests before spending CPU on JSON deserialization.
    let _permit = state.try_acquire_write_permit(body_len).map_err(|e| {
        state.metrics.record_error();
        state
            .metrics_registry
            .enhanced_api
            .record_backpressure_rejection();
        ApiError::from(e)
    })?;

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let (payload, partition, timestamp_ms, idempotency_key) =
        if content_type.starts_with("application/x-protobuf") {
            // Parse protobuf Event
            let event = proto::Event::decode(body).map_err(|e| {
                state.metrics.record_error();
                ApiError::BadRequest(format!("Invalid protobuf: {}", e))
            })?;

            let timestamp = if event.timestamp_ms != 0 {
                event.timestamp_ms
            } else {
                current_timestamp_ms()
            };

            let idempotency_key = if event.idempotency_key.is_empty() {
                None
            } else {
                Some(event.idempotency_key)
            };

            // Partition from X-Partition header, default 0
            let partition = headers
                .get("x-partition")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0u32);

            (event.payload, partition, timestamp, idempotency_key)
        } else {
            // Parse JSON
            let request: WriteRecordRequest = serde_json::from_slice(&body).map_err(|e| {
                state.metrics.record_error();
                ApiError::BadRequest(format!("Invalid JSON: {}", e))
            })?;

            let timestamp = request.timestamp_ms.unwrap_or_else(current_timestamp_ms);

            (
                request.payload.into_bytes(),
                request.partition,
                timestamp,
                request.idempotency_key,
            )
        };

    let offset = state
        .storage
        .write(
            &table,
            partition,
            &payload,
            timestamp_ms,
            idempotency_key.as_deref(),
        )
        .map_err(|e| {
            state.metrics.record_error();
            ApiError::from(e)
        })?;

    let latency_us = start.elapsed().as_micros() as u64;
    state.metrics.record_write(body_len, latency_us);

    // Record enhanced metrics
    state
        .metrics_registry
        .enhanced_api
        .record_write(&table, latency_us);
    state
        .metrics_registry
        .consumer
        .update_high_watermark(&table, partition, offset);

    Ok((
        StatusCode::ACCEPTED,
        Json(WriteRecordResponse {
            offset,
            partition,
            table,
        }),
    ))
}

/// POST /tables/{table}/bulk
/// Bulk write multiple records in a single batch (#1 Bulk Write API).
/// Reduces HTTP overhead and uses a single RocksDB WriteBatch.
pub async fn bulk_write<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<BulkWriteResponse>), ApiError> {
    let start = Instant::now();
    let body_len = body.len() as u64;

    // Check backpressure against raw body size (not parsed payload size) so we
    // reject oversized requests before spending CPU on JSON deserialization.
    let _permit = state.try_acquire_write_permit(body_len).map_err(|e| {
        state.metrics.record_error();
        state
            .metrics_registry
            .enhanced_api
            .record_backpressure_rejection();
        ApiError::from(e)
    })?;

    let request: BulkWriteRequest = serde_json::from_slice(&body).map_err(|e| {
        state.metrics.record_error();
        ApiError::BadRequest(format!("Invalid JSON: {}", e))
    })?;

    let record_count = request.records.len();

    if record_count == 0 {
        return Ok((
            StatusCode::ACCEPTED,
            Json(BulkWriteResponse {
                offsets: Vec::new(),
                count: 0,
                table,
            }),
        ));
    }

    // Convert to BulkWriteEvent format
    let events: Vec<crate::contracts::BulkWriteEvent> = request
        .records
        .into_iter()
        .map(|r| crate::contracts::BulkWriteEvent {
            partition: r.partition,
            payload: r.payload.into_bytes(),
            timestamp_ms: r.timestamp_ms.unwrap_or_else(current_timestamp_ms),
            idempotency_key: r.idempotency_key,
        })
        .collect();

    let total_bytes: u64 = events.iter().map(|e| e.payload.len() as u64).sum();

    // Single batch write for all events
    let offsets = state.storage.write_batch(&table, &events).map_err(|e| {
        state.metrics.record_error();
        ApiError::from(e)
    })?;

    let latency_us = start.elapsed().as_micros() as u64;

    // Record metrics for each event (averaged latency)
    let per_event_latency = latency_us / record_count.max(1) as u64;
    for _ in 0..record_count {
        state
            .metrics
            .record_write(total_bytes / record_count as u64, per_event_latency);
    }

    // Record to enhanced metrics (histogram) - once per bulk request
    state
        .metrics_registry
        .enhanced_api
        .record_write(&table, latency_us);

    Ok((
        StatusCode::ACCEPTED,
        Json(BulkWriteResponse {
            count: offsets.len(),
            offsets,
            table,
        }),
    ))
}

fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Calculates rate per second, returning 0.0 if duration is zero.
#[inline]
fn safe_rate(count: u64, duration_secs: f64) -> f64 {
    if duration_secs > 0.0 {
        count as f64 / duration_secs
    } else {
        0.0
    }
}

/// Calculates average, returning 0.0 if count is zero.
#[inline]
fn safe_avg(sum: u64, count: u64) -> f64 {
    if count > 0 {
        sum as f64 / count as f64
    } else {
        0.0
    }
}

/// Parses and validates the `fields` query parameter into a `ColumnProjection`.
fn parse_projection(fields: &Option<String>) -> Result<ColumnProjection, ApiError> {
    match fields {
        None => Ok(ColumnProjection::all()),
        Some(raw) => {
            let field_names: Vec<String> = raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if field_names.is_empty() {
                return Ok(ColumnProjection::all());
            }

            for name in &field_names {
                if !KNOWN_COLUMNS.contains(&name.as_str()) {
                    return Err(ApiError::BadRequest(format!(
                        "Unknown field '{}'. Valid fields: {}",
                        name,
                        KNOWN_COLUMNS.join(", ")
                    )));
                }
            }

            Ok(ColumnProjection::select(field_names))
        }
    }
}

/// Builds a JSON value for a single event, projecting only the requested fields.
fn project_event(
    event: &crate::contracts::StoredEvent,
    projection: &ColumnProjection,
) -> serde_json::Value {
    match &projection.fields {
        None => {
            // Default response: payload + timestamp_ms (backward compatible)
            serde_json::json!({
                "payload": String::from_utf8_lossy(&event.payload),
                "timestamp_ms": event.timestamp_ms,
            })
        }
        Some(fields) => {
            let mut map = serde_json::Map::new();
            for field in fields {
                match field.as_str() {
                    "sequence" => {
                        map.insert("sequence".into(), serde_json::json!(event.sequence));
                    }
                    "topic" => {
                        map.insert("topic".into(), serde_json::json!(event.topic));
                    }
                    "partition" => {
                        map.insert("partition".into(), serde_json::json!(event.partition));
                    }
                    "payload" => {
                        map.insert(
                            "payload".into(),
                            serde_json::json!(String::from_utf8_lossy(&event.payload)),
                        );
                    }
                    "timestamp_ms" => {
                        map.insert("timestamp_ms".into(), serde_json::json!(event.timestamp_ms));
                    }
                    "idempotency_key" => {
                        map.insert(
                            "idempotency_key".into(),
                            serde_json::json!(event.idempotency_key),
                        );
                    }
                    _ => {}
                }
            }
            serde_json::Value::Object(map)
        }
    }
}

/// GET /tables/{table}
/// Read records from a table (unified read from hot and cold storage).
/// Results are ordered by timestamp.
/// Supports optional `fields` query parameter for column projection.
pub async fn read_records<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
    Query(query): Query<ReadRecordsQuery>,
) -> Result<Json<ReadRecordsResponse>, ApiError> {
    let start = Instant::now();

    // Parse and validate projection
    let projection = parse_projection(&query.fields)?;

    // Read from hot storage (pass None for start_offsets - could be added for consumer groups later)
    let mut all_events = state
        .storage
        .read_all_partitions(&table, None, query.since, query.limit + 1)
        .map_err(|e| {
            state.metrics.record_error();
            ApiError::from(e)
        })?;

    // Also read from cold storage if configured - parallel reads for all partitions
    if let Some(ref cold) = state.cold_storage {
        let partitions = state.storage.list_partitions(&table).unwrap_or_default();

        // Create futures for all partition reads
        let projection_ref = &projection;
        let read_futures: Vec<_> = partitions
            .iter()
            .map(|&partition| {
                let cold = cold.clone();
                let table = table.clone();
                async move {
                    cold.read_events(
                        &table,
                        partition,
                        0,
                        query.limit + 1,
                        query.since,
                        None,
                        projection_ref,
                    )
                    .await
                    .unwrap_or_default()
                }
            })
            .collect();

        // Execute all reads in parallel
        let results = join_all(read_futures).await;

        // Collect and filter results
        for cold_events in results {
            let cold_events: Vec<_> = if let Some(since) = query.since {
                cold_events
                    .into_iter()
                    .filter(|e| e.timestamp_ms >= since)
                    .collect()
            } else {
                cold_events
            };
            all_events.extend(cold_events);
        }

        // Re-sort by timestamp and deduplicate
        all_events.sort_by_key(|e| e.timestamp_ms);
        all_events.dedup_by_key(|e| (e.timestamp_ms, e.sequence));
    }

    let has_more = all_events.len() > query.limit;
    let events: Vec<_> = all_events.into_iter().take(query.limit).collect();

    let records: Vec<serde_json::Value> = events
        .iter()
        .map(|e| project_event(e, &projection))
        .collect();

    let count = records.len();

    let latency_us = start.elapsed().as_micros() as u64;
    state.metrics.record_read(count as u64, latency_us);

    // Record enhanced metrics
    state
        .metrics_registry
        .enhanced_api
        .record_read(&table, latency_us);

    Ok(Json(ReadRecordsResponse {
        records,
        count,
        has_more,
    }))
}

/// GET /health
/// Health check endpoint.
pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy"
    }))
}

/// Response for stats endpoint.
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub uptime_secs: f64,
    pub writes: WriteStats,
    pub reads: ReadStats,
    pub errors_total: u64,
}

#[derive(Debug, Serialize)]
pub struct WriteStats {
    pub total: u64,
    pub bytes_total: u64,
    pub rate_per_sec: f64,
    pub avg_latency_us: f64,
}

#[derive(Debug, Serialize)]
pub struct ReadStats {
    pub total: u64,
    pub records_total: u64,
    pub rate_per_sec: f64,
    pub avg_latency_us: f64,
}

/// GET /stats
/// Server statistics and metrics.
pub async fn get_stats<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
) -> impl IntoResponse {
    let metrics = &state.metrics;

    let uptime_secs = metrics
        .start_time
        .get()
        .map(|t| t.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    let writes_total = metrics.writes_total.load(Ordering::Relaxed);
    let writes_bytes = metrics.writes_bytes.load(Ordering::Relaxed);
    let write_latency_sum = metrics.write_latency_sum_us.load(Ordering::Relaxed);

    let reads_total = metrics.reads_total.load(Ordering::Relaxed);
    let read_records_total = metrics.read_records_total.load(Ordering::Relaxed);
    let read_latency_sum = metrics.read_latency_sum_us.load(Ordering::Relaxed);

    let errors_total = metrics.errors_total.load(Ordering::Relaxed);

    Json(StatsResponse {
        uptime_secs,
        writes: WriteStats {
            total: writes_total,
            bytes_total: writes_bytes,
            rate_per_sec: safe_rate(writes_total, uptime_secs),
            avg_latency_us: safe_avg(write_latency_sum, writes_total),
        },
        reads: ReadStats {
            total: reads_total,
            records_total: read_records_total,
            rate_per_sec: safe_rate(reads_total, uptime_secs),
            avg_latency_us: safe_avg(read_latency_sum, reads_total),
        },
        errors_total,
    })
}

/// Request body for committing consumer offsets.
#[derive(Debug, Deserialize)]
pub struct CommitOffsetRequest {
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    pub offset: u64,
}

/// Response for commit operations.
#[derive(Debug, Serialize)]
pub struct CommitOffsetResponse {
    pub group: String,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

/// Query parameters for getting consumer offset.
#[derive(Debug, Deserialize)]
pub struct GetOffsetQuery {
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
}

/// Response for get offset operations.
#[derive(Debug, Serialize)]
pub struct GetOffsetResponse {
    pub group: String,
    pub topic: String,
    pub partition: u32,
    pub offset: Option<u64>,
}

/// POST /consumers/{group}/commit
/// Commit a consumer group offset.
pub async fn commit_offset<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(group): Path<String>,
    Json(request): Json<CommitOffsetRequest>,
) -> Result<Json<CommitOffsetResponse>, ApiError> {
    state
        .storage
        .commit_offset(&group, &request.topic, request.partition, request.offset)?;

    // Record consumer metrics
    state.metrics_registry.consumer.update_committed_offset(
        &group,
        &request.topic,
        request.partition,
        request.offset,
    );

    Ok(Json(CommitOffsetResponse {
        group,
        topic: request.topic,
        partition: request.partition,
        offset: request.offset,
    }))
}

/// GET /consumers/{group}/offset
/// Get the committed offset for a consumer group.
pub async fn get_offset<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(group): Path<String>,
    Query(query): Query<GetOffsetQuery>,
) -> Result<Json<GetOffsetResponse>, ApiError> {
    let offset = state
        .storage
        .get_offset(&group, &query.topic, query.partition)?;

    Ok(Json(GetOffsetResponse {
        group,
        topic: query.topic,
        partition: query.partition,
        offset,
    }))
}

// ============================================================================
// Iceberg Admin Endpoints
// ============================================================================

/// Response for table metadata endpoint.
#[derive(Debug, Serialize)]
pub struct TableMetadataResponse {
    pub table: String,
    pub storage_type: String,
    pub iceberg_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_path: Option<String>,
}

/// Response for flush endpoint.
#[derive(Debug, Serialize)]
pub struct FlushResponse {
    pub status: String,
    pub message: String,
}

/// Response for compact endpoint.
#[derive(Debug, Serialize)]
pub struct CompactResponse {
    pub status: String,
    pub message: String,
}

/// GET /tables/{table}/metadata
/// Returns metadata about the table storage, including Iceberg location if enabled.
pub async fn get_table_metadata<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
) -> Result<Json<TableMetadataResponse>, ApiError> {
    let response = if let Some(ref cold) = state.cold_storage {
        let info = cold.storage_info();
        let metadata_location = cold.iceberg_metadata_location(&table);

        TableMetadataResponse {
            table,
            storage_type: info.storage_type,
            iceberg_enabled: info.iceberg_enabled,
            metadata_location,
            bucket: Some(info.bucket),
            base_path: Some(info.base_path),
        }
    } else {
        TableMetadataResponse {
            table,
            storage_type: "hot_only".into(),
            iceberg_enabled: false,
            metadata_location: None,
            bucket: None,
            base_path: None,
        }
    };

    Ok(Json(response))
}

/// POST /tables/{table}/flush
/// Forces a flush of pending events to cold storage.
/// Note: This endpoint requires the flusher to be wired into AppState.
pub async fn flush_table<H: HotStorage, C: ColdStorage>(
    State(_state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
) -> Result<Json<FlushResponse>, ApiError> {
    // TODO: Wire flusher into AppState to enable this endpoint
    // For now, return 501 Not Implemented
    Ok(Json(FlushResponse {
        status: "not_implemented".into(),
        message: format!(
            "Flush for table '{}' is not yet wired. Add flusher to AppState to enable.",
            table
        ),
    }))
}

/// POST /tables/{table}/compact
/// Triggers compaction for the table to merge small files.
/// Note: This endpoint requires the compactor to be wired into AppState.
pub async fn compact_table<H: HotStorage, C: ColdStorage>(
    State(_state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
) -> Result<Json<CompactResponse>, ApiError> {
    // TODO: Wire compactor into AppState to enable this endpoint
    // For now, return 501 Not Implemented
    Ok(Json(CompactResponse {
        status: "not_implemented".into(),
        message: format!(
            "Compaction for table '{}' is not yet wired. Add compactor to AppState to enable.",
            table
        ),
    }))
}

// ============================================================================
// Health Check Endpoints (#10)
// ============================================================================

/// GET /health/live
/// Kubernetes liveness probe - checks if the process is running.
/// This is a lightweight check that always returns OK if the server is responsive.
pub async fn health_live() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Response for readiness endpoint.
#[derive(Debug, Serialize)]
pub struct ReadinessResponse {
    pub status: String,
    pub hot_storage: ComponentHealth,
    pub cold_storage: ComponentHealth,
    pub backpressure: BackpressureHealth,
}

/// Health status for a single component.
#[derive(Debug, Serialize)]
pub struct ComponentHealth {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Backpressure health information.
#[derive(Debug, Serialize)]
pub struct BackpressureHealth {
    pub status: String,
    pub inflight_writes: usize,
    pub inflight_bytes: u64,
    pub max_inflight_bytes: u64,
}

/// GET /health/ready
/// Kubernetes readiness probe - checks if the server can handle traffic.
/// Verifies storage backends are accessible and backpressure limits aren't exceeded.
pub async fn health_ready<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
) -> Result<Json<ReadinessResponse>, (StatusCode, Json<ReadinessResponse>)> {
    let mut overall_ready = true;

    // Check hot storage by calling list_topics() - fast RocksDB metadata read
    let hot_storage = match state.storage.list_topics() {
        Ok(_) => ComponentHealth {
            status: "ok".into(),
            error: None,
        },
        Err(e) => {
            overall_ready = false;
            ComponentHealth {
                status: "error".into(),
                error: Some(e.to_string()),
            }
        }
    };

    // Check cold storage if configured
    let cold_storage = if let Some(ref cold) = state.cold_storage {
        let info = cold.storage_info();
        if info.storage_type == "none" {
            ComponentHealth {
                status: "not_configured".into(),
                error: None,
            }
        } else {
            ComponentHealth {
                status: "ok".into(),
                error: None,
            }
        }
    } else {
        ComponentHealth {
            status: "not_configured".into(),
            error: None,
        }
    };

    // Check backpressure status
    let inflight_bytes = state.inflight_bytes.load(Ordering::Relaxed);
    let max_inflight_bytes = state.max_inflight_bytes;
    let inflight_writes = state.write_semaphore.available_permits();

    // Consider backpressure unhealthy if at 90% capacity
    let backpressure_threshold = (max_inflight_bytes as f64 * 0.9) as u64;
    let backpressure_ok = inflight_bytes < backpressure_threshold;

    let backpressure = BackpressureHealth {
        status: if backpressure_ok { "ok" } else { "warning" }.into(),
        inflight_writes,
        inflight_bytes,
        max_inflight_bytes,
    };

    if !backpressure_ok {
        overall_ready = false;
    }

    let response = ReadinessResponse {
        status: if overall_ready { "ready" } else { "not_ready" }.into(),
        hot_storage,
        cold_storage,
        backpressure,
    };

    if overall_ready {
        Ok(Json(response))
    } else {
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(response)))
    }
}

// ============================================================================
// Prometheus Metrics Endpoint (#9)
// ============================================================================

/// GET /metrics
/// Returns metrics in Prometheus text exposition format.
/// Compatible with Prometheus scraping and the existing Grafana dashboard.
pub async fn metrics<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
) -> impl IntoResponse {
    refresh_hot_storage_metrics(state.as_ref());
    let metrics = &state.metrics;

    let uptime_secs = metrics
        .start_time
        .get()
        .map(|t| t.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    let writes_total = metrics.writes_total.load(Ordering::Relaxed);
    let writes_bytes = metrics.writes_bytes.load(Ordering::Relaxed);
    let write_latency_sum = metrics.write_latency_sum_us.load(Ordering::Relaxed);

    let reads_total = metrics.reads_total.load(Ordering::Relaxed);
    let read_records_total = metrics.read_records_total.load(Ordering::Relaxed);
    let read_latency_sum = metrics.read_latency_sum_us.load(Ordering::Relaxed);

    let errors_total = metrics.errors_total.load(Ordering::Relaxed);

    // Backpressure metrics
    let inflight_bytes = state.inflight_bytes.load(Ordering::Relaxed);
    let inflight_writes_available = state.write_semaphore.available_permits();

    // Calculate rates and averages
    let writes_rate = safe_rate(writes_total, uptime_secs);
    let writes_avg_latency = safe_avg(write_latency_sum, writes_total);
    let reads_rate = safe_rate(reads_total, uptime_secs);
    let reads_avg_latency = safe_avg(read_latency_sum, reads_total);

    // Build Prometheus text format output
    let mut output = format!(
        "# HELP zombi_uptime_secs Server uptime in seconds\n\
         # TYPE zombi_uptime_secs gauge\n\
         zombi_uptime_secs {:.3}\n\
         \n\
         # HELP zombi_writes_total Total write requests\n\
         # TYPE zombi_writes_total counter\n\
         zombi_writes_total {}\n\
         \n\
         # HELP zombi_writes_bytes_total Total bytes written\n\
         # TYPE zombi_writes_bytes_total counter\n\
         zombi_writes_bytes_total {}\n\
         \n\
         # HELP zombi_writes_rate_per_sec Current write rate (events per second)\n\
         # TYPE zombi_writes_rate_per_sec gauge\n\
         zombi_writes_rate_per_sec {:.2}\n\
         \n\
         # HELP zombi_writes_avg_latency_us Average write latency in microseconds\n\
         # TYPE zombi_writes_avg_latency_us gauge\n\
         zombi_writes_avg_latency_us {:.2}\n\
         \n\
         # HELP zombi_reads_total Total read requests\n\
         # TYPE zombi_reads_total counter\n\
         zombi_reads_total {}\n\
         \n\
         # HELP zombi_reads_records_total Total records read\n\
         # TYPE zombi_reads_records_total counter\n\
         zombi_reads_records_total {}\n\
         \n\
         # HELP zombi_reads_rate_per_sec Current read rate (requests per second)\n\
         # TYPE zombi_reads_rate_per_sec gauge\n\
         zombi_reads_rate_per_sec {:.2}\n\
         \n\
         # HELP zombi_reads_avg_latency_us Average read latency in microseconds\n\
         # TYPE zombi_reads_avg_latency_us gauge\n\
         zombi_reads_avg_latency_us {:.2}\n\
         \n\
         # HELP zombi_errors_total Total errors\n\
         # TYPE zombi_errors_total counter\n\
         zombi_errors_total {}\n\
         \n\
         # HELP zombi_inflight_bytes Current inflight write bytes\n\
         # TYPE zombi_inflight_bytes gauge\n\
         zombi_inflight_bytes {}\n\
         \n\
         # HELP zombi_inflight_writes_available Available write permits\n\
         # TYPE zombi_inflight_writes_available gauge\n\
         zombi_inflight_writes_available {}\n",
        uptime_secs,
        writes_total,
        writes_bytes,
        writes_rate,
        writes_avg_latency,
        reads_total,
        read_records_total,
        reads_rate,
        reads_avg_latency,
        errors_total,
        inflight_bytes,
        inflight_writes_available,
    );

    // Append enhanced metrics from MetricsRegistry
    output.push_str(&state.metrics_registry.format_prometheus());

    (
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        output,
    )
}

fn refresh_hot_storage_metrics<H: HotStorage, C: ColdStorage>(state: &AppState<H, C>) {
    let topics = match state.storage.list_topics() {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to list topics for hot storage metrics");
            return;
        }
    };

    for topic in topics {
        let partitions = match state.storage.list_partitions(&topic) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    topic = %topic,
                    error = %e,
                    "Failed to list partitions for hot storage metrics"
                );
                continue;
            }
        };

        for partition in partitions {
            let low = match state.storage.low_watermark(&topic, partition) {
                Ok(v) => v,
                Err(e) => {
                    tracing::debug!(
                        topic = %topic,
                        partition = partition,
                        error = %e,
                        "Failed to read low watermark for metrics"
                    );
                    continue;
                }
            };

            let high = match state.storage.high_watermark(&topic, partition) {
                Ok(v) => v,
                Err(e) => {
                    tracing::debug!(
                        topic = %topic,
                        partition = partition,
                        error = %e,
                        "Failed to read high watermark for metrics"
                    );
                    continue;
                }
            };

            state
                .metrics_registry
                .hot
                .update(&topic, partition, low, high);
        }
    }
}
