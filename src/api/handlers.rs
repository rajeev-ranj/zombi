use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::contracts::{ColdStorage, HotStorage, StorageError};
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
        self.write_latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_read(&self, records: u64, latency_us: u64) {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        self.read_records_total.fetch_add(records, Ordering::Relaxed);
        self.read_latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Application state shared across handlers.
pub struct AppState<H: HotStorage, C: ColdStorage = NoopColdStorage> {
    pub storage: Arc<H>,
    pub cold_storage: Option<Arc<C>>,
    pub metrics: Arc<Metrics>,
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

/// Query parameters for reading records.
#[derive(Debug, Deserialize)]
pub struct ReadRecordsQuery {
    /// Starting timestamp in milliseconds (for time-based reads)
    pub since: Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// Response for read operations.
#[derive(Debug, Serialize)]
pub struct ReadRecordsResponse {
    pub records: Vec<RecordResponse>,
    pub count: usize,
    pub has_more: bool,
}

/// Single record in read response.
#[derive(Debug, Serialize)]
pub struct RecordResponse {
    pub payload: String,
    pub timestamp_ms: i64,
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

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let (payload, partition, timestamp_ms, idempotency_key) = if content_type
        .starts_with("application/x-protobuf")
    {
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
        let request: WriteRecordRequest = serde_json::from_slice(&body)
            .map_err(|e| {
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

    let offset = state.storage.write(
        &table,
        partition,
        &payload,
        timestamp_ms,
        idempotency_key.as_deref(),
    ).map_err(|e| {
        state.metrics.record_error();
        ApiError::from(e)
    })?;

    let latency_us = start.elapsed().as_micros() as u64;
    state.metrics.record_write(body_len, latency_us);

    Ok((
        StatusCode::ACCEPTED,
        Json(WriteRecordResponse {
            offset,
            partition,
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

/// GET /tables/{table}
/// Read records from a table (unified read from hot and cold storage).
/// Results are ordered by timestamp.
pub async fn read_records<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(table): Path<String>,
    Query(query): Query<ReadRecordsQuery>,
) -> Result<Json<ReadRecordsResponse>, ApiError> {
    let start = Instant::now();

    // Read from hot storage
    let mut all_events = state
        .storage
        .read_all_partitions(&table, query.since, query.limit + 1)
        .map_err(|e| {
            state.metrics.record_error();
            ApiError::from(e)
        })?;

    // Also read from cold storage if configured
    if let Some(ref cold) = state.cold_storage {
        let partitions = state.storage.list_partitions(&table).unwrap_or_default();
        for partition in partitions {
            let cold_events = cold
                .read_events(&table, partition, 0, query.limit + 1)
                .await
                .unwrap_or_default();

            // Filter by timestamp if specified
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

    let records: Vec<RecordResponse> = events
        .iter()
        .map(|e| RecordResponse {
            payload: String::from_utf8_lossy(&e.payload).to_string(),
            timestamp_ms: e.timestamp_ms,
        })
        .collect();

    let count = records.len();

    let latency_us = start.elapsed().as_micros() as u64;
    state.metrics.record_read(count as u64, latency_us);

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
            rate_per_sec: if uptime_secs > 0.0 {
                writes_total as f64 / uptime_secs
            } else {
                0.0
            },
            avg_latency_us: if writes_total > 0 {
                write_latency_sum as f64 / writes_total as f64
            } else {
                0.0
            },
        },
        reads: ReadStats {
            total: reads_total,
            records_total: read_records_total,
            rate_per_sec: if uptime_secs > 0.0 {
                reads_total as f64 / uptime_secs
            } else {
                0.0
            },
            avg_latency_us: if reads_total > 0 {
                read_latency_sum as f64 / reads_total as f64
            } else {
                0.0
            },
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
