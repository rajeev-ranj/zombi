use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::future::join_all;
use prost::Message;
use tower::ServiceExt;

use zombi::api::{create_router, AppState, BackpressureConfig, Metrics, NoopColdStorage};
use zombi::contracts::{ColdStorage, IcebergCatalogTable, StorageError};
use zombi::metrics::MetricsRegistry;
use zombi::proto;
use zombi::storage::{RocksDbStorage, WriteCombiner, WriteCombinerConfig};

/// Cold storage mock that panics if `read_events` is called.
/// Proves the read handler does not query cold storage.
struct PanicOnReadColdStorage;

impl ColdStorage for PanicOnReadColdStorage {
    async fn write_segment(
        &self,
        _topic: &str,
        _partition: u32,
        _events: &[zombi::contracts::StoredEvent],
    ) -> Result<String, StorageError> {
        Err(StorageError::S3("not configured".into()))
    }

    async fn read_events(
        &self,
        _topic: &str,
        _partition: u32,
        _start_offset: u64,
        _limit: usize,
        _since_ms: Option<i64>,
        _until_ms: Option<i64>,
        _projection: &zombi::contracts::ColumnProjection,
    ) -> Result<Vec<zombi::contracts::StoredEvent>, StorageError> {
        panic!("read_events must not be called — HTTP reads should use hot storage only");
    }

    async fn list_segments(
        &self,
        _topic: &str,
        _partition: u32,
    ) -> Result<Vec<zombi::contracts::SegmentInfo>, StorageError> {
        Ok(Vec::new())
    }

    fn storage_info(&self) -> zombi::contracts::ColdStorageInfo {
        zombi::contracts::ColdStorageInfo {
            storage_type: "panic-on-read".into(),
            iceberg_enabled: false,
            bucket: String::new(),
            base_path: String::new(),
        }
    }
}

/// Iceberg catalog mock used by /v1 REST catalog integration tests.
struct CatalogColdStorageMock {
    tables: HashMap<String, IcebergCatalogTable>,
}

impl CatalogColdStorageMock {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            "events".into(),
            IcebergCatalogTable {
                metadata_location: "s3://test-bucket/tables/events/metadata/v7.metadata.json"
                    .into(),
                metadata_json: serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "4f5f45df-fa40-4f44-b95d-f3ae6f2f68d0",
                    "location": "s3://test-bucket/tables/events",
                    "last-sequence-number": 7,
                    "last-updated-ms": 1700000000000_i64,
                    "last-column-id": 8,
                    "schemas": [{
                        "type": "struct",
                        "schema-id": 0,
                        "fields": []
                    }],
                    "current-schema-id": 0,
                    "partition-specs": [{
                        "spec-id": 0,
                        "fields": []
                    }],
                    "default-spec-id": 0,
                    "last-partition-id": 999,
                    "properties": {},
                    "current-snapshot-id": serde_json::Value::Null,
                    "snapshots": [],
                    "snapshot-log": [],
                    "sort-orders": [{
                        "order-id": 0,
                        "fields": []
                    }],
                    "default-sort-order-id": 0
                })
                .to_string(),
            },
        );
        Self { tables }
    }
}

impl ColdStorage for CatalogColdStorageMock {
    async fn write_segment(
        &self,
        _topic: &str,
        _partition: u32,
        _events: &[zombi::contracts::StoredEvent],
    ) -> Result<String, StorageError> {
        Err(StorageError::S3("not configured".into()))
    }

    async fn read_events(
        &self,
        _topic: &str,
        _partition: u32,
        _start_offset: u64,
        _limit: usize,
        _since_ms: Option<i64>,
        _until_ms: Option<i64>,
        _projection: &zombi::contracts::ColumnProjection,
    ) -> Result<Vec<zombi::contracts::StoredEvent>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_segments(
        &self,
        _topic: &str,
        _partition: u32,
    ) -> Result<Vec<zombi::contracts::SegmentInfo>, StorageError> {
        Ok(Vec::new())
    }

    fn storage_info(&self) -> zombi::contracts::ColdStorageInfo {
        zombi::contracts::ColdStorageInfo {
            storage_type: "iceberg".into(),
            iceberg_enabled: true,
            bucket: "test-bucket".into(),
            base_path: "tables".into(),
        }
    }

    async fn list_iceberg_tables(&self) -> Result<Vec<String>, StorageError> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    async fn load_iceberg_table(
        &self,
        topic: &str,
    ) -> Result<Option<IcebergCatalogTable>, StorageError> {
        Ok(self.tables.get(topic).cloned())
    }

    async fn iceberg_table_exists(&self, topic: &str) -> Result<bool, StorageError> {
        Ok(self.tables.contains_key(topic))
    }
}

fn create_test_app_with_backpressure(
    config: BackpressureConfig,
) -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    let state = Arc::new(AppState::new(
        Arc::new(storage),
        None::<Arc<NoopColdStorage>>,
        Arc::new(Metrics::new()),
        Arc::new(MetricsRegistry::new()),
        config,
        vec!["zombi".into()],
    ));
    let router = create_router(state);
    (router, dir)
}

fn create_test_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    let state = Arc::new(AppState::new(
        Arc::new(storage),
        None::<Arc<NoopColdStorage>>,
        Arc::new(Metrics::new()),
        Arc::new(MetricsRegistry::new()),
        BackpressureConfig::default(),
        vec!["zombi".into()],
    ));
    let router = create_router(state);
    (router, dir)
}

fn create_test_app_with_combiner(config: WriteCombinerConfig) -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = Arc::new(RocksDbStorage::open(dir.path()).unwrap());
    let metrics_registry = Arc::new(MetricsRegistry::new());
    let combiner = Arc::new(WriteCombiner::new(
        Arc::clone(&storage),
        config,
        Arc::clone(&metrics_registry.combiner),
    ));
    let state = Arc::new(
        AppState::new(
            storage,
            None::<Arc<NoopColdStorage>>,
            Arc::new(Metrics::new()),
            metrics_registry,
            BackpressureConfig::default(),
            vec!["zombi".into()],
        )
        .with_write_combiner(Some(combiner)),
    );
    let router = create_router(state);
    (router, dir)
}

fn create_test_app_with_cold_storage() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    let state = Arc::new(AppState::new(
        Arc::new(storage),
        Some(Arc::new(NoopColdStorage)),
        Arc::new(Metrics::new()),
        Arc::new(MetricsRegistry::new()),
        BackpressureConfig::default(),
        vec!["zombi".into()],
    ));
    let router = create_router(state);
    (router, dir)
}

fn create_test_app_with_panic_cold_storage() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    let state = Arc::new(AppState::new(
        Arc::new(storage),
        Some(Arc::new(PanicOnReadColdStorage)),
        Arc::new(Metrics::new()),
        Arc::new(MetricsRegistry::new()),
        BackpressureConfig::default(),
        vec!["zombi".into()],
    ));
    let router = create_router(state);
    (router, dir)
}

fn create_test_app_with_catalog_storage() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    let state = Arc::new(AppState::new(
        Arc::new(storage),
        Some(Arc::new(CatalogColdStorageMock::new())),
        Arc::new(Metrics::new()),
        Arc::new(MetricsRegistry::new()),
        BackpressureConfig::default(),
        vec!["zombi".into()],
    ));
    let router = create_router(state);
    (router, dir)
}

fn configured_catalog_namespace() -> String {
    std::env::var("ZOMBI_CATALOG_NAMESPACE").unwrap_or_else(|_| "zombi".into())
}

#[tokio::test]
async fn test_health_check() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_catalog_config_endpoint() {
    let (app, _dir) = create_test_app_with_catalog_storage();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(json["defaults"].is_object());
    assert!(json["overrides"].is_object());
    assert_eq!(json["overrides"]["warehouse"], "s3://test-bucket/tables");
    assert_eq!(json["overrides"]["namespace-separator"], "%1F");

    let endpoints = json["endpoints"].as_array().unwrap();
    assert!(endpoints
        .iter()
        .any(|v| v.as_str() == Some("GET /v1/namespaces")));
    assert!(endpoints
        .iter()
        .any(|v| v.as_str() == Some("GET /v1/namespaces/{namespace}")));
    assert!(endpoints
        .iter()
        .any(|v| v.as_str() == Some("GET /v1/namespaces/{namespace}/tables/{table}")));
    assert!(endpoints
        .iter()
        .any(|v| v.as_str() == Some("HEAD /v1/namespaces/{namespace}/tables/{table}")));
}

#[tokio::test]
async fn test_catalog_list_namespaces() {
    let (app, _dir) = create_test_app_with_catalog_storage();
    let expected = configured_catalog_namespace();
    let expected_top = expected.split('.').next().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["namespaces"][0][0], expected_top);
}

#[tokio::test]
async fn test_catalog_list_tables() {
    let (app, _dir) = create_test_app_with_catalog_storage();
    let namespace = configured_catalog_namespace();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/namespaces/{}/tables", namespace))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["identifiers"].as_array().unwrap().len(), 1);
    assert_eq!(json["identifiers"][0]["name"], "events");
}

#[tokio::test]
async fn test_catalog_load_table() {
    let (app, _dir) = create_test_app_with_catalog_storage();
    let namespace = configured_catalog_namespace();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/namespaces/{}/tables/events", namespace))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        json["metadata-location"],
        "s3://test-bucket/tables/events/metadata/v7.metadata.json"
    );
    assert_eq!(json["metadata"]["format-version"], 2);
}

#[tokio::test]
async fn test_catalog_load_table_not_found() {
    let (app, _dir) = create_test_app_with_catalog_storage();
    let namespace = configured_catalog_namespace();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/namespaces/{}/tables/missing", namespace))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["type"], "NoSuchTableException");
    assert_eq!(json["error"]["code"], 404);
}

#[tokio::test]
async fn test_catalog_namespace_not_found() {
    let (app, _dir) = create_test_app_with_catalog_storage();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/unknown/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["type"], "NoSuchNamespaceException");
    assert_eq!(json["error"]["code"], 404);
}

#[tokio::test]
async fn test_catalog_load_namespace() {
    let (app, _dir) = create_test_app_with_catalog_storage();
    let namespace = configured_catalog_namespace();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/namespaces/{}", namespace))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["namespace"], serde_json::json!(["zombi"]));
    assert!(json["properties"].is_object());
}

#[tokio::test]
async fn test_catalog_load_namespace_not_found() {
    let (app, _dir) = create_test_app_with_catalog_storage();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["type"], "NoSuchNamespaceException");
    assert_eq!(json["error"]["code"], 404);
}

#[tokio::test]
async fn test_catalog_table_exists_head() {
    let (app, _dir) = create_test_app_with_catalog_storage();
    let namespace = configured_catalog_namespace();

    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/namespaces/{}/tables/events", namespace))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_catalog_table_not_exists_head() {
    let (app, _dir) = create_test_app_with_catalog_storage();
    let namespace = configured_catalog_namespace();

    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/namespaces/{}/tables/missing", namespace))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_catalog_list_tables_no_cold_storage() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/zombi/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["identifiers"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_catalog_load_table_no_cold_storage() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/zombi/tables/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["type"], "NoSuchTableException");
}

#[tokio::test]
async fn test_catalog_table_exists_no_cold_storage() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/v1/namespaces/zombi/tables/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_write_record() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/user_events")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "hello world", "partition": 0}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["offset"], 1);
    assert_eq!(json["partition"], 0);
    assert_eq!(json["table"], "user_events");
}

#[tokio::test]
async fn test_write_record_with_combiner() {
    let config = WriteCombinerConfig {
        enabled: true,
        batch_size: 5,
        window: std::time::Duration::from_micros(500),
        queue_capacity: 50,
        ..Default::default()
    };
    let (app, _dir) = create_test_app_with_combiner(config);

    let requests = (0..20).map(|i| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tables/user_events")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        "{{\"payload\": \"hello {}\", \"partition\": 0}}",
                        i
                    )))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    });

    let responses = join_all(requests).await;
    let mut offsets: Vec<u64> = Vec::new();
    for response in responses {
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        offsets.push(json["offset"].as_u64().unwrap());
    }

    offsets.sort_unstable();
    let expected: Vec<u64> = (1..=20).collect();
    assert_eq!(offsets, expected);
}

#[tokio::test]
async fn test_write_record_protobuf_with_combiner() {
    let config = WriteCombinerConfig {
        enabled: true,
        batch_size: 4,
        window: std::time::Duration::from_micros(500),
        queue_capacity: 50,
        ..Default::default()
    };
    let (app, _dir) = create_test_app_with_combiner(config);

    let requests = (0..10).map(|i| {
        let app = app.clone();
        async move {
            let event = proto::Event {
                payload: format!("proto {}", i).into_bytes(),
                timestamp_ms: 1234567890,
                idempotency_key: String::new(),
                headers: std::collections::HashMap::new(),
            };
            let mut buf = Vec::new();
            event.encode(&mut buf).unwrap();

            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tables/proto_events")
                    .header("content-type", "application/x-protobuf")
                    .header("x-partition", "0")
                    .body(Body::from(buf))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    });

    let responses = join_all(requests).await;
    let mut offsets: Vec<u64> = Vec::new();
    for response in responses {
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        offsets.push(json["offset"].as_u64().unwrap());
    }

    offsets.sort_unstable();
    let expected: Vec<u64> = (1..=10).collect();
    assert_eq!(offsets, expected);
}

#[tokio::test]
async fn test_combiner_interleaved_writes() {
    let config = WriteCombinerConfig {
        enabled: true,
        batch_size: 4,
        window: std::time::Duration::from_micros(500),
        queue_capacity: 100,
        ..Default::default()
    };
    let (app, _dir) = create_test_app_with_combiner(config);

    let requests = (0..20).map(|i| {
        let app = app.clone();
        let table = if i % 2 == 0 { "table_a" } else { "table_b" };
        async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/tables/{}", table))
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        "{{\"payload\": \"hello {}\", \"partition\": 0}}",
                        i
                    )))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    });

    let responses = join_all(requests).await;
    let mut offsets_a = Vec::new();
    let mut offsets_b = Vec::new();

    for response in responses {
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let table = json["table"].as_str().unwrap();
        let offset = json["offset"].as_u64().unwrap();
        match table {
            "table_a" => offsets_a.push(offset),
            "table_b" => offsets_b.push(offset),
            other => panic!("unexpected table: {}", other),
        }
    }

    offsets_a.sort_unstable();
    offsets_b.sort_unstable();
    assert_eq!(offsets_a, (1..=10).collect::<Vec<_>>());
    assert_eq!(offsets_b, (1..=10).collect::<Vec<_>>());
}

#[tokio::test]
async fn test_read_records() {
    let (app, _dir) = create_test_app();

    // First write a record
    let app_clone = app.clone();
    let response = app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/user_events")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "hello world", "partition": 0}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    // Then read it back (no partition needed - reads from all partitions)
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/user_events?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["records"].as_array().unwrap().len(), 1);
    assert_eq!(json["records"][0]["payload"], "hello world");
    assert_eq!(json["count"], 1);
}

#[tokio::test]
async fn test_idempotent_write() {
    let (app, _dir) = create_test_app();

    // Write with idempotency key
    let app_clone = app.clone();
    let response = app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/user_events")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload": "hello", "partition": 0, "idempotency_key": "req-123"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json1: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let offset1 = json1["offset"].as_u64().unwrap();

    // Write again with same idempotency key
    let app_clone = app.clone();
    let response = app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/user_events")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload": "hello", "partition": 0, "idempotency_key": "req-123"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let offset2 = json2["offset"].as_u64().unwrap();

    // Should get same offset
    assert_eq!(offset1, offset2);

    // Should only have one record
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/user_events?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["records"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_multiple_writes_and_reads() {
    let (app, _dir) = create_test_app();

    // Write multiple records
    for i in 0..10 {
        let app_clone = app.clone();
        let response = app_clone
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tables/user_events")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"payload": "record-{}", "partition": 0}}"#,
                        i
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    // Read all records
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/user_events?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["records"].as_array().unwrap().len(), 10);
    assert_eq!(json["count"], 10);
}

#[tokio::test]
async fn test_commit_and_get_offset() {
    let (app, _dir) = create_test_app();

    // Initially no offset
    let app_clone = app.clone();
    let response = app_clone
        .oneshot(
            Request::builder()
                .uri("/consumers/my-group/offset?topic=user_events&partition=0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["offset"], serde_json::Value::Null);

    // Commit an offset
    let app_clone = app.clone();
    let response = app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/consumers/my-group/commit")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"topic": "user_events", "partition": 0, "offset": 100}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["group"], "my-group");
    assert_eq!(json["topic"], "user_events");
    assert_eq!(json["offset"], 100);

    // Now should be able to read it back
    let response = app
        .oneshot(
            Request::builder()
                .uri("/consumers/my-group/offset?topic=user_events&partition=0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["offset"], 100);
}

#[tokio::test]
async fn test_table_metadata_hot_only() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/user_events/metadata")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["table"], "user_events");
    assert_eq!(json["storage_type"], "hot_only");
    assert_eq!(json["iceberg_enabled"], false);
    assert!(json.get("metadata_location").is_none());
}

#[tokio::test]
async fn test_table_metadata_with_cold_storage() {
    // This test uses NoopColdStorage which returns "none" as storage_type
    // In production with real S3 or Iceberg storage, storage_type would be "s3" or "iceberg"
    let (app, _dir) = create_test_app_with_cold_storage();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/user_events/metadata")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["table"], "user_events");
    assert_eq!(json["storage_type"], "none"); // NoopColdStorage returns "none"
    assert_eq!(json["iceberg_enabled"], false);
}

#[tokio::test]
async fn test_flush_table_not_implemented() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/user_events/flush")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "not_implemented");
    assert!(json["message"]
        .as_str()
        .unwrap_or("")
        .contains("not yet wired"));
}

#[tokio::test]
async fn test_compact_table_not_implemented() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/user_events/compact")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "not_implemented");
    assert!(json["message"]
        .as_str()
        .unwrap_or("")
        .contains("not yet wired"));
}

#[tokio::test]
async fn test_write_record_protobuf() {
    let (app, _dir) = create_test_app();

    // Create protobuf Event
    let event = proto::Event {
        payload: b"protobuf payload".to_vec(),
        timestamp_ms: 1234567890,
        idempotency_key: String::new(),
        headers: std::collections::HashMap::new(),
    };

    let mut buf = Vec::new();
    event.encode(&mut buf).unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/proto_events")
                .header("content-type", "application/x-protobuf")
                .header("x-partition", "0")
                .body(Body::from(buf))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["offset"], 1);
    assert_eq!(json["partition"], 0);
    assert_eq!(json["table"], "proto_events");

    // Read it back and verify
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/proto_events?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["records"].as_array().unwrap().len(), 1);
    assert_eq!(json["records"][0]["payload"], "protobuf payload");
    assert_eq!(json["records"][0]["timestamp_ms"], 1234567890);
}

#[tokio::test]
async fn test_stats_endpoint() {
    let (app, _dir) = create_test_app();

    // Write some records to generate metrics
    for _ in 0..5 {
        let app_clone = app.clone();
        let response = app_clone
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tables/stats_test")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"payload": "test", "partition": 0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    // Read records to generate read metrics
    let app_clone = app.clone();
    let response = app_clone
        .oneshot(
            Request::builder()
                .uri("/tables/stats_test?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Check stats endpoint
    let response = app
        .oneshot(
            Request::builder()
                .uri("/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Verify stats structure
    assert!(json["uptime_secs"].as_f64().unwrap() >= 0.0);
    assert_eq!(json["writes"]["total"], 5);
    assert!(json["writes"]["bytes_total"].as_u64().unwrap() > 0);
    assert_eq!(json["reads"]["total"], 1);
    assert_eq!(json["reads"]["records_total"], 5);
    assert_eq!(json["errors_total"], 0);
}

#[tokio::test]
async fn test_backpressure_bytes_limit() {
    // Create app with very low byte limit to test backpressure
    let config = BackpressureConfig {
        max_inflight_writes: 10000,
        max_inflight_bytes: 10, // Only 10 bytes allowed
    };
    let (app, _dir) = create_test_app_with_backpressure(config);

    // First write should succeed (within limit)
    let app_clone = app.clone();
    let response = app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/test")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "small"}"#)) // ~20 bytes total
                .unwrap(),
        )
        .await
        .unwrap();

    // Should get 503 Service Unavailable due to byte limit exceeded
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "SERVER_OVERLOADED");
}

#[tokio::test]
async fn test_bulk_backpressure_bytes_limit() {
    // Create app with very low byte limit to test backpressure
    let config = BackpressureConfig {
        max_inflight_writes: 10000,
        max_inflight_bytes: 10, // Only 10 bytes allowed
    };
    let (app, _dir) = create_test_app_with_backpressure(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/test/bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"records":[{"payload":"small"}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "SERVER_OVERLOADED");
}

#[tokio::test]
async fn test_bulk_invalid_json_returns_400_when_under_limit() {
    // Create app with sufficient byte limit so parsing happens
    let config = BackpressureConfig {
        max_inflight_writes: 10000,
        max_inflight_bytes: 1024,
    };
    let (app, _dir) = create_test_app_with_backpressure(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/test/bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"records":[{"payload":"oops"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "BAD_REQUEST");
}

#[tokio::test]
async fn test_bulk_write_succeeds_under_backpressure_limit() {
    let config = BackpressureConfig {
        max_inflight_writes: 10000,
        max_inflight_bytes: 1024,
    };
    let (app, _dir) = create_test_app_with_backpressure(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/test/bulk")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"records":[{"payload":"a","partition":0},{"payload":"b","partition":0}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 2);
    assert_eq!(json["offsets"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_bulk_write_protobuf() {
    let (app, _dir) = create_test_app();

    let request = proto::BulkWriteRequest {
        records: vec![
            proto::BulkWriteRecord {
                payload: b"alpha".to_vec(),
                partition: 0,
                timestamp_ms: 0,
                idempotency_key: String::new(),
            },
            proto::BulkWriteRecord {
                payload: b"beta".to_vec(),
                partition: 1,
                timestamp_ms: 123456789,
                idempotency_key: "idem-1".into(),
            },
        ],
    };

    let mut buf = Vec::new();
    request.encode(&mut buf).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/test/bulk")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(buf))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 2);
    assert_eq!(json["offsets"].as_array().unwrap().len(), 2);
}

// ============================================================================
// Health Check Tests (#10)
// ============================================================================

#[tokio::test]
async fn test_health_live_returns_ok() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn test_health_ready_checks_storage() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Verify response structure
    assert_eq!(json["status"], "ready");
    assert_eq!(json["hot_storage"]["status"], "ok");
    assert_eq!(json["cold_storage"]["status"], "not_configured");
    assert_eq!(json["backpressure"]["status"], "ok");

    // Verify backpressure fields exist and are valid
    assert!(json["backpressure"]["max_inflight_bytes"].is_number());
    assert!(json["backpressure"]["inflight_bytes"].is_number());
    assert!(json["backpressure"]["inflight_writes"].is_number());
}

#[tokio::test]
async fn test_health_ready_with_cold_storage() {
    let (app, _dir) = create_test_app_with_cold_storage();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "ready");
    assert_eq!(json["hot_storage"]["status"], "ok");
    // NoopColdStorage returns "none" as storage_type, so it shows as not_configured
    assert_eq!(json["cold_storage"]["status"], "not_configured");
}

// ============================================================================
// Prometheus Metrics Tests (#9)
// ============================================================================

#[tokio::test]
async fn test_metrics_returns_prometheus_format() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify content type is text/plain
    let content_type = response
        .headers()
        .get("content-type")
        .expect("content-type header missing");
    assert!(content_type.to_str().unwrap().starts_with("text/plain"));

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Verify Prometheus format with HELP and TYPE lines
    assert!(body_str.contains("# HELP zombi_writes_total"));
    assert!(body_str.contains("# TYPE zombi_writes_total counter"));
    assert!(body_str.contains("# HELP zombi_uptime_secs"));
    assert!(body_str.contains("# TYPE zombi_uptime_secs gauge"));
    assert!(body_str.contains("# HELP zombi_errors_total"));
    assert!(body_str.contains("zombi_inflight_bytes"));
    assert!(body_str.contains("zombi_inflight_writes_available"));
}

#[tokio::test]
async fn test_metrics_values_update_after_writes() {
    let (app, _dir) = create_test_app();

    // Write some records to generate metrics
    for _ in 0..3 {
        let app_clone = app.clone();
        let response = app_clone
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tables/metrics_test")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"payload": "test data", "partition": 0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    // Get metrics
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Verify writes_total is 3
    assert!(body_str.contains("zombi_writes_total 3"));
    // Verify bytes were written
    assert!(body_str.contains("zombi_writes_bytes_total"));
}

#[tokio::test]
async fn test_metrics_include_hot_storage_events_after_writes() {
    let (app, _dir) = create_test_app();

    // Write a record to create topic/partition
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/hot_metrics_test")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "hot", "partition": 0}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    // Scrape metrics (triggers refresh_hot_storage_metrics)
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(
        body_str.contains("zombi_hot_storage_events{topic=\"hot_metrics_test\",partition=\"0\"} 1")
    );
}

// ============================================================================
// Column Projection Tests (#38)
// ============================================================================

#[tokio::test]
async fn test_read_without_fields_returns_default_response() {
    let (app, _dir) = create_test_app();

    // Write a record
    let app_clone = app.clone();
    app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/proj_test")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload": "hello projection", "partition": 0}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Read without fields param
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/proj_test?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Default response has payload + timestamp_ms (backward compatible)
    let record = &json["records"][0];
    assert_eq!(record["payload"], "hello projection");
    assert!(record["timestamp_ms"].is_number());
    // Should NOT have sequence, topic, partition, idempotency_key by default
    assert!(record.get("sequence").is_none());
    assert!(record.get("topic").is_none());
}

#[tokio::test]
async fn test_read_with_fields_returns_projected_response() {
    let (app, _dir) = create_test_app();

    // Write a record
    let app_clone = app.clone();
    app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/proj_test2")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload": "field test", "partition": 0, "idempotency_key": "key-1"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Read with fields=payload,timestamp_ms,sequence
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/proj_test2?limit=10&fields=payload,timestamp_ms,sequence")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let record = &json["records"][0];
    // Requested fields present
    assert_eq!(record["payload"], "field test");
    assert!(record["timestamp_ms"].is_number());
    assert!(record["sequence"].is_number());
    // Non-requested fields absent
    assert!(record.get("topic").is_none());
    assert!(record.get("partition").is_none());
    assert!(record.get("idempotency_key").is_none());
}

#[tokio::test]
async fn test_read_with_invalid_field_returns_400() {
    let (app, _dir) = create_test_app();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/proj_test3?fields=bogus_field")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(json["error"]
        .as_str()
        .unwrap()
        .contains("Unknown field 'bogus_field'"));
}

#[tokio::test]
async fn test_read_with_single_field() {
    let (app, _dir) = create_test_app();

    // Write a record
    let app_clone = app.clone();
    app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/proj_test4")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "single field", "partition": 0}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Read with fields=payload only
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/proj_test4?limit=10&fields=payload")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let record = &json["records"][0];
    assert_eq!(record["payload"], "single field");
    // Only payload should be present
    let obj = record.as_object().unwrap();
    assert_eq!(obj.len(), 1);
    assert!(obj.contains_key("payload"));
}

#[tokio::test]
async fn test_read_with_fields_and_since_filter() {
    let (app, _dir) = create_test_app();

    // Write two records with different timestamps
    let app_clone = app.clone();
    app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/proj_test5")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload": "old event", "partition": 0, "timestamp_ms": 1000}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let app_clone2 = app.clone();
    app_clone2
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/proj_test5")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload": "new event", "partition": 0, "timestamp_ms": 2000}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Read with fields + since filter
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/proj_test5?limit=10&fields=sequence,payload&since=1500")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["count"], 1);
    let record = &json["records"][0];
    assert_eq!(record["payload"], "new event");
    assert!(record["sequence"].is_number());
    assert!(record.get("timestamp_ms").is_none());
}

// ============================================================================
// Sharded Write Combiner Tests (#94)
// ============================================================================

#[tokio::test]
async fn test_combiner_shard_isolation() {
    // Use 2 shards so tables hash to different shards.
    // Write heavily to one table; verify the other table still completes promptly.
    let config = WriteCombinerConfig {
        enabled: true,
        batch_size: 5,
        window: std::time::Duration::from_millis(50),
        queue_capacity: 200,
        shards: 2,
        ..Default::default()
    };
    let (app, _dir) = create_test_app_with_combiner(config);

    // Fire 40 writes to table_heavy and 10 to table_light concurrently
    let requests = (0..50).map(|i| {
        let app = app.clone();
        let table = if i < 40 { "table_heavy" } else { "table_light" };
        async move {
            let resp = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/tables/{}", table))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            "{{\"payload\": \"event {}\", \"partition\": 0}}",
                            i
                        )))
                        .unwrap(),
                )
                .await
                .unwrap();
            (table.to_string(), resp)
        }
    });

    let results = join_all(requests).await;
    let mut heavy_ok = 0;
    let mut light_ok = 0;
    for (table, resp) in results {
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        match table.as_str() {
            "table_heavy" => heavy_ok += 1,
            "table_light" => light_ok += 1,
            _ => panic!("unexpected table"),
        }
    }
    assert_eq!(heavy_ok, 40);
    assert_eq!(light_ok, 10);
}

#[tokio::test]
async fn test_combiner_selective_flush() {
    // Use a long window so timer-based flush is the only way batches drain.
    // Write to two tables, then wait for the window to expire.
    // Both tables should eventually flush (via selective flush).
    let config = WriteCombinerConfig {
        enabled: true,
        batch_size: 100, // high batch_size so size-triggered flush won't fire
        window: std::time::Duration::from_millis(50),
        queue_capacity: 200,
        shards: 1, // single shard to test selective flush logic specifically
        ..Default::default()
    };
    let (app, _dir) = create_test_app_with_combiner(config);

    // Write 3 events to table_a
    for i in 0..3 {
        let app = app.clone();
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/table_a")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    "{{\"payload\": \"a-{}\", \"partition\": 0}}",
                    i
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // Write 2 events to table_b
    for i in 0..2 {
        let app = app.clone();
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/table_b")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    "{{\"payload\": \"b-{}\", \"partition\": 0}}",
                    i
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // Wait for the combiner window to expire and flush
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Read back both tables — they should have all events
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tables/table_a?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 3);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/table_b?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 2);
}

#[tokio::test]
async fn test_combiner_byte_backpressure() {
    // Set a very small byte limit (1 KB) to trigger byte-based backpressure.
    // Use high batch_size + long window so the combiner never flushes, keeping
    // inflight bytes accumulated across concurrent writes.
    let config = WriteCombinerConfig {
        enabled: true,
        batch_size: 100,
        window: std::time::Duration::from_secs(10),
        queue_capacity: 10_000,
        shards: 1,
        max_queue_bytes: 1024, // 1 KB byte limit
    };
    let (app, _dir) = create_test_app_with_combiner(config);

    // Fire many concurrent writes with large payloads so inflight bytes accumulate
    // before any flush can drain them.
    let large_payload = "x".repeat(600); // ~600 bytes each
    let requests = (0..10).map(|i| {
        let app = app.clone();
        let payload = large_payload.clone();
        async move {
            let resp = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/tables/byte_test")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            "{{\"payload\": \"{}\", \"partition\": 0}}",
                            payload
                        )))
                        .unwrap(),
                )
                .await
                .unwrap();
            (i, resp.status())
        }
    });

    let results = join_all(requests).await;
    let saw_overload = results
        .iter()
        .any(|(_, status)| *status == StatusCode::SERVICE_UNAVAILABLE);

    assert!(
        saw_overload,
        "Expected byte backpressure to trigger 503 when concurrent writes exceed 1KB limit"
    );
}

#[tokio::test]
async fn test_combiner_metrics_in_prometheus_output() {
    let config = WriteCombinerConfig {
        enabled: true,
        batch_size: 100,
        window: std::time::Duration::from_secs(10),
        shards: 2,
        ..Default::default()
    };
    let (app, _dir) = create_test_app_with_combiner(config);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Per-shard gauges should be present (registered at combiner creation)
    assert!(body_str.contains("zombi_write_combiner_queue_depth{shard=\"0\"}"));
    assert!(body_str.contains("zombi_write_combiner_queue_depth{shard=\"1\"}"));
    assert!(body_str.contains("zombi_write_combiner_queue_depth_total"));
}

// ============================================================================
// Hot-Only Read Tests (#102)
// ============================================================================

#[tokio::test]
async fn test_read_with_cold_storage_returns_only_hot_data() {
    // Use PanicOnReadColdStorage: if the handler calls read_events on cold storage,
    // the test will panic — proving the handler only reads from hot storage.
    let (app, _dir) = create_test_app_with_panic_cold_storage();

    // Write one event to hot storage
    let app_clone = app.clone();
    let write_resp = app_clone
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/events")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload": "hot-only-event", "partition": 0}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write_resp.status(), StatusCode::ACCEPTED);

    // Read back — must succeed without touching cold storage (would panic otherwise)
    let read_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tables/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(read_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["count"], 1);
    assert_eq!(json["records"][0]["payload"], "hot-only-event");
}

// ============================================================================
// Table Name Validation Tests (#101)
// ============================================================================

#[tokio::test]
async fn test_write_rejects_table_starting_with_number() {
    let (app, _dir) = create_test_app();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/123bad")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "test", "partition": 0}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_write_rejects_table_with_special_chars() {
    let (app, _dir) = create_test_app();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/bad%21name")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "test", "partition": 0}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_read_rejects_invalid_table_name() {
    let (app, _dir) = create_test_app();
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tables/123invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_write_accepts_valid_table_with_hyphens_and_underscores() {
    let (app, _dir) = create_test_app();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/my_events-v2")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload": "test", "partition": 0}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn test_bulk_write_rejects_invalid_table_name() {
    let (app, _dir) = create_test_app();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/123bad/bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"events": [{"payload": "test"}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
