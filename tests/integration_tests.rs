use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use prost::Message;
use tower::ServiceExt;

use zombi::api::{create_router, AppState, Metrics, NoopColdStorage};
use zombi::proto;
use zombi::storage::RocksDbStorage;

fn create_test_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    let state = Arc::new(AppState {
        storage: Arc::new(storage),
        cold_storage: None::<Arc<NoopColdStorage>>,
        metrics: Arc::new(Metrics::new()),
    });
    let router = create_router(state);
    (router, dir)
}

fn create_test_app_with_cold_storage() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    let state = Arc::new(AppState {
        storage: Arc::new(storage),
        cold_storage: Some(Arc::new(NoopColdStorage)),
        metrics: Arc::new(Metrics::new()),
    });
    let router = create_router(state);
    (router, dir)
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
