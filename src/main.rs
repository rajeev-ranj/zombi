use std::sync::Arc;

use tracing_subscriber::EnvFilter;

use zombi::api::{start_server, AppState, Metrics, ServerConfig};
use zombi::contracts::Flusher;
use zombi::flusher::{BackgroundFlusher, FlusherConfig};
use zombi::storage::{RocksDbStorage, S3Storage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("zombi=info".parse()?))
        .init();

    tracing::info!("Zombi starting...");

    // Initialize hot storage (RocksDB)
    let data_dir = std::env::var("ZOMBI_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let storage = Arc::new(RocksDbStorage::open(&data_dir)?);
    tracing::info!("Opened RocksDB at {}", data_dir);

    // Initialize cold storage (S3) if configured
    let s3_bucket = std::env::var("ZOMBI_S3_BUCKET").ok();
    let cold_storage = if let Some(bucket) = s3_bucket {
        let endpoint = std::env::var("ZOMBI_S3_ENDPOINT").ok();
        let region = std::env::var("ZOMBI_S3_REGION").unwrap_or_else(|_| "us-east-1".into());

        let s3 = if let Some(endpoint) = endpoint {
            tracing::info!("Connecting to S3 at {} (bucket: {})", endpoint, bucket);
            S3Storage::with_endpoint(&bucket, &endpoint, &region).await?
        } else {
            tracing::info!("Connecting to AWS S3 (bucket: {})", bucket);
            S3Storage::new(&bucket).await?
        };

        Some(Arc::new(s3))
    } else {
        tracing::info!("S3 not configured, running with hot storage only");
        None
    };

    // Start background flusher if S3 is configured
    let _flusher = if let Some(ref cold) = cold_storage {
        let config = FlusherConfig {
            interval: std::time::Duration::from_secs(
                std::env::var("ZOMBI_FLUSH_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5),
            ),
            batch_size: std::env::var("ZOMBI_FLUSH_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            max_segment_size: std::env::var("ZOMBI_FLUSH_MAX_SEGMENT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10000),
        };

        let flusher = Arc::new(BackgroundFlusher::new(
            Arc::clone(&storage),
            Arc::clone(cold),
            config,
        ));

        flusher.start().await?;
        tracing::info!("Background flusher started");

        Some(flusher)
    } else {
        None
    };

    // Create app state
    let state = Arc::new(AppState {
        storage,
        cold_storage,
        metrics: Arc::new(Metrics::new()),
    });

    // Start server
    let config = ServerConfig {
        host: std::env::var("ZOMBI_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
        port: std::env::var("ZOMBI_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080),
    };

    start_server(config, state).await?;

    Ok(())
}
