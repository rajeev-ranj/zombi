use std::sync::Arc;

use tracing_subscriber::EnvFilter;

use zombi::api::{start_server, AppState, Metrics, ServerConfig};
use zombi::contracts::Flusher;
use zombi::flusher::{BackgroundFlusher, FlusherConfig};
use zombi::storage::{ColdStorageBackend, IcebergStorage, RocksDbStorage, S3Storage};

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

    // Initialize cold storage (S3 or Iceberg) if configured
    let s3_bucket = std::env::var("ZOMBI_S3_BUCKET").ok();
    let iceberg_enabled = std::env::var("ZOMBI_ICEBERG_ENABLED")
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let cold_storage = if let Some(bucket) = s3_bucket {
        let endpoint = std::env::var("ZOMBI_S3_ENDPOINT").ok();
        let region = std::env::var("ZOMBI_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let storage_path = std::env::var("ZOMBI_STORAGE_PATH").unwrap_or_else(|_| "tables".into());

        let backend = if iceberg_enabled {
            // Use Iceberg storage (Parquet + metadata)
            let iceberg = if let Some(ref endpoint) = endpoint {
                tracing::info!(
                    "Connecting to Iceberg storage at {} (bucket: {}, path: {})",
                    endpoint,
                    bucket,
                    storage_path
                );
                IcebergStorage::with_endpoint(&bucket, &storage_path, endpoint, &region).await?
            } else {
                tracing::info!(
                    "Connecting to Iceberg storage on AWS S3 (bucket: {}, path: {})",
                    bucket,
                    storage_path
                );
                IcebergStorage::new(&bucket, &storage_path).await?
            };
            ColdStorageBackend::iceberg(iceberg)
        } else {
            // Use plain S3 storage (JSON segments)
            let s3 = if let Some(ref endpoint) = endpoint {
                tracing::info!("Connecting to S3 at {} (bucket: {})", endpoint, bucket);
                S3Storage::with_endpoint(&bucket, endpoint, &region).await?
            } else {
                tracing::info!("Connecting to AWS S3 (bucket: {})", bucket);
                S3Storage::new(&bucket).await?
            };
            ColdStorageBackend::s3(s3)
        };

        Some(Arc::new(backend))
    } else {
        tracing::info!("S3 not configured, running with hot storage only");
        None
    };

    // Start background flusher if cold storage is configured
    let _flusher = if let Some(ref cold) = cold_storage {
        // Use Iceberg defaults when enabled, otherwise use standard defaults
        let base_config = if iceberg_enabled {
            FlusherConfig::iceberg_defaults()
        } else {
            FlusherConfig::default()
        };

        // Override with environment variables if specified
        let config = FlusherConfig {
            interval: std::env::var("ZOMBI_FLUSH_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(std::time::Duration::from_secs)
                .unwrap_or(base_config.interval),
            batch_size: std::env::var("ZOMBI_FLUSH_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(base_config.batch_size),
            max_segment_size: std::env::var("ZOMBI_FLUSH_MAX_SEGMENT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(base_config.max_segment_size),
            target_file_size_bytes: std::env::var("ZOMBI_TARGET_FILE_SIZE_MB")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .map(|mb| mb * 1024 * 1024)
                .unwrap_or(base_config.target_file_size_bytes),
            iceberg_enabled,
        };

        if iceberg_enabled {
            tracing::info!(
                interval_secs = config.interval.as_secs(),
                batch_size = config.batch_size,
                max_segment_size = config.max_segment_size,
                target_file_size_mb = config.target_file_size_bytes / (1024 * 1024),
                "Iceberg mode enabled - using optimized flush settings"
            );
        }

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
