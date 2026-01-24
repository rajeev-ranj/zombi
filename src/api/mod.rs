mod handlers;

use std::future::Future;
use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use crate::contracts::{ColdStorage, HotStorage};

pub use handlers::{
    AppState, BackpressureConfig, Metrics, NoopColdStorage, WriteRecordRequest, WriteRecordResponse,
};

/// Creates the API router.
pub fn create_router<H: HotStorage + 'static, C: ColdStorage + 'static>(
    state: Arc<AppState<H, C>>,
) -> Router {
    Router::new()
        .route("/health", get(handlers::health_check))
        .route("/stats", get(handlers::get_stats::<H, C>))
        .route("/tables/:table/bulk", post(handlers::bulk_write::<H, C>))
        .route("/tables/:table", post(handlers::write_record::<H, C>))
        .route("/tables/:table", get(handlers::read_records::<H, C>))
        // Iceberg admin endpoints
        .route(
            "/tables/:table/metadata",
            get(handlers::get_table_metadata::<H, C>),
        )
        .route("/tables/:table/flush", post(handlers::flush_table::<H, C>))
        .route(
            "/tables/:table/compact",
            post(handlers::compact_table::<H, C>),
        )
        // Consumer offset management
        .route(
            "/consumers/:group/commit",
            post(handlers::commit_offset::<H, C>),
        )
        .route(
            "/consumers/:group/offset",
            get(handlers::get_offset::<H, C>),
        )
        .with_state(state)
}

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 8080,
        }
    }
}

/// Starts the HTTP server.
pub async fn start_server<H, C, F>(
    config: ServerConfig,
    state: Arc<AppState<H, C>>,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    H: HotStorage + 'static,
    C: ColdStorage + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let router = create_router(state);
    let addr = format!("{}:{}", config.host, config.port);

    tracing::info!("Starting HTTP server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await?;

    Ok(())
}
