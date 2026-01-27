//! Observability metrics for Zombi.
//!
//! This module provides comprehensive metrics collection for monitoring
//! Zombi's performance and health. All metrics use lock-free atomics
//! for minimal hot-path impact.

pub mod histogram;
pub mod registry;

pub use histogram::Histogram;
pub use registry::{
    ConsumerMetrics, EnhancedApiMetrics, FlushMetrics, HotStorageMetrics, IcebergMetrics,
    MetricsRegistry,
};
