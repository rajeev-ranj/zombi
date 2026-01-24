//! S3 retry configuration and error classification.

use backon::ExponentialBuilder;
use std::time::Duration;

/// Macro to execute an S3 operation with retry and logging.
///
/// # Usage
/// ```ignore
/// s3_retry!(
///     operation = { client.put_object().bucket(b).key(k).send().await },
///     retry_config = self.retry_config,
///     context = format!("PUT {}", key),
/// )?;
/// ```
#[macro_export]
macro_rules! s3_retry {
    (
        operation = $op:expr,
        retry_config = $config:expr,
        context = $ctx:expr $(,)?
    ) => {{
        use backon::Retryable;
        use $crate::storage::retry::is_retryable_s3_error;

        let context = $ctx;
        (|| async { $op })
            .retry($config.backoff())
            .when(|e| is_retryable_s3_error(&e.to_string()))
            .notify(|err, dur| {
                tracing::warn!(
                    context = %context,
                    error = %err,
                    retry_in = ?dur,
                    "S3 operation failed, retrying"
                );
            })
            .await
            .map_err(|e| $crate::contracts::StorageError::S3(e.to_string()))
    }};
}

/// Configuration for S3 retry with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: usize,
    /// Initial delay between retries in milliseconds.
    pub initial_delay_ms: u64,
    /// Maximum delay between retries in milliseconds.
    pub max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_delay_ms: 100,
            max_delay_ms: 10_000,
        }
    }
}

impl RetryConfig {
    /// Creates a RetryConfig from environment variables.
    ///
    /// Environment variables:
    /// - `ZOMBI_S3_MAX_RETRIES`: Maximum retry attempts (default: 5)
    /// - `ZOMBI_S3_RETRY_INITIAL_MS`: Initial backoff delay in ms (default: 100)
    /// - `ZOMBI_S3_RETRY_MAX_MS`: Maximum backoff delay in ms (default: 10000)
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            max_retries: std::env::var("ZOMBI_S3_MAX_RETRIES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.max_retries),
            initial_delay_ms: std::env::var("ZOMBI_S3_RETRY_INITIAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.initial_delay_ms),
            max_delay_ms: std::env::var("ZOMBI_S3_RETRY_MAX_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.max_delay_ms),
        }
    }

    /// Creates an exponential backoff builder with jitter.
    pub fn backoff(&self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(self.initial_delay_ms))
            .with_max_delay(Duration::from_millis(self.max_delay_ms))
            .with_max_times(self.max_retries)
            .with_jitter()
    }
}

/// Classifies S3 errors as retryable or not.
///
/// Retryable errors include:
/// - Network issues (timeout, connection reset, connection refused, broken pipe)
/// - Service unavailability (503, ServiceUnavailable, InternalError)
/// - Throttling (429, SlowDown, ThrottlingException)
/// - Request timeout
pub fn is_retryable_s3_error(err: &str) -> bool {
    let retryable_patterns = [
        "timeout",
        "timed out",
        "connection reset",
        "serviceunavailable",
        "503",
        "slowdown",
        "429",
        "throttlingexception",
        "requesttimeout",
        "internalerror",
        "connection refused",
        "broken pipe",
    ];
    let err_lower = err.to_lowercase();
    retryable_patterns.iter().any(|p| err_lower.contains(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 10_000);
    }

    #[test]
    fn test_is_retryable_timeout() {
        assert!(is_retryable_s3_error("Connection timed out"));
        assert!(is_retryable_s3_error("Request timeout after 30s"));
        assert!(is_retryable_s3_error("Timeout waiting for response"));
    }

    #[test]
    fn test_is_retryable_connection() {
        assert!(is_retryable_s3_error("Connection reset by peer"));
        assert!(is_retryable_s3_error("connection refused"));
        assert!(is_retryable_s3_error("Broken pipe"));
    }

    #[test]
    fn test_is_retryable_service_errors() {
        assert!(is_retryable_s3_error("ServiceUnavailable"));
        assert!(is_retryable_s3_error("503 Service Unavailable"));
        assert!(is_retryable_s3_error("InternalError"));
    }

    #[test]
    fn test_is_retryable_throttling() {
        assert!(is_retryable_s3_error("SlowDown"));
        assert!(is_retryable_s3_error("429 Too Many Requests"));
        assert!(is_retryable_s3_error("ThrottlingException"));
    }

    #[test]
    fn test_not_retryable() {
        assert!(!is_retryable_s3_error("NoSuchBucket"));
        assert!(!is_retryable_s3_error("AccessDenied"));
        assert!(!is_retryable_s3_error("InvalidAccessKeyId"));
        assert!(!is_retryable_s3_error("NoSuchKey"));
        assert!(!is_retryable_s3_error("BucketNotFound"));
    }

    #[test]
    fn test_backoff_config() {
        let config = RetryConfig {
            max_retries: 3,
            initial_delay_ms: 50,
            max_delay_ms: 1000,
        };
        let _builder = config.backoff();
        // Builder is configured correctly if it compiles
    }

    #[test]
    fn test_from_env_with_defaults() {
        // Clear any env vars that might be set
        std::env::remove_var("ZOMBI_S3_MAX_RETRIES");
        std::env::remove_var("ZOMBI_S3_RETRY_INITIAL_MS");
        std::env::remove_var("ZOMBI_S3_RETRY_MAX_MS");

        let config = RetryConfig::from_env();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 10_000);
    }

    #[test]
    fn test_from_env_with_custom_values() {
        std::env::set_var("ZOMBI_S3_MAX_RETRIES", "3");
        std::env::set_var("ZOMBI_S3_RETRY_INITIAL_MS", "200");
        std::env::set_var("ZOMBI_S3_RETRY_MAX_MS", "5000");

        let config = RetryConfig::from_env();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay_ms, 200);
        assert_eq!(config.max_delay_ms, 5000);

        // Clean up
        std::env::remove_var("ZOMBI_S3_MAX_RETRIES");
        std::env::remove_var("ZOMBI_S3_RETRY_INITIAL_MS");
        std::env::remove_var("ZOMBI_S3_RETRY_MAX_MS");
    }

    #[test]
    fn test_from_env_ignores_invalid_values() {
        std::env::set_var("ZOMBI_S3_MAX_RETRIES", "not_a_number");
        std::env::set_var("ZOMBI_S3_RETRY_INITIAL_MS", "");
        std::env::set_var("ZOMBI_S3_RETRY_MAX_MS", "-100");

        let config = RetryConfig::from_env();
        // Should fall back to defaults for invalid values
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 10_000);

        // Clean up
        std::env::remove_var("ZOMBI_S3_MAX_RETRIES");
        std::env::remove_var("ZOMBI_S3_RETRY_INITIAL_MS");
        std::env::remove_var("ZOMBI_S3_RETRY_MAX_MS");
    }
}
