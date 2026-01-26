//! Thread-safe histogram implementation for latency tracking.
//!
//! Uses fixed buckets optimized for microsecond latencies, enabling
//! `histogram_quantile()` calculations in Prometheus.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

/// Fixed histogram buckets in microseconds.
/// Optimized for typical storage operation latencies (10μs to 50ms).
pub const HISTOGRAM_BUCKETS: [u64; 12] = [
    10,     // 10μs
    25,     // 25μs
    50,     // 50μs
    100,    // 100μs
    250,    // 250μs
    500,    // 500μs
    1_000,  // 1ms
    2_500,  // 2.5ms
    5_000,  // 5ms
    10_000, // 10ms
    25_000, // 25ms
    50_000, // 50ms
];

/// Thread-safe histogram for tracking latency distributions.
///
/// Uses lock-free atomic operations for minimal overhead in the hot path.
/// Observations are placed into fixed buckets, enabling efficient
/// percentile calculations via Prometheus's `histogram_quantile()`.
///
/// # Example
///
/// ```
/// use zombi::metrics::Histogram;
///
/// let histogram = Histogram::new();
/// histogram.observe(150); // 150μs observation
///
/// let (sum, count, buckets) = histogram.snapshot();
/// assert_eq!(count, 1);
/// assert_eq!(sum, 150);
/// ```
pub struct Histogram {
    /// Sum of all observed values (for average calculation)
    sum: AtomicU64,
    /// Total count of observations
    count: AtomicU64,
    /// Bucket counts (cumulative - each bucket includes smaller values)
    buckets: [AtomicU64; 12],
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    /// Creates a new empty histogram.
    #[allow(clippy::declare_interior_mutable_const)]
    pub fn new() -> Self {
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            buckets: [ZERO; 12],
        }
    }

    /// Records an observation in microseconds.
    ///
    /// This operation is lock-free and safe for concurrent use.
    /// Uses `Ordering::Relaxed` since exact ordering isn't critical for metrics.
    #[inline]
    pub fn observe(&self, value_us: u64) {
        self.sum.fetch_add(value_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        // Increment all buckets where value <= bucket boundary
        // This gives cumulative bucket counts as required by Prometheus
        for (i, &boundary) in HISTOGRAM_BUCKETS.iter().enumerate() {
            if value_us <= boundary {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns a snapshot of the histogram state.
    ///
    /// Returns `(sum, count, bucket_counts)` where bucket_counts are cumulative.
    pub fn snapshot(&self) -> (u64, u64, [u64; 12]) {
        let sum = self.sum.load(Ordering::Relaxed);
        let count = self.count.load(Ordering::Relaxed);

        let mut buckets = [0u64; 12];
        for (i, bucket) in self.buckets.iter().enumerate() {
            buckets[i] = bucket.load(Ordering::Relaxed);
        }

        (sum, count, buckets)
    }

    /// Returns the current count of observations.
    #[inline]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Returns the current sum of all observations.
    #[inline]
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Formats the histogram as Prometheus exposition format.
    ///
    /// # Arguments
    /// * `name` - The metric name (e.g., "zombi_write_latency_us")
    /// * `help` - The help text for the metric
    pub fn format_prometheus(&self, name: &str, help: &str) -> String {
        let (sum, count, buckets) = self.snapshot();

        let mut output = String::with_capacity(1024);

        // Using write! macro is more efficient than push_str + format!
        // as it avoids intermediate String allocations
        let _ = writeln!(output, "# HELP {} {}", name, help);
        let _ = writeln!(output, "# TYPE {} histogram", name);

        // Output bucket counts (cumulative)
        for (i, &boundary) in HISTOGRAM_BUCKETS.iter().enumerate() {
            let _ = writeln!(
                output,
                "{}_bucket{{le=\"{}\"}} {}",
                name, boundary, buckets[i]
            );
        }

        // +Inf bucket (total count - all observations fall into this)
        let _ = writeln!(output, "{}_bucket{{le=\"+Inf\"}} {}", name, count);
        let _ = writeln!(output, "{}_sum {}", name, sum);
        let _ = writeln!(output, "{}_count {}", name, count);

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_observe() {
        let h = Histogram::new();
        h.observe(50);
        h.observe(100);
        h.observe(500);

        let (sum, count, _) = h.snapshot();
        assert_eq!(sum, 650);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_histogram_bucket_distribution() {
        let h = Histogram::new();
        h.observe(5); // <= 10
        h.observe(15); // <= 25
        h.observe(75); // <= 100
        h.observe(100_000); // > all buckets

        let (_, count, buckets) = h.snapshot();
        assert_eq!(count, 4);

        // Buckets are cumulative
        assert_eq!(buckets[0], 1); // <= 10
        assert_eq!(buckets[1], 2); // <= 25
        assert_eq!(buckets[2], 2); // <= 50
        assert_eq!(buckets[3], 3); // <= 100
    }

    #[test]
    fn test_histogram_prometheus_format() {
        let h = Histogram::new();
        h.observe(50);
        h.observe(100);

        let output = h.format_prometheus("test_latency", "Test latency histogram");
        assert!(output.contains("# HELP test_latency Test latency histogram"));
        assert!(output.contains("# TYPE test_latency histogram"));
        assert!(output.contains("test_latency_bucket{le=\"+Inf\"} 2"));
        assert!(output.contains("test_latency_sum 150"));
        assert!(output.contains("test_latency_count 2"));
    }

    #[test]
    fn test_histogram_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let h = Arc::new(Histogram::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let h_clone = Arc::clone(&h);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    h_clone.observe(i);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(h.count(), 10_000);
    }
}
