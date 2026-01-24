use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Date32Array, Int32Array, Int64Array, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::contracts::{StorageError, StoredEvent};

/// Unix epoch date for Date32 calculations.
pub(crate) const UNIX_EPOCH_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => panic!("Invalid date"),
};

/// Derives partition columns (event_date, event_hour) from timestamp_ms.
///
/// Returns (days_since_epoch, hour_of_day).
pub fn derive_partition_columns(timestamp_ms: i64) -> (i32, i32) {
    let datetime = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());

    let date = datetime.date_naive();
    let days_since_epoch = (date - UNIX_EPOCH_DATE).num_days() as i32;
    let hour = datetime.hour() as i32;

    (days_since_epoch, hour)
}

/// Formats Date32 (days since epoch) to YYYY-MM-DD string for partition keys.
pub fn format_partition_date(event_date: i32) -> String {
    let date = UNIX_EPOCH_DATE + chrono::Duration::days(event_date as i64);
    date.format("%Y-%m-%d").to_string()
}

/// Partition values for a Parquet file.
#[derive(Debug, Clone, Default)]
pub struct PartitionValues {
    /// Minimum event_date (days since epoch).
    pub min_event_date: i32,
    /// Maximum event_date (days since epoch).
    pub max_event_date: i32,
    /// Minimum event_hour (0-23).
    pub min_event_hour: i32,
    /// Maximum event_hour (0-23).
    pub max_event_hour: i32,
}

/// Metadata about a written Parquet file.
#[derive(Debug, Clone)]
pub struct ParquetFileMetadata {
    /// Full path to the written file.
    pub path: String,
    /// Number of rows in the file.
    pub row_count: usize,
    /// Minimum sequence number in the file.
    pub min_sequence: u64,
    /// Maximum sequence number in the file.
    pub max_sequence: u64,
    /// Minimum timestamp in the file.
    pub min_timestamp_ms: i64,
    /// Maximum timestamp in the file.
    pub max_timestamp_ms: i64,
    /// File size in bytes.
    pub file_size_bytes: u64,
    /// Partition column bounds.
    pub partition_values: PartitionValues,
}

/// Returns the Arrow schema for StoredEvent.
/// Includes partition columns (event_date, event_hour) derived from timestamp_ms.
pub fn event_schema() -> Schema {
    Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::UInt32, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("idempotency_key", DataType::Utf8, true),
        // Partition columns derived from timestamp_ms
        Field::new("event_date", DataType::Date32, false),
        Field::new("event_hour", DataType::Int32, false),
    ])
}

/// Converts a slice of StoredEvent to an Arrow RecordBatch.
/// Automatically derives event_date and event_hour partition columns from timestamp_ms.
pub fn events_to_record_batch(events: &[StoredEvent]) -> Result<RecordBatch, StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot create batch from empty events".into(),
        ));
    }

    let schema = Arc::new(event_schema());

    // Build arrays from events
    let sequences: Vec<u64> = events.iter().map(|e| e.sequence).collect();
    let topics: Vec<&str> = events.iter().map(|e| e.topic.as_str()).collect();
    let partitions: Vec<u32> = events.iter().map(|e| e.partition).collect();
    let payloads: Vec<&[u8]> = events.iter().map(|e| e.payload.as_slice()).collect();
    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_ms).collect();
    let idempotency_keys: Vec<Option<&str>> = events
        .iter()
        .map(|e| e.idempotency_key.as_deref())
        .collect();

    // Derive partition columns from timestamps
    let partition_values: Vec<(i32, i32)> = events
        .iter()
        .map(|e| derive_partition_columns(e.timestamp_ms))
        .collect();
    let event_dates: Vec<i32> = partition_values.iter().map(|(d, _)| *d).collect();
    let event_hours: Vec<i32> = partition_values.iter().map(|(_, h)| *h).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(sequences)),
        Arc::new(StringArray::from(topics)),
        Arc::new(UInt32Array::from(partitions)),
        Arc::new(BinaryArray::from(payloads)),
        Arc::new(Int64Array::from(timestamps)),
        Arc::new(StringArray::from(idempotency_keys)),
        Arc::new(Date32Array::from(event_dates)),
        Arc::new(Int32Array::from(event_hours)),
    ];

    RecordBatch::try_new(schema, columns).map_err(|e| StorageError::Serialization(e.to_string()))
}

/// Writes events to a Parquet file.
///
/// # Arguments
/// * `events` - Events to write
/// * `output_path` - Path for the output Parquet file
///
/// # Returns
/// Metadata about the written file.
pub fn write_parquet<P: AsRef<Path>>(
    events: &[StoredEvent],
    output_path: P,
) -> Result<ParquetFileMetadata, StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot write empty events to Parquet".into(),
        ));
    }

    let path = output_path.as_ref();

    // Create parent directories if needed
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StorageError::Io(e.to_string()))?;
    }

    // Convert events to RecordBatch
    let batch = events_to_record_batch(events)?;

    // Configure Parquet writer for optimal Iceberg performance:
    // - ZSTD compression for good ratio with fast decompression
    // - Large row groups (1M rows max) to minimize metadata overhead
    //   Note: set_max_row_group_size takes ROW COUNT, not bytes
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(1_000_000)
        .build();

    // Write to file
    let file = File::create(path).map_err(|e| StorageError::Io(e.to_string()))?;

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    writer
        .write(&batch)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    writer
        .close()
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    // Calculate statistics
    // SAFETY: events is guaranteed non-empty (checked at function start)
    let min_sequence = events.iter().map(|e| e.sequence).min().unwrap();
    let max_sequence = events.iter().map(|e| e.sequence).max().unwrap();
    let min_timestamp = events.iter().map(|e| e.timestamp_ms).min().unwrap();
    let max_timestamp = events.iter().map(|e| e.timestamp_ms).max().unwrap();

    // Calculate partition bounds
    let partition_values = compute_partition_bounds(events);

    let file_size = std::fs::metadata(path)
        .map_err(|e| StorageError::Io(e.to_string()))?
        .len();

    Ok(ParquetFileMetadata {
        path: path.to_string_lossy().to_string(),
        row_count: events.len(),
        min_sequence,
        max_sequence,
        min_timestamp_ms: min_timestamp,
        max_timestamp_ms: max_timestamp,
        file_size_bytes: file_size,
        partition_values,
    })
}

/// Computes partition column bounds for a set of events.
fn compute_partition_bounds(events: &[StoredEvent]) -> PartitionValues {
    if events.is_empty() {
        return PartitionValues::default();
    }

    let partition_cols: Vec<(i32, i32)> = events
        .iter()
        .map(|e| derive_partition_columns(e.timestamp_ms))
        .collect();

    // SAFETY: events is guaranteed non-empty (checked at function start)
    let min_date = partition_cols.iter().map(|(d, _)| *d).min().unwrap();
    let max_date = partition_cols.iter().map(|(d, _)| *d).max().unwrap();
    let min_hour = partition_cols.iter().map(|(_, h)| *h).min().unwrap();
    let max_hour = partition_cols.iter().map(|(_, h)| *h).max().unwrap();

    PartitionValues {
        min_event_date: min_date,
        max_event_date: max_date,
        min_event_hour: min_hour,
        max_event_hour: max_hour,
    }
}

/// Writes events to a Parquet file in memory and returns bytes.
/// Useful for S3 uploads.
pub fn write_parquet_to_bytes(
    events: &[StoredEvent],
) -> Result<(Vec<u8>, ParquetFileMetadata), StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot write empty events to Parquet".into(),
        ));
    }

    // Convert events to RecordBatch
    let batch = events_to_record_batch(events)?;

    // Configure Parquet writer for optimal Iceberg performance:
    // - ZSTD compression for good ratio with fast decompression
    // - Large row groups (1M rows max) to minimize metadata overhead
    //   Note: set_max_row_group_size takes ROW COUNT, not bytes
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(1_000_000)
        .build();

    // Write to buffer
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    writer
        .write(&batch)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    writer
        .close()
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    // Calculate statistics
    // SAFETY: events is guaranteed non-empty (checked at function start)
    let min_sequence = events.iter().map(|e| e.sequence).min().unwrap();
    let max_sequence = events.iter().map(|e| e.sequence).max().unwrap();
    let min_timestamp = events.iter().map(|e| e.timestamp_ms).min().unwrap();
    let max_timestamp = events.iter().map(|e| e.timestamp_ms).max().unwrap();

    // Calculate partition bounds
    let partition_values = compute_partition_bounds(events);

    let metadata = ParquetFileMetadata {
        path: String::new(), // No path for in-memory
        row_count: events.len(),
        min_sequence,
        max_sequence,
        min_timestamp_ms: min_timestamp,
        max_timestamp_ms: max_timestamp,
        file_size_bytes: buffer.len() as u64,
        partition_values,
    };

    Ok((buffer, metadata))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_test_events() -> Vec<StoredEvent> {
        vec![
            StoredEvent {
                sequence: 1,
                topic: "test".into(),
                partition: 0,
                payload: b"hello".to_vec(),
                timestamp_ms: 1000,
                idempotency_key: Some("key1".into()),
            },
            StoredEvent {
                sequence: 2,
                topic: "test".into(),
                partition: 0,
                payload: b"world".to_vec(),
                timestamp_ms: 2000,
                idempotency_key: None,
            },
        ]
    }

    #[test]
    fn test_events_to_record_batch() {
        let events = make_test_events();
        let batch = events_to_record_batch(&events).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 8); // 6 original + 2 partition columns
    }

    #[test]
    fn test_derive_partition_columns() {
        // Test with known timestamp: 2024-01-15 14:30:00 UTC
        // = 1705329000000 ms since epoch
        let timestamp_ms = 1705329000000_i64;
        let (event_date, event_hour) = derive_partition_columns(timestamp_ms);

        // 2024-01-15 is day 19737 since epoch (1970-01-01)
        // (54 years * 365 days + 13 leap days + 14 days in Jan)
        assert_eq!(event_date, 19737);
        assert_eq!(event_hour, 14);
    }

    #[test]
    fn test_derive_partition_columns_midnight() {
        // Test midnight boundary: 2024-01-15 00:00:00 UTC
        let timestamp_ms = 1705276800000_i64;
        let (event_date, event_hour) = derive_partition_columns(timestamp_ms);

        assert_eq!(event_date, 19737);
        assert_eq!(event_hour, 0);
    }

    #[test]
    fn test_derive_partition_columns_end_of_day() {
        // Test end of day: 2024-01-15 23:59:59 UTC
        let timestamp_ms = 1705363199000_i64;
        let (event_date, event_hour) = derive_partition_columns(timestamp_ms);

        assert_eq!(event_date, 19737);
        assert_eq!(event_hour, 23);
    }

    #[test]
    fn test_write_parquet_file() {
        let events = make_test_events();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let metadata = write_parquet(&events, &path).unwrap();

        assert_eq!(metadata.row_count, 2);
        assert_eq!(metadata.min_sequence, 1);
        assert_eq!(metadata.max_sequence, 2);
        assert_eq!(metadata.min_timestamp_ms, 1000);
        assert_eq!(metadata.max_timestamp_ms, 2000);
        assert!(metadata.file_size_bytes > 0);
        assert!(path.exists());
    }

    #[test]
    fn test_write_parquet_to_bytes() {
        let events = make_test_events();

        let (bytes, metadata) = write_parquet_to_bytes(&events).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(metadata.row_count, 2);
        assert!(metadata.file_size_bytes > 0);
    }

    #[test]
    fn test_partition_values_in_metadata() {
        // Create events spanning multiple hours
        let events = vec![
            StoredEvent {
                sequence: 1,
                topic: "test".into(),
                partition: 0,
                payload: b"event1".to_vec(),
                timestamp_ms: 1705276800000, // 2024-01-15 00:00:00 UTC
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 2,
                topic: "test".into(),
                partition: 0,
                payload: b"event2".to_vec(),
                timestamp_ms: 1705329000000, // 2024-01-15 14:30:00 UTC
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 3,
                topic: "test".into(),
                partition: 0,
                payload: b"event3".to_vec(),
                timestamp_ms: 1705363199000, // 2024-01-15 23:59:59 UTC
                idempotency_key: None,
            },
        ];

        let (_, metadata) = write_parquet_to_bytes(&events).unwrap();

        // All events are on 2024-01-15 (day 19737)
        assert_eq!(metadata.partition_values.min_event_date, 19737);
        assert_eq!(metadata.partition_values.max_event_date, 19737);
        // Hours span from 0 to 23
        assert_eq!(metadata.partition_values.min_event_hour, 0);
        assert_eq!(metadata.partition_values.max_event_hour, 23);
    }

    #[test]
    fn test_partition_values_multiple_days() {
        // Create events spanning multiple days
        let events = vec![
            StoredEvent {
                sequence: 1,
                topic: "test".into(),
                partition: 0,
                payload: b"day1".to_vec(),
                timestamp_ms: 1705276800000, // 2024-01-15 00:00:00 UTC (day 19737)
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 2,
                topic: "test".into(),
                partition: 0,
                payload: b"day2".to_vec(),
                timestamp_ms: 1705449600000, // 2024-01-17 00:00:00 UTC (day 19739)
                idempotency_key: None,
            },
        ];

        let (_, metadata) = write_parquet_to_bytes(&events).unwrap();

        // Days span from 19737 to 19739
        assert_eq!(metadata.partition_values.min_event_date, 19737);
        assert_eq!(metadata.partition_values.max_event_date, 19739);
    }

    #[test]
    fn test_empty_events_error() {
        let events: Vec<StoredEvent> = vec![];

        assert!(events_to_record_batch(&events).is_err());
        assert!(write_parquet_to_bytes(&events).is_err());
    }

    /// Test that larger batches produce better compression ratios.
    #[test]
    fn test_larger_batches_compress_better() {
        let small_events: Vec<StoredEvent> = (0..100)
            .map(|i| StoredEvent {
                sequence: i as u64,
                topic: "test".into(),
                partition: 0,
                payload: format!("event payload data {}", i).into_bytes(),
                timestamp_ms: 1705276800000 + (i as i64 * 1000),
                idempotency_key: Some(format!("key-{}", i)),
            })
            .collect();

        let large_events: Vec<StoredEvent> = (0..1000)
            .map(|i| StoredEvent {
                sequence: i as u64,
                topic: "test".into(),
                partition: 0,
                payload: format!("event payload data {}", i).into_bytes(),
                timestamp_ms: 1705276800000 + (i as i64 * 1000),
                idempotency_key: Some(format!("key-{}", i)),
            })
            .collect();

        let (small_bytes, small_meta) = write_parquet_to_bytes(&small_events).unwrap();
        let (large_bytes, large_meta) = write_parquet_to_bytes(&large_events).unwrap();

        let small_bpe = small_bytes.len() as f64 / small_meta.row_count as f64;
        let large_bpe = large_bytes.len() as f64 / large_meta.row_count as f64;

        assert!(large_bpe < small_bpe, "Larger batch should compress better");
    }

    /// Test that Parquet files use ZSTD compression.
    #[test]
    fn test_parquet_uses_zstd_compression() {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let events = make_test_events();
        let (bytes, _) = write_parquet_to_bytes(&events).unwrap();
        let bytes = Bytes::from(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let metadata = builder.metadata();

        assert!(metadata.num_row_groups() > 0);
        assert_eq!(
            metadata.row_group(0).column(0).compression(),
            parquet::basic::Compression::ZSTD(Default::default())
        );
    }

    /// Test that row group size is configured correctly (in rows, not bytes).
    #[test]
    fn test_row_group_size_is_reasonable() {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let events: Vec<StoredEvent> = (0..500)
            .map(|i| StoredEvent {
                sequence: i as u64,
                topic: "test".into(),
                partition: 0,
                payload: b"test".to_vec(),
                timestamp_ms: 1705276800000,
                idempotency_key: None,
            })
            .collect();

        let (bytes, _) = write_parquet_to_bytes(&events).unwrap();
        let bytes = Bytes::from(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let metadata = builder.metadata();

        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.row_group(0).num_rows(), 500);
    }

    /// Test compression effectiveness.
    #[test]
    fn test_compression_effectiveness() {
        let events: Vec<StoredEvent> = (0..1000)
            .map(|i| StoredEvent {
                sequence: i as u64,
                topic: "orders".into(),
                partition: 0,
                payload: b"repeated payload data".to_vec(),
                timestamp_ms: 1705276800000,
                idempotency_key: None,
            })
            .collect();

        let (bytes, meta) = write_parquet_to_bytes(&events).unwrap();
        let raw = events.iter().map(|e| e.payload.len()).sum::<usize>();

        assert!(bytes.len() < raw);
        assert_eq!(meta.row_count, 1000);
    }
}
