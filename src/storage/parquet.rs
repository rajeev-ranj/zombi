use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::contracts::{FieldType, StorageError, StoredEvent, TableSchemaConfig};
use crate::storage::payload_extractor::{self, ExtractedValue};

/// Creates default Parquet writer properties optimized for Iceberg.
///
/// Configuration:
/// - ZSTD compression for excellent compression ratio
/// - 1M row max row group size to minimize metadata overhead
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(1_000_000)
        .build()
}

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

/// Column-level statistics for Iceberg DataFile bounds.
/// Contains min/max values for each indexed column, keyed by Iceberg field ID.
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Field ID 1: sequence (u64, stored as i64 for Iceberg)
    pub sequence_min: u64,
    pub sequence_max: u64,
    /// Field ID 3: partition (u32, stored as i32 for Iceberg)
    pub partition_min: u32,
    pub partition_max: u32,
    /// Field ID 5: timestamp_ms (i64)
    pub timestamp_min: i64,
    pub timestamp_max: i64,
    /// Field ID 7: event_date (Date32 - days since epoch as i32)
    pub event_date_min: i32,
    pub event_date_max: i32,
    /// Field ID 8: event_hour (i32)
    pub event_hour_min: i32,
    pub event_hour_max: i32,
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
    /// Column statistics for Iceberg DataFile bounds.
    pub column_stats: ColumnStatistics,
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

/// Returns an Arrow schema with extracted columns for structured payloads.
///
/// Layout:
/// - System columns: sequence, topic, partition, timestamp_ms, idempotency_key
/// - Extracted columns: one per `TableSchemaConfig.fields` entry (nullable)
/// - Overflow column: `_payload_overflow` (Binary, nullable) if `preserve_overflow`
/// - Partition columns: event_date, event_hour
pub fn event_schema_with_extraction(config: &TableSchemaConfig) -> Schema {
    let mut fields = vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::UInt32, false),
        // payload (field ID 4) is replaced by extracted + overflow columns
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("idempotency_key", DataType::Utf8, true),
    ];

    // Add extracted field columns
    for f in &config.fields {
        let arrow_type = match f.data_type {
            FieldType::Utf8 => DataType::Utf8,
            FieldType::Int32 => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::Float64 => DataType::Float64,
            FieldType::Boolean => DataType::Boolean,
            FieldType::Binary => DataType::Binary,
        };
        fields.push(Field::new(&f.name, arrow_type, f.nullable));
    }

    // Overflow column (replaces original payload column at field ID 4)
    if config.preserve_overflow {
        fields.push(Field::new("_payload_overflow", DataType::Binary, true));
    }

    // Partition columns
    fields.push(Field::new("event_date", DataType::Date32, false));
    fields.push(Field::new("event_hour", DataType::Int32, false));

    Schema::new(fields)
}

/// Converts events to a RecordBatch with structured extracted columns.
///
/// Parses each event's payload according to the schema config, extracts typed
/// columns, and collects overflow data. This is the core of the structured
/// columns feature — called only at flush time (cold path).
pub fn events_to_record_batch_structured(
    events: &[StoredEvent],
    config: &TableSchemaConfig,
) -> Result<RecordBatch, StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot create batch from empty events".into(),
        ));
    }

    let schema = Arc::new(event_schema_with_extraction(config));

    // Extract fields from all payloads
    let payloads: Vec<&[u8]> = events.iter().map(|e| e.payload.as_slice()).collect();
    let extractions = payload_extractor::extract_batch(&payloads, config)?;

    // Build system columns
    let sequences: Vec<u64> = events.iter().map(|e| e.sequence).collect();
    let topics: Vec<&str> = events.iter().map(|e| e.topic.as_str()).collect();
    let partitions: Vec<u32> = events.iter().map(|e| e.partition).collect();
    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_ms).collect();
    let idempotency_keys: Vec<Option<&str>> = events
        .iter()
        .map(|e| e.idempotency_key.as_deref())
        .collect();

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(sequences)),
        Arc::new(StringArray::from(topics)),
        Arc::new(UInt32Array::from(partitions)),
        // Note: payload column is omitted — replaced by extracted + overflow
        Arc::new(Int64Array::from(timestamps)),
        Arc::new(StringArray::from(idempotency_keys)),
    ];

    // Build extracted field columns
    for (field_idx, field_def) in config.fields.iter().enumerate() {
        let array: ArrayRef = match field_def.data_type {
            FieldType::Utf8 => {
                let vals: Vec<Option<String>> = extractions
                    .iter()
                    .map(|ex| match &ex.values[field_idx] {
                        Some(ExtractedValue::Utf8(s)) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                Arc::new(StringArray::from(vals))
            }
            FieldType::Int32 => {
                let vals: Vec<Option<i32>> = extractions
                    .iter()
                    .map(|ex| match &ex.values[field_idx] {
                        Some(ExtractedValue::Int32(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(vals))
            }
            FieldType::Int64 => {
                let vals: Vec<Option<i64>> = extractions
                    .iter()
                    .map(|ex| match &ex.values[field_idx] {
                        Some(ExtractedValue::Int64(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(vals))
            }
            FieldType::Float64 => {
                let vals: Vec<Option<f64>> = extractions
                    .iter()
                    .map(|ex| match &ex.values[field_idx] {
                        Some(ExtractedValue::Float64(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(vals))
            }
            FieldType::Boolean => {
                let vals: Vec<Option<bool>> = extractions
                    .iter()
                    .map(|ex| match &ex.values[field_idx] {
                        Some(ExtractedValue::Boolean(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                Arc::new(BooleanArray::from(vals))
            }
            FieldType::Binary => {
                let vals: Vec<Option<Vec<u8>>> = extractions
                    .iter()
                    .map(|ex| match &ex.values[field_idx] {
                        Some(ExtractedValue::Binary(v)) => Some(v.clone()),
                        _ => None,
                    })
                    .collect();
                let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
                Arc::new(BinaryArray::from(refs))
            }
        };
        columns.push(array);
    }

    // Overflow column
    if config.preserve_overflow {
        let overflow_vals: Vec<Option<&[u8]>> = extractions
            .iter()
            .map(|ex| ex.overflow.as_deref())
            .collect();
        columns.push(Arc::new(BinaryArray::from(overflow_vals)));
    }

    // Partition columns
    let partition_values: Vec<(i32, i32)> = events
        .iter()
        .map(|e| derive_partition_columns(e.timestamp_ms))
        .collect();
    let event_dates: Vec<i32> = partition_values.iter().map(|(d, _)| *d).collect();
    let event_hours: Vec<i32> = partition_values.iter().map(|(_, h)| *h).collect();
    columns.push(Arc::new(Date32Array::from(event_dates)));
    columns.push(Arc::new(Int32Array::from(event_hours)));

    RecordBatch::try_new(schema, columns).map_err(|e| StorageError::Serialization(e.to_string()))
}

/// Writes events to Parquet bytes with structured column extraction.
pub fn write_parquet_to_bytes_structured(
    events: &[StoredEvent],
    config: &TableSchemaConfig,
) -> Result<(Vec<u8>, ParquetFileMetadata), StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot write empty events to Parquet".into(),
        ));
    }

    let batch = events_to_record_batch_structured(events, config)?;
    let props = default_writer_properties();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    writer
        .write(&batch)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    writer
        .close()
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    let column_stats = compute_column_statistics(events)?;
    let partition_values = compute_partition_bounds(events)?;

    let metadata = ParquetFileMetadata {
        path: String::new(),
        row_count: events.len(),
        min_sequence: column_stats.sequence_min,
        max_sequence: column_stats.sequence_max,
        min_timestamp_ms: column_stats.timestamp_min,
        max_timestamp_ms: column_stats.timestamp_max,
        file_size_bytes: buffer.len() as u64,
        partition_values,
        column_stats,
    };

    Ok((buffer, metadata))
}

/// Writes events to Parquet bytes with structured extraction and sorting.
pub fn write_parquet_to_bytes_structured_sorted(
    events: &mut [StoredEvent],
    config: &TableSchemaConfig,
) -> Result<(Vec<u8>, ParquetFileMetadata), StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot write empty events to Parquet".into(),
        ));
    }

    events.sort_by(|a, b| {
        a.timestamp_ms
            .cmp(&b.timestamp_ms)
            .then(a.sequence.cmp(&b.sequence))
    });

    write_parquet_to_bytes_structured(events, config)
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

    // Use default writer properties optimized for Iceberg
    let props = default_writer_properties();

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

    // Calculate statistics - events is guaranteed non-empty (checked at function start)
    let column_stats = compute_column_statistics(events)?;

    // Calculate partition bounds
    let partition_values = compute_partition_bounds(events)?;

    let file_size = std::fs::metadata(path)
        .map_err(|e| StorageError::Io(e.to_string()))?
        .len();

    Ok(ParquetFileMetadata {
        path: path.to_string_lossy().to_string(),
        row_count: events.len(),
        min_sequence: column_stats.sequence_min,
        max_sequence: column_stats.sequence_max,
        min_timestamp_ms: column_stats.timestamp_min,
        max_timestamp_ms: column_stats.timestamp_max,
        file_size_bytes: file_size,
        partition_values,
        column_stats,
    })
}

/// Computes partition column bounds for a set of events.
fn compute_partition_bounds(events: &[StoredEvent]) -> Result<PartitionValues, StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot compute partition bounds for empty events".into(),
        ));
    }

    let partition_cols: Vec<(i32, i32)> = events
        .iter()
        .map(|e| derive_partition_columns(e.timestamp_ms))
        .collect();

    let min_date = partition_cols
        .iter()
        .map(|(d, _)| *d)
        .min()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;
    let max_date = partition_cols
        .iter()
        .map(|(d, _)| *d)
        .max()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;
    let min_hour = partition_cols
        .iter()
        .map(|(_, h)| *h)
        .min()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;
    let max_hour = partition_cols
        .iter()
        .map(|(_, h)| *h)
        .max()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;

    Ok(PartitionValues {
        min_event_date: min_date,
        max_event_date: max_date,
        min_event_hour: min_hour,
        max_event_hour: max_hour,
    })
}

/// Computes column statistics for all indexed columns.
/// These statistics are used for Iceberg DataFile lower_bounds/upper_bounds.
fn compute_column_statistics(events: &[StoredEvent]) -> Result<ColumnStatistics, StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot compute column statistics for empty events".into(),
        ));
    }

    // Sequence bounds (field 1)
    let sequence_min = events
        .iter()
        .map(|e| e.sequence)
        .min()
        .ok_or_else(|| StorageError::InvalidInput("Empty events slice".into()))?;
    let sequence_max = events
        .iter()
        .map(|e| e.sequence)
        .max()
        .ok_or_else(|| StorageError::InvalidInput("Empty events slice".into()))?;

    // Partition bounds (field 3)
    let partition_min = events
        .iter()
        .map(|e| e.partition)
        .min()
        .ok_or_else(|| StorageError::InvalidInput("Empty events slice".into()))?;
    let partition_max = events
        .iter()
        .map(|e| e.partition)
        .max()
        .ok_or_else(|| StorageError::InvalidInput("Empty events slice".into()))?;

    // Timestamp bounds (field 5)
    let timestamp_min = events
        .iter()
        .map(|e| e.timestamp_ms)
        .min()
        .ok_or_else(|| StorageError::InvalidInput("Empty events slice".into()))?;
    let timestamp_max = events
        .iter()
        .map(|e| e.timestamp_ms)
        .max()
        .ok_or_else(|| StorageError::InvalidInput("Empty events slice".into()))?;

    // Derive partition column bounds from timestamps
    let partition_cols: Vec<(i32, i32)> = events
        .iter()
        .map(|e| derive_partition_columns(e.timestamp_ms))
        .collect();

    let event_date_min = partition_cols
        .iter()
        .map(|(d, _)| *d)
        .min()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;
    let event_date_max = partition_cols
        .iter()
        .map(|(d, _)| *d)
        .max()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;
    let event_hour_min = partition_cols
        .iter()
        .map(|(_, h)| *h)
        .min()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;
    let event_hour_max = partition_cols
        .iter()
        .map(|(_, h)| *h)
        .max()
        .ok_or_else(|| StorageError::InvalidInput("Empty partition columns".into()))?;

    Ok(ColumnStatistics {
        sequence_min,
        sequence_max,
        partition_min,
        partition_max,
        timestamp_min,
        timestamp_max,
        event_date_min,
        event_date_max,
        event_hour_min,
        event_hour_max,
    })
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

    // Use default writer properties optimized for Iceberg
    let props = default_writer_properties();

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

    // Calculate statistics - events is guaranteed non-empty (checked at function start)
    let column_stats = compute_column_statistics(events)?;

    // Calculate partition bounds
    let partition_values = compute_partition_bounds(events)?;

    let metadata = ParquetFileMetadata {
        path: String::new(), // No path for in-memory
        row_count: events.len(),
        min_sequence: column_stats.sequence_min,
        max_sequence: column_stats.sequence_max,
        min_timestamp_ms: column_stats.timestamp_min,
        max_timestamp_ms: column_stats.timestamp_max,
        file_size_bytes: buffer.len() as u64,
        partition_values,
        column_stats,
    };

    Ok((buffer, metadata))
}

/// Writes events to a Parquet file in memory with sorting.
/// Events are sorted by timestamp_ms ASC, then sequence ASC before writing.
/// This matches the Iceberg SortOrder::timestamp_sequence() definition.
///
/// Sorting improves:
/// - Query performance for time-range queries (better data locality)
/// - Column statistics quality (tighter bounds per file)
/// - Compression ratios (similar values grouped together)
pub fn write_parquet_to_bytes_sorted(
    events: &mut [StoredEvent],
) -> Result<(Vec<u8>, ParquetFileMetadata), StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot write empty events to Parquet".into(),
        ));
    }

    // Sort by timestamp_ms ASC, then sequence ASC for determinism
    events.sort_by(|a, b| {
        a.timestamp_ms
            .cmp(&b.timestamp_ms)
            .then(a.sequence.cmp(&b.sequence))
    });

    // Delegate to the non-sorted version (now that events are sorted)
    write_parquet_to_bytes(events)
}

/// Writes events to a Parquet file on disk with sorting.
/// Events are sorted by timestamp_ms ASC, then sequence ASC before writing.
pub fn write_parquet_sorted<P: AsRef<Path>>(
    events: &mut [StoredEvent],
    output_path: P,
) -> Result<ParquetFileMetadata, StorageError> {
    if events.is_empty() {
        return Err(StorageError::InvalidInput(
            "Cannot write empty events to Parquet".into(),
        ));
    }

    // Sort by timestamp_ms ASC, then sequence ASC for determinism
    events.sort_by(|a, b| {
        a.timestamp_ms
            .cmp(&b.timestamp_ms)
            .then(a.sequence.cmp(&b.sequence))
    });

    // Delegate to the non-sorted version (now that events are sorted)
    write_parquet(events, output_path)
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

    /// Test that a small batch fits in a single row group (verifies row count config).
    #[test]
    fn test_small_batch_fits_single_row_group() {
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

    #[test]
    fn test_write_parquet_to_bytes_sorted_orders_by_timestamp() {
        // Create events out of order by timestamp
        let mut events = vec![
            StoredEvent {
                sequence: 3,
                topic: "test".into(),
                partition: 0,
                payload: b"third".to_vec(),
                timestamp_ms: 3000, // Latest
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 1,
                topic: "test".into(),
                partition: 0,
                payload: b"first".to_vec(),
                timestamp_ms: 1000, // Earliest
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 2,
                topic: "test".into(),
                partition: 0,
                payload: b"second".to_vec(),
                timestamp_ms: 2000, // Middle
                idempotency_key: None,
            },
        ];

        let (_, metadata) = write_parquet_to_bytes_sorted(&mut events).unwrap();

        // After sorting, events should be in timestamp order
        assert_eq!(events[0].timestamp_ms, 1000);
        assert_eq!(events[1].timestamp_ms, 2000);
        assert_eq!(events[2].timestamp_ms, 3000);

        // Metadata should still capture correct min/max
        assert_eq!(metadata.min_timestamp_ms, 1000);
        assert_eq!(metadata.max_timestamp_ms, 3000);
    }

    #[test]
    fn test_write_parquet_to_bytes_sorted_secondary_sort_by_sequence() {
        // Create events with same timestamp but different sequences
        let mut events = vec![
            StoredEvent {
                sequence: 100,
                topic: "test".into(),
                partition: 0,
                payload: b"high seq".to_vec(),
                timestamp_ms: 1000,
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 50,
                topic: "test".into(),
                partition: 0,
                payload: b"low seq".to_vec(),
                timestamp_ms: 1000,
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 75,
                topic: "test".into(),
                partition: 0,
                payload: b"mid seq".to_vec(),
                timestamp_ms: 1000,
                idempotency_key: None,
            },
        ];

        let _ = write_parquet_to_bytes_sorted(&mut events).unwrap();

        // Same timestamp, so should be sorted by sequence as secondary key
        assert_eq!(events[0].sequence, 50);
        assert_eq!(events[1].sequence, 75);
        assert_eq!(events[2].sequence, 100);
    }

    #[test]
    fn test_write_parquet_sorted_file() {
        let mut events = vec![
            StoredEvent {
                sequence: 2,
                topic: "test".into(),
                partition: 0,
                payload: b"second".to_vec(),
                timestamp_ms: 2000,
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 1,
                topic: "test".into(),
                partition: 0,
                payload: b"first".to_vec(),
                timestamp_ms: 1000,
                idempotency_key: None,
            },
        ];

        let dir = tempdir().unwrap();
        let path = dir.path().join("sorted.parquet");

        let metadata = write_parquet_sorted(&mut events, &path).unwrap();

        // Verify file was created
        assert!(path.exists());
        assert_eq!(metadata.row_count, 2);

        // Events should be sorted
        assert_eq!(events[0].timestamp_ms, 1000);
        assert_eq!(events[1].timestamp_ms, 2000);
    }

    #[test]
    fn test_structured_schema_has_extracted_columns() {
        use crate::contracts::{ExtractedField, FieldType, PayloadFormat, TableSchemaConfig};

        let config = TableSchemaConfig {
            table: "clicks".into(),
            payload_format: PayloadFormat::Json,
            fields: vec![
                ExtractedField {
                    name: "user_id".into(),
                    json_path: "user_id".into(),
                    data_type: FieldType::Utf8,
                    nullable: true,
                },
                ExtractedField {
                    name: "value".into(),
                    json_path: "value".into(),
                    data_type: FieldType::Int64,
                    nullable: true,
                },
            ],
            preserve_overflow: true,
        };

        let schema = event_schema_with_extraction(&config);
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // System columns + extracted + overflow + partition
        assert!(field_names.contains(&"sequence"));
        assert!(field_names.contains(&"topic"));
        assert!(field_names.contains(&"partition"));
        assert!(field_names.contains(&"timestamp_ms"));
        assert!(field_names.contains(&"idempotency_key"));
        assert!(field_names.contains(&"user_id"));
        assert!(field_names.contains(&"value"));
        assert!(field_names.contains(&"_payload_overflow"));
        assert!(field_names.contains(&"event_date"));
        assert!(field_names.contains(&"event_hour"));

        // payload column should NOT be present
        assert!(!field_names.contains(&"payload"));
    }

    #[test]
    fn test_structured_record_batch_roundtrip() {
        use crate::contracts::{ExtractedField, FieldType, PayloadFormat, TableSchemaConfig};

        let config = TableSchemaConfig {
            table: "clicks".into(),
            payload_format: PayloadFormat::Json,
            fields: vec![
                ExtractedField {
                    name: "user_id".into(),
                    json_path: "user_id".into(),
                    data_type: FieldType::Utf8,
                    nullable: true,
                },
                ExtractedField {
                    name: "value".into(),
                    json_path: "value".into(),
                    data_type: FieldType::Int64,
                    nullable: true,
                },
            ],
            preserve_overflow: true,
        };

        let events = vec![
            StoredEvent {
                sequence: 1,
                topic: "clicks".into(),
                partition: 0,
                payload: br#"{"user_id":"u1","value":42,"extra":"x"}"#.to_vec(),
                timestamp_ms: 1000,
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 2,
                topic: "clicks".into(),
                partition: 0,
                payload: br#"{"user_id":"u2","value":99}"#.to_vec(),
                timestamp_ms: 2000,
                idempotency_key: Some("k2".into()),
            },
        ];

        let (bytes, metadata) = write_parquet_to_bytes_structured(&events, &config).unwrap();
        assert_eq!(metadata.row_count, 2);
        assert!(!bytes.is_empty());

        // Read back and verify columns exist
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = Bytes::from(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let reader = builder.build().unwrap();

        let batches: Vec<_> = reader.into_iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        // Verify user_id column
        let user_id_col = batch
            .column_by_name("user_id")
            .expect("user_id column should exist");
        let user_ids = user_id_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(user_ids.value(0), "u1");
        assert_eq!(user_ids.value(1), "u2");

        // Verify value column
        let value_col = batch
            .column_by_name("value")
            .expect("value column should exist");
        let values = value_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), 42);
        assert_eq!(values.value(1), 99);

        // Verify overflow column exists
        use arrow::array::Array;
        let overflow_col = batch
            .column_by_name("_payload_overflow")
            .expect("overflow column should exist");
        let overflow = overflow_col.as_any().downcast_ref::<BinaryArray>().unwrap();

        // First event had "extra" field → overflow should contain it
        assert!(!overflow.is_null(0));
        let overflow_json: serde_json::Value = serde_json::from_slice(overflow.value(0)).unwrap();
        assert_eq!(overflow_json, serde_json::json!({"extra": "x"}));

        // Second event had no extra fields → overflow should be null
        assert!(overflow.is_null(1));
    }

    #[test]
    fn test_structured_sorted_preserves_order() {
        use crate::contracts::{ExtractedField, FieldType, PayloadFormat, TableSchemaConfig};

        let config = TableSchemaConfig {
            table: "test".into(),
            payload_format: PayloadFormat::Json,
            fields: vec![ExtractedField {
                name: "v".into(),
                json_path: "v".into(),
                data_type: FieldType::Int64,
                nullable: true,
            }],
            preserve_overflow: false,
        };

        let mut events = vec![
            StoredEvent {
                sequence: 2,
                topic: "test".into(),
                partition: 0,
                payload: br#"{"v":2}"#.to_vec(),
                timestamp_ms: 2000,
                idempotency_key: None,
            },
            StoredEvent {
                sequence: 1,
                topic: "test".into(),
                partition: 0,
                payload: br#"{"v":1}"#.to_vec(),
                timestamp_ms: 1000,
                idempotency_key: None,
            },
        ];

        let (_, metadata) = write_parquet_to_bytes_structured_sorted(&mut events, &config).unwrap();

        // Events should be sorted by timestamp
        assert_eq!(events[0].timestamp_ms, 1000);
        assert_eq!(events[1].timestamp_ms, 2000);
        assert_eq!(metadata.min_timestamp_ms, 1000);
        assert_eq!(metadata.max_timestamp_ms, 2000);
    }

    #[test]
    fn test_no_config_path_unchanged() {
        // Verify existing behavior is completely untouched when no schema config
        let events = make_test_events();
        let batch = events_to_record_batch(&events).unwrap();

        assert_eq!(batch.num_columns(), 8); // 6 original + 2 partition columns
        assert_eq!(batch.schema().field(3).name(), "payload");
    }
}
