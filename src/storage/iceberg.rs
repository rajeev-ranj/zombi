use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::StorageError;
use crate::storage::ParquetFileMetadata;

/// Iceberg schema field IDs.
/// These must match the field IDs in the schema definition and remain stable
/// across schema evolution. Used for column statistics (lower/upper bounds).
#[allow(dead_code)]
pub mod field_ids {
    /// sequence (long) - monotonically increasing event ID
    pub const SEQUENCE: i32 = 1;
    /// topic (string) - event topic/table name
    pub const TOPIC: i32 = 2;
    /// partition (int) - partition number
    pub const PARTITION: i32 = 3;
    /// payload (binary) - event payload
    pub const PAYLOAD: i32 = 4;
    /// timestamp_ms (long) - event timestamp in milliseconds
    pub const TIMESTAMP_MS: i32 = 5;
    /// idempotency_key (string, optional) - deduplication key
    pub const IDEMPOTENCY_KEY: i32 = 6;
    /// event_date (date) - partition column, days since epoch
    pub const EVENT_DATE: i32 = 7;
    /// event_hour (int) - partition column, hour of day (0-23)
    pub const EVENT_HOUR: i32 = 8;
}

/// Iceberg binary encoding utilities for column statistics.
/// Iceberg uses big-endian encoding for all numeric types in lower_bounds/upper_bounds.
pub mod iceberg_encoding {
    /// Encodes an i64 (long) to big-endian bytes for Iceberg.
    pub fn encode_long(value: i64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encodes a u64 as i64 (long) to big-endian bytes for Iceberg.
    ///
    /// # Note
    /// Iceberg uses signed long for all integer types. Values greater than
    /// `i64::MAX` (9,223,372,036,854,775,807) will wrap to negative values
    /// due to two's complement representation. For Zombi sequence numbers,
    /// this limit is unlikely to be reached in practice (would require ~292
    /// years at 1 billion events/second).
    pub fn encode_u64_as_long(value: u64) -> Vec<u8> {
        (value as i64).to_be_bytes().to_vec()
    }

    /// Encodes an i32 (int) to big-endian bytes for Iceberg.
    pub fn encode_int(value: i32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encodes a u32 as i32 (int) to big-endian bytes for Iceberg.
    pub fn encode_u32_as_int(value: u32) -> Vec<u8> {
        (value as i32).to_be_bytes().to_vec()
    }

    /// Encodes a date (days since epoch) to big-endian bytes for Iceberg.
    /// Iceberg date type is stored as i32 days since 1970-01-01.
    pub fn encode_date(days_since_epoch: i32) -> Vec<u8> {
        encode_int(days_since_epoch)
    }
}

/// Iceberg table location on S3.
#[derive(Debug, Clone)]
pub struct IcebergTableConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Base path within the bucket (e.g., "tables/events").
    pub base_path: String,
    /// Topic name (becomes the table name).
    pub table_name: String,
}

impl IcebergTableConfig {
    /// Returns the full S3 prefix for this table.
    pub fn table_prefix(&self) -> String {
        format!("{}/{}", self.base_path, self.table_name)
    }

    /// Returns the path for metadata files.
    pub fn metadata_path(&self) -> String {
        format!("{}/metadata", self.table_prefix())
    }

    /// Returns the path for data files.
    pub fn data_path(&self) -> String {
        format!("{}/data", self.table_prefix())
    }
}

/// Iceberg schema for Zombi events.
/// Uses Iceberg schema format (schema_id, fields with field_ids).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSchema {
    #[serde(rename = "type")]
    pub schema_type: String,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub fields: Vec<IcebergField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergField {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub required: bool,
}

impl Default for IcebergSchema {
    fn default() -> Self {
        Self {
            schema_type: "struct".into(),
            schema_id: 0,
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "sequence".into(),
                    field_type: "long".into(),
                    required: true,
                },
                IcebergField {
                    id: 2,
                    name: "topic".into(),
                    field_type: "string".into(),
                    required: true,
                },
                IcebergField {
                    id: 3,
                    name: "partition".into(),
                    field_type: "int".into(),
                    required: true,
                },
                IcebergField {
                    id: 4,
                    name: "payload".into(),
                    field_type: "binary".into(),
                    required: true,
                },
                IcebergField {
                    id: 5,
                    name: "timestamp_ms".into(),
                    field_type: "long".into(),
                    required: true,
                },
                IcebergField {
                    id: 6,
                    name: "idempotency_key".into(),
                    field_type: "string".into(),
                    required: false,
                },
                // Partition columns derived from timestamp_ms
                IcebergField {
                    id: 7,
                    name: "event_date".into(),
                    field_type: "date".into(),
                    required: true,
                },
                IcebergField {
                    id: 8,
                    name: "event_hour".into(),
                    field_type: "int".into(),
                    required: true,
                },
            ],
        }
    }
}

/// Partition field for Iceberg partition spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    /// Source field ID from the schema.
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Unique field ID within the partition spec.
    #[serde(rename = "field-id")]
    pub field_id: i32,
    /// Name of the partition field.
    pub name: String,
    /// Transform to apply (identity, day, hour, month, year, bucket, truncate).
    pub transform: String,
}

/// Partition spec for time-based partitioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

impl Default for PartitionSpec {
    /// Creates a partition spec with event_date and event_hour partitioning.
    fn default() -> Self {
        Self {
            spec_id: 0,
            fields: vec![
                PartitionField {
                    source_id: 7, // event_date field
                    field_id: 1000,
                    name: "event_date".into(),
                    transform: "identity".into(),
                },
                PartitionField {
                    source_id: 8, // event_hour field
                    field_id: 1001,
                    name: "event_hour".into(),
                    transform: "identity".into(),
                },
            ],
        }
    }
}

impl PartitionSpec {
    /// Creates an unpartitioned spec (for backwards compatibility).
    pub fn unpartitioned() -> Self {
        Self {
            spec_id: 0,
            fields: vec![],
        }
    }
}

/// Iceberg sort field definition per the Iceberg spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    /// Transform applied before sorting (identity, bucket, truncate, etc.)
    pub transform: String,
    /// Source field ID from schema
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Sort direction (asc or desc)
    pub direction: String,
    /// Null ordering (nulls-first or nulls-last)
    #[serde(rename = "null-order")]
    pub null_order: String,
}

/// Sort order defining how data is sorted within files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SortOrder {
    #[serde(rename = "order-id")]
    pub order_id: i32,
    #[serde(default)]
    pub fields: Vec<SortField>,
}

impl SortOrder {
    /// Creates an unsorted (empty) sort order.
    pub fn unsorted() -> Self {
        Self {
            order_id: 0,
            fields: vec![],
        }
    }

    /// Creates a sort order by timestamp_ms ASC, sequence ASC.
    /// This is the recommended sort order for Zombi events as it:
    /// - Optimizes time-range queries (most common pattern)
    /// - Provides deterministic ordering within same timestamp
    /// - Enables better column statistics per file
    pub fn timestamp_sequence() -> Self {
        use crate::storage::iceberg::field_ids;
        Self {
            order_id: 1,
            fields: vec![
                SortField {
                    transform: "identity".into(),
                    source_id: field_ids::TIMESTAMP_MS,
                    direction: "asc".into(),
                    null_order: "nulls-last".into(),
                },
                SortField {
                    transform: "identity".into(),
                    source_id: field_ids::SEQUENCE,
                    direction: "asc".into(),
                    null_order: "nulls-last".into(),
                },
            ],
        }
    }
}

/// Iceberg snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
}

/// Iceberg snapshot summary operation.
#[derive(Debug, Clone, Copy)]
pub enum SnapshotOperation {
    Append,
    Replace,
}

impl SnapshotOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Replace => "replace",
        }
    }
}

/// Iceberg table metadata (v2 format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    #[serde(rename = "format-version")]
    pub format_version: i32,
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    pub location: String,
    #[serde(rename = "last-sequence-number")]
    pub last_sequence_number: i64,
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    pub schemas: Vec<IcebergSchema>,
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    #[serde(rename = "partition-specs")]
    pub partition_specs: Vec<PartitionSpec>,
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    pub properties: HashMap<String, String>,
    #[serde(
        rename = "current-snapshot-id",
        skip_serializing_if = "Option::is_none"
    )]
    pub current_snapshot_id: Option<i64>,
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    #[serde(rename = "snapshot-log", default)]
    pub snapshot_log: Vec<SnapshotLogEntry>,
    #[serde(rename = "sort-orders")]
    pub sort_orders: Vec<SortOrder>,
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

impl TableMetadata {
    /// Creates new table metadata for a Zombi topic.
    /// Uses timestamp_sequence sort order by default for optimized time-range queries.
    pub fn new(location: &str) -> Self {
        Self {
            format_version: 2,
            table_uuid: Uuid::new_v4().to_string(),
            location: location.to_string(),
            last_sequence_number: 0,
            last_updated_ms: current_timestamp_ms(),
            last_column_id: 8, // sequence(1), topic(2), partition(3), payload(4), timestamp_ms(5), idempotency_key(6), event_date(7), event_hour(8)
            schemas: vec![IcebergSchema::default()],
            current_schema_id: 0,
            partition_specs: vec![PartitionSpec::default()],
            default_spec_id: 0,
            last_partition_id: 999,
            properties: HashMap::new(),
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            // Provide both unsorted (0) and timestamp_sequence (1) sort orders
            // Default to timestamp_sequence for optimized queries
            sort_orders: vec![SortOrder::unsorted(), SortOrder::timestamp_sequence()],
            default_sort_order_id: 1, // Use timestamp_sequence by default
        }
    }

    /// Adds a new snapshot to the table.
    pub fn add_snapshot(
        &mut self,
        manifest_list_path: &str,
        added_files: usize,
        added_rows: i64,
        operation: SnapshotOperation,
    ) -> i64 {
        let snapshot_id = generate_snapshot_id();
        let now = current_timestamp_ms();

        let mut summary = HashMap::new();
        summary.insert("operation".into(), operation.as_str().into());
        summary.insert("added-files-size".into(), "0".into());
        summary.insert("added-data-files".into(), added_files.to_string());
        summary.insert("added-records".into(), added_rows.to_string());
        summary.insert("total-records".into(), added_rows.to_string());
        summary.insert("total-data-files".into(), added_files.to_string());

        let snapshot = Snapshot {
            snapshot_id,
            parent_snapshot_id: self.current_snapshot_id,
            timestamp_ms: now,
            manifest_list: manifest_list_path.to_string(),
            summary,
            schema_id: 0,
        };

        self.snapshots.push(snapshot);
        self.snapshot_log.push(SnapshotLogEntry {
            snapshot_id,
            timestamp_ms: now,
        });
        self.current_snapshot_id = Some(snapshot_id);
        self.last_sequence_number += 1;
        self.last_updated_ms = now;

        snapshot_id
    }

    /// Serializes metadata to JSON.
    pub fn to_json(&self) -> Result<String, StorageError> {
        serde_json::to_string_pretty(self).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    /// Deserializes metadata from JSON.
    pub fn from_json(json: &str) -> Result<Self, StorageError> {
        serde_json::from_str(json).map_err(|e| StorageError::Serialization(e.to_string()))
    }
}

/// Manifest entry for a data file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub status: i32, // 0=existing, 1=added, 2=deleted
    #[serde(rename = "snapshot_id")]
    pub snapshot_id: i64,
    #[serde(rename = "data_file")]
    pub data_file: DataFile,
}

/// Iceberg data file metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    pub content: i32, // 0=data, 1=position deletes, 2=equality deletes
    pub file_path: String,
    pub file_format: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub column_sizes: Option<HashMap<i32, i64>>,
    pub value_counts: Option<HashMap<i32, i64>>,
    pub null_value_counts: Option<HashMap<i32, i64>>,
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    pub split_offsets: Option<Vec<i64>>,
}

impl DataFile {
    /// Creates a DataFile from ParquetFileMetadata.
    /// Populates column statistics (lower/upper bounds) from Parquet metadata.
    /// Field IDs are defined in the `field_ids` module.
    pub fn from_parquet_metadata(metadata: &ParquetFileMetadata, s3_path: &str) -> Self {
        use crate::storage::iceberg::field_ids;
        use iceberg_encoding::*;

        let mut lower_bounds = HashMap::new();
        let mut upper_bounds = HashMap::new();

        // sequence (long)
        lower_bounds.insert(
            field_ids::SEQUENCE,
            encode_u64_as_long(metadata.column_stats.sequence_min),
        );
        upper_bounds.insert(
            field_ids::SEQUENCE,
            encode_u64_as_long(metadata.column_stats.sequence_max),
        );

        // partition (int)
        lower_bounds.insert(
            field_ids::PARTITION,
            encode_u32_as_int(metadata.column_stats.partition_min),
        );
        upper_bounds.insert(
            field_ids::PARTITION,
            encode_u32_as_int(metadata.column_stats.partition_max),
        );

        // timestamp_ms (long)
        lower_bounds.insert(
            field_ids::TIMESTAMP_MS,
            encode_long(metadata.column_stats.timestamp_min),
        );
        upper_bounds.insert(
            field_ids::TIMESTAMP_MS,
            encode_long(metadata.column_stats.timestamp_max),
        );

        // event_date (date - days since epoch)
        lower_bounds.insert(
            field_ids::EVENT_DATE,
            encode_date(metadata.column_stats.event_date_min),
        );
        upper_bounds.insert(
            field_ids::EVENT_DATE,
            encode_date(metadata.column_stats.event_date_max),
        );

        // event_hour (int)
        lower_bounds.insert(
            field_ids::EVENT_HOUR,
            encode_int(metadata.column_stats.event_hour_min),
        );
        upper_bounds.insert(
            field_ids::EVENT_HOUR,
            encode_int(metadata.column_stats.event_hour_max),
        );

        Self {
            content: 0, // Data file
            file_path: s3_path.to_string(),
            file_format: "PARQUET".into(),
            record_count: metadata.row_count as i64,
            file_size_in_bytes: metadata.file_size_bytes as i64,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            lower_bounds: Some(lower_bounds),
            upper_bounds: Some(upper_bounds),
            split_offsets: None,
        }
    }
}

/// Manifest file containing a list of data files.
#[derive(Debug, Clone)]
pub struct ManifestFile {
    pub entries: Vec<ManifestEntry>,
    pub snapshot_id: i64,
    pub sequence_number: i64,
}

impl ManifestFile {
    pub fn new(snapshot_id: i64, sequence_number: i64) -> Self {
        Self {
            entries: vec![],
            snapshot_id,
            sequence_number,
        }
    }

    pub fn add_data_file(&mut self, data_file: DataFile) {
        self.entries.push(ManifestEntry {
            status: 1, // Added
            snapshot_id: self.snapshot_id,
            data_file,
        });
    }
}

/// Manifest list entry pointing to a manifest file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestListEntry {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub content: i32, // 0=data, 1=deletes
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: i32,
    pub existing_files_count: i32,
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
}

/// Generates a unique snapshot ID.
pub fn generate_snapshot_id() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap() // SAFETY: SystemTime::now() is always after UNIX_EPOCH
        .as_nanos();
    (now & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Returns current timestamp in milliseconds.
pub fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap() // SAFETY: SystemTime::now() is always after UNIX_EPOCH
        .as_millis() as i64
}

/// Generates a metadata file name.
pub fn metadata_file_name(version: i64) -> String {
    format!("v{}.metadata.json", version)
}

/// Generates a manifest list file name.
pub fn manifest_list_file_name(snapshot_id: i64) -> String {
    format!("snap-{}-{}.avro", snapshot_id, Uuid::new_v4())
}

/// Generates a manifest file name.
pub fn manifest_file_name() -> String {
    format!("{}-m0.avro", Uuid::new_v4())
}

/// Generates a data file name.
pub fn data_file_name() -> String {
    format!("{}.parquet", Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_metadata_creation() {
        let metadata = TableMetadata::new("s3://bucket/tables/events");

        assert_eq!(metadata.format_version, 2);
        assert!(!metadata.table_uuid.is_empty());
        assert_eq!(metadata.location, "s3://bucket/tables/events");
        assert!(metadata.current_snapshot_id.is_none());
        assert!(metadata.snapshots.is_empty());
    }

    #[test]
    fn test_add_snapshot() {
        let mut metadata = TableMetadata::new("s3://bucket/tables/events");

        let snapshot_id = metadata.add_snapshot(
            "s3://bucket/tables/events/metadata/snap-123.avro",
            1,
            100,
            SnapshotOperation::Append,
        );

        assert!(snapshot_id > 0);
        assert_eq!(metadata.current_snapshot_id, Some(snapshot_id));
        assert_eq!(metadata.snapshots.len(), 1);
        assert_eq!(metadata.snapshot_log.len(), 1);
        assert_eq!(metadata.last_sequence_number, 1);
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = TableMetadata::new("s3://bucket/tables/events");

        let json = metadata.to_json().unwrap();
        let parsed = TableMetadata::from_json(&json).unwrap();

        assert_eq!(parsed.table_uuid, metadata.table_uuid);
        assert_eq!(parsed.location, metadata.location);
    }

    #[test]
    fn test_iceberg_schema_default() {
        let schema = IcebergSchema::default();

        assert_eq!(schema.schema_type, "struct");
        assert_eq!(schema.fields.len(), 8);
        assert_eq!(schema.fields[0].name, "sequence");
        assert_eq!(schema.fields[4].name, "timestamp_ms");
        // Verify partition columns
        assert_eq!(schema.fields[6].name, "event_date");
        assert_eq!(schema.fields[6].field_type, "date");
        assert_eq!(schema.fields[7].name, "event_hour");
        assert_eq!(schema.fields[7].field_type, "int");
    }

    #[test]
    fn test_partition_spec_default() {
        let spec = PartitionSpec::default();

        assert_eq!(spec.spec_id, 0);
        assert_eq!(spec.fields.len(), 2);
        assert_eq!(spec.fields[0].name, "event_date");
        assert_eq!(spec.fields[0].source_id, 7);
        assert_eq!(spec.fields[0].transform, "identity");
        assert_eq!(spec.fields[1].name, "event_hour");
        assert_eq!(spec.fields[1].source_id, 8);
    }

    #[test]
    fn test_partition_spec_unpartitioned() {
        let spec = PartitionSpec::unpartitioned();

        assert_eq!(spec.spec_id, 0);
        assert!(spec.fields.is_empty());
    }

    #[test]
    fn test_data_file_from_parquet_metadata() {
        use crate::storage::parquet::{ColumnStatistics, PartitionValues};

        let column_stats = ColumnStatistics {
            sequence_min: 1,
            sequence_max: 1000,
            partition_min: 0,
            partition_max: 5,
            timestamp_min: 100,
            timestamp_max: 200,
            event_date_min: 19737,
            event_date_max: 19737,
            event_hour_min: 10,
            event_hour_max: 14,
        };

        let parquet_meta = ParquetFileMetadata {
            path: "/tmp/test.parquet".into(),
            row_count: 1000,
            min_sequence: 1,
            max_sequence: 1000,
            min_timestamp_ms: 100,
            max_timestamp_ms: 200,
            file_size_bytes: 50000,
            partition_values: PartitionValues::default(),
            column_stats,
        };

        let data_file =
            DataFile::from_parquet_metadata(&parquet_meta, "s3://bucket/data/test.parquet");

        assert_eq!(data_file.content, 0);
        assert_eq!(data_file.file_path, "s3://bucket/data/test.parquet");
        assert_eq!(data_file.file_format, "PARQUET");
        assert_eq!(data_file.record_count, 1000);
        assert_eq!(data_file.file_size_in_bytes, 50000);

        // Verify column statistics are now populated
        let lower = data_file
            .lower_bounds
            .expect("lower_bounds should be populated");
        let upper = data_file
            .upper_bounds
            .expect("upper_bounds should be populated");

        // Verify all fields are present
        assert!(lower.contains_key(&1), "sequence lower bound should exist");
        assert!(lower.contains_key(&3), "partition lower bound should exist");
        assert!(lower.contains_key(&5), "timestamp lower bound should exist");
        assert!(
            lower.contains_key(&7),
            "event_date lower bound should exist"
        );
        assert!(
            lower.contains_key(&8),
            "event_hour lower bound should exist"
        );

        assert!(upper.contains_key(&1), "sequence upper bound should exist");
        assert!(upper.contains_key(&3), "partition upper bound should exist");
        assert!(upper.contains_key(&5), "timestamp upper bound should exist");
        assert!(
            upper.contains_key(&7),
            "event_date upper bound should exist"
        );
        assert!(
            upper.contains_key(&8),
            "event_hour upper bound should exist"
        );
    }

    #[test]
    fn test_iceberg_encoding_long() {
        // Test encoding i64 as big-endian bytes
        let encoded = iceberg_encoding::encode_long(1705276800000_i64);
        assert_eq!(encoded.len(), 8);
        // Verify big-endian by checking first byte is most significant
        let decoded = i64::from_be_bytes(encoded.try_into().unwrap());
        assert_eq!(decoded, 1705276800000_i64);
    }

    #[test]
    fn test_iceberg_encoding_u64_as_long() {
        let encoded = iceberg_encoding::encode_u64_as_long(12345_u64);
        assert_eq!(encoded.len(), 8);
        let decoded = i64::from_be_bytes(encoded.try_into().unwrap());
        assert_eq!(decoded, 12345_i64);
    }

    #[test]
    fn test_iceberg_encoding_int() {
        let encoded = iceberg_encoding::encode_int(19737_i32);
        assert_eq!(encoded.len(), 4);
        let decoded = i32::from_be_bytes(encoded.try_into().unwrap());
        assert_eq!(decoded, 19737_i32);
    }

    #[test]
    fn test_iceberg_encoding_u32_as_int() {
        let encoded = iceberg_encoding::encode_u32_as_int(100_u32);
        assert_eq!(encoded.len(), 4);
        let decoded = i32::from_be_bytes(encoded.try_into().unwrap());
        assert_eq!(decoded, 100_i32);
    }

    #[test]
    fn test_iceberg_encoding_date() {
        // 2024-01-15 is day 19737 since epoch
        let encoded = iceberg_encoding::encode_date(19737);
        assert_eq!(encoded.len(), 4);
        let decoded = i32::from_be_bytes(encoded.try_into().unwrap());
        assert_eq!(decoded, 19737);
    }

    #[test]
    fn test_data_file_bounds_encoding() {
        use crate::storage::parquet::{ColumnStatistics, PartitionValues};

        let column_stats = ColumnStatistics {
            sequence_min: 100,
            sequence_max: 200,
            partition_min: 0,
            partition_max: 5,
            timestamp_min: 1705276800000,
            timestamp_max: 1705363200000,
            event_date_min: 19737,
            event_date_max: 19738,
            event_hour_min: 0,
            event_hour_max: 23,
        };

        let parquet_meta = ParquetFileMetadata {
            path: "/tmp/test.parquet".into(),
            row_count: 100,
            min_sequence: 100,
            max_sequence: 200,
            min_timestamp_ms: 1705276800000,
            max_timestamp_ms: 1705363200000,
            file_size_bytes: 10000,
            partition_values: PartitionValues::default(),
            column_stats,
        };

        let data_file = DataFile::from_parquet_metadata(&parquet_meta, "s3://test/file.parquet");

        let lower = data_file.lower_bounds.unwrap();
        let upper = data_file.upper_bounds.unwrap();

        // Verify sequence encoding (field 1)
        let seq_min_bytes: [u8; 8] = lower.get(&1).unwrap().clone().try_into().unwrap();
        assert_eq!(i64::from_be_bytes(seq_min_bytes), 100);

        let seq_max_bytes: [u8; 8] = upper.get(&1).unwrap().clone().try_into().unwrap();
        assert_eq!(i64::from_be_bytes(seq_max_bytes), 200);

        // Verify timestamp encoding (field 5)
        let ts_min_bytes: [u8; 8] = lower.get(&5).unwrap().clone().try_into().unwrap();
        assert_eq!(i64::from_be_bytes(ts_min_bytes), 1705276800000);

        let ts_max_bytes: [u8; 8] = upper.get(&5).unwrap().clone().try_into().unwrap();
        assert_eq!(i64::from_be_bytes(ts_max_bytes), 1705363200000);

        // Verify event_date encoding (field 7)
        let date_min_bytes: [u8; 4] = lower.get(&7).unwrap().clone().try_into().unwrap();
        assert_eq!(i32::from_be_bytes(date_min_bytes), 19737);

        let date_max_bytes: [u8; 4] = upper.get(&7).unwrap().clone().try_into().unwrap();
        assert_eq!(i32::from_be_bytes(date_max_bytes), 19738);
    }

    #[test]
    fn test_table_config() {
        let config = IcebergTableConfig {
            bucket: "my-bucket".into(),
            base_path: "tables".into(),
            table_name: "events".into(),
        };

        assert_eq!(config.table_prefix(), "tables/events");
        assert_eq!(config.metadata_path(), "tables/events/metadata");
        assert_eq!(config.data_path(), "tables/events/data");
    }

    #[test]
    fn test_sort_order_unsorted() {
        let sort_order = SortOrder::unsorted();

        assert_eq!(sort_order.order_id, 0);
        assert!(sort_order.fields.is_empty());
    }

    #[test]
    fn test_sort_order_timestamp_sequence() {
        let sort_order = SortOrder::timestamp_sequence();

        assert_eq!(sort_order.order_id, 1);
        assert_eq!(sort_order.fields.len(), 2);

        // First field: timestamp_ms (source-id 5)
        let first = &sort_order.fields[0];
        assert_eq!(first.source_id, 5);
        assert_eq!(first.transform, "identity");
        assert_eq!(first.direction, "asc");
        assert_eq!(first.null_order, "nulls-last");

        // Second field: sequence (source-id 1)
        let second = &sort_order.fields[1];
        assert_eq!(second.source_id, 1);
        assert_eq!(second.transform, "identity");
        assert_eq!(second.direction, "asc");
        assert_eq!(second.null_order, "nulls-last");
    }

    #[test]
    fn test_table_metadata_uses_timestamp_sequence_sort() {
        let metadata = TableMetadata::new("s3://bucket/tables/events");

        // Should have two sort orders: unsorted (0) and timestamp_sequence (1)
        assert_eq!(metadata.sort_orders.len(), 2);
        assert_eq!(metadata.sort_orders[0].order_id, 0);
        assert!(metadata.sort_orders[0].fields.is_empty());
        assert_eq!(metadata.sort_orders[1].order_id, 1);
        assert_eq!(metadata.sort_orders[1].fields.len(), 2);

        // Default should be timestamp_sequence (order-id 1)
        assert_eq!(metadata.default_sort_order_id, 1);
    }

    #[test]
    fn test_sort_order_serialization() {
        let sort_order = SortOrder::timestamp_sequence();

        let json = serde_json::to_string(&sort_order).unwrap();

        // Verify JSON contains expected field names per Iceberg spec
        assert!(json.contains("\"order-id\":1"));
        assert!(json.contains("\"source-id\":5"));
        assert!(json.contains("\"source-id\":1"));
        assert!(json.contains("\"direction\":\"asc\""));
        assert!(json.contains("\"null-order\":\"nulls-last\""));
        assert!(json.contains("\"transform\":\"identity\""));

        // Verify round-trip
        let parsed: SortOrder = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.order_id, 1);
        assert_eq!(parsed.fields.len(), 2);
    }
}
