use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::StorageError;
use crate::storage::ParquetFileMetadata;

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

/// Sort order (unsorted for append-only).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SortOrder {
    #[serde(rename = "order-id")]
    pub order_id: i32,
    #[serde(default)]
    pub fields: Vec<serde_json::Value>,
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
            sort_orders: vec![SortOrder::default()],
            default_sort_order_id: 0,
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
    pub fn from_parquet_metadata(metadata: &ParquetFileMetadata, s3_path: &str) -> Self {
        Self {
            content: 0, // Data file
            file_path: s3_path.to_string(),
            file_format: "PARQUET".into(),
            record_count: metadata.row_count as i64,
            file_size_in_bytes: metadata.file_size_bytes as i64,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
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
        .unwrap()
        .as_nanos();
    (now & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Returns current timestamp in milliseconds.
pub fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
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
        use crate::storage::parquet::PartitionValues;

        let parquet_meta = ParquetFileMetadata {
            path: "/tmp/test.parquet".into(),
            row_count: 1000,
            min_sequence: 1,
            max_sequence: 1000,
            min_timestamp_ms: 100,
            max_timestamp_ms: 200,
            file_size_bytes: 50000,
            partition_values: PartitionValues::default(),
        };

        let data_file =
            DataFile::from_parquet_metadata(&parquet_meta, "s3://bucket/data/test.parquet");

        assert_eq!(data_file.content, 0);
        assert_eq!(data_file.file_path, "s3://bucket/data/test.parquet");
        assert_eq!(data_file.file_format, "PARQUET");
        assert_eq!(data_file.record_count, 1000);
        assert_eq!(data_file.file_size_in_bytes, 50000);
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
}
