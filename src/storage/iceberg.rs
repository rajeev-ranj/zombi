use std::collections::HashMap;
use std::sync::OnceLock;

use apache_avro::types::{Record, Value as AvroValue};
use apache_avro::{Schema as AvroSchema, Writer as AvroWriter};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::{StorageError, TableSchemaConfig};
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
    /// First field ID for extracted payload columns.
    /// Extracted fields are assigned IDs 100, 101, 102, ... to avoid conflicts
    /// with system field IDs (1-8) and partition field IDs (1000+).
    pub const EXTRACTED_START: i32 = 100;
    /// Field ID for the _payload_overflow column (replaces payload when extraction is active).
    pub const PAYLOAD_OVERFLOW: i32 = 99;
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

impl IcebergSchema {
    /// Creates an Iceberg schema with extracted payload columns.
    ///
    /// The schema replaces the opaque `payload` (binary) column with:
    /// - One typed column per `ExtractedField` (field IDs starting at 100)
    /// - A `_payload_overflow` (binary, nullable) column (field ID 99) for unextracted data
    pub fn with_extracted_fields(config: &TableSchemaConfig) -> Self {
        let mut fields = vec![
            IcebergField {
                id: field_ids::SEQUENCE,
                name: "sequence".into(),
                field_type: "long".into(),
                required: true,
            },
            IcebergField {
                id: field_ids::TOPIC,
                name: "topic".into(),
                field_type: "string".into(),
                required: true,
            },
            IcebergField {
                id: field_ids::PARTITION,
                name: "partition".into(),
                field_type: "int".into(),
                required: true,
            },
            // payload (field ID 4) is omitted — replaced by extracted + overflow
            IcebergField {
                id: field_ids::TIMESTAMP_MS,
                name: "timestamp_ms".into(),
                field_type: "long".into(),
                required: true,
            },
            IcebergField {
                id: field_ids::IDEMPOTENCY_KEY,
                name: "idempotency_key".into(),
                field_type: "string".into(),
                required: false,
            },
        ];

        // Add extracted columns
        for (i, f) in config.fields.iter().enumerate() {
            fields.push(IcebergField {
                id: field_ids::EXTRACTED_START + i as i32,
                name: f.name.clone(),
                field_type: f.data_type.iceberg_type().into(),
                required: !f.nullable,
            });
        }

        // Overflow column
        if config.preserve_overflow {
            fields.push(IcebergField {
                id: field_ids::PAYLOAD_OVERFLOW,
                name: "_payload_overflow".into(),
                field_type: "binary".into(),
                required: false,
            });
        }

        // Partition columns
        fields.push(IcebergField {
            id: field_ids::EVENT_DATE,
            name: "event_date".into(),
            field_type: "date".into(),
            required: true,
        });
        fields.push(IcebergField {
            id: field_ids::EVENT_HOUR,
            name: "event_hour".into(),
            field_type: "int".into(),
            required: true,
        });

        Self {
            schema_type: "struct".into(),
            schema_id: 1, // New schema version for structured columns
            fields,
        }
    }

    /// Returns the highest field ID in this schema.
    pub fn last_column_id(&self) -> i32 {
        self.fields.iter().map(|f| f.id).max().unwrap_or(0)
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

    /// Creates table metadata with structured column extraction.
    ///
    /// Uses a schema derived from `TableSchemaConfig` where payload bytes are
    /// split into typed columns + overflow, instead of an opaque binary blob.
    pub fn with_schema_config(location: &str, config: &TableSchemaConfig) -> Self {
        let schema = IcebergSchema::with_extracted_fields(config);
        let last_col = schema.last_column_id();
        let schema_id = schema.schema_id;

        Self {
            format_version: 2,
            table_uuid: Uuid::new_v4().to_string(),
            location: location.to_string(),
            last_sequence_number: 0,
            last_updated_ms: current_timestamp_ms(),
            last_column_id: last_col,
            schemas: vec![schema],
            current_schema_id: schema_id,
            partition_specs: vec![PartitionSpec::default()],
            default_spec_id: 0,
            last_partition_id: 999,
            properties: HashMap::new(),
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            sort_orders: vec![SortOrder::unsorted(), SortOrder::timestamp_sequence()],
            default_sort_order_id: 1,
        }
    }

    /// Adds a new snapshot to the table.
    pub fn add_snapshot(
        &mut self,
        manifest_list_path: &str,
        added_files: usize,
        added_rows: i64,
        operation: SnapshotOperation,
        extra_summary: HashMap<String, String>,
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
        for (key, value) in extra_summary {
            if !key.starts_with("zombi.") {
                continue;
            }
            summary.entry(key).or_insert(value);
        }

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

    /// Serializes this manifest file to Avro binary format per Iceberg v2 spec.
    ///
    /// The Avro file includes metadata headers required by Iceberg:
    /// `schema`, `schema-id`, `partition-spec`, `partition-spec-id`, `format-version`, `content`.
    pub fn to_avro_bytes(&self, table_metadata: &TableMetadata) -> Result<Vec<u8>, StorageError> {
        let avro_schema = manifest_entry_avro_schema();
        let mut writer = AvroWriter::new(avro_schema, Vec::new());

        // Set Iceberg-required metadata on the Avro file
        let schema_json = serde_json::to_string(
            table_metadata
                .schemas
                .first()
                .unwrap_or(&IcebergSchema::default()),
        )
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let partition_spec_json = serde_json::to_string(
            table_metadata
                .partition_specs
                .first()
                .map(|s| &s.fields)
                .unwrap_or(&vec![]),
        )
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

        writer
            .add_user_metadata("schema".to_string(), &schema_json)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        writer
            .add_user_metadata("schema-id".to_string(), "0")
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        writer
            .add_user_metadata("partition-spec".to_string(), &partition_spec_json)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        writer
            .add_user_metadata("partition-spec-id".to_string(), "0")
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        writer
            .add_user_metadata("format-version".to_string(), "2")
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        writer
            .add_user_metadata("content".to_string(), "data")
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        for entry in &self.entries {
            let record = manifest_entry_to_avro_record(avro_schema, entry)?;
            writer
                .append(record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
        }

        writer
            .into_inner()
            .map_err(|e| StorageError::Serialization(e.to_string()))
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

/// Cached Avro schema for manifest entries (parsed once, reused forever).
/// This eliminates repeated JSON parsing overhead on every snapshot commit.
static MANIFEST_ENTRY_SCHEMA: OnceLock<AvroSchema> = OnceLock::new();

/// Cached Avro schema for manifest list entries (parsed once, reused forever).
static MANIFEST_LIST_SCHEMA: OnceLock<AvroSchema> = OnceLock::new();

/// JSON schema string for Iceberg v2 manifest entry.
const MANIFEST_ENTRY_SCHEMA_JSON: &str = r#"
{
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int"},
        {"name": "snapshot_id", "type": ["null", "long"], "default": null},
        {"name": "sequence_number", "type": ["null", "long"], "default": null},
        {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
        {
            "name": "data_file",
            "type": {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "content", "type": "int", "default": 0},
                    {"name": "file_path", "type": "string"},
                    {"name": "file_format", "type": "string"},
                    {"name": "record_count", "type": "long"},
                    {"name": "file_size_in_bytes", "type": "long"},
                    {
                        "name": "column_sizes",
                        "type": ["null", {"type": "map", "values": "long"}],
                        "default": null
                    },
                    {
                        "name": "value_counts",
                        "type": ["null", {"type": "map", "values": "long"}],
                        "default": null
                    },
                    {
                        "name": "null_value_counts",
                        "type": ["null", {"type": "map", "values": "long"}],
                        "default": null
                    },
                    {
                        "name": "lower_bounds",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                        "default": null
                    },
                    {
                        "name": "upper_bounds",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                        "default": null
                    },
                    {
                        "name": "split_offsets",
                        "type": ["null", {"type": "array", "items": "long"}],
                        "default": null
                    }
                ]
            }
        }
    ]
}
"#;

/// JSON schema string for Iceberg v2 manifest list entry.
const MANIFEST_LIST_SCHEMA_JSON: &str = r#"
{
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        {"name": "content", "type": "int"},
        {"name": "sequence_number", "type": "long"},
        {"name": "min_sequence_number", "type": "long"},
        {"name": "added_snapshot_id", "type": "long"},
        {"name": "added_files_count", "type": "int"},
        {"name": "existing_files_count", "type": "int"},
        {"name": "deleted_files_count", "type": "int"},
        {"name": "added_rows_count", "type": "long"},
        {"name": "existing_rows_count", "type": "long"},
        {"name": "deleted_rows_count", "type": "long"}
    ]
}
"#;

/// Returns the cached Avro schema for an Iceberg v2 manifest entry.
///
/// This is a simplified schema covering the fields Zombi actually populates.
/// The `data_file` sub-record includes file path, format, record count, size,
/// and optional column statistics maps.
///
/// The schema is parsed once on first call and cached for all subsequent calls,
/// eliminating the overhead of repeated JSON parsing.
fn manifest_entry_avro_schema() -> &'static AvroSchema {
    MANIFEST_ENTRY_SCHEMA.get_or_init(|| {
        AvroSchema::parse_str(MANIFEST_ENTRY_SCHEMA_JSON).expect("manifest_entry schema is valid")
    })
}

/// Converts a `ManifestEntry` to an Avro `Record`.
fn manifest_entry_to_avro_record<'a>(
    schema: &'a AvroSchema,
    entry: &ManifestEntry,
) -> Result<Record<'a>, StorageError> {
    let mut record = Record::new(schema).ok_or_else(|| {
        StorageError::Serialization("Failed to create manifest_entry record".into())
    })?;

    record.put("status", AvroValue::Int(entry.status));
    record.put(
        "snapshot_id",
        AvroValue::Union(1, Box::new(AvroValue::Long(entry.snapshot_id))),
    );
    record.put(
        "sequence_number",
        AvroValue::Union(0, Box::new(AvroValue::Null)),
    );
    record.put(
        "file_sequence_number",
        AvroValue::Union(0, Box::new(AvroValue::Null)),
    );

    // Build data_file sub-record
    let df = &entry.data_file;
    let data_file_schema = match schema {
        AvroSchema::Record(rs) => rs
            .fields
            .iter()
            .find(|f| f.name == "data_file")
            .map(|f| &f.schema)
            .ok_or_else(|| StorageError::Serialization("data_file field not found".into()))?,
        _ => return Err(StorageError::Serialization("Expected record schema".into())),
    };

    let mut df_record = Record::new(data_file_schema)
        .ok_or_else(|| StorageError::Serialization("Failed to create data_file record".into()))?;

    df_record.put("content", AvroValue::Int(df.content));
    df_record.put("file_path", AvroValue::String(df.file_path.clone()));
    df_record.put("file_format", AvroValue::String(df.file_format.clone()));
    df_record.put("record_count", AvroValue::Long(df.record_count));
    df_record.put("file_size_in_bytes", AvroValue::Long(df.file_size_in_bytes));

    // Optional map fields for column statistics
    fn i32_key_map_to_avro_long(map: &Option<HashMap<i32, i64>>) -> AvroValue {
        match map {
            Some(m) => {
                let avro_map: HashMap<String, AvroValue> = m
                    .iter()
                    .map(|(k, v)| (k.to_string(), AvroValue::Long(*v)))
                    .collect();
                AvroValue::Union(1, Box::new(AvroValue::Map(avro_map)))
            }
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        }
    }

    fn i32_key_map_to_avro_bytes(map: &Option<HashMap<i32, Vec<u8>>>) -> AvroValue {
        match map {
            Some(m) => {
                let avro_map: HashMap<String, AvroValue> = m
                    .iter()
                    .map(|(k, v)| (k.to_string(), AvroValue::Bytes(v.clone())))
                    .collect();
                AvroValue::Union(1, Box::new(AvroValue::Map(avro_map)))
            }
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        }
    }

    df_record.put("column_sizes", i32_key_map_to_avro_long(&df.column_sizes));
    df_record.put("value_counts", i32_key_map_to_avro_long(&df.value_counts));
    df_record.put(
        "null_value_counts",
        i32_key_map_to_avro_long(&df.null_value_counts),
    );
    df_record.put("lower_bounds", i32_key_map_to_avro_bytes(&df.lower_bounds));
    df_record.put("upper_bounds", i32_key_map_to_avro_bytes(&df.upper_bounds));

    match &df.split_offsets {
        Some(offsets) => {
            let arr: Vec<AvroValue> = offsets.iter().map(|o| AvroValue::Long(*o)).collect();
            df_record.put(
                "split_offsets",
                AvroValue::Union(1, Box::new(AvroValue::Array(arr))),
            );
        }
        None => {
            df_record.put(
                "split_offsets",
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            );
        }
    }

    record.put("data_file", df_record);
    Ok(record)
}

/// Returns the cached Avro schema for an Iceberg v2 manifest list entry.
///
/// The schema is parsed once on first call and cached for all subsequent calls,
/// eliminating the overhead of repeated JSON parsing.
fn manifest_list_entry_avro_schema() -> &'static AvroSchema {
    MANIFEST_LIST_SCHEMA.get_or_init(|| {
        AvroSchema::parse_str(MANIFEST_LIST_SCHEMA_JSON).expect("manifest_list schema is valid")
    })
}

/// Serializes manifest list entries to Avro binary format per Iceberg v2 spec.
pub fn manifest_list_to_avro_bytes(
    entries: &[ManifestListEntry],
    _metadata: &TableMetadata,
) -> Result<Vec<u8>, StorageError> {
    let schema = manifest_list_entry_avro_schema();
    let mut writer = AvroWriter::new(schema, Vec::new());

    writer
        .add_user_metadata("format-version".to_string(), "2")
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    for entry in entries {
        let mut record = Record::new(schema).ok_or_else(|| {
            StorageError::Serialization("Failed to create manifest_file record".into())
        })?;

        record.put(
            "manifest_path",
            AvroValue::String(entry.manifest_path.clone()),
        );
        record.put("manifest_length", AvroValue::Long(entry.manifest_length));
        record.put("partition_spec_id", AvroValue::Int(entry.partition_spec_id));
        record.put("content", AvroValue::Int(entry.content));
        record.put("sequence_number", AvroValue::Long(entry.sequence_number));
        record.put(
            "min_sequence_number",
            AvroValue::Long(entry.min_sequence_number),
        );
        record.put(
            "added_snapshot_id",
            AvroValue::Long(entry.added_snapshot_id),
        );
        record.put("added_files_count", AvroValue::Int(entry.added_files_count));
        record.put(
            "existing_files_count",
            AvroValue::Int(entry.existing_files_count),
        );
        record.put(
            "deleted_files_count",
            AvroValue::Int(entry.deleted_files_count),
        );
        record.put("added_rows_count", AvroValue::Long(entry.added_rows_count));
        record.put(
            "existing_rows_count",
            AvroValue::Long(entry.existing_rows_count),
        );
        record.put(
            "deleted_rows_count",
            AvroValue::Long(entry.deleted_rows_count),
        );

        writer
            .append(record)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
    }

    writer
        .into_inner()
        .map_err(|e| StorageError::Serialization(e.to_string()))
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
            HashMap::new(),
        );

        assert!(snapshot_id > 0);
        assert_eq!(metadata.current_snapshot_id, Some(snapshot_id));
        assert_eq!(metadata.snapshots.len(), 1);
        assert_eq!(metadata.snapshot_log.len(), 1);
        assert_eq!(metadata.last_sequence_number, 1);
    }

    #[test]
    fn add_snapshot_merges_extra_summary_and_drops_non_zombi_keys() {
        let mut metadata = TableMetadata::new("s3://bucket/tables/events");

        let mut extra = HashMap::new();
        extra.insert("zombi.watermark.0".to_string(), "42".to_string());
        extra.insert("zombi.high_watermark.0".to_string(), "100".to_string());
        extra.insert("not_zombi_key".to_string(), "should_be_dropped".to_string());

        let snapshot_id = metadata.add_snapshot(
            "s3://bucket/tables/events/metadata/snap-1.avro",
            2,
            50,
            SnapshotOperation::Append,
            extra,
        );

        let snapshot = metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == snapshot_id)
            .expect("snapshot should exist");

        // Zombi keys are present
        assert_eq!(snapshot.summary.get("zombi.watermark.0").unwrap(), "42");
        assert_eq!(
            snapshot.summary.get("zombi.high_watermark.0").unwrap(),
            "100"
        );

        // Non-zombi key was dropped
        assert!(!snapshot.summary.contains_key("not_zombi_key"));

        // Standard Iceberg keys are intact
        assert_eq!(snapshot.summary.get("operation").unwrap(), "append");
        assert_eq!(snapshot.summary.get("added-data-files").unwrap(), "2");
        assert_eq!(snapshot.summary.get("added-records").unwrap(), "50");
        assert_eq!(snapshot.summary.get("total-records").unwrap(), "50");
        assert_eq!(snapshot.summary.get("total-data-files").unwrap(), "2");
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

    #[test]
    fn test_iceberg_schema_with_extracted_fields() {
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

        let schema = IcebergSchema::with_extracted_fields(&config);

        assert_eq!(schema.schema_id, 1);

        // Check field names
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"sequence"));
        assert!(names.contains(&"topic"));
        assert!(names.contains(&"partition"));
        assert!(names.contains(&"timestamp_ms"));
        assert!(names.contains(&"idempotency_key"));
        assert!(names.contains(&"user_id"));
        assert!(names.contains(&"value"));
        assert!(names.contains(&"_payload_overflow"));
        assert!(names.contains(&"event_date"));
        assert!(names.contains(&"event_hour"));

        // payload should NOT be present
        assert!(!names.contains(&"payload"));

        // Extracted fields should have IDs starting at 100
        let user_id_field = schema.fields.iter().find(|f| f.name == "user_id").unwrap();
        assert_eq!(user_id_field.id, field_ids::EXTRACTED_START);
        assert_eq!(user_id_field.field_type, "string");
        assert!(!user_id_field.required); // nullable

        let value_field = schema.fields.iter().find(|f| f.name == "value").unwrap();
        assert_eq!(value_field.id, field_ids::EXTRACTED_START + 1);
        assert_eq!(value_field.field_type, "long");

        // Overflow field
        let overflow = schema
            .fields
            .iter()
            .find(|f| f.name == "_payload_overflow")
            .unwrap();
        assert_eq!(overflow.id, field_ids::PAYLOAD_OVERFLOW);
        assert!(!overflow.required);
    }

    #[test]
    fn test_table_metadata_with_schema_config() {
        use crate::contracts::{ExtractedField, FieldType, PayloadFormat, TableSchemaConfig};

        let config = TableSchemaConfig {
            table: "clicks".into(),
            payload_format: PayloadFormat::Json,
            fields: vec![ExtractedField {
                name: "user_id".into(),
                json_path: "user_id".into(),
                data_type: FieldType::Utf8,
                nullable: true,
            }],
            preserve_overflow: true,
        };

        let metadata = TableMetadata::with_schema_config("s3://bucket/tables/clicks", &config);

        assert_eq!(metadata.current_schema_id, 1);
        assert!(metadata.last_column_id >= field_ids::EXTRACTED_START);
        assert_eq!(metadata.schemas.len(), 1);
        assert_eq!(metadata.schemas[0].schema_id, 1);
    }

    #[test]
    fn test_iceberg_schema_last_column_id() {
        let schema = IcebergSchema::default();
        assert_eq!(schema.last_column_id(), 8);

        use crate::contracts::{ExtractedField, FieldType, PayloadFormat, TableSchemaConfig};

        let config = TableSchemaConfig {
            table: "test".into(),
            payload_format: PayloadFormat::Json,
            fields: vec![
                ExtractedField {
                    name: "a".into(),
                    json_path: "a".into(),
                    data_type: FieldType::Utf8,
                    nullable: true,
                },
                ExtractedField {
                    name: "b".into(),
                    json_path: "b".into(),
                    data_type: FieldType::Int64,
                    nullable: true,
                },
            ],
            preserve_overflow: true,
        };

        let schema = IcebergSchema::with_extracted_fields(&config);
        // Max should be extracted field: 101 (100 + 1)
        // But we also have system fields up to 8, and partition fields 7,8
        assert_eq!(schema.last_column_id(), 101); // EXTRACTED_START + 1
    }

    // =========================================================================
    // Avro Serialization Tests
    // =========================================================================

    #[test]
    fn test_manifest_file_to_avro_roundtrip() {
        use crate::storage::parquet::{ColumnStatistics, PartitionValues};

        let metadata = TableMetadata::new("s3://bucket/tables/events");

        let parquet_meta = ParquetFileMetadata {
            path: "/tmp/test.parquet".into(),
            row_count: 500,
            min_sequence: 1,
            max_sequence: 500,
            min_timestamp_ms: 1000,
            max_timestamp_ms: 2000,
            file_size_bytes: 25000,
            partition_values: PartitionValues::default(),
            column_stats: ColumnStatistics {
                sequence_min: 1,
                sequence_max: 500,
                partition_min: 0,
                partition_max: 0,
                timestamp_min: 1000,
                timestamp_max: 2000,
                event_date_min: 19737,
                event_date_max: 19737,
                event_hour_min: 10,
                event_hour_max: 14,
            },
        };

        let mut manifest = ManifestFile::new(12345, 1);
        manifest.add_data_file(DataFile::from_parquet_metadata(
            &parquet_meta,
            "s3://bucket/data/test.parquet",
        ));

        let avro_bytes = manifest.to_avro_bytes(&metadata).unwrap();
        assert!(!avro_bytes.is_empty());

        // Parse back with apache-avro
        let reader = apache_avro::Reader::new(&avro_bytes[..]).unwrap();

        // Verify metadata headers
        let file_meta = reader.user_metadata();
        assert_eq!(
            std::str::from_utf8(file_meta.get("format-version").unwrap()).unwrap(),
            "2"
        );
        assert_eq!(
            std::str::from_utf8(file_meta.get("content").unwrap()).unwrap(),
            "data"
        );
        assert!(file_meta.contains_key("schema"));
        assert!(file_meta.contains_key("partition-spec"));

        // Verify records
        let records: Vec<_> = reader.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1);

        // Check the first record's data_file has correct file_path
        if let apache_avro::types::Value::Record(fields) = &records[0] {
            let status = fields.iter().find(|(k, _)| k == "status").unwrap();
            assert_eq!(status.1, apache_avro::types::Value::Int(1));

            let df = fields.iter().find(|(k, _)| k == "data_file").unwrap();
            if let apache_avro::types::Value::Record(df_fields) = &df.1 {
                let path = df_fields.iter().find(|(k, _)| k == "file_path").unwrap();
                assert_eq!(
                    path.1,
                    apache_avro::types::Value::String("s3://bucket/data/test.parquet".into())
                );
            } else {
                panic!("data_file should be a Record");
            }
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_manifest_list_to_avro_roundtrip() {
        let metadata = TableMetadata::new("s3://bucket/tables/events");

        let entries = vec![ManifestListEntry {
            manifest_path: "s3://bucket/metadata/abc-m0.avro".into(),
            manifest_length: 1234,
            partition_spec_id: 0,
            content: 0,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 99999,
            added_files_count: 5,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: 10000,
            existing_rows_count: 0,
            deleted_rows_count: 0,
        }];

        let avro_bytes = manifest_list_to_avro_bytes(&entries, &metadata).unwrap();
        assert!(!avro_bytes.is_empty());

        // Parse back
        let reader = apache_avro::Reader::new(&avro_bytes[..]).unwrap();

        let file_meta = reader.user_metadata();
        assert_eq!(
            std::str::from_utf8(file_meta.get("format-version").unwrap()).unwrap(),
            "2"
        );

        let records: Vec<_> = reader.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1);

        if let apache_avro::types::Value::Record(fields) = &records[0] {
            let path = fields.iter().find(|(k, _)| k == "manifest_path").unwrap();
            assert_eq!(
                path.1,
                apache_avro::types::Value::String("s3://bucket/metadata/abc-m0.avro".into())
            );

            let rows = fields
                .iter()
                .find(|(k, _)| k == "added_rows_count")
                .unwrap();
            assert_eq!(rows.1, apache_avro::types::Value::Long(10000));
        }
    }
}
