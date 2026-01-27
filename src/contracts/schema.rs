use serde::{Deserialize, Serialize};

/// Configuration for extracting structured columns from event payloads.
///
/// When configured for a table, the flusher will parse payloads at flush time
/// and write extracted fields as typed Parquet columns instead of opaque binary blobs.
/// This enables columnar optimizations: predicate pushdown, column pruning, and
/// efficient compression via dictionary/delta encoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchemaConfig {
    /// Table name this schema applies to (matches topic name).
    pub table: String,
    /// Format of the payload bytes.
    pub payload_format: PayloadFormat,
    /// Fields to extract from the payload into typed columns.
    pub fields: Vec<ExtractedField>,
    /// Whether to preserve unextracted fields in a `_payload_overflow` column.
    /// Default: true. When false, unextracted fields are silently dropped.
    #[serde(default = "default_true")]
    pub preserve_overflow: bool,
}

fn default_true() -> bool {
    true
}

/// Payload content format.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    /// Parse payload as JSON, extract fields by key path.
    Json,
    /// No extraction possible, store as binary blob (current behavior).
    #[default]
    Binary,
}

/// A field to extract from the payload into a typed Parquet column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedField {
    /// Column name in Parquet output.
    pub name: String,
    /// Path to extract from the payload (e.g., "user_id" or "meta.nested").
    /// For JSON payloads, dot-separated keys are used for nested access.
    pub json_path: String,
    /// Target Arrow/Parquet data type.
    pub data_type: FieldType,
    /// Whether the column allows NULL (when field is missing or type mismatches).
    #[serde(default = "default_true")]
    pub nullable: bool,
}

/// Supported field types for extracted columns.
/// Maps to Arrow DataTypes for Parquet writing and Iceberg schema types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FieldType {
    Utf8,
    Int32,
    Int64,
    Float64,
    Boolean,
    Binary,
}

impl FieldType {
    /// Returns the corresponding Iceberg type string.
    pub fn iceberg_type(&self) -> &'static str {
        match self {
            Self::Utf8 => "string",
            Self::Int32 => "int",
            Self::Int64 => "long",
            Self::Float64 => "double",
            Self::Boolean => "boolean",
            Self::Binary => "binary",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_table_schema_config() {
        let json = r#"{
            "table": "clicks",
            "payload_format": "json",
            "fields": [
                {"name": "user_id", "json_path": "user_id", "data_type": "Utf8", "nullable": true},
                {"name": "action", "json_path": "action", "data_type": "Utf8", "nullable": true},
                {"name": "value", "json_path": "value", "data_type": "Int64", "nullable": true}
            ],
            "preserve_overflow": true
        }"#;

        let config: TableSchemaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.table, "clicks");
        assert_eq!(config.payload_format, PayloadFormat::Json);
        assert_eq!(config.fields.len(), 3);
        assert_eq!(config.fields[0].name, "user_id");
        assert_eq!(config.fields[0].data_type, FieldType::Utf8);
        assert!(config.preserve_overflow);
    }

    #[test]
    fn test_deserialize_defaults() {
        let json = r#"{
            "table": "events",
            "payload_format": "binary",
            "fields": []
        }"#;

        let config: TableSchemaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.payload_format, PayloadFormat::Binary);
        assert!(config.preserve_overflow); // default true
    }

    #[test]
    fn test_field_type_iceberg_mapping() {
        assert_eq!(FieldType::Utf8.iceberg_type(), "string");
        assert_eq!(FieldType::Int32.iceberg_type(), "int");
        assert_eq!(FieldType::Int64.iceberg_type(), "long");
        assert_eq!(FieldType::Float64.iceberg_type(), "double");
        assert_eq!(FieldType::Boolean.iceberg_type(), "boolean");
        assert_eq!(FieldType::Binary.iceberg_type(), "binary");
    }

    #[test]
    fn test_deserialize_nested_path() {
        let json = r#"{
            "table": "events",
            "payload_format": "json",
            "fields": [
                {"name": "nested_val", "json_path": "meta.nested.value", "data_type": "Int64", "nullable": true}
            ]
        }"#;

        let config: TableSchemaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.fields[0].json_path, "meta.nested.value");
    }

    #[test]
    fn test_serialize_roundtrip() {
        let config = TableSchemaConfig {
            table: "test".into(),
            payload_format: PayloadFormat::Json,
            fields: vec![ExtractedField {
                name: "user_id".into(),
                json_path: "user_id".into(),
                data_type: FieldType::Utf8,
                nullable: true,
            }],
            preserve_overflow: true,
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: TableSchemaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.table, "test");
        assert_eq!(parsed.fields.len(), 1);
    }
}
