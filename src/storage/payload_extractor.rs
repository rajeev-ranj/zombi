use crate::contracts::{FieldType, PayloadFormat, StorageError, TableSchemaConfig};

/// Result of extracting fields from a single event payload.
#[derive(Debug)]
pub struct ExtractionResult {
    /// Extracted values in the same order as `TableSchemaConfig.fields`.
    /// `None` means the field was missing or had a type mismatch.
    pub values: Vec<Option<ExtractedValue>>,
    /// Remaining fields not covered by the schema (serialized as JSON bytes).
    /// `None` if `preserve_overflow` is false or all fields were extracted.
    pub overflow: Option<Vec<u8>>,
}

/// A typed value extracted from a payload.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtractedValue {
    Utf8(String),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Binary(Vec<u8>),
}

/// Extracts fields from a single event payload according to the schema config.
///
/// For JSON payloads:
/// - Parses the payload bytes as JSON
/// - Extracts each declared field by `json_path` (dot-separated for nesting)
/// - Coerces values to the target `FieldType`
/// - Collects remaining (unextracted) fields into overflow bytes
///
/// For Binary payloads:
/// - Returns all values as `None` (no extraction possible)
/// - The entire payload goes to overflow
pub fn extract_fields(
    payload: &[u8],
    config: &TableSchemaConfig,
) -> Result<ExtractionResult, StorageError> {
    match config.payload_format {
        PayloadFormat::Binary => {
            let values = vec![None; config.fields.len()];
            let overflow = if config.preserve_overflow {
                Some(payload.to_vec())
            } else {
                None
            };
            Ok(ExtractionResult { values, overflow })
        }
        PayloadFormat::Json => extract_json_fields(payload, config),
    }
}

fn extract_json_fields(
    payload: &[u8],
    config: &TableSchemaConfig,
) -> Result<ExtractionResult, StorageError> {
    let mut json_value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|e| StorageError::Serialization(format!("Failed to parse JSON payload: {}", e)))?;

    let mut values = Vec::with_capacity(config.fields.len());

    // Track which top-level keys were extracted (for overflow computation)
    let mut extracted_paths: Vec<&str> = Vec::new();

    for field in &config.fields {
        let val = resolve_json_path(&json_value, &field.json_path);
        let extracted = val.and_then(|v| coerce_value(v, &field.data_type));
        values.push(extracted);
        extracted_paths.push(&field.json_path);
    }

    // Build overflow: remove extracted top-level keys from the JSON object
    let overflow = if config.preserve_overflow {
        if let serde_json::Value::Object(ref mut map) = json_value {
            // Remove only simple (non-nested) extracted paths
            for path in &extracted_paths {
                if !path.contains('.') {
                    map.remove(*path);
                }
            }
            if map.is_empty() {
                None
            } else {
                Some(
                    serde_json::to_vec(&map)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?,
                )
            }
        } else {
            // Non-object JSON (array, scalar) — entire payload is overflow
            Some(payload.to_vec())
        }
    } else {
        None
    };

    Ok(ExtractionResult { values, overflow })
}

/// Resolves a dot-separated path in a JSON value.
/// E.g., "meta.nested.value" traverses {"meta": {"nested": {"value": 42}}} → 42
fn resolve_json_path<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path.split('.') {
        current = current.get(key)?;
    }
    Some(current)
}

/// Coerces a JSON value to the target field type.
/// Returns `None` if the value cannot be coerced (type mismatch).
fn coerce_value(value: &serde_json::Value, target: &FieldType) -> Option<ExtractedValue> {
    match target {
        FieldType::Utf8 => match value {
            serde_json::Value::String(s) => Some(ExtractedValue::Utf8(s.clone())),
            serde_json::Value::Number(n) => Some(ExtractedValue::Utf8(n.to_string())),
            serde_json::Value::Bool(b) => Some(ExtractedValue::Utf8(b.to_string())),
            serde_json::Value::Null => None,
            _ => None,
        },
        FieldType::Int32 => value
            .as_i64()
            .and_then(|n| i32::try_from(n).ok().map(ExtractedValue::Int32)),
        FieldType::Int64 => value.as_i64().map(ExtractedValue::Int64),
        FieldType::Float64 => value.as_f64().map(ExtractedValue::Float64),
        FieldType::Boolean => value.as_bool().map(ExtractedValue::Boolean),
        FieldType::Binary => match value {
            serde_json::Value::String(s) => Some(ExtractedValue::Binary(s.as_bytes().to_vec())),
            _ => None,
        },
    }
}

/// Batch-extracts fields from multiple payloads.
/// Returns one `ExtractionResult` per payload, in order.
pub fn extract_batch(
    payloads: &[&[u8]],
    config: &TableSchemaConfig,
) -> Result<Vec<ExtractionResult>, StorageError> {
    payloads.iter().map(|p| extract_fields(p, config)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{ExtractedField, FieldType, PayloadFormat, TableSchemaConfig};

    fn test_config(fields: Vec<ExtractedField>) -> TableSchemaConfig {
        TableSchemaConfig {
            table: "test".into(),
            payload_format: PayloadFormat::Json,
            fields,
            preserve_overflow: true,
        }
    }

    fn field(name: &str, path: &str, data_type: FieldType) -> ExtractedField {
        ExtractedField {
            name: name.into(),
            json_path: path.into(),
            data_type,
            nullable: true,
        }
    }

    #[test]
    fn test_extract_flat_json_fields() {
        let config = test_config(vec![
            field("user_id", "user_id", FieldType::Utf8),
            field("value", "value", FieldType::Int64),
        ]);

        let payload = br#"{"user_id": "u123", "value": 42, "extra": "data"}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values.len(), 2);
        assert_eq!(result.values[0], Some(ExtractedValue::Utf8("u123".into())));
        assert_eq!(result.values[1], Some(ExtractedValue::Int64(42)));

        // Overflow should contain only "extra"
        let overflow = result.overflow.unwrap();
        let overflow_json: serde_json::Value = serde_json::from_slice(&overflow).unwrap();
        assert_eq!(overflow_json, serde_json::json!({"extra": "data"}));
    }

    #[test]
    fn test_extract_missing_field_returns_none() {
        let config = test_config(vec![
            field("user_id", "user_id", FieldType::Utf8),
            field("missing", "nonexistent", FieldType::Int64),
        ]);

        let payload = br#"{"user_id": "u123"}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], Some(ExtractedValue::Utf8("u123".into())));
        assert_eq!(result.values[1], None); // missing field
    }

    #[test]
    fn test_extract_type_mismatch_returns_none() {
        let config = test_config(vec![field("value", "value", FieldType::Int64)]);

        let payload = br#"{"value": "not_a_number"}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], None); // type mismatch
    }

    #[test]
    fn test_extract_nested_path() {
        let config = test_config(vec![field(
            "nested_val",
            "meta.nested.value",
            FieldType::Int64,
        )]);

        let payload = br#"{"meta": {"nested": {"value": 99}}}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], Some(ExtractedValue::Int64(99)));
    }

    #[test]
    fn test_extract_boolean_field() {
        let config = test_config(vec![field("active", "active", FieldType::Boolean)]);

        let payload = br#"{"active": true}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], Some(ExtractedValue::Boolean(true)));
    }

    #[test]
    fn test_extract_float64_field() {
        let config = test_config(vec![field("price", "price", FieldType::Float64)]);

        let payload = br#"{"price": 19.99}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], Some(ExtractedValue::Float64(19.99)));
    }

    #[test]
    fn test_extract_int32_field() {
        let config = test_config(vec![field("count", "count", FieldType::Int32)]);

        let payload = br#"{"count": 42}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], Some(ExtractedValue::Int32(42)));
    }

    #[test]
    fn test_extract_int32_overflow_returns_none() {
        let config = test_config(vec![field("big", "big", FieldType::Int32)]);

        // Value exceeds i32::MAX
        let payload = br#"{"big": 3000000000}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], None);
    }

    #[test]
    fn test_binary_format_no_extraction() {
        let config = TableSchemaConfig {
            table: "test".into(),
            payload_format: PayloadFormat::Binary,
            fields: vec![field("user_id", "user_id", FieldType::Utf8)],
            preserve_overflow: true,
        };

        let payload = b"opaque binary data";
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], None);
        assert_eq!(result.overflow.unwrap(), payload.to_vec());
    }

    #[test]
    fn test_preserve_overflow_false() {
        let config = TableSchemaConfig {
            table: "test".into(),
            payload_format: PayloadFormat::Json,
            fields: vec![field("user_id", "user_id", FieldType::Utf8)],
            preserve_overflow: false,
        };

        let payload = br#"{"user_id": "u123", "extra": "data"}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert!(result.overflow.is_none());
    }

    #[test]
    fn test_all_fields_extracted_no_overflow() {
        let config = test_config(vec![
            field("a", "a", FieldType::Utf8),
            field("b", "b", FieldType::Int64),
        ]);

        let payload = br#"{"a": "hello", "b": 42}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert!(result.overflow.is_none()); // all fields extracted, map is empty
    }

    #[test]
    fn test_invalid_json_returns_error() {
        let config = test_config(vec![field("a", "a", FieldType::Utf8)]);

        let payload = b"not json";
        let result = extract_fields(payload, &config);

        assert!(result.is_err());
    }

    #[test]
    fn test_utf8_coercion_from_number() {
        let config = test_config(vec![field("id", "id", FieldType::Utf8)]);

        let payload = br#"{"id": 12345}"#;
        let result = extract_fields(payload, &config).unwrap();

        assert_eq!(result.values[0], Some(ExtractedValue::Utf8("12345".into())));
    }

    #[test]
    fn test_batch_extraction() {
        let config = test_config(vec![field("user_id", "user_id", FieldType::Utf8)]);

        let p1 = br#"{"user_id": "u1"}"#;
        let p2 = br#"{"user_id": "u2"}"#;
        let payloads: Vec<&[u8]> = vec![p1.as_slice(), p2.as_slice()];

        let results = extract_batch(&payloads, &config).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].values[0],
            Some(ExtractedValue::Utf8("u1".into()))
        );
        assert_eq!(
            results[1].values[0],
            Some(ExtractedValue::Utf8("u2".into()))
        );
    }
}
