use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::api::handlers::{validate_table_name, AppState};
use crate::contracts::{ColdStorage, HotStorage};

const DEFAULT_NAMESPACE: &str = "zombi";
const NAMESPACE_SEPARATOR: char = '\u{1F}';

#[derive(Debug, Serialize)]
pub struct CatalogConfigResponse {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
    pub endpoints: Vec<String>,
    #[serde(
        rename = "idempotency-key-lifetime",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key_lifetime: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListNamespacesQuery {
    pub parent: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListNamespacesResponse {
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub namespaces: Vec<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct LoadNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub identifiers: Vec<TableIdentifier>,
}

#[derive(Debug, Serialize)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    pub metadata: serde_json::Value,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct IcebergErrorResponse {
    pub error: IcebergErrorModel,
}

#[derive(Debug, Serialize)]
pub struct IcebergErrorModel {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: u16,
}

type CatalogError = (StatusCode, Json<IcebergErrorResponse>);

fn parse_namespace_path(raw: &str) -> Vec<String> {
    raw.split(NAMESPACE_SEPARATOR)
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_string())
        .collect()
}

fn parse_namespace_config(raw: &str) -> Vec<String> {
    if raw.contains(NAMESPACE_SEPARATOR) {
        return parse_namespace_path(raw);
    }

    raw.split('.')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_string())
        .collect()
}

fn configured_namespace() -> Vec<String> {
    let raw = std::env::var("ZOMBI_CATALOG_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
    let parsed = parse_namespace_config(&raw);
    if parsed.is_empty() {
        vec![DEFAULT_NAMESPACE.into()]
    } else {
        parsed
    }
}

fn iceberg_error(
    status: StatusCode,
    error_type: &'static str,
    message: impl Into<String>,
) -> CatalogError {
    (
        status,
        Json(IcebergErrorResponse {
            error: IcebergErrorModel {
                message: message.into(),
                error_type: error_type.into(),
                code: status.as_u16(),
            },
        }),
    )
}

fn no_such_namespace(raw_namespace: &str) -> CatalogError {
    iceberg_error(
        StatusCode::NOT_FOUND,
        "NoSuchNamespaceException",
        format!("Namespace does not exist: {}", raw_namespace),
    )
}

fn no_such_table(namespace: &str, table: &str) -> CatalogError {
    iceberg_error(
        StatusCode::NOT_FOUND,
        "NoSuchTableException",
        format!("Table does not exist: {}.{}", namespace, table),
    )
}

fn internal_error(message: impl Into<String>) -> CatalogError {
    iceberg_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "RESTException",
        message.into(),
    )
}

fn validate_exact_namespace(path_namespace: &str, expected: &[String]) -> Result<(), CatalogError> {
    let actual = parse_namespace_path(path_namespace);
    if actual == expected {
        Ok(())
    } else {
        Err(no_such_namespace(path_namespace))
    }
}

fn list_namespaces_for_parent(
    expected: &[String],
    parent: Option<&str>,
) -> Result<Vec<Vec<String>>, CatalogError> {
    if expected.is_empty() {
        return Ok(Vec::new());
    }

    let top_level = vec![expected[0].clone()];
    let Some(raw_parent) = parent.filter(|raw| !raw.is_empty()) else {
        return Ok(vec![top_level]);
    };

    let parent_parts = parse_namespace_path(raw_parent);
    if parent_parts.is_empty() {
        return Ok(vec![top_level]);
    }
    if parent_parts.len() > expected.len() || !expected.starts_with(&parent_parts) {
        return Err(no_such_namespace(raw_parent));
    }
    if parent_parts.len() == expected.len() {
        return Ok(Vec::new());
    }

    Ok(vec![expected[..(parent_parts.len() + 1)].to_vec()])
}

/// GET /v1/config
pub async fn get_catalog_config<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
) -> Json<CatalogConfigResponse> {
    let mut overrides = HashMap::new();
    if let Some(cold) = &state.cold_storage {
        let info = cold.storage_info();
        if info.iceberg_enabled && !info.bucket.is_empty() && !info.base_path.is_empty() {
            overrides.insert(
                "warehouse".into(),
                format!(
                    "s3://{}/{}",
                    info.bucket,
                    info.base_path.trim_end_matches('/')
                ),
            );
        }
    }
    overrides.insert("namespace-separator".into(), "%1F".into());

    Json(CatalogConfigResponse {
        defaults: HashMap::new(),
        overrides,
        endpoints: vec![
            "GET /v1/config".into(),
            "GET /v1/{prefix}/namespaces".into(),
            "GET /v1/{prefix}/namespaces/{namespace}".into(),
            "GET /v1/{prefix}/namespaces/{namespace}/tables".into(),
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}".into(),
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}".into(),
        ],
        idempotency_key_lifetime: None,
    })
}

/// GET /v1/namespaces
pub async fn list_namespaces(
    Query(query): Query<ListNamespacesQuery>,
) -> Result<Json<ListNamespacesResponse>, CatalogError> {
    let expected = configured_namespace();
    let namespaces = list_namespaces_for_parent(&expected, query.parent.as_deref())?;
    Ok(Json(ListNamespacesResponse {
        next_page_token: None,
        namespaces,
    }))
}

/// GET /v1/namespaces/{namespace}
pub async fn load_namespace(
    Path(namespace): Path<String>,
) -> Result<Json<LoadNamespaceResponse>, CatalogError> {
    let expected = configured_namespace();
    validate_exact_namespace(&namespace, &expected)?;
    Ok(Json(LoadNamespaceResponse {
        namespace: expected,
        properties: HashMap::new(),
    }))
}

/// GET /v1/namespaces/{namespace}/tables
pub async fn list_tables<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path(namespace): Path<String>,
) -> Result<Json<ListTablesResponse>, CatalogError> {
    let expected_namespace = configured_namespace();
    validate_exact_namespace(&namespace, &expected_namespace)?;

    let Some(cold) = &state.cold_storage else {
        return Ok(Json(ListTablesResponse {
            next_page_token: None,
            identifiers: Vec::new(),
        }));
    };

    if !cold.storage_info().iceberg_enabled {
        return Ok(Json(ListTablesResponse {
            next_page_token: None,
            identifiers: Vec::new(),
        }));
    }

    let mut table_names = cold
        .list_iceberg_tables()
        .await
        .map_err(|e| internal_error(format!("Failed to list Iceberg tables: {}", e)))?;

    table_names.retain(|name| validate_table_name(name).is_ok());
    table_names.sort();
    table_names.dedup();

    let identifiers = table_names
        .into_iter()
        .map(|name| TableIdentifier {
            namespace: expected_namespace.clone(),
            name,
        })
        .collect();

    Ok(Json(ListTablesResponse {
        next_page_token: None,
        identifiers,
    }))
}

/// GET /v1/namespaces/{namespace}/tables/{table}
pub async fn load_table<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<LoadTableResponse>, CatalogError> {
    let expected_namespace = configured_namespace();
    validate_exact_namespace(&namespace, &expected_namespace)?;

    if validate_table_name(&table).is_err() {
        return Err(no_such_table(&namespace, &table));
    }

    let Some(cold) = &state.cold_storage else {
        return Err(no_such_table(&namespace, &table));
    };
    if !cold.storage_info().iceberg_enabled {
        return Err(no_such_table(&namespace, &table));
    }

    let loaded = cold
        .load_iceberg_table(&table)
        .await
        .map_err(|e| internal_error(format!("Failed to load table metadata: {}", e)))?;
    let Some(loaded) = loaded else {
        return Err(no_such_table(&namespace, &table));
    };

    let metadata: serde_json::Value = serde_json::from_str(&loaded.metadata_json)
        .map_err(|e| internal_error(format!("Invalid table metadata JSON: {}", e)))?;

    Ok(Json(LoadTableResponse {
        metadata_location: loaded.metadata_location,
        metadata,
        config: HashMap::new(),
    }))
}

/// HEAD /v1/namespaces/{namespace}/tables/{table}
pub async fn table_exists<H: HotStorage, C: ColdStorage>(
    State(state): State<Arc<AppState<H, C>>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<StatusCode, CatalogError> {
    let expected_namespace = configured_namespace();
    validate_exact_namespace(&namespace, &expected_namespace)?;

    if validate_table_name(&table).is_err() {
        return Err(no_such_table(&namespace, &table));
    }

    let Some(cold) = &state.cold_storage else {
        return Err(no_such_table(&namespace, &table));
    };
    if !cold.storage_info().iceberg_enabled {
        return Err(no_such_table(&namespace, &table));
    }

    let exists = cold
        .load_iceberg_table(&table)
        .await
        .map_err(|e| internal_error(format!("Failed to check table existence: {}", e)))?
        .is_some();

    if exists {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(no_such_table(&namespace, &table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_path_parsing_uses_unit_separator_only() {
        assert_eq!(
            parse_namespace_path("accounting\u{1F}tax"),
            vec!["accounting".to_string(), "tax".to_string()]
        );
        assert_eq!(
            parse_namespace_path("accounting.tax"),
            vec!["accounting.tax".to_string()]
        );
    }

    #[test]
    fn namespace_config_parsing_accepts_dot_for_env_compatibility() {
        assert_eq!(
            parse_namespace_config("accounting.tax"),
            vec!["accounting".to_string(), "tax".to_string()]
        );
        assert_eq!(
            parse_namespace_config("accounting\u{1F}tax"),
            vec!["accounting".to_string(), "tax".to_string()]
        );
    }
}
