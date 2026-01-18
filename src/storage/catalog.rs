use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::contracts::StorageError;
use crate::storage::TableMetadata;

/// Configuration for Iceberg REST catalog.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// REST catalog base URL (e.g., "http://localhost:8181").
    pub base_url: String,
    /// Namespace for tables (e.g., "zombi").
    pub namespace: String,
    /// Optional authentication token.
    pub auth_token: Option<String>,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8181".into(),
            namespace: "zombi".into(),
            auth_token: None,
            timeout_secs: 30,
        }
    }
}

/// Iceberg REST catalog client.
pub struct CatalogClient {
    config: CatalogConfig,
    client: reqwest::Client,
}

/// REST API request/response types.
#[derive(Debug, Serialize)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct CreateTableRequest {
    name: String,
    location: String,
    schema: serde_json::Value,
    #[serde(rename = "partition-spec")]
    partition_spec: serde_json::Value,
    #[serde(rename = "write-order")]
    write_order: serde_json::Value,
    properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct UpdateTableRequest {
    identifier: TableIdentifier,
    requirements: Vec<serde_json::Value>,
    updates: Vec<TableUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action")]
enum TableUpdate {
    #[serde(rename = "add-snapshot")]
    AddSnapshot { snapshot: serde_json::Value },
    #[serde(rename = "set-current-snapshot-id")]
    SetCurrentSnapshotId {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ListNamespacesResponse {
    namespaces: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ListTablesResponse {
    identifiers: Vec<TableIdentifier>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: Option<String>,
    metadata: serde_json::Value,
}

impl CatalogClient {
    /// Creates a new catalog client.
    pub fn new(config: CatalogConfig) -> Result<Self, StorageError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| StorageError::S3(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self { config, client })
    }

    /// Returns the base URL for the catalog API.
    fn api_url(&self, path: &str) -> String {
        format!("{}/v1{}", self.config.base_url.trim_end_matches('/'), path)
    }

    /// Adds authentication header if configured.
    fn add_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.config.auth_token {
            request.header("Authorization", format!("Bearer {}", token))
        } else {
            request
        }
    }

    /// Checks if the namespace exists, creates if not.
    pub async fn ensure_namespace(&self) -> Result<(), StorageError> {
        let url = self.api_url(&format!("/namespaces/{}", self.config.namespace));

        let request = self.add_auth(self.client.head(&url));
        let response = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("Catalog request failed: {}", e)))?;

        if response.status().is_success() {
            return Ok(());
        }

        // Create namespace
        let create_url = self.api_url("/namespaces");
        let body = CreateNamespaceRequest {
            namespace: vec![self.config.namespace.clone()],
            properties: HashMap::new(),
        };

        let request = self.add_auth(self.client.post(&create_url)).json(&body);

        let response = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("Catalog request failed: {}", e)))?;

        if response.status().is_success() || response.status().as_u16() == 409 {
            // 409 = already exists
            Ok(())
        } else {
            Err(StorageError::S3(format!(
                "Failed to create namespace: {}",
                response.status()
            )))
        }
    }

    /// Registers a table with the catalog.
    pub async fn register_table(
        &self,
        table_name: &str,
        metadata: &TableMetadata,
    ) -> Result<(), StorageError> {
        self.ensure_namespace().await?;

        let url = self.api_url(&format!("/namespaces/{}/tables", self.config.namespace));

        // Convert our metadata to Iceberg REST format
        let schema = serde_json::json!({
            "type": "struct",
            "schema-id": 0,
            "fields": metadata.schemas.first().map(|s| &s.fields).unwrap_or(&vec![])
        });

        let body = CreateTableRequest {
            name: table_name.to_string(),
            location: metadata.location.clone(),
            schema,
            partition_spec: serde_json::json!({"spec-id": 0, "fields": []}),
            write_order: serde_json::json!({"order-id": 0, "fields": []}),
            properties: metadata.properties.clone(),
        };

        let request = self.add_auth(self.client.post(&url)).json(&body);

        let response = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("Catalog request failed: {}", e)))?;

        if response.status().is_success() || response.status().as_u16() == 409 {
            tracing::info!(
                namespace = self.config.namespace,
                table = table_name,
                "Table registered with catalog"
            );
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(StorageError::S3(format!(
                "Failed to register table: {} - {}",
                status, body
            )))
        }
    }

    /// Updates table metadata in the catalog after a new snapshot.
    pub async fn update_table(
        &self,
        table_name: &str,
        metadata: &TableMetadata,
    ) -> Result<(), StorageError> {
        let url = self.api_url(&format!(
            "/namespaces/{}/tables/{}",
            self.config.namespace, table_name
        ));

        // Get the latest snapshot
        let snapshot = metadata
            .snapshots
            .last()
            .ok_or_else(|| StorageError::S3("No snapshots to update".into()))?;

        let body = UpdateTableRequest {
            identifier: TableIdentifier {
                namespace: vec![self.config.namespace.clone()],
                name: table_name.to_string(),
            },
            requirements: vec![],
            updates: vec![
                TableUpdate::AddSnapshot {
                    snapshot: serde_json::to_value(snapshot)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?,
                },
                TableUpdate::SetCurrentSnapshotId {
                    snapshot_id: snapshot.snapshot_id,
                },
            ],
        };

        let request = self.add_auth(self.client.post(&url)).json(&body);

        let response = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("Catalog request failed: {}", e)))?;

        if response.status().is_success() {
            tracing::info!(
                namespace = self.config.namespace,
                table = table_name,
                snapshot_id = snapshot.snapshot_id,
                "Table metadata updated in catalog"
            );
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(StorageError::S3(format!(
                "Failed to update table: {} - {}",
                status, body
            )))
        }
    }

    /// Lists all tables in the namespace.
    pub async fn list_tables(&self) -> Result<Vec<String>, StorageError> {
        let url = self.api_url(&format!("/namespaces/{}/tables", self.config.namespace));

        let request = self.add_auth(self.client.get(&url));

        let response = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("Catalog request failed: {}", e)))?;

        if response.status().is_success() {
            let body: ListTablesResponse = response
                .json()
                .await
                .map_err(|e| StorageError::S3(format!("Failed to parse response: {}", e)))?;

            Ok(body.identifiers.into_iter().map(|t| t.name).collect())
        } else {
            Err(StorageError::S3(format!(
                "Failed to list tables: {}",
                response.status()
            )))
        }
    }

    /// Gets table metadata from the catalog.
    pub async fn load_table(&self, table_name: &str) -> Result<serde_json::Value, StorageError> {
        let url = self.api_url(&format!(
            "/namespaces/{}/tables/{}",
            self.config.namespace, table_name
        ));

        let request = self.add_auth(self.client.get(&url));

        let response = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("Catalog request failed: {}", e)))?;

        if response.status().is_success() {
            let body: LoadTableResponse = response
                .json()
                .await
                .map_err(|e| StorageError::S3(format!("Failed to parse response: {}", e)))?;

            Ok(body.metadata)
        } else if response.status().as_u16() == 404 {
            Err(StorageError::TopicNotFound(table_name.to_string()))
        } else {
            Err(StorageError::S3(format!(
                "Failed to load table: {}",
                response.status()
            )))
        }
    }

    /// Drops a table from the catalog.
    pub async fn drop_table(&self, table_name: &str) -> Result<(), StorageError> {
        let url = self.api_url(&format!(
            "/namespaces/{}/tables/{}",
            self.config.namespace, table_name
        ));

        let request = self.add_auth(self.client.delete(&url));

        let response = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("Catalog request failed: {}", e)))?;

        if response.status().is_success() || response.status().as_u16() == 404 {
            tracing::info!(
                namespace = self.config.namespace,
                table = table_name,
                "Table dropped from catalog"
            );
            Ok(())
        } else {
            Err(StorageError::S3(format!(
                "Failed to drop table: {}",
                response.status()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_config_default() {
        let config = CatalogConfig::default();
        assert_eq!(config.base_url, "http://localhost:8181");
        assert_eq!(config.namespace, "zombi");
        assert!(config.auth_token.is_none());
    }

    #[test]
    fn test_api_url_construction() {
        let config = CatalogConfig {
            base_url: "http://catalog.example.com".into(),
            ..Default::default()
        };
        let client = CatalogClient::new(config).unwrap();

        assert_eq!(
            client.api_url("/namespaces"),
            "http://catalog.example.com/v1/namespaces"
        );
    }

    #[test]
    fn test_api_url_trailing_slash() {
        let config = CatalogConfig {
            base_url: "http://catalog.example.com/".into(),
            ..Default::default()
        };
        let client = CatalogClient::new(config).unwrap();

        assert_eq!(
            client.api_url("/namespaces"),
            "http://catalog.example.com/v1/namespaces"
        );
    }
}
