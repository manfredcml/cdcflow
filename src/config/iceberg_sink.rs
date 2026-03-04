use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Iceberg sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSinkConfig {
    pub catalog: IcebergCatalogConfig,
    /// Iceberg namespace (e.g., ["my_database"]).
    pub namespace: Vec<String>,
    /// Optional table name prefix (e.g., "cdc_" → "cdc_users").
    #[serde(default)]
    pub table_prefix: String,
}

/// Catalog backend configuration — tagged enum for extensibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum IcebergCatalogConfig {
    #[serde(rename = "rest")]
    Rest(RestCatalogConfig),
}

/// REST catalog connection settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestCatalogConfig {
    pub uri: String,
    /// Optional warehouse location (e.g., "s3://bucket/warehouse").
    #[serde(default)]
    pub warehouse: Option<String>,
    /// Extra properties passed to RestCatalogBuilder.
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Timeout in seconds for catalog connections and HTTP requests.
    #[serde(default = "default_catalog_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_catalog_timeout_secs() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, IcebergCatalogConfig, SinkConfig};

    #[test]
    fn test_iceberg_sink_config_deserialization() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "iceberg",
                "catalog": {
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://my-bucket/warehouse"
                },
                "namespace": ["my_database"],
                "table_prefix": "cdc_"
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Iceberg(ic) => {
                assert_eq!(ic.namespace, vec!["my_database"]);
                assert_eq!(ic.table_prefix, "cdc_");
                match &ic.catalog {
                    IcebergCatalogConfig::Rest(rest) => {
                        assert_eq!(rest.uri, "http://localhost:8181");
                        assert_eq!(rest.warehouse.as_deref(), Some("s3://my-bucket/warehouse"));
                    }
                }

            }
            _ => panic!("expected Iceberg sink config"),
        }
    }

    #[test]
    fn test_iceberg_sink_config_default_table_prefix() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "iceberg",
                "catalog": {
                    "type": "rest",
                    "uri": "http://localhost:8181"
                },
                "namespace": ["db"]
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Iceberg(ic) => {
                assert_eq!(ic.table_prefix, "");
                match &ic.catalog {
                    IcebergCatalogConfig::Rest(rest) => {
                        assert!(rest.warehouse.is_none());
                        assert!(rest.properties.is_empty());
                        assert_eq!(rest.timeout_secs, 30);
                    }
                }

            }
            _ => panic!("expected Iceberg sink config"),
        }
    }

    #[test]
    fn test_rest_catalog_config_with_properties() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "iceberg",
                "catalog": {
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "properties": {
                        "s3.access-key-id": "admin",
                        "s3.secret-access-key": "password"
                    }
                },
                "namespace": ["db"]
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Iceberg(ic) => match &ic.catalog {
                IcebergCatalogConfig::Rest(rest) => {
                    assert_eq!(rest.properties.len(), 2);
                    assert_eq!(
                        rest.properties.get("s3.access-key-id").map(String::as_str),
                        Some("admin")
                    );
                    assert_eq!(
                        rest.properties.get("s3.secret-access-key").map(String::as_str),
                        Some("password")
                    );
                }
            },
            _ => panic!("expected Iceberg sink config"),
        }
    }

    #[test]
    fn test_rest_catalog_config_custom_timeout() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "iceberg",
                "catalog": {
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "timeout_secs": 60
                },
                "namespace": ["db"]
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Iceberg(ic) => match &ic.catalog {
                IcebergCatalogConfig::Rest(rest) => {
                    assert_eq!(rest.timeout_secs, 60);
                }
            },
            _ => panic!("expected Iceberg sink config"),
        }
    }
}
