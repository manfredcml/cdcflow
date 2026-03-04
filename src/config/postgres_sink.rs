use serde::{Deserialize, Serialize};

use super::postgres_source::default_pg_port;

/// PostgreSQL sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSinkConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    pub database: String,
    /// Target schema for sink tables (default: "public").
    #[serde(default = "default_pg_sink_schema")]
    pub schema: String,
    /// Optional table name prefix (e.g., "cdc_" → "cdc_users").
    #[serde(default)]
    pub table_prefix: String,
}

fn default_pg_sink_schema() -> String {
    "public".to_string()
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, SinkConfig};

    #[test]
    fn test_postgres_sink_config_deserialization() {
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
                "type": "postgres",
                "host": "target-db",
                "port": 5433,
                "user": "writer",
                "password": "secret",
                "database": "target_db",
                "schema": "cdc_data",
                "table_prefix": "cdc_"
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Postgres(pg) => {
                assert_eq!(pg.host, "target-db");
                assert_eq!(pg.port, 5433);
                assert_eq!(pg.user, "writer");
                assert_eq!(pg.password.as_deref(), Some("secret"));
                assert_eq!(pg.database, "target_db");
                assert_eq!(pg.schema, "cdc_data");
                assert_eq!(pg.table_prefix, "cdc_");
            }
            _ => panic!("expected Postgres sink config"),
        }
    }

    #[test]
    fn test_postgres_sink_config_defaults() {
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
                "type": "postgres",
                "host": "target-db",
                "user": "writer",
                "database": "target_db"
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Postgres(pg) => {
                assert_eq!(pg.port, 5432);
                assert_eq!(pg.schema, "public");
                assert_eq!(pg.table_prefix, "");
                assert!(pg.password.is_none());
            }
            _ => panic!("expected Postgres sink config"),
        }
    }
}
