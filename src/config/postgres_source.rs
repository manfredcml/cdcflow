use serde::{Deserialize, Serialize};

/// PostgreSQL-specific source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    pub database: String,
    pub slot_name: String,
    #[serde(default = "default_publication_name")]
    pub publication_name: String,
    /// If true (default), auto-create the publication if it doesn't exist.
    #[serde(default = "default_true")]
    pub create_publication: bool,
    /// Tables to capture. If empty, all tables in the publication are captured.
    #[serde(default)]
    pub tables: Vec<String>,
}

pub(crate) fn default_pg_port() -> u16 {
    5432
}

fn default_publication_name() -> String {
    "cdcflow_pub".to_string()
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, OffsetConfig, SinkConfig, SourceConfig};

    #[test]
    fn test_config_deserialization() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "port": 5432,
                "user": "replicator",
                "password": "secret",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "stdout"
            },
            "offset": {
                "type": "sqlite",
                "path": "/tmp/offset.db",
                "key": "pg-offset"
            }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Postgres(pg) => {
                assert_eq!(pg.host, "localhost");
                assert_eq!(pg.port, 5432);
                assert_eq!(pg.user, "replicator");
                assert_eq!(pg.password.as_deref(), Some("secret"));
                assert_eq!(pg.database, "mydb");
                assert_eq!(pg.slot_name, "cdc_slot");
                assert_eq!(pg.publication_name, "cdc_pub");
                assert!(pg.tables.is_empty());
            }
            _ => panic!("expected Postgres config"),
        }
        assert!(matches!(config.sink, SinkConfig::Stdout));
        assert!(matches!(config.offset, OffsetConfig::Sqlite { .. }));
    }

    #[test]
    fn test_config_default_port() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Postgres(pg) => {
                assert_eq!(pg.port, 5432);
                assert!(pg.password.is_none());
            }
            _ => panic!("expected Postgres config"),
        }
    }

    #[test]
    fn test_config_with_tables() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub",
                "tables": ["public.users", "public.orders"]
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Postgres(pg) => {
                assert_eq!(pg.tables, vec!["public.users", "public.orders"]);
            }
            _ => panic!("expected Postgres config"),
        }
    }

    #[test]
    fn test_create_publication_defaults_to_true() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Postgres(pg) => {
                assert!(pg.create_publication);
            }
            _ => panic!("expected Postgres config"),
        }
    }

    #[test]
    fn test_create_publication_explicit_false() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub",
                "create_publication": false
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Postgres(pg) => {
                assert!(!pg.create_publication);
            }
            _ => panic!("expected Postgres config"),
        }
    }

    #[test]
    fn test_publication_name_defaults_to_cdcflow_pub() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot"
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Postgres(pg) => {
                assert_eq!(pg.publication_name, "cdcflow_pub");
            }
            _ => panic!("expected Postgres config"),
        }
    }
}
