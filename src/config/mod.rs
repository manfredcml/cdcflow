mod iceberg_sink;
mod kafka_sink;
mod mysql_source;
mod offset;
mod postgres_sink;
mod postgres_source;
mod sink;
mod source;

pub use self::iceberg_sink::{IcebergCatalogConfig, IcebergSinkConfig, RestCatalogConfig};
pub use self::kafka_sink::KafkaSinkConfig;
pub use self::mysql_source::MySqlSourceConfig;
pub use self::offset::OffsetConfig;
pub use self::postgres_sink::PostgresSinkConfig;
pub use self::postgres_source::PostgresSourceConfig;
pub use self::sink::SinkConfig;
pub use self::source::SourceConfig;

use serde::{Deserialize, Serialize};

/// Sink operating mode.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkMode {
    /// Append-only CDC log with nested metadata + row JSON
    #[default]
    Cdc,
    /// Replicate source table state using Iceberg v2 equality deletes.
    Replication,
}

/// Source DB connection for schema inference during table auto-creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConnectionConfig {
    #[serde(rename = "postgres")]
    Postgres { url: String },
    #[serde(rename = "mysql")]
    Mysql { url: String },
}

/// Top-level configuration for the CDC agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub offset: OffsetConfig,
    #[serde(default)]
    pub mode: SinkMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_mode_default_is_cdc() {
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
        assert_eq!(config.mode, SinkMode::Cdc);
    }

    #[test]
    fn test_sink_mode_replication() {
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
            "offset": { "type": "memory" },
            "mode": "replication"
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.mode, SinkMode::Replication);
    }

    #[test]
    fn test_invalid_config_missing_required() {
        let json = r#"{"source": {"type": "postgres"}}"#;
        assert!(serde_json::from_str::<Config>(json).is_err());
    }
}
