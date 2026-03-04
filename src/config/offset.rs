use serde::{Deserialize, Serialize};

/// Offset store configuration — tagged enum for future store types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OffsetConfig {
    #[serde(rename = "sqlite")]
    Sqlite { path: String, key: String },
    #[serde(rename = "memory")]
    Memory,
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, OffsetConfig};

    #[test]
    fn test_memory_offset_config_deserialization() {
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
        assert!(matches!(config.offset, OffsetConfig::Memory));
    }

    #[test]
    fn test_sqlite_offset_config_deserialization() {
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
                "path": "/tmp/cdc-offsets.db",
                "key": "cdc-offset:test"
            }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.offset {
            OffsetConfig::Sqlite { path, key } => {
                assert_eq!(path, "/tmp/cdc-offsets.db");
                assert_eq!(key, "cdc-offset:test");
            }
            _ => panic!("expected Sqlite offset config"),
        }
    }
}
