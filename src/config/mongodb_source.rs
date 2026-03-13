use serde::{Deserialize, Serialize};

/// MongoDB-specific source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongodbSourceConfig {
    /// MongoDB connection string (e.g. `mongodb://host:port/?replicaSet=rs0`).
    pub connection_string: String,
    /// Database to capture changes from.
    pub database: String,
    /// Collections to capture. Empty means all collections.
    #[serde(default)]
    pub collections: Vec<String>,
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, SourceConfig};

    #[test]
    fn test_mongodb_config_deserialization() {
        let json = r#"{
            "source": {
                "type": "mongodb",
                "connection_string": "mongodb://localhost:27017/?replicaSet=rs0",
                "database": "mydb",
                "collections": ["users", "orders"]
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Mongodb(mg) => {
                assert_eq!(
                    mg.connection_string,
                    "mongodb://localhost:27017/?replicaSet=rs0"
                );
                assert_eq!(mg.database, "mydb");
                assert_eq!(mg.collections, vec!["users", "orders"]);
            }
            _ => panic!("expected Mongodb config"),
        }
    }

    #[test]
    fn test_mongodb_config_empty_collections_default() {
        let json = r#"{
            "source": {
                "type": "mongodb",
                "connection_string": "mongodb://localhost:27017/?replicaSet=rs0",
                "database": "mydb"
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "sqlite", "path": "/tmp/offset.db", "key": "mongo-offset" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Mongodb(mg) => {
                assert!(mg.collections.is_empty());
            }
            _ => panic!("expected Mongodb config"),
        }
    }
}
