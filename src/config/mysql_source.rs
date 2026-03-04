use serde::{Deserialize, Serialize};

/// MySQL-specific source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlSourceConfig {
    pub host: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    pub database: String,
    /// Unique server ID for this CDC agent (must be unique among all replicas).
    pub server_id: u32,
    /// Tables to capture in "database.table" format.
    pub tables: Vec<String>,
}

fn default_mysql_port() -> u16 {
    3306
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, SourceConfig};

    #[test]
    fn test_mysql_config_deserialization() {
        let json = r#"{
            "source": {
                "type": "mysql",
                "host": "localhost",
                "port": 3306,
                "user": "cdc_user",
                "password": "secret",
                "database": "mydb",
                "server_id": 1000,
                "tables": ["mydb.users", "mydb.orders"]
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Mysql(my) => {
                assert_eq!(my.host, "localhost");
                assert_eq!(my.port, 3306);
                assert_eq!(my.user, "cdc_user");
                assert_eq!(my.password.as_deref(), Some("secret"));
                assert_eq!(my.database, "mydb");
                assert_eq!(my.server_id, 1000);
                assert_eq!(my.tables, vec!["mydb.users", "mydb.orders"]);
            }
            _ => panic!("expected Mysql config"),
        }
    }

    #[test]
    fn test_mysql_config_default_port() {
        let json = r#"{
            "source": {
                "type": "mysql",
                "host": "localhost",
                "user": "cdc_user",
                "database": "mydb",
                "server_id": 1000,
                "tables": ["mydb.users"]
            },
            "sink": { "type": "stdout" },
            "offset": { "type": "sqlite", "path": "/tmp/offset.db", "key": "mysql-offset" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.source {
            SourceConfig::Mysql(my) => {
                assert_eq!(my.port, 3306);
                assert!(my.password.is_none());
            }
            _ => panic!("expected Mysql config"),
        }
    }
}
