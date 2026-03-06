use serde::{Deserialize, Serialize};

use super::mysql_source::MySqlSourceConfig;
use super::postgres_source::PostgresSourceConfig;

/// Source configuration — tagged enum for multiple database types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "postgres")]
    Postgres(PostgresSourceConfig),
    #[serde(rename = "mysql")]
    Mysql(MySqlSourceConfig),
}

impl SourceConfig {
    /// Build a [`super::SourceConnectionConfig`] from the source's connection fields.
    ///
    /// This lets the pipeline derive the schema-inference connection from the
    /// already-configured source, so users don't have to duplicate it in the
    /// sink config.
    pub fn to_connection_config(&self) -> super::SourceConnectionConfig {
        match self {
            SourceConfig::Postgres(pg) => super::SourceConnectionConfig::Postgres {
                url: format!(
                    "postgres://{}{}@{}:{}/{}",
                    pg.user,
                    pg.password
                        .as_deref()
                        .map(|p| format!(":{p}"))
                        .unwrap_or_default(),
                    pg.host,
                    pg.port,
                    pg.database,
                ),
            },
            SourceConfig::Mysql(my) => super::SourceConnectionConfig::Mysql {
                url: format!(
                    "mysql://{}{}@{}:{}/{}",
                    my.user,
                    my.password
                        .as_deref()
                        .map(|p| format!(":{p}"))
                        .unwrap_or_default(),
                    my.host,
                    my.port,
                    my.database,
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceConnectionConfig;

    #[test]
    fn test_postgres_to_connection_config_with_password() {
        let config = SourceConfig::Postgres(PostgresSourceConfig {
            host: "db.example.com".into(),
            port: 5432,
            user: "replicator".into(),
            password: Some("secret".into()),
            database: "mydb".into(),
            slot_name: "slot".into(),
            publication_name: "pub".into(),
            create_publication: true,
            tables: vec![],
        });
        match config.to_connection_config() {
            SourceConnectionConfig::Postgres { url } => {
                assert_eq!(url, "postgres://replicator:secret@db.example.com:5432/mydb");
            }
            _ => panic!("expected Postgres variant"),
        }
    }

    #[test]
    fn test_postgres_to_connection_config_without_password() {
        let config = SourceConfig::Postgres(PostgresSourceConfig {
            host: "localhost".into(),
            port: 5433,
            user: "user".into(),
            password: None,
            database: "testdb".into(),
            slot_name: "slot".into(),
            publication_name: "pub".into(),
            create_publication: true,
            tables: vec![],
        });
        match config.to_connection_config() {
            SourceConnectionConfig::Postgres { url } => {
                assert_eq!(url, "postgres://user@localhost:5433/testdb");
            }
            _ => panic!("expected Postgres variant"),
        }
    }

    #[test]
    fn test_mysql_to_connection_config_with_password() {
        let config = SourceConfig::Mysql(MySqlSourceConfig {
            host: "mysql.example.com".into(),
            port: 3306,
            user: "cdc_user".into(),
            password: Some("cdc_password".into()),
            database: "demo".into(),
            server_id: 1003,
            tables: vec!["demo.users".into()],
        });
        match config.to_connection_config() {
            SourceConnectionConfig::Mysql { url } => {
                assert_eq!(
                    url,
                    "mysql://cdc_user:cdc_password@mysql.example.com:3306/demo"
                );
            }
            _ => panic!("expected Mysql variant"),
        }
    }

    #[test]
    fn test_mysql_to_connection_config_without_password() {
        let config = SourceConfig::Mysql(MySqlSourceConfig {
            host: "localhost".into(),
            port: 3307,
            user: "root".into(),
            password: None,
            database: "app".into(),
            server_id: 1,
            tables: vec![],
        });
        match config.to_connection_config() {
            SourceConnectionConfig::Mysql { url } => {
                assert_eq!(url, "mysql://root@localhost:3307/app");
            }
            _ => panic!("expected Mysql variant"),
        }
    }
}
