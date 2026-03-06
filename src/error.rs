use thiserror::Error;

#[derive(Debug, Error)]
pub enum CdcError {
    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("snapshot error: {0}")]
    Snapshot(String),

    #[error("mysql error: {0}")]
    Mysql(String),

    #[error("iceberg error: {0}")]
    Iceberg(String),

    #[error("kafka error: {0}")]
    Kafka(String),

    #[error("http error: {0}")]
    Http(String),

    #[error("admin error: {0}")]
    Admin(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("schema error: {0}")]
    Schema(String),
}

pub type Result<T> = std::result::Result<T, CdcError>;
