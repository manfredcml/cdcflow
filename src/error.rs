use thiserror::Error;

#[derive(Debug, Error)]
pub enum CdcError {
    #[error("postgres error: {}", format_postgres_error(.0))]
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

    #[error("mongodb error: {0}")]
    Mongodb(String),

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

/// `tokio_postgres::Error` renders server failures as the bare string
/// "db error" — the useful text only lives in the wrapped `DbError`.
fn format_postgres_error(err: &tokio_postgres::Error) -> String {
    match err.as_db_error() {
        Some(db) => {
            let mut msg = format!("{}: {}", db.severity(), db.message());
            if let Some(detail) = db.detail() {
                msg.push_str(&format!(" (detail: {detail})"));
            }
            if let Some(column) = db.column() {
                msg.push_str(&format!(" (column: {column})"));
            }
            msg
        }
        None => err.to_string(),
    }
}

pub type Result<T> = std::result::Result<T, CdcError>;
