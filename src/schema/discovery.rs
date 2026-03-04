use crate::config::SourceConnectionConfig;
use crate::error::{CdcError, Result};

use super::type_mapping::parse_source_type;
use super::{ColumnInfo, SourceDialect};

/// Fetch column metadata from the source database.
///
/// Returns columns ordered by ordinal position with both the raw source type
/// (for passthrough) and the parsed canonical type (for cross-dialect mapping).
pub async fn fetch_columns(
    source_conn: &SourceConnectionConfig,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    match source_conn {
        SourceConnectionConfig::Postgres { url } => {
            fetch_columns_postgres(url, schema, table).await
        }
        SourceConnectionConfig::Mysql { url } => fetch_columns_mysql(url, schema, table).await,
    }
}

/// Fetch primary key column names from the source database.
pub async fn fetch_primary_keys(
    source_conn: &SourceConnectionConfig,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    match source_conn {
        SourceConnectionConfig::Postgres { url } => {
            fetch_primary_keys_postgres(url, schema, table).await
        }
        SourceConnectionConfig::Mysql { url } => {
            fetch_primary_keys_mysql(url, schema, table).await
        }
    }
}

/// Fetch column metadata for a specific subset of columns.
///
/// Returns columns in the same order as `column_names`. Falls back to
/// `CanonicalType::Text` for any column not found in the source.
pub async fn fetch_column_subset(
    source_conn: &SourceConnectionConfig,
    schema: &str,
    table: &str,
    column_names: &[String],
) -> Result<Vec<ColumnInfo>> {
    match source_conn {
        SourceConnectionConfig::Postgres { url } => {
            fetch_column_subset_postgres(url, schema, table, column_names).await
        }
        SourceConnectionConfig::Mysql { url } => {
            fetch_column_subset_mysql(url, schema, table, column_names).await
        }
    }
}

async fn fetch_columns_postgres(url: &str, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| CdcError::Schema(format!("pg connect: {e}")))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("pg connection error: {e}");
        }
    });

    // Use pg_attribute + format_type for exact DDL types (e.g. "character varying(255)")
    // and information_schema for is_nullable + ordinal_position.
    let rows = client
        .query(
            "SELECT a.attname,
                    format_type(a.atttypid, a.atttypmod) AS source_type,
                    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
                    a.attnum AS ordinal_position
             FROM pg_attribute a
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = $1
               AND c.relname = $2
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&schema, &table],
        )
        .await
        .map_err(|e| CdcError::Schema(format!("pg query columns: {e}")))?;

    let dialect = SourceDialect::Postgres;
    let columns = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let source_type: String = row.get(1);
            let is_nullable_str: String = row.get(2);
            let ordinal_position: i16 = row.get(3);
            let canonical_type = parse_source_type(dialect, &source_type);
            ColumnInfo {
                name,
                source_type,
                canonical_type,
                is_nullable: is_nullable_str.eq_ignore_ascii_case("yes"),
                ordinal_position: ordinal_position as i32,
            }
        })
        .collect();

    Ok(columns)
}

async fn fetch_columns_mysql(url: &str, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
    use mysql_async::prelude::*;

    let pool = mysql_async::Pool::new(url);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| CdcError::Schema(format!("mysql connect: {e}")))?;

    let rows: Vec<(String, String, String, i32)> = conn
        .exec(
            "SELECT column_name, column_type, is_nullable, ordinal_position \
             FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await
        .map_err(|e| CdcError::Schema(format!("mysql query columns: {e}")))?;

    let dialect = SourceDialect::Mysql;
    let columns = rows
        .into_iter()
        .map(|(name, source_type, is_nullable_str, ordinal_position)| {
            let canonical_type = parse_source_type(dialect, &source_type);
            ColumnInfo {
                name,
                source_type,
                canonical_type,
                is_nullable: is_nullable_str.eq_ignore_ascii_case("yes"),
                ordinal_position,
            }
        })
        .collect();

    drop(conn);
    pool.disconnect()
        .await
        .map_err(|e| CdcError::Schema(format!("mysql disconnect: {e}")))?;

    Ok(columns)
}

async fn fetch_primary_keys_postgres(
    url: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| CdcError::Schema(format!("pg connect: {e}")))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("pg connection error: {e}");
        }
    });

    let rows = client
        .query(
            "SELECT kcu.column_name \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage kcu \
               ON tc.constraint_name = kcu.constraint_name \
              AND tc.table_schema = kcu.table_schema \
             WHERE tc.constraint_type = 'PRIMARY KEY' \
               AND tc.table_schema = $1 AND tc.table_name = $2 \
             ORDER BY kcu.ordinal_position",
            &[&schema, &table],
        )
        .await
        .map_err(|e| CdcError::Schema(format!("pg pk query: {e}")))?;

    Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
}

async fn fetch_primary_keys_mysql(url: &str, schema: &str, table: &str) -> Result<Vec<String>> {
    use mysql_async::prelude::*;

    let pool = mysql_async::Pool::new(url);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| CdcError::Schema(format!("mysql connect: {e}")))?;

    let rows: Vec<String> = conn
        .exec(
            "SELECT column_name \
             FROM information_schema.key_column_usage \
             WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' \
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await
        .map_err(|e| CdcError::Schema(format!("mysql pk query: {e}")))?;

    drop(conn);
    pool.disconnect()
        .await
        .map_err(|e| CdcError::Schema(format!("mysql disconnect: {e}")))?;

    Ok(rows)
}

async fn fetch_column_subset_postgres(
    url: &str,
    schema: &str,
    table: &str,
    column_names: &[String],
) -> Result<Vec<ColumnInfo>> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| CdcError::Schema(format!("pg connect: {e}")))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("pg connection error: {e}");
        }
    });

    let rows = client
        .query(
            "SELECT a.attname,
                    format_type(a.atttypid, a.atttypmod) AS source_type,
                    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
                    a.attnum AS ordinal_position
             FROM pg_attribute a
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = $1
               AND c.relname = $2
               AND a.attnum > 0
               AND NOT a.attisdropped
               AND a.attname = ANY($3)",
            &[&schema, &table, &column_names],
        )
        .await
        .map_err(|e| CdcError::Schema(format!("pg query column subset: {e}")))?;

    let dialect = SourceDialect::Postgres;
    let type_map: std::collections::HashMap<String, ColumnInfo> = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let source_type: String = row.get(1);
            let is_nullable_str: String = row.get(2);
            let ordinal_position: i16 = row.get(3);
            let canonical_type = parse_source_type(dialect, &source_type);
            let info = ColumnInfo {
                name: name.clone(),
                source_type,
                canonical_type,
                is_nullable: is_nullable_str.eq_ignore_ascii_case("yes"),
                ordinal_position: ordinal_position as i32,
            };
            (name, info)
        })
        .collect();

    // Return in the same order as column_names, with fallback for missing columns
    Ok(column_names
        .iter()
        .map(|name| {
            type_map.get(name).cloned().unwrap_or_else(|| {
                ColumnInfo {
                    name: name.clone(),
                    source_type: "text".into(),
                    canonical_type: super::CanonicalType::Text,
                    is_nullable: true,
                    ordinal_position: 0,
                }
            })
        })
        .collect())
}

async fn fetch_column_subset_mysql(
    url: &str,
    schema: &str,
    table: &str,
    column_names: &[String],
) -> Result<Vec<ColumnInfo>> {
    use mysql_async::prelude::*;

    let pool = mysql_async::Pool::new(url);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| CdcError::Schema(format!("mysql connect: {e}")))?;

    let rows: Vec<(String, String, String, i32)> = conn
        .exec(
            "SELECT column_name, column_type, is_nullable, ordinal_position \
             FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await
        .map_err(|e| CdcError::Schema(format!("mysql query column subset: {e}")))?;

    let dialect = SourceDialect::Mysql;
    let type_map: std::collections::HashMap<String, ColumnInfo> = rows
        .into_iter()
        .map(|(name, source_type, is_nullable_str, ordinal_position)| {
            let canonical_type = parse_source_type(dialect, &source_type);
            let info = ColumnInfo {
                name: name.clone(),
                source_type,
                canonical_type,
                is_nullable: is_nullable_str.eq_ignore_ascii_case("yes"),
                ordinal_position,
            };
            (name, info)
        })
        .collect();

    let result = column_names
        .iter()
        .map(|name| {
            type_map.get(name).cloned().unwrap_or_else(|| {
                ColumnInfo {
                    name: name.clone(),
                    source_type: "text".into(),
                    canonical_type: super::CanonicalType::Text,
                    is_nullable: true,
                    ordinal_position: 0,
                }
            })
        })
        .collect();

    drop(conn);
    pool.disconnect()
        .await
        .map_err(|e| CdcError::Schema(format!("mysql disconnect: {e}")))?;

    Ok(result)
}
