use std::collections::{HashMap, HashSet};

use mysql_async::prelude::*;
use mysql_async::{Conn, Pool, Row as MysqlRow};
use tokio::sync::mpsc;

use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp, ColumnValue, Lsn, Row, TableId};
use crate::source::SourceEvent;

use super::event_converter::format_mysql_offset;

/// Result of performing a snapshot.
pub struct SnapshotResult {
    /// Binlog filename at the time of the snapshot.
    pub binlog_filename: String,
    /// Binlog position at the time of the snapshot.
    pub binlog_position: u64,
    /// Column names for each (schema, table) pair, populated from query metadata.
    pub column_names: HashMap<(String, String), Vec<String>>,
    /// Boolean column names (TINYINT(1)) for each (schema, table) pair.
    pub boolean_columns: HashMap<(String, String), HashSet<String>>,
    /// Primary key column names for each (schema, table) pair.
    pub pk_columns: HashMap<(String, String), Vec<String>>,
    /// Column types (column_name → column_type) for each (schema, table) pair.
    /// Used to correctly interpret `Value::Bytes` from MySQL wire protocol.
    pub column_types: HashMap<(String, String), HashMap<String, String>>,
}

/// Perform a consistent snapshot of the specified tables.
///
/// Uses `START TRANSACTION WITH CONSISTENT SNAPSHOT` + `SHOW MASTER STATUS`
/// to get a consistent binlog position, then `SELECT *` for each table.
pub async fn perform_snapshot(
    pool: &Pool,
    tables: &[String],
    sender: &mpsc::Sender<SourceEvent>,
) -> Result<SnapshotResult> {
    if tables.is_empty() {
        tracing::info!("no tables to snapshot");
        // Still need the current binlog position
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::Mysql(format!("failed to get connection: {e}")))?;
        let (filename, position) = get_binlog_position(&mut conn).await?;
        return Ok(SnapshotResult {
            binlog_filename: filename,
            binlog_position: position,
            column_names: HashMap::new(),
            boolean_columns: HashMap::new(),
            pk_columns: HashMap::new(),
            column_types: HashMap::new(),
        });
    }

    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| CdcError::Mysql(format!("failed to get connection: {e}")))?;

    // Start a consistent snapshot transaction
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(|e| CdcError::Mysql(format!("START TRANSACTION failed: {e}")))?;

    let (binlog_filename, binlog_position) = get_binlog_position(&mut conn).await?;

    tracing::info!(
        binlog_filename,
        binlog_position,
        num_tables = tables.len(),
        "starting MySQL snapshot"
    );

    let mut column_names: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut boolean_columns: HashMap<(String, String), HashSet<String>> = HashMap::new();
    let mut pk_columns_map: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut column_types_map: HashMap<(String, String), HashMap<String, String>> = HashMap::new();

    for table in tables {
        let (schema, name) = parse_table_name(table);
        let bools = get_boolean_columns(&mut conn, schema, name).await?;
        let pk_cols = get_table_pk_columns(&mut conn, schema, name).await?;
        let col_types = get_column_types(&mut conn, schema, name).await?;
        let cols = snapshot_table(
            &mut conn,
            schema,
            name,
            binlog_position,
            &bools,
            &col_types,
            &pk_cols,
            sender,
        )
        .await?;
        let key = (schema.to_string(), name.to_string());
        column_names.insert(key.clone(), cols);
        boolean_columns.insert(key.clone(), bools);
        pk_columns_map.insert(key.clone(), pk_cols);
        column_types_map.insert(key, col_types);
    }

    conn.query_drop("COMMIT")
        .await
        .map_err(|e| CdcError::Mysql(format!("COMMIT failed: {e}")))?;

    // Send checkpoint so the pipeline persists the snapshot offset
    let offset = format_mysql_offset(&binlog_filename, binlog_position);
    sender
        .send(SourceEvent::Checkpoint { offset })
        .await
        .map_err(|e| CdcError::Snapshot(format!("send checkpoint: {e}")))?;

    tracing::info!(binlog_filename, binlog_position, "MySQL snapshot complete");

    Ok(SnapshotResult {
        binlog_filename,
        binlog_position,
        column_names,
        boolean_columns,
        pk_columns: pk_columns_map,
        column_types: column_types_map,
    })
}

/// Snapshot a single table via SELECT *.
#[allow(clippy::too_many_arguments)]
async fn snapshot_table(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    binlog_position: u64,
    boolean_columns: &HashSet<String>,
    column_types: &HashMap<String, String>,
    pk_columns: &[String],
    sender: &mpsc::Sender<SourceEvent>,
) -> Result<Vec<String>> {
    tracing::info!(schema, table, "snapshotting table");

    let query = format!("SELECT * FROM `{schema}`.`{table}`");
    let rows: Vec<MysqlRow> = conn
        .query(&query)
        .await
        .map_err(|e| CdcError::Mysql(format!("SELECT from {schema}.{table}: {e}")))?;

    if rows.is_empty() {
        tracing::info!(
            schema,
            table,
            row_count = 0,
            "table snapshot complete (empty)"
        );
        // Get column names from an empty result — query information_schema
        let col_names = get_table_columns(conn, schema, table).await?;
        return Ok(col_names);
    }

    // Get column names from the first row's metadata
    let col_names: Vec<String> = rows[0]
        .columns_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();

    let table_id = TableId {
        schema: schema.to_string(),
        name: table.to_string(),
        oid: 0,
    };

    let timestamp_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;

    let mut row_count = 0u64;

    for row in &rows {
        let cdc_row = mysql_row_to_cdc_row(row, &col_names, boolean_columns, column_types);

        let event = CdcEvent {
            lsn: Lsn(binlog_position),
            timestamp_us,
            xid: 0,
            table: table_id.clone(),
            op: ChangeOp::Snapshot,
            new: Some(cdc_row),
            old: None,
            primary_key_columns: pk_columns.to_vec(),
        };

        sender
            .send(SourceEvent::Change(event))
            .await
            .map_err(|e| CdcError::Snapshot(format!("send event: {e}")))?;

        row_count += 1;
    }

    tracing::info!(schema, table, row_count, "table snapshot complete");
    Ok(col_names)
}

/// Get column names from information_schema.
pub async fn get_table_columns(conn: &mut Conn, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows: Vec<MysqlRow> = conn
        .exec(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await
        .map_err(|e| CdcError::Mysql(format!("get columns for {schema}.{table}: {e}")))?;

    Ok(rows
        .iter()
        .map(|r| r.get::<String, _>(0).unwrap_or_default())
        .collect())
}

/// Get primary key column names for a table, in ordinal position order.
pub async fn get_table_pk_columns(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows: Vec<MysqlRow> = conn
        .exec(
            "SELECT column_name FROM information_schema.key_column_usage \
             WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' \
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await
        .map_err(|e| CdcError::Mysql(format!("get PK columns for {schema}.{table}: {e}")))?;

    Ok(rows
        .iter()
        .map(|r| r.get::<String, _>(0).unwrap_or_default())
        .collect())
}

/// Get column names that are TINYINT(1) (MySQL's boolean type).
pub async fn get_boolean_columns(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<HashSet<String>> {
    let rows: Vec<MysqlRow> = conn
        .exec(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? AND column_type = 'tinyint(1)' \
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await
        .map_err(|e| CdcError::Mysql(format!("get boolean columns for {schema}.{table}: {e}")))?;

    Ok(rows
        .iter()
        .map(|r| r.get::<String, _>(0).unwrap_or_default())
        .collect())
}

/// Get column types from information_schema for type-aware Bytes parsing.
///
/// Returns a map of column_name → column_type (e.g. "int(11)", "varchar(255)").
pub async fn get_column_types(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<HashMap<String, String>> {
    let rows: Vec<MysqlRow> = conn
        .exec(
            "SELECT column_name, column_type FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await
        .map_err(|e| CdcError::Mysql(format!("get column types for {schema}.{table}: {e}")))?;

    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0).unwrap_or_default();
            let col_type: String = r.get(1).unwrap_or_default();
            (name, col_type)
        })
        .collect())
}

/// Parse MySQL `Value::Bytes` using column type metadata for correct type inference.
///
/// MySQL's wire protocol sends many numeric types as byte strings. This function
/// uses the column's declared type from `information_schema.columns` to parse the
/// bytes into the correct `ColumnValue` variant.
pub fn parse_mysql_bytes_with_type(bytes: &[u8], column_type: &str) -> ColumnValue {
    let text = String::from_utf8_lossy(bytes);

    // Strip display width and unsigned suffix: "int(11) unsigned" → "int"
    let base_type = column_type
        .split('(')
        .next()
        .unwrap_or(column_type)
        .trim()
        .to_lowercase();
    let base_type = base_type
        .trim_end_matches(" unsigned")
        .trim_end_matches(" zerofill");

    match base_type {
        "int" | "integer" | "mediumint" | "bigint" | "smallint" | "tinyint" => text
            .trim()
            .parse::<i64>()
            .map_or_else(|_| ColumnValue::Text(text.into_owned()), ColumnValue::Int),
        "float" | "double" | "real" => text
            .trim()
            .parse::<f64>()
            .map_or_else(|_| ColumnValue::Text(text.into_owned()), ColumnValue::float),
        // decimal/numeric: keep as Text — downstream handles string-based decimals
        _ => ColumnValue::Text(text.into_owned()),
    }
}

/// Convert a mysql_async Row to our CDC Row type.
///
/// `boolean_columns` contains column names that are `TINYINT(1)` and should
/// be converted from `Int(0/1)` to `Bool`.
/// `column_types` maps column names to their MySQL type strings (e.g. "int(11)",
/// "varchar(255)") for type-aware parsing of `Value::Bytes`.
fn mysql_row_to_cdc_row(
    row: &MysqlRow,
    col_names: &[String],
    boolean_columns: &HashSet<String>,
    column_types: &HashMap<String, String>,
) -> Row {
    col_names
        .iter()
        .enumerate()
        .map(|(i, col_name)| {
            let value: Option<mysql_async::Value> = row.get(i);
            let is_bool = boolean_columns.contains(col_name);
            let cv = match value {
                None | Some(mysql_async::Value::NULL) => ColumnValue::Null,
                Some(mysql_async::Value::Int(n)) if is_bool => ColumnValue::Bool(n != 0),
                Some(mysql_async::Value::Int(n)) => ColumnValue::Int(n),
                Some(mysql_async::Value::UInt(u)) if is_bool => ColumnValue::Bool(u != 0),
                Some(mysql_async::Value::UInt(u)) => {
                    if u <= i64::MAX as u64 {
                        ColumnValue::Int(u as i64)
                    } else {
                        ColumnValue::Text(u.to_string())
                    }
                }
                Some(mysql_async::Value::Float(f)) => ColumnValue::float(f as f64),
                Some(mysql_async::Value::Double(d)) => ColumnValue::float(d),
                Some(mysql_async::Value::Bytes(ref b)) => {
                    if let Some(col_type) = column_types.get(col_name) {
                        parse_mysql_bytes_with_type(b, col_type)
                    } else {
                        ColumnValue::Text(String::from_utf8_lossy(b).to_string())
                    }
                }
                Some(mysql_async::Value::Date(y, mo, d, h, mi, s, us)) => {
                    if h == 0 && mi == 0 && s == 0 && us == 0 {
                        ColumnValue::Date(format!("{y:04}-{mo:02}-{d:02}"))
                    } else if us == 0 {
                        ColumnValue::Timestamp(format!(
                            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}"
                        ))
                    } else {
                        ColumnValue::Timestamp(format!(
                            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
                        ))
                    }
                }
                Some(mysql_async::Value::Time(neg, days, h, mi, s, us)) => {
                    let sign = if neg { "-" } else { "" };
                    let total_hours = (days as u64) * 24 + (h as u64);
                    if us == 0 {
                        ColumnValue::Time(format!("{sign}{total_hours}:{mi:02}:{s:02}"))
                    } else {
                        ColumnValue::Time(format!("{sign}{total_hours}:{mi:02}:{s:02}.{us:06}"))
                    }
                }
            };
            (col_name.clone(), cv)
        })
        .collect()
}

/// Get the current binlog filename and position.
async fn get_binlog_position(conn: &mut Conn) -> Result<(String, u64)> {
    // Try `SHOW BINARY LOG STATUS` first (MySQL 8.4+), fallback to `SHOW MASTER STATUS`
    let result: std::result::Result<Vec<MysqlRow>, _> = conn.query("SHOW BINARY LOG STATUS").await;

    let rows = match result {
        Ok(rows) => rows,
        Err(_) => conn
            .query("SHOW MASTER STATUS")
            .await
            .map_err(|e| CdcError::Mysql(format!("SHOW MASTER STATUS failed: {e}")))?,
    };

    if rows.is_empty() {
        return Err(CdcError::Mysql(
            "SHOW MASTER STATUS returned no rows — is binary logging enabled?".to_string(),
        ));
    }

    let filename: String = rows[0]
        .get(0)
        .ok_or_else(|| CdcError::Mysql("missing binlog filename".to_string()))?;
    let position: u64 = rows[0]
        .get(1)
        .ok_or_else(|| CdcError::Mysql("missing binlog position".to_string()))?;

    Ok((filename, position))
}

/// Parse "schema.table" into (schema, table).
pub fn parse_table_name(full_name: &str) -> (&str, &str) {
    match full_name.split_once('.') {
        Some((schema, name)) => (schema, name),
        None => ("", full_name), // MySQL has no default schema for table names without dots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_name_with_schema() {
        assert_eq!(parse_table_name("mydb.users"), ("mydb", "users"));
        assert_eq!(parse_table_name("testdb.orders"), ("testdb", "orders"));
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        assert_eq!(parse_table_name("users"), ("", "users"));
    }

    #[test]
    fn test_parse_mysql_bytes_integer_types() {
        let cases = vec![
            ("int", "42", ColumnValue::Int(42)),
            ("int(11)", "42", ColumnValue::Int(42)),
            ("integer", "100", ColumnValue::Int(100)),
            ("mediumint", "999", ColumnValue::Int(999)),
            ("bigint", "9999999999", ColumnValue::Int(9999999999)),
            ("smallint", "32000", ColumnValue::Int(32000)),
            ("tinyint", "127", ColumnValue::Int(127)),
            ("int(11) unsigned", "42", ColumnValue::Int(42)),
            ("bigint unsigned", "100", ColumnValue::Int(100)),
        ];
        for (col_type, input, expected) in cases {
            let result = parse_mysql_bytes_with_type(input.as_bytes(), col_type);
            assert_eq!(result, expected, "type={col_type} input={input}");
        }
    }

    #[test]
    fn test_parse_mysql_bytes_float_types() {
        let cases = vec![("float", "1.23"), ("double", "4.567"), ("real", "99.9")];
        for (col_type, input) in cases {
            let result = parse_mysql_bytes_with_type(input.as_bytes(), col_type);
            let expected_val: f64 = input.parse().unwrap();
            assert_eq!(
                result,
                ColumnValue::float(expected_val),
                "type={col_type} input={input}"
            );
        }
    }

    #[test]
    fn test_parse_mysql_bytes_decimal_stays_text() {
        let result = parse_mysql_bytes_with_type(b"123.45", "decimal(10,2)");
        assert_eq!(result, ColumnValue::Text("123.45".to_string()));

        let result = parse_mysql_bytes_with_type(b"99.99", "numeric(5,2)");
        assert_eq!(result, ColumnValue::Text("99.99".to_string()));
    }

    #[test]
    fn test_parse_mysql_bytes_text_types() {
        let result = parse_mysql_bytes_with_type(b"hello", "varchar(255)");
        assert_eq!(result, ColumnValue::Text("hello".to_string()));

        let result = parse_mysql_bytes_with_type(b"world", "text");
        assert_eq!(result, ColumnValue::Text("world".to_string()));
    }

    #[test]
    fn test_parse_mysql_bytes_int_parse_failure_falls_back_to_text() {
        let result = parse_mysql_bytes_with_type(b"not_a_number", "int(11)");
        assert_eq!(result, ColumnValue::Text("not_a_number".to_string()));
    }

    #[test]
    fn test_parse_mysql_bytes_float_parse_failure_falls_back_to_text() {
        let result = parse_mysql_bytes_with_type(b"not_a_float", "double");
        assert_eq!(result, ColumnValue::Text("not_a_float".to_string()));
    }

    #[test]
    fn test_parse_mysql_bytes_negative_integers() {
        let result = parse_mysql_bytes_with_type(b"-42", "int(11)");
        assert_eq!(result, ColumnValue::Int(-42));
    }

    #[test]
    fn test_parse_mysql_bytes_negative_float() {
        let result = parse_mysql_bytes_with_type(b"-3.14", "double");
        assert_eq!(result, ColumnValue::float(-3.14));
    }
}
