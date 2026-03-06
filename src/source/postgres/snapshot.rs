use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_postgres::Client;

use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp, ColumnValue, Lsn, Row, TableId};
use crate::source::SourceEvent;

use super::relation_cache::parse_pg_text;

/// How often (in rows) to send a checkpoint during snapshot.
/// Each checkpoint triggers the pipeline to flush the accumulated batch to the sink.
const SNAPSHOT_CHECKPOINT_INTERVAL: u64 = 10_000;

/// Performs a consistent snapshot of the specified tables.
///
/// Uses `COPY table TO STDOUT` within a `REPEATABLE READ` transaction
/// to get a consistent view of the data. Each row is emitted as a
/// `CdcEvent` with `snapshot: true`.
pub async fn perform_snapshot(
    client: &Client,
    tables: &[String],
    snapshot_lsn: Lsn,
    sender: &mpsc::Sender<SourceEvent>,
) -> Result<()> {
    if tables.is_empty() {
        tracing::info!("no tables to snapshot");
        return Ok(());
    }

    tracing::info!(
        num_tables = tables.len(),
        %snapshot_lsn,
        "starting snapshot"
    );

    // Begin a REPEATABLE READ transaction so all COPY commands see the same
    // consistent snapshot across tables.
    client
        .simple_query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(|e| CdcError::Snapshot(format!("begin snapshot transaction: {e}")))?;

    let result: Result<()> = async {
        for table in tables {
            snapshot_table(client, table, snapshot_lsn, sender).await?;
        }
        Ok(())
    }
    .await;

    // Always commit (REPEATABLE READ is read-only here, but we must close the TX)
    let _ = client.simple_query("COMMIT").await;

    result?;

    // Send final checkpoint so the pipeline persists the snapshot LSN
    sender
        .send(SourceEvent::Checkpoint {
            offset: snapshot_lsn.to_string(),
        })
        .await
        .map_err(|e| CdcError::Snapshot(format!("send checkpoint: {e}")))?;

    tracing::info!(%snapshot_lsn, "snapshot complete");
    Ok(())
}

/// Snapshot a single table using COPY TO STDOUT.
///
/// Streams COPY data incrementally instead of collecting into memory,
/// and emits periodic checkpoints to trigger pipeline flushes.
async fn snapshot_table(
    client: &Client,
    table: &str,
    snapshot_lsn: Lsn,
    sender: &mpsc::Sender<SourceEvent>,
) -> Result<()> {
    let (schema, name) = parse_table_name(table);

    tracing::info!(schema, name, "snapshotting table");

    let columns = get_table_columns(client, schema, name).await?;
    if columns.is_empty() {
        tracing::warn!(schema, name, "table has no columns, skipping");
        return Ok(());
    }

    let oid = get_table_oid(client, schema, name).await?;

    let pk_columns = get_table_pk_columns(client, schema, name).await?;

    let table_id = TableId {
        schema: schema.to_string(),
        name: name.to_string(),
        oid,
    };

    let timestamp_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;

    let copy_query = format!(
        "COPY {}.{} TO STDOUT",
        quote_ident(schema),
        quote_ident(name)
    );
    let stream = client
        .copy_out(&copy_query)
        .await
        .map_err(|e| CdcError::Snapshot(format!("COPY {schema}.{name}: {e}")))?;

    // Stream COPY data incrementally — process complete lines as they arrive
    let mut row_count = 0u64;
    let mut buffer = String::new();
    let mut stream = std::pin::pin!(stream);

    while let Some(chunk_result) = stream.next().await {
        let chunk =
            chunk_result.map_err(|e| CdcError::Snapshot(format!("COPY stream error: {e}")))?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(newline_pos) = buffer.find('\n') {
            let line = &buffer[..newline_pos];
            if !line.is_empty() {
                let event = parse_copy_line(
                    line,
                    &columns,
                    &table_id,
                    snapshot_lsn,
                    timestamp_us,
                    &pk_columns,
                );
                sender
                    .send(SourceEvent::Change(event))
                    .await
                    .map_err(|e| CdcError::Snapshot(format!("send event: {e}")))?;

                row_count += 1;

                // Emit periodic checkpoints to trigger pipeline flushes
                if row_count.is_multiple_of(SNAPSHOT_CHECKPOINT_INTERVAL) {
                    sender
                        .send(SourceEvent::Checkpoint {
                            offset: snapshot_lsn.to_string(),
                        })
                        .await
                        .map_err(|e| CdcError::Snapshot(format!("send checkpoint: {e}")))?;
                }
            }
            buffer = buffer[newline_pos + 1..].to_string();
        }
    }

    if !buffer.is_empty() {
        let event = parse_copy_line(
            &buffer,
            &columns,
            &table_id,
            snapshot_lsn,
            timestamp_us,
            &pk_columns,
        );
        sender
            .send(SourceEvent::Change(event))
            .await
            .map_err(|e| CdcError::Snapshot(format!("send event: {e}")))?;
        row_count += 1;
    }

    tracing::info!(schema, name, row_count, "table snapshot complete");
    Ok(())
}

/// Parse a single COPY text-format line into a CdcEvent.
fn parse_copy_line(
    line: &str,
    columns: &[(String, u32)],
    table_id: &TableId,
    snapshot_lsn: Lsn,
    timestamp_us: i64,
    pk_columns: &[String],
) -> CdcEvent {
    let values: Vec<&str> = line.split('\t').collect();
    let row: Row = columns
        .iter()
        .zip(values.iter())
        .map(|((col_name, type_oid), &val)| {
            let cv = if val == "\\N" {
                ColumnValue::Null
            } else {
                parse_pg_text(*type_oid, &unescape_copy_text(val))
            };
            (col_name.clone(), cv)
        })
        .collect();

    CdcEvent {
        lsn: snapshot_lsn,
        timestamp_us,
        xid: 0,
        table: table_id.clone(),
        op: ChangeOp::Snapshot,
        new: Some(row),
        old: None,
        primary_key_columns: pk_columns.to_vec(),
    }
}

/// Parse "schema.table" into (schema, table). Defaults to "public" if no schema.
pub fn parse_table_name(full_name: &str) -> (&str, &str) {
    match full_name.split_once('.') {
        Some((schema, name)) => (schema, name),
        None => ("public", full_name),
    }
}

/// Get column names and type OIDs for a table from pg_attribute.
async fn get_table_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<(String, u32)>> {
    let rows = client
        .query(
            "SELECT a.attname, a.atttypid \
             FROM pg_attribute a \
             JOIN pg_class c ON a.attrelid = c.oid \
             JOIN pg_namespace n ON c.relnamespace = n.oid \
             WHERE n.nspname = $1 AND c.relname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&schema, &table],
        )
        .await
        .map_err(|e| CdcError::Snapshot(format!("get columns for {schema}.{table}: {e}")))?;

    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            let oid: u32 = r.get(1);
            (name, oid)
        })
        .collect())
}

/// Get primary key column names for a table, in ordinal order.
pub async fn get_table_pk_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT a.attname \
             FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             JOIN pg_class c ON c.oid = i.indrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary \
             ORDER BY array_position(i.indkey, a.attnum)",
            &[&schema, &table],
        )
        .await
        .map_err(|e| CdcError::Snapshot(format!("get PK columns for {schema}.{table}: {e}")))?;

    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Get the OID for a table.
async fn get_table_oid(client: &Client, schema: &str, table: &str) -> Result<u32> {
    let row = client
        .query_one(
            "SELECT c.oid FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await
        .map_err(|e| CdcError::Snapshot(format!("get OID for {schema}.{table}: {e}")))?;

    let oid: u32 = row.get(0);
    Ok(oid)
}

/// Quote a SQL identifier to prevent injection.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Unescape COPY text format special characters.
/// In COPY text format: \N=NULL, \t=tab, \n=newline, \\=backslash.
fn unescape_copy_text(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some('\\') => result.push('\\'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_name_with_schema() {
        assert_eq!(parse_table_name("public.users"), ("public", "users"));
        assert_eq!(parse_table_name("myschema.orders"), ("myschema", "orders"));
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        assert_eq!(parse_table_name("users"), ("public", "users"));
    }

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn test_quote_ident_with_quotes() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn test_unescape_copy_text_plain() {
        assert_eq!(unescape_copy_text("hello"), "hello");
    }

    #[test]
    fn test_unescape_copy_text_newline() {
        assert_eq!(unescape_copy_text("line1\\nline2"), "line1\nline2");
    }

    #[test]
    fn test_unescape_copy_text_tab() {
        assert_eq!(unescape_copy_text("col1\\tcol2"), "col1\tcol2");
    }

    #[test]
    fn test_unescape_copy_text_backslash() {
        assert_eq!(unescape_copy_text("path\\\\file"), "path\\file");
    }

    #[test]
    fn test_unescape_copy_text_carriage_return() {
        assert_eq!(unescape_copy_text("a\\rb"), "a\rb");
    }

    #[test]
    fn test_unescape_copy_text_unknown_escape() {
        assert_eq!(unescape_copy_text("a\\xb"), "a\\xb");
    }

    #[test]
    fn test_unescape_copy_text_trailing_backslash() {
        assert_eq!(unescape_copy_text("trail\\"), "trail\\");
    }

    #[test]
    fn test_unescape_copy_text_empty() {
        assert_eq!(unescape_copy_text(""), "");
    }

    #[test]
    fn test_parse_copy_line_basic() {
        let table_id = TableId {
            schema: "public".into(),
            name: "users".into(),
            oid: 1,
        };
        // Type OID 25 = text
        let columns = vec![
            ("id".to_string(), 23u32),   // int4
            ("name".to_string(), 25u32), // text
        ];
        let event = parse_copy_line(
            "42\tAlice",
            &columns,
            &table_id,
            Lsn(100),
            1000,
            &["id".into()],
        );
        assert_eq!(event.op, ChangeOp::Snapshot);
        assert!(event.old.is_none());
        assert_eq!(event.lsn, Lsn(100));
        assert_eq!(event.xid, 0);
        assert_eq!(event.timestamp_us, 1000);
        assert_eq!(event.primary_key_columns, vec!["id"]);
        let row = event.new.unwrap();
        assert_eq!(row.get("id"), Some(&ColumnValue::Int(42)));
        assert_eq!(row.get("name"), Some(&ColumnValue::Text("Alice".into())));
    }

    #[test]
    fn test_parse_copy_line_null_value() {
        let table_id = TableId {
            schema: "public".into(),
            name: "t".into(),
            oid: 1,
        };
        let columns = vec![("id".to_string(), 23u32), ("email".to_string(), 25u32)];
        let event = parse_copy_line("1\t\\N", &columns, &table_id, Lsn(0), 0, &[]);
        let row = event.new.unwrap();
        assert_eq!(row.get("id"), Some(&ColumnValue::Int(1)));
        assert_eq!(row.get("email"), Some(&ColumnValue::Null));
    }

    #[test]
    fn test_parse_copy_line_with_escaped_content() {
        let table_id = TableId {
            schema: "public".into(),
            name: "t".into(),
            oid: 1,
        };
        let columns = vec![("bio".to_string(), 25u32)]; // text
                                                        // COPY text with escaped newline
        let event = parse_copy_line("hello\\nworld", &columns, &table_id, Lsn(0), 0, &[]);
        let row = event.new.unwrap();
        assert_eq!(
            row.get("bio"),
            Some(&ColumnValue::Text("hello\nworld".into()))
        );
    }

    #[test]
    fn test_snapshot_row_with_escaped_content() {
        // In COPY text format, literal tabs delimit columns.
        // Escaped sequences like \n, \t, \\ appear as literal backslash+char.
        // Test: a single-column row with escaped sequences (no literal tabs → one field)
        let line = "hello\\nworld\\ttab\\\\slash";
        let values: Vec<&str> = line.split('\t').collect();
        assert_eq!(values.len(), 1); // no literal tabs in this string

        let unescaped = unescape_copy_text(values[0]);
        assert_eq!(unescaped, "hello\nworld\ttab\\slash");
    }
}
