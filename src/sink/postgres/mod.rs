mod schema;

use std::collections::{HashMap, HashSet};

use tokio_postgres::{types::ToSql, types::Type, Client, NoTls};

use crate::config::{PostgresSinkConfig, SinkMode, SourceConnectionConfig};
use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp, ColumnValue};
use crate::schema::SourceDialect;
use crate::sink::Sink;

use schema::{
    add_columns, build_cdc_insert, canonical_to_pg_ddl, create_cdc_table,
    create_replication_table, drop_columns, fetch_target_columns_with_types, fetch_target_pk,
    table_exists,
};

use crate::schema::value::column_value_to_string;

/// Cached information about a target table.
struct TableInfo {
    /// Fully qualified target table name: "{schema}"."{table}"
    target_fqn: String,
    /// Ordered column names (replication mode only).
    columns: Vec<String>,
    /// Primary key column names (replication mode only).
    pk_columns: Vec<String>,
    /// Column name → SQL type mapping for generating typed parameter casts in DML.
    column_types: HashMap<String, String>,
}

/// A sink that writes CDC events to a PostgreSQL database.
///
/// Supports two modes:
/// - **CDC mode**: Append-only log with CDC metadata + typed source columns.
/// - **Replication mode**: Live replica with UPSERT/DELETE on individual TEXT columns.
pub struct PostgresSink {
    client: Client,
    config: PostgresSinkConfig,
    mode: SinkMode,
    /// Source DB connection config for schema inference (derived from top-level source config).
    source_connection: Option<SourceConnectionConfig>,
    /// Cached table metadata: (source_schema, source_table) → TableInfo
    tables: HashMap<(String, String), TableInfo>,
}

impl PostgresSink {
    pub async fn new(
        config: PostgresSinkConfig,
        mode: SinkMode,
        source_connection: Option<SourceConnectionConfig>,
    ) -> Result<Self> {
        let conn_str = connection_string(&config);
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres sink connection error: {e}");
            }
        });

        Ok(Self {
            client,
            config,
            mode,
            source_connection,
            tables: HashMap::new(),
        })
    }

    /// Ensure the target table is ready.
    async fn ensure_table(
        &mut self,
        source_schema: &str,
        source_table: &str,
    ) -> Result<()> {
        let key = (source_schema.to_string(), source_table.to_string());
        if self.tables.contains_key(&key) {
            return Ok(());
        }

        let target_table = format!("{}{}", self.config.table_prefix, source_table);
        // Clone to avoid borrowing self.config while we need &mut self later.
        let target_schema = self.config.schema.clone();
        let target_fqn = format!("\"{}\".\"{}\"", target_schema, target_table);

        match self.mode {
            SinkMode::Cdc => {
                // CDC mode uses flattened typed columns: 7 _cdc_* metadata + N source + N _old_
                let exists =
                    table_exists(&self.client, &target_schema, &target_table).await?;

                if exists {
                    let typed_columns =
                        fetch_target_columns_with_types(&self.client, &target_schema, &target_table)
                            .await?;
                    let source_cols: Vec<String> = typed_columns
                        .iter()
                        .filter(|(name, _)| !name.starts_with("_cdc_") && !name.starts_with("_old_"))
                        .map(|(name, _)| name.clone())
                        .collect();
                    let column_types: HashMap<String, String> = typed_columns
                        .iter()
                        .filter(|(name, _)| !name.starts_with("_cdc_") && !name.starts_with("_old_"))
                        .cloned()
                        .collect();

                    self.tables.insert(
                        key,
                        TableInfo {
                            target_fqn,
                            columns: source_cols,
                            pk_columns: Vec::new(),
                            column_types,
                        },
                    );
                } else {
                    let source_conn = self.source_connection.as_ref().ok_or_else(|| {
                        CdcError::Config(
                            "postgres sink CDC mode requires source_connection for auto-creating tables".into(),
                        )
                    })?;
                    let dialect = SourceDialect::from(source_conn);
                    let columns = crate::schema::discovery::fetch_columns(
                        source_conn,
                        source_schema,
                        source_table,
                    )
                    .await?;

                    let typed_columns: Vec<(String, String)> = columns
                        .iter()
                        .map(|col| {
                            let sql_type = match dialect {
                                SourceDialect::Postgres => col.source_type.clone(),
                                SourceDialect::Mysql => canonical_to_pg_ddl(&col.canonical_type),
                            };
                            (col.name.clone(), sql_type)
                        })
                        .collect();

                    create_cdc_table(
                        &self.client,
                        &target_schema,
                        &target_table,
                        &typed_columns,
                    )
                    .await?;

                    let col_names: Vec<String> =
                        typed_columns.iter().map(|(name, _)| name.clone()).collect();
                    let column_types: HashMap<String, String> =
                        typed_columns.into_iter().collect();

                    self.tables.insert(
                        key,
                        TableInfo {
                            target_fqn,
                            columns: col_names,
                            pk_columns: Vec::new(),
                            column_types,
                        },
                    );
                }
            }
            SinkMode::Replication => {
                let exists =
                    table_exists(&self.client, &target_schema, &target_table).await?;

                if exists {
                    let typed_columns =
                        fetch_target_columns_with_types(&self.client, &target_schema, &target_table)
                            .await?;
                    let pk_columns =
                        fetch_target_pk(&self.client, &target_schema, &target_table).await?;
                    if pk_columns.is_empty() {
                        return Err(CdcError::Config(format!(
                            "replication mode requires a primary key on target table {}.{}",
                            target_schema, target_table
                        )));
                    }
                    let columns: Vec<String> =
                        typed_columns.iter().map(|(name, _)| name.clone()).collect();
                    let column_types: HashMap<String, String> =
                        typed_columns.into_iter().collect();
                    self.tables.insert(
                        key,
                        TableInfo {
                            target_fqn,
                            columns,
                            pk_columns,
                            column_types,
                        },
                    );
                } else {
                    let source_conn = self.source_connection.as_ref().ok_or_else(|| {
                        CdcError::Config(
                            "postgres sink replication mode requires source_connection for auto-creating tables".into(),
                        )
                    })?;
                    let dialect = SourceDialect::from(source_conn);
                    let columns = crate::schema::discovery::fetch_columns(
                        source_conn,
                        source_schema,
                        source_table,
                    )
                    .await?;
                    let pk_columns = crate::schema::discovery::fetch_primary_keys(
                        source_conn,
                        source_schema,
                        source_table,
                    )
                    .await?;

                    if columns.is_empty() {
                        return Err(CdcError::Config(format!(
                            "source table {}.{} has no columns",
                            source_schema, source_table
                        )));
                    }
                    if pk_columns.is_empty() {
                        return Err(CdcError::Config(format!(
                            "source table {}.{} has no primary key",
                            source_schema, source_table
                        )));
                    }

                    // For PG-to-PG: use source_type directly (exact DDL from format_type).
                    // For MySQL-to-PG: convert via canonical type.
                    let typed_columns: Vec<(String, String)> = columns
                        .iter()
                        .map(|col| {
                            let sql_type = match dialect {
                                SourceDialect::Postgres => col.source_type.clone(),
                                SourceDialect::Mysql => canonical_to_pg_ddl(&col.canonical_type),
                            };
                            (col.name.clone(), sql_type)
                        })
                        .collect();

                    create_replication_table(
                        &self.client,
                        &target_schema,
                        &target_table,
                        &typed_columns,
                        &pk_columns,
                    )
                    .await?;

                    let col_names: Vec<String> =
                        typed_columns.iter().map(|(name, _)| name.clone()).collect();
                    let column_types: HashMap<String, String> =
                        typed_columns.into_iter().collect();

                    self.tables.insert(
                        key,
                        TableInfo {
                            target_fqn,
                            columns: col_names,
                            pk_columns,
                            column_types,
                        },
                    );
                }
            }
        }

        Ok(())
    }

    /// Write a batch of events in CDC mode.
    ///
    /// Two-phase approach (same as replication mode):
    ///   Phase 1: Detect new columns and ALTER TABLE to add both `col` and `_old_col`.
    ///   Phase 2: INSERT individual typed parameters per event.
    async fn write_batch_cdc(&mut self, events: &[CdcEvent]) -> Result<()> {
        let mut groups: HashMap<(String, String), Vec<&CdcEvent>> = HashMap::new();
        for event in events {
            let key = (event.table.schema.clone(), event.table.name.clone());
            groups.entry(key).or_default().push(event);
        }

        // Phase 1: detect and add new columns (DDL, outside transaction).
        for ((schema, table), group) in &groups {
            let key = (schema.clone(), table.clone());
            let info = self.tables.get(&key).ok_or_else(|| {
                CdcError::Config(format!("table {}.{} not prepared", schema, table))
            })?;

            let new_cols =
                crate::schema::evolution::detect_new_columns(&info.columns, group);

            if !new_cols.is_empty() {
                let source_conn = self.source_connection.as_ref().ok_or_else(|| {
                    CdcError::Config(
                        "postgres sink CDC mode requires source_connection for schema evolution".into(),
                    )
                })?;
                let dialect = SourceDialect::from(source_conn);
                let source_columns = crate::schema::discovery::fetch_column_subset(
                    source_conn,
                    schema,
                    table,
                    &new_cols,
                )
                .await?;

                // Build paired columns: both source col and _old_ col
                let mut typed_cols: Vec<(String, String)> = Vec::new();
                for col in &source_columns {
                    let sql_type = match dialect {
                        SourceDialect::Postgres => col.source_type.clone(),
                        SourceDialect::Mysql => canonical_to_pg_ddl(&col.canonical_type),
                    };
                    typed_cols.push((col.name.clone(), sql_type.clone()));
                    typed_cols.push((format!("_old_{}", col.name), sql_type));
                }

                let target_schema = &self.config.schema;
                let target_table =
                    format!("{}{}", self.config.table_prefix, table);
                add_columns(&self.client, target_schema, &target_table, &typed_cols)
                    .await?;

                let info_mut = self.tables.get_mut(&key).unwrap();
                for col in &source_columns {
                    let sql_type = match dialect {
                        SourceDialect::Postgres => col.source_type.clone(),
                        SourceDialect::Mysql => canonical_to_pg_ddl(&col.canonical_type),
                    };
                    info_mut.columns.push(col.name.clone());
                    info_mut.column_types.insert(col.name.clone(), sql_type);
                }
            }

            // Note: no drop-column detection in CDC mode. The target table is
            // append-only — historical rows still reference dropped columns, so they
            // must stay in the schema. New events simply write NULL for removed columns.
        }

        let tx = self.client.transaction().await?;

        for ((schema, table), group) in &groups {
            let key = (schema.clone(), table.clone());
            let info = self.tables.get(&key).ok_or_else(|| {
                CdcError::Config(format!("table {}.{} not prepared", schema, table))
            })?;

            let sql = build_cdc_insert(&info.target_fqn, &info.columns, &info.column_types);
            let num_params = 7 + info.columns.len() * 2;
            let param_types: Vec<Type> = vec![Type::TEXT; num_params];
            let stmt = tx.prepare_typed(&sql, &param_types).await?;

            for event in group {
                let mut values: Vec<Option<String>> = Vec::with_capacity(num_params);

                // 7 metadata values
                values.push(Some(event.op.as_op_code().into()));
                values.push(Some((event.lsn.0 as i64).to_string()));
                values.push(Some(event.timestamp_us.to_string()));
                values.push(Some((event.op == ChangeOp::Snapshot).to_string()));
                values.push(Some(event.table.schema.clone()));
                values.push(Some(event.table.name.clone()));
                values.push(Some(
                    serde_json::to_string(&event.primary_key_columns)
                        .unwrap_or_else(|_| "[]".into()),
                ));

                for col in &info.columns {
                    let val = event
                        .new
                        .as_ref()
                        .and_then(|r| r.get(col));
                    values.push(column_value_to_string(val));
                }

                for col in &info.columns {
                    let val = event
                        .old
                        .as_ref()
                        .and_then(|r| r.get(col));
                    values.push(column_value_to_string(val));
                }

                let params: Vec<&(dyn ToSql + Sync)> =
                    values.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
                tx.execute(&stmt, &params).await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    /// Write a batch of events in replication mode.
    ///
    /// Two-phase approach:
    ///   Phase 1 (before transaction): Detect new columns in incoming events
    ///     and ALTER TABLE to add them. DDL auto-commits in PostgreSQL so it
    ///     cannot be inside the DML transaction.
    ///   Phase 2 (inside transaction): Perform upserts/deletes using the
    ///     now-updated column cache.
    async fn write_batch_replication(&mut self, events: &[CdcEvent]) -> Result<()> {
        let mut groups: HashMap<(String, String), Vec<&CdcEvent>> = HashMap::new();
        for event in events {
            let key = (event.table.schema.clone(), event.table.name.clone());
            groups.entry(key).or_default().push(event);
        }

        // Phase 1: detect and add new columns with proper types (DDL, outside transaction).
        for ((schema, table), group) in &groups {
            let key = (schema.clone(), table.clone());
            let info = self.tables.get(&key).ok_or_else(|| {
                CdcError::Config(format!("table {}.{} not prepared", schema, table))
            })?;

            let new_cols =
                crate::schema::evolution::detect_new_columns(&info.columns, group);

            if !new_cols.is_empty() {
                // Look up the actual SQL types from the source database.
                let source_conn = self.source_connection.as_ref().ok_or_else(|| {
                    CdcError::Config(
                        "postgres sink replication mode requires source_connection for schema evolution".into(),
                    )
                })?;
                let dialect = SourceDialect::from(source_conn);
                let source_columns = crate::schema::discovery::fetch_column_subset(
                    source_conn,
                    schema,
                    table,
                    &new_cols,
                )
                .await?;

                let typed_cols: Vec<(String, String)> = source_columns
                    .iter()
                    .map(|col| {
                        let sql_type = match dialect {
                            SourceDialect::Postgres => col.source_type.clone(),
                            SourceDialect::Mysql => canonical_to_pg_ddl(&col.canonical_type),
                        };
                        (col.name.clone(), sql_type)
                    })
                    .collect();

                let target_schema = &self.config.schema;
                let target_table =
                    format!("{}{}", self.config.table_prefix, table);
                add_columns(&self.client, target_schema, &target_table, &typed_cols)
                    .await?;

                let info_mut = self.tables.get_mut(&key).unwrap();
                for (name, typ) in typed_cols {
                    info_mut.columns.push(name.clone());
                    info_mut.column_types.insert(name, typ);
                }
            }

            // Detect dropped columns: only use Insert/Update/Snapshot events
            // (which carry full column sets). Delete events with REPLICA IDENTITY
            // DEFAULT only carry PK columns and would cause false positives.
            let mut event_columns: HashSet<String> = HashSet::new();
            let mut has_full_row_events = false;
            for event in group {
                match event.op {
                    ChangeOp::Insert | ChangeOp::Update | ChangeOp::Snapshot => {
                        has_full_row_events = true;
                        if let Some(row) = event.new.as_ref() {
                            for col_name in row.keys() {
                                event_columns.insert(col_name.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }

            tracing::debug!(
                table = %format!("{}.{}", schema, table),
                ?event_columns,
                has_full_row_events,
                "drop detection: columns collected from events",
            );

            if has_full_row_events {
                let info = self.tables.get(&key).unwrap();
                let missing_from_events: Vec<String> = info
                    .columns
                    .iter()
                    .filter(|col| !event_columns.contains(col.as_str()))
                    .cloned()
                    .collect();

                tracing::debug!(
                    table = %format!("{}.{}", schema, table),
                    cached_columns = ?info.columns,
                    ?missing_from_events,
                    "drop detection: compared event columns against cached columns",
                );

                if !missing_from_events.is_empty() {
                    // Confirm the drop by querying the source database
                    if let Some(source_conn) = self.source_connection.as_ref() {
                        let source_cols = crate::schema::discovery::fetch_columns(
                            source_conn,
                            schema,
                            table,
                        )
                        .await?;
                        let source_col_names: Vec<String> =
                            source_cols.iter().map(|c| c.name.clone()).collect();

                        tracing::debug!(
                            table = %format!("{}.{}", schema, table),
                            ?source_col_names,
                            "drop detection: fetched current source DB columns",
                        );

                        let dropped = crate::schema::evolution::detect_dropped_columns(
                            &missing_from_events,
                            &source_col_names,
                        );

                        tracing::debug!(
                            table = %format!("{}.{}", schema, table),
                            ?dropped,
                            "drop detection: confirmed dropped columns",
                        );

                        if !dropped.is_empty() {
                            let target_schema = &self.config.schema;
                            let target_table =
                                format!("{}{}", self.config.table_prefix, table);
                            drop_columns(
                                &self.client,
                                target_schema,
                                &target_table,
                                &dropped,
                            )
                            .await?;
                            tracing::info!(
                                table = %format!("{}.{}", schema, table),
                                ?dropped,
                                "dropped columns from target table"
                            );

                            let info_mut = self.tables.get_mut(&key).unwrap();
                            let drop_set: HashSet<&str> =
                                dropped.iter().map(|s| s.as_str()).collect();
                            info_mut.columns.retain(|c| !drop_set.contains(c.as_str()));
                            for col in &dropped {
                                info_mut.column_types.remove(col);
                            }
                        }
                    }
                }
            }
        }

        let tx = self.client.transaction().await?;

        for ((schema, table), group) in &groups {
            let key = (schema.clone(), table.clone());
            let info = self.tables.get(&key).ok_or_else(|| {
                CdcError::Config(format!("table {}.{} not prepared", schema, table))
            })?;

            for event in group {
                match event.op {
                    ChangeOp::Insert | ChangeOp::Update | ChangeOp::Snapshot => {
                        let row = event.new.as_ref().ok_or_else(|| {
                            CdcError::Protocol(format!(
                                "{:?} event for {}.{} has no new row",
                                event.op, schema, table
                            ))
                        })?;
                        upsert_row(&tx, info, row).await?;
                    }
                    ChangeOp::Delete => {
                        let row = event.old.as_ref().ok_or_else(|| {
                            CdcError::Protocol(format!(
                                "delete event for {}.{} has no old row",
                                schema, table
                            ))
                        })?;
                        delete_row(&tx, info, row).await?;
                    }
                    ChangeOp::Truncate => {
                        let sql = format!("TRUNCATE {}", info.target_fqn);
                        tx.execute(&sql, &[]).await?;
                    }
                }
            }
        }

        tx.commit().await?;
        Ok(())
    }
}

impl Sink for PostgresSink {
    async fn write_batch(&mut self, events: &[CdcEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let table_keys: Vec<(String, String)> = events
            .iter()
            .map(|e| (e.table.schema.clone(), e.table.name.clone()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        for (schema, table) in &table_keys {
            self.ensure_table(schema, table).await?;
        }

        match self.mode {
            SinkMode::Cdc => self.write_batch_cdc(events).await,
            SinkMode::Replication => self.write_batch_replication(events).await,
        }
    }

    async fn flush(&mut self) -> Result<()> {
        // tokio-postgres executes immediately — no buffering to flush.
        Ok(())
    }
}

/// Build the SQL string for an UPSERT statement.
///
/// Parameters are cast via `$N::type` so tokio-postgres sends them as text
/// while PostgreSQL handles the text→typed conversion server-side.
fn build_upsert_sql(
    target_fqn: &str,
    columns: &[String],
    pk_columns: &[String],
    column_types: &HashMap<String, String>,
) -> String {
    let col_list: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c)).collect();
    let placeholders: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let typ = column_types.get(c).map(|t| t.as_str()).unwrap_or("text");
            format!("${}::{}", i + 1, typ)
        })
        .collect();
    let update_sets: Vec<String> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !pk_columns.contains(c))
        .map(|(i, c)| {
            let typ = column_types.get(c).map(|t| t.as_str()).unwrap_or("text");
            format!("\"{}\" = ${}::{}", c, i + 1, typ)
        })
        .collect();
    let pk_list: Vec<String> = pk_columns.iter().map(|c| format!("\"{}\"", c)).collect();

    if update_sets.is_empty() {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
            target_fqn,
            col_list.join(", "),
            placeholders.join(", "),
            pk_list.join(", ")
        )
    } else {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            target_fqn,
            col_list.join(", "),
            placeholders.join(", "),
            pk_list.join(", "),
            update_sets.join(", ")
        )
    }
}

/// Build the SQL string for a DELETE by PK statement.
///
/// Parameters are cast via `$N::type` so tokio-postgres sends them as text
/// while PostgreSQL handles the text→typed conversion server-side.
fn build_delete_sql(
    target_fqn: &str,
    pk_columns: &[String],
    column_types: &HashMap<String, String>,
) -> String {
    let where_clauses: Vec<String> = pk_columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let typ = column_types.get(c).map(|t| t.as_str()).unwrap_or("text");
            format!("\"{}\" = ${}::{}", c, i + 1, typ)
        })
        .collect();
    format!(
        "DELETE FROM {} WHERE {}",
        target_fqn,
        where_clauses.join(" AND ")
    )
}

/// UPSERT a row: INSERT ... ON CONFLICT (pk) DO UPDATE SET ...
///
/// Uses `prepare_typed` with all `Type::TEXT` so tokio-postgres sends params
/// as text strings. The `$N::type` casts in the SQL handle the TEXT→typed
/// conversion server-side.
async fn upsert_row(
    tx: &tokio_postgres::Transaction<'_>,
    info: &TableInfo,
    row: &std::collections::BTreeMap<String, ColumnValue>,
) -> Result<()> {
    let cols = &info.columns;

    let values: Vec<Option<String>> = cols
        .iter()
        .map(|c| crate::schema::value::column_value_to_string(row.get(c)))
        .collect();

    let sql = build_upsert_sql(&info.target_fqn, cols, &info.pk_columns, &info.column_types);

    let param_types: Vec<Type> = vec![Type::TEXT; values.len()];
    let stmt = tx.prepare_typed(&sql, &param_types).await?;
    let params: Vec<&(dyn ToSql + Sync)> =
        values.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
    tx.execute(&stmt, &params).await?;
    Ok(())
}

/// DELETE a row by PK.
///
/// Uses `prepare_typed` with all `Type::TEXT` so tokio-postgres sends params
/// as text strings. The `$N::type` casts in the SQL handle the TEXT→typed
/// conversion server-side.
async fn delete_row(
    tx: &tokio_postgres::Transaction<'_>,
    info: &TableInfo,
    row: &std::collections::BTreeMap<String, ColumnValue>,
) -> Result<()> {
    let pk_cols = &info.pk_columns;

    let pk_values: Vec<Option<String>> = pk_cols
        .iter()
        .map(|c| crate::schema::value::column_value_to_string(row.get(c)))
        .collect();

    let sql = build_delete_sql(&info.target_fqn, pk_cols, &info.column_types);

    let param_types: Vec<Type> = vec![Type::TEXT; pk_values.len()];
    let stmt = tx.prepare_typed(&sql, &param_types).await?;
    let params: Vec<&(dyn ToSql + Sync)> =
        pk_values.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
    tx.execute(&stmt, &params).await?;
    Ok(())
}

/// Build a connection string from config fields.
fn connection_string(config: &PostgresSinkConfig) -> String {
    let mut s = format!(
        "host={} port={} user={} dbname={}",
        config.host, config.port, config.user, config.database
    );
    if let Some(ref pw) = config.password {
        s.push_str(&format!(" password={}", pw));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_string_with_password() {
        let config = PostgresSinkConfig {
            host: "db.example.com".into(),
            port: 5433,
            user: "writer".into(),
            password: Some("secret".into()),
            database: "target".into(),
            schema: "public".into(),
            table_prefix: "".into(),
        };
        let s = connection_string(&config);
        assert_eq!(
            s,
            "host=db.example.com port=5433 user=writer dbname=target password=secret"
        );
    }

    #[test]
    fn test_connection_string_without_password() {
        let config = PostgresSinkConfig {
            host: "localhost".into(),
            port: 5432,
            user: "user".into(),
            password: None,
            database: "mydb".into(),
            schema: "public".into(),
            table_prefix: "".into(),
        };
        let s = connection_string(&config);
        assert_eq!(s, "host=localhost port=5432 user=user dbname=mydb");
    }

    fn make_column_types(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn test_build_upsert_sql() {
        let cols = vec!["id".into(), "name".into(), "email".into()];
        let pk = vec!["id".into()];
        let types = make_column_types(&[("id", "integer"), ("name", "text"), ("email", "character varying(255)")]);
        let sql = build_upsert_sql("\"public\".\"users\"", &cols, &pk, &types);
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"users\" (\"id\", \"name\", \"email\") VALUES ($1::integer, $2::text, $3::character varying(255)) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = $2::text, \"email\" = $3::character varying(255)"
        );
    }

    #[test]
    fn test_build_upsert_sql_all_pk() {
        let cols = vec!["id".into()];
        let pk = vec!["id".into()];
        let types = make_column_types(&[("id", "integer")]);
        let sql = build_upsert_sql("\"public\".\"users\"", &cols, &pk, &types);
        assert!(sql.contains("DO NOTHING"));
    }

    #[test]
    fn test_build_upsert_sql_no_types_falls_back_to_text() {
        let cols = vec!["id".into(), "name".into()];
        let pk = vec!["id".into()];
        let types = HashMap::new();
        let sql = build_upsert_sql("\"public\".\"users\"", &cols, &pk, &types);
        assert!(sql.contains("$1::text"));
        assert!(sql.contains("$2::text"));
    }

    #[test]
    fn test_build_delete_sql() {
        let pk = vec!["tenant_id".into(), "user_id".into()];
        let types = make_column_types(&[("tenant_id", "uuid"), ("user_id", "bigint")]);
        let sql = build_delete_sql("\"public\".\"users\"", &pk, &types);
        assert_eq!(
            sql,
            "DELETE FROM \"public\".\"users\" WHERE \"tenant_id\" = $1::uuid AND \"user_id\" = $2::bigint"
        );
    }

}
