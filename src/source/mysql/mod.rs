pub mod binlog_stream;
pub mod event_converter;
pub mod snapshot;

use mysql_async::{BinlogStreamRequest, Opts, Pool};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::MySqlSourceConfig;
use crate::error::{CdcError, Result};
use crate::offset::OffsetStore;
use crate::source::Source;
use crate::source::SourceEvent;

use self::binlog_stream::BinlogStreamRunner;
use self::event_converter::{parse_mysql_offset, EventConverter};

/// MySQL CDC source implementing the Source trait.
///
/// Orchestrates: check offset → snapshot if needed → stream binlog events.
/// Uses `mysql_async` for both snapshot (regular queries) and binlog streaming.
pub struct MySqlSource<O: OffsetStore> {
    config: MySqlSourceConfig,
    offset_store: O,
}

impl<O: OffsetStore> MySqlSource<O> {
    pub fn new(config: MySqlSourceConfig, offset_store: O) -> Self {
        Self {
            config,
            offset_store,
        }
    }

    /// Build a mysql_async connection pool from config.
    fn build_pool(&self) -> Pool {
        let url = connection_url(&self.config);
        let opts =
            Opts::from_url(&url).unwrap_or_else(|e| panic!("invalid MySQL connection URL: {e}"));
        Pool::new(opts)
    }
}

/// Build a MySQL connection URL from config.
fn connection_url(config: &MySqlSourceConfig) -> String {
    if let Some(ref password) = config.password {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            config.user, password, config.host, config.port, config.database
        )
    } else {
        format!(
            "mysql://{}@{}:{}/{}",
            config.user, config.host, config.port, config.database
        )
    }
}

impl<O: OffsetStore + Sync> Source for MySqlSource<O> {
    async fn start(
        &mut self,
        sender: mpsc::Sender<SourceEvent>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let pool = self.build_pool();

        // Phase 1: Determine start position (snapshot or resume)
        let saved_offset = self.offset_store.load().await?;

        let (binlog_file, binlog_pos, column_names, boolean_columns, pk_columns, column_types) =
            match saved_offset {
                Some(offset_str) => {
                    let (file, pos) = parse_mysql_offset(&offset_str)?;
                    tracing::info!(file, pos, "resuming from saved offset, skipping snapshot");

                    // Load column names, boolean column info, primary keys, and column
                    // types from information_schema so binlog events have real column
                    // names, correct boolean typing, PK metadata, and type-aware Bytes
                    // parsing.
                    let mut conn = pool.get_conn().await.map_err(|e| {
                        CdcError::Mysql(format!(
                            "failed to get connection for column metadata: {e}"
                        ))
                    })?;
                    let mut column_names = std::collections::HashMap::new();
                    let mut boolean_columns = std::collections::HashMap::new();
                    let mut pk_columns = std::collections::HashMap::new();
                    let mut column_types = std::collections::HashMap::new();
                    for table in &self.config.tables {
                        let (schema, name) = snapshot::parse_table_name(table);
                        let cols = snapshot::get_table_columns(&mut conn, schema, name).await?;
                        let bools = snapshot::get_boolean_columns(&mut conn, schema, name).await?;
                        let pks = snapshot::get_table_pk_columns(&mut conn, schema, name).await?;
                        let types = snapshot::get_column_types(&mut conn, schema, name).await?;
                        let key = (schema.to_string(), name.to_string());
                        column_names.insert(key.clone(), cols);
                        boolean_columns.insert(key.clone(), bools);
                        pk_columns.insert(key.clone(), pks);
                        column_types.insert(key, types);
                    }
                    drop(conn);

                    (
                        file,
                        pos,
                        column_names,
                        boolean_columns,
                        pk_columns,
                        column_types,
                    )
                }
                None => {
                    tracing::info!("no saved offset, performing initial snapshot");
                    let result =
                        snapshot::perform_snapshot(&pool, &self.config.tables, &sender).await?;
                    (
                        result.binlog_filename,
                        result.binlog_position,
                        result.column_names,
                        result.boolean_columns,
                        result.pk_columns,
                        result.column_types,
                    )
                }
            };

        if shutdown.is_cancelled() {
            drop(pool);
            return Ok(());
        }

        // Phase 2: Stream binlog events
        tracing::info!(binlog_file, binlog_pos, "starting binlog streaming");

        let conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::Mysql(format!("failed to get connection for binlog: {e}")))?;

        let request = BinlogStreamRequest::new(self.config.server_id)
            .with_filename(binlog_file.as_bytes())
            .with_pos(binlog_pos);

        let mut stream = conn
            .get_binlog_stream(request)
            .await
            .map_err(|e| CdcError::Mysql(format!("failed to start binlog stream: {e}")))?;

        let converter = EventConverter::new(
            binlog_file,
            column_names,
            boolean_columns,
            pk_columns,
            column_types,
        );
        let mut runner = BinlogStreamRunner::new(converter, pool.clone());
        runner.run(&mut stream, &sender, &shutdown).await?;

        // Drop the stream and pool instead of calling pool.disconnect().
        // The binlog connection is in a special replication state that
        // mysql_async cannot close cleanly, causing disconnect() to hang
        // indefinitely on shutdown.
        drop(stream);
        drop(pool);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_url_with_password() {
        let config = MySqlSourceConfig {
            host: "db.example.com".into(),
            port: 3307,
            user: "cdc_user".into(),
            password: Some("s3cret".into()),
            database: "mydb".into(),
            server_id: 1000,
            tables: vec![],
        };
        assert_eq!(
            connection_url(&config),
            "mysql://cdc_user:s3cret@db.example.com:3307/mydb"
        );
    }

    #[test]
    fn test_connection_url_without_password() {
        let config = MySqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "root".into(),
            password: None,
            database: "testdb".into(),
            server_id: 1000,
            tables: vec![],
        };
        assert_eq!(
            connection_url(&config),
            "mysql://root@localhost:3306/testdb"
        );
    }
}
