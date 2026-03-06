use futures_util::StreamExt;
use mysql_async::binlog::events::EventData;
use mysql_async::{BinlogStream, Pool};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{CdcError, Result};
use crate::source::SourceEvent;

use super::event_converter::EventConverter;
use super::snapshot;

/// Streams MySQL binlog events and converts them to SourceEvents.
///
/// Consumes a `mysql_async::BinlogStream` and emits CDC events through
/// an mpsc channel, mirroring the pattern of PostgreSQL's ReplicationStream.
pub struct BinlogStreamRunner {
    converter: EventConverter,
    /// Pool for refreshing column metadata on schema changes.
    pool: Pool,
}

impl BinlogStreamRunner {
    pub fn new(converter: EventConverter, pool: Pool) -> Self {
        Self { converter, pool }
    }

    /// Run the binlog stream event loop.
    ///
    /// Reads events from the binlog stream, converts them to SourceEvents,
    /// and sends them through the channel. Handles shutdown gracefully.
    pub async fn run(
        &mut self,
        stream: &mut BinlogStream,
        sender: &mpsc::Sender<SourceEvent>,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("binlog stream shutting down");
                    return Ok(());
                }
                event = stream.next() => {
                    match event {
                        None => {
                            tracing::info!("binlog stream ended");
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            return Err(CdcError::Mysql(format!("binlog stream error: {e}")));
                        }
                        Some(Ok(ev)) => {
                            self.process_event(&ev, stream, sender).await?;
                        }
                    }
                }
            }
        }
    }

    /// Process a single binlog event.
    async fn process_event(
        &mut self,
        event: &mysql_async::binlog::events::Event,
        stream: &BinlogStream,
        sender: &mpsc::Sender<SourceEvent>,
    ) -> Result<()> {
        let header = event.header();
        let log_pos = header.log_pos() as u64;
        let timestamp_us = header.timestamp() as i64 * 1_000_000;

        let data = match event.read_data() {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::trace!(
                    event_type = ?header.event_type(),
                    log_pos,
                    "binlog event returned no data (unparseable or unknown type)"
                );
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(
                    event_type = ?header.event_type(),
                    log_pos,
                    "failed to parse binlog event: {e}"
                );
                return Ok(());
            }
        };

        match data {
            EventData::TableMapEvent(_) => {
                // BinlogStream caches these automatically via get_tme()
                // No action needed.
            }

            EventData::RowsEvent(ref rows_data) => {
                let table_id = rows_data.table_id();
                let tme = stream.get_tme(table_id).ok_or_else(|| {
                    CdcError::Mysql(format!("no TableMapEvent cached for table_id {table_id}"))
                })?;

                let schema = tme.database_name().into_owned();
                let table = tme.table_name().into_owned();
                let num_columns = tme.columns_count() as usize;

                // Refresh column name cache when schema changes (e.g. ALTER TABLE ADD COLUMN).
                // Prefer TABLE_MAP optional metadata (race-free) over info_schema query.
                if self
                    .converter
                    .cache_needs_refresh(&schema, &table, num_columns)
                {
                    if let Some(names) = EventConverter::extract_column_names_from_tme(tme) {
                        tracing::info!(
                            schema,
                            table,
                            num_columns,
                            "column count mismatch — refreshing column cache from TABLE_MAP metadata"
                        );
                        self.converter.update_column_cache(&schema, &table, names);
                    } else {
                        self.converter.warn_no_metadata_once(&schema, &table);
                        tracing::info!(
                            schema,
                            table,
                            num_columns,
                            "column count mismatch — refreshing column cache from information_schema"
                        );
                        let mut conn = self.pool.get_conn().await.map_err(|e| {
                            CdcError::Mysql(format!(
                                "failed to get connection for column cache refresh: {e}"
                            ))
                        })?;
                        let fresh_columns =
                            snapshot::get_table_columns(&mut conn, &schema, &table).await?;
                        let fresh_booleans =
                            snapshot::get_boolean_columns(&mut conn, &schema, &table).await?;
                        let fresh_pk =
                            snapshot::get_table_pk_columns(&mut conn, &schema, &table).await?;
                        let fresh_types =
                            snapshot::get_column_types(&mut conn, &schema, &table).await?;
                        self.converter
                            .update_column_cache(&schema, &table, fresh_columns);
                        self.converter
                            .update_boolean_cache(&schema, &table, fresh_booleans);
                        self.converter.update_pk_cache(&schema, &table, fresh_pk);
                        self.converter
                            .update_column_types(&schema, &table, fresh_types);
                    }
                }

                let results = self.converter.convert_rows_event(
                    rows_data,
                    &schema,
                    &table,
                    log_pos,
                    timestamp_us,
                    tme,
                )?;

                for result in results {
                    match result {
                        super::event_converter::ConvertResult::Event(event) => {
                            sender
                                .send(SourceEvent::Change(event))
                                .await
                                .map_err(|e| CdcError::Mysql(format!("send event: {e}")))?;
                        }
                        super::event_converter::ConvertResult::Commit { offset } => {
                            sender
                                .send(SourceEvent::Commit { offset })
                                .await
                                .map_err(|e| CdcError::Mysql(format!("send commit: {e}")))?;
                        }
                        super::event_converter::ConvertResult::Skip => {}
                    }
                }
            }

            EventData::QueryEvent(ref qe) => {
                let query = qe.query().into_owned();
                let schema = qe.schema().into_owned();
                tracing::trace!(
                    schema,
                    query_preview = &query[..query.len().min(200)],
                    log_pos,
                    "received QueryEvent"
                );

                // Check for TRUNCATE
                if let Some((truncate_schema, truncate_table)) =
                    EventConverter::parse_truncate_query(&query, &schema)
                {
                    let pk_columns = self
                        .converter
                        .get_pk_columns(&truncate_schema, &truncate_table);
                    let event = EventConverter::make_truncate_event(
                        &truncate_schema,
                        &truncate_table,
                        log_pos,
                        timestamp_us,
                        pk_columns,
                    );
                    sender
                        .send(SourceEvent::Change(event))
                        .await
                        .map_err(|e| CdcError::Mysql(format!("send truncate: {e}")))?;

                    // TRUNCATE is DDL (auto-commit) — no XidEvent follows,
                    // so emit a Commit to flush the pipeline batch.
                    let offset = self.converter.format_offset(log_pos);
                    sender
                        .send(SourceEvent::Commit { offset })
                        .await
                        .map_err(|e| CdcError::Mysql(format!("send truncate commit: {e}")))?;
                }
                // Other QueryEvents (BEGIN, DDL, etc.) are skipped
            }

            EventData::XidEvent(ref xe) => {
                // Transaction committed — emit commit with current offset
                let offset = self.converter.format_offset(log_pos);
                tracing::debug!(xid = xe.xid, offset, "transaction committed");
                sender
                    .send(SourceEvent::Commit { offset })
                    .await
                    .map_err(|e| CdcError::Mysql(format!("send commit: {e}")))?;
            }

            EventData::RotateEvent(ref re) => {
                if !re.is_fake() {
                    let new_filename = re.name().into_owned();
                    tracing::info!(
                        new_filename,
                        position = re.position(),
                        "binlog file rotated"
                    );
                    self.converter.set_binlog_filename(new_filename);
                }
            }

            ref other => {
                // GtidEvent, FormatDescriptionEvent, HeartbeatEvent, etc. — skip
                tracing::trace!(
                    event_discriminant = ?std::mem::discriminant(other),
                    log_pos,
                    "skipping unhandled binlog event type"
                );
            }
        }

        Ok(())
    }
}
