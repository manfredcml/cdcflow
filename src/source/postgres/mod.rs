pub mod event_converter;
pub mod protocol;
pub mod publication;
pub mod relation_cache;
pub mod replication_client;
pub mod replication_stream;
pub mod slot;
pub mod snapshot;

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::PostgresSourceConfig;
use crate::error::{CdcError, Result};
use crate::event::Lsn;
use crate::offset::OffsetStore;
use crate::source::{Source, SourceEvent};

use self::replication_client::ReplicationClient;
use self::replication_stream::ReplicationStream;

/// PostgreSQL CDC source implementing the Source trait.
///
/// Orchestrates: check offset → snapshot if needed → stream changes.
/// Uses two connections:
/// - Regular `tokio-postgres` for snapshot (COPY) and slot management
/// - Custom `ReplicationClient` for WAL streaming
pub struct PostgresSource<O: OffsetStore> {
    config: PostgresSourceConfig,
    offset_store: O,
}

impl<O: OffsetStore> PostgresSource<O> {
    pub fn new(config: PostgresSourceConfig, offset_store: O) -> Self {
        Self {
            config,
            offset_store,
        }
    }

    /// Connect the regular tokio-postgres client for snapshot/slot operations.
    async fn connect_regular(&self) -> Result<tokio_postgres::Client> {
        let mut pg_config = tokio_postgres::Config::new();
        pg_config
            .host(&self.config.host)
            .port(self.config.port)
            .user(&self.config.user)
            .dbname(&self.config.database);

        if let Some(ref pw) = self.config.password {
            pg_config.password(pw);
        }

        let (client, connection) = pg_config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|e| {
                let mut msg = format!("regular connection failed: {e}");
                let mut source = std::error::Error::source(&e);
                while let Some(cause) = source {
                    msg.push_str(&format!(": {cause}"));
                    source = std::error::Error::source(cause);
                }
                CdcError::Protocol(msg)
            })?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("regular connection error: {e}");
            }
        });

        Ok(client)
    }

    /// Connect the replication client for WAL streaming.
    async fn connect_replication(&self) -> Result<ReplicationClient> {
        ReplicationClient::connect(
            &self.config.host,
            self.config.port,
            &self.config.user,
            self.config.password.as_deref(),
            &self.config.database,
        )
        .await
    }

    /// Perform the initial snapshot if no offset exists.
    async fn maybe_snapshot(
        &self,
        client: &tokio_postgres::Client,
        sender: &mpsc::Sender<SourceEvent>,
    ) -> Result<Lsn> {
        let saved_offset = self.offset_store.load().await?;

        if let Some(offset_str) = saved_offset {
            let lsn: Lsn = offset_str
                .parse()
                .map_err(|e: CdcError| CdcError::Parse(format!("invalid saved offset: {e}")))?;
            tracing::info!(%lsn, "resuming from saved offset, skipping snapshot");
            return Ok(lsn);
        }

        tracing::info!("no saved offset, performing initial snapshot");

        // If the slot already exists but we have no offset, a previous run must have
        // crashed after creating the slot but before completing the snapshot.
        // Drop the stale slot so we can start fresh.
        if slot::slot_exists(client, &self.config.slot_name).await? {
            tracing::warn!(
                slot_name = self.config.slot_name,
                "slot exists but no offset saved, dropping stale slot from previous crash"
            );
            slot::drop_slot(client, &self.config.slot_name).await?;
        }

        let slot_lsn = slot::create_slot(client, &self.config.slot_name).await?;

        let tables = if self.config.tables.is_empty() {
            // Get tables from the publication
            publication::get_publication_tables(client, &self.config.publication_name).await?
        } else {
            self.config.tables.clone()
        };

        snapshot::perform_snapshot(client, &tables, slot_lsn, sender).await?;

        Ok(slot_lsn)
    }
}

impl<O: OffsetStore + Sync> Source for PostgresSource<O> {
    async fn start(
        &mut self,
        sender: mpsc::Sender<SourceEvent>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let regular_client = self.connect_regular().await?;

        publication::ensure_publication(
            &regular_client,
            &self.config.publication_name,
            self.config.create_publication,
            &self.config.tables,
        )
        .await?;

        // Phase 1: Snapshot (if needed)
        let start_lsn = self.maybe_snapshot(&regular_client, &sender).await?;

        if shutdown.is_cancelled() {
            return Ok(());
        }

        // Ensure slot exists (it was created during snapshot, or should already exist for resume)
        if !slot::slot_exists(&regular_client, &self.config.slot_name).await? {
            slot::create_slot(&regular_client, &self.config.slot_name).await?;
        }

        // Build primary key cache from pg_index (WAL flags are unreliable with REPLICA IDENTITY FULL)
        let tables = if self.config.tables.is_empty() {
            publication::get_publication_tables(&regular_client, &self.config.publication_name)
                .await?
        } else {
            self.config.tables.clone()
        };

        let mut pk_cache = HashMap::new();
        for table_name in &tables {
            let (schema, name) = snapshot::parse_table_name(table_name);
            match snapshot::get_table_pk_columns(&regular_client, schema, name).await {
                Ok(pk_cols) => {
                    pk_cache.insert((schema.to_string(), name.to_string()), pk_cols);
                }
                Err(e) => {
                    tracing::warn!(table = table_name, "failed to fetch PK columns: {e}");
                }
            }
        }
        tracing::info!(
            tables = pk_cache.len(),
            "populated primary key cache from pg_index"
        );

        // Phase 2: Stream changes
        tracing::info!(%start_lsn, "starting WAL streaming");
        let repl_client = self.connect_replication().await?;
        let mut stream = ReplicationStream::new(repl_client, pk_cache);

        stream
            .start_replication(
                &self.config.slot_name,
                start_lsn,
                &self.config.publication_name,
            )
            .await?;

        stream.run(&sender, &shutdown).await?;

        Ok(())
    }
}
