pub mod bson_mapping;
pub mod change_stream;
pub mod event_converter;
pub mod offset;
pub mod snapshot;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::MongodbSourceConfig;
use crate::error::{CdcError, Result};
use crate::offset::OffsetStore;
use crate::source::Source;

use self::change_stream::{
    is_resume_point_lost, open_change_stream, pump_change_stream, ResumePoint,
};
use self::offset::MongoOffset;
use self::snapshot::perform_snapshot;

/// Fail fast on a configured collection that does not exist — MongoDB answers
/// reads on one with empty results, so a typo would silently replicate nothing.
async fn validate_collections(
    client: &mongodb::Client,
    database: &str,
    collections: &[String],
) -> Result<()> {
    if collections.is_empty() {
        return Ok(());
    }

    let existing = client
        .database(database)
        .list_collection_names()
        .await
        .map_err(|e| CdcError::Mongodb(format!("failed to list collections: {e}")))?;

    let missing: Vec<&str> = collections
        .iter()
        .map(|c| c.as_str())
        .filter(|c| !existing.iter().any(|e| e == c))
        .collect();

    if !missing.is_empty() {
        return Err(CdcError::Mongodb(format!(
            "configured collection(s) not found in database {database}: {}. Available: {}",
            missing.join(", "),
            if existing.is_empty() {
                "(none)".to_string()
            } else {
                existing.join(", ")
            }
        )));
    }

    Ok(())
}

pub(crate) enum SendOutcome {
    Sent,
    /// The pipeline stopped receiving while a shutdown was in flight.
    ShuttingDown,
}

/// Send an event to the pipeline. The receive loop exits as soon as the
/// shutdown token fires, so a closed channel then is a clean stop, not an error.
pub(crate) async fn send_event(
    sender: &mpsc::Sender<crate::source::SourceEvent>,
    event: crate::source::SourceEvent,
    shutdown: &CancellationToken,
) -> Result<SendOutcome> {
    match sender.send(event).await {
        Ok(()) => Ok(SendOutcome::Sent),
        Err(e) => {
            if shutdown.is_cancelled() {
                tracing::info!("pipeline stopped receiving during shutdown");
                Ok(SendOutcome::ShuttingDown)
            } else {
                Err(CdcError::Mongodb(format!("failed to send event: {e}")))
            }
        }
    }
}

/// MongoDB CDC source implementing the Source trait.
///
/// Orchestrates: check offset → snapshot if needed → stream change events.
/// Uses the official `mongodb` crate driver. Requires a replica set deployment.
pub struct MongodbSource<O: OffsetStore> {
    config: MongodbSourceConfig,
    offset_store: O,
}

impl<O: OffsetStore> MongodbSource<O> {
    pub fn new(config: MongodbSourceConfig, offset_store: O) -> Self {
        Self {
            config,
            offset_store,
        }
    }
}

impl<O: OffsetStore + Sync> Source for MongodbSource<O> {
    async fn start(
        &mut self,
        sender: mpsc::Sender<crate::source::SourceEvent>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let client = mongodb::Client::with_uri_str(&self.config.connection_string)
            .await
            .map_err(|e| CdcError::Mongodb(format!("failed to connect: {e}")))?;

        validate_collections(&client, &self.config.database, &self.config.collections).await?;

        // Phase 1: Determine start position (snapshot or resume)
        let saved_offset = self.offset_store.load().await?;

        let stored = match saved_offset {
            Some(offset_str) => Some(MongoOffset::from_json(&offset_str)?),
            None => None,
        };

        let resume_point = match stored {
            Some(MongoOffset::Token(token)) => {
                tracing::info!("resuming change stream from saved resume token");
                ResumePoint::Token(token)
            }
            Some(MongoOffset::SnapshotComplete { cluster_time }) => {
                tracing::info!(
                    cluster_time = %format!("{}:{}", cluster_time.time, cluster_time.increment),
                    "snapshot already complete, starting change stream at recorded cluster time"
                );
                ResumePoint::AfterSnapshot {
                    snapshot_time: cluster_time.into(),
                }
            }
            other => {
                match &other {
                    Some(MongoOffset::SnapshotInProgress { collection, count }) => {
                        tracing::warn!(
                            %collection,
                            count,
                            "previous snapshot was interrupted; snapshots are not resumable, re-running it"
                        );
                    }
                    _ => tracing::info!("no saved offset, performing initial snapshot"),
                }

                let result = perform_snapshot(
                    &client,
                    &self.config.database,
                    &self.config.collections,
                    &sender,
                    &shutdown,
                )
                .await?;

                if !result.completed {
                    return Ok(());
                }

                ResumePoint::AfterSnapshot {
                    snapshot_time: result.snapshot_time,
                }
            }
        };

        if shutdown.is_cancelled() {
            return Ok(());
        }

        // Phase 2: Stream change events
        tracing::info!(
            database = %self.config.database,
            "starting change stream"
        );

        let mut stream = match open_change_stream(
            &client,
            &self.config.database,
            &self.config.collections,
            &resume_point,
        )
        .await
        {
            Ok(stream) => stream,
            // Position gone from the oplog: re-snapshot, or every restart wedges.
            Err(e) if is_resume_point_lost(&e) => {
                tracing::warn!(
                    error = %e,
                    "stored change stream position is no longer in the oplog; re-running the snapshot"
                );

                let result = perform_snapshot(
                    &client,
                    &self.config.database,
                    &self.config.collections,
                    &sender,
                    &shutdown,
                )
                .await?;

                if !result.completed {
                    return Ok(());
                }

                open_change_stream(
                    &client,
                    &self.config.database,
                    &self.config.collections,
                    &ResumePoint::AfterSnapshot {
                        snapshot_time: result.snapshot_time,
                    },
                )
                .await?
            }
            Err(e) => return Err(e),
        };

        pump_change_stream(&mut stream, &self.config.database, &sender, &shutdown).await
    }
}
