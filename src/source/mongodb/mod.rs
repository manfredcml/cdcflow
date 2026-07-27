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

use self::change_stream::{run_change_stream, ResumePoint};
use self::offset::MongoOffset;
use self::snapshot::perform_snapshot;

/// Outcome of sending an event to the pipeline.
pub(crate) enum SendOutcome {
    Sent,
    /// The pipeline stopped receiving while a shutdown was in flight.
    ShuttingDown,
}

/// Send an event to the pipeline, treating a closed channel during shutdown as
/// a clean stop rather than an error.
///
/// The pipeline's receive loop exits as soon as the shutdown token fires, which
/// can happen between a source's own cancellation checks and its next send.
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

        run_change_stream(
            &client,
            &self.config.database,
            &self.config.collections,
            resume_point,
            &sender,
            &shutdown,
        )
        .await
    }
}
