pub mod bson_mapping;
pub mod change_stream;
pub mod event_converter;
pub mod snapshot;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::MongodbSourceConfig;
use crate::error::{CdcError, Result};
use crate::offset::OffsetStore;
use crate::source::Source;

use self::change_stream::{run_change_stream, ResumePoint};
use self::event_converter::deserialize_resume_token;
use self::snapshot::perform_snapshot;

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

        let resume_point = match saved_offset {
            Some(offset_str) => {
                tracing::info!("resuming from saved offset, skipping snapshot");
                let token = deserialize_resume_token(&offset_str)?;
                ResumePoint::Token(token)
            }
            None => {
                tracing::info!("no saved offset, performing initial snapshot");
                let result = perform_snapshot(
                    &client,
                    &self.config.database,
                    &self.config.collections,
                    &sender,
                )
                .await?;

                if shutdown.is_cancelled() {
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
