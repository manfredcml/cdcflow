use futures_util::StreamExt;
use mongodb::bson::{doc, Timestamp};
use mongodb::Client;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{CdcError, Result};
use crate::source::SourceEvent;

use super::event_converter::snapshot_doc_to_cdc_event;
use super::offset::MongoOffset;
use super::{send_event, SendOutcome};

/// Number of documents between snapshot progress checkpoints.
const SNAPSHOT_CHECKPOINT_INTERVAL: u64 = 1000;

/// Result of a snapshot operation.
pub struct SnapshotResult {
    /// The cluster time recorded before the snapshot started.
    /// Used to start change stream from the correct point.
    pub snapshot_time: Timestamp,
    /// False if the snapshot was interrupted by a shutdown signal.
    pub completed: bool,
}

/// Perform initial snapshot of the specified collections.
///
/// 1. Record the current cluster time via a `ping` command.
/// 2. Scan each collection with `find({})`.
/// 3. Send Change + Checkpoint events for each document batch.
///
/// Progress checkpoints are tagged `snapshot_in_progress`: snapshots are not
/// resumable mid-way, so restarting from one re-runs the whole snapshot. The
/// final checkpoint is tagged `snapshot_complete` and carries the cluster time,
/// letting a restart skip straight to the change stream.
pub async fn perform_snapshot(
    client: &Client,
    database: &str,
    collections: &[String],
    sender: &mpsc::Sender<SourceEvent>,
    shutdown: &CancellationToken,
) -> Result<SnapshotResult> {
    let db = client.database(database);

    // Record cluster time before snapshot
    let ping_result = db
        .run_command(doc! { "ping": 1 })
        .await
        .map_err(|e| CdcError::Mongodb(format!("failed to ping for cluster time: {e}")))?;

    let snapshot_time = ping_result
        .get("operationTime")
        .and_then(|v| v.as_timestamp())
        .ok_or_else(|| {
            CdcError::Mongodb("ping response missing operationTime — is this a replica set?".into())
        })?;

    tracing::info!(
        snapshot_time = %format!("{}:{}", snapshot_time.time, snapshot_time.increment),
        "recorded cluster time for snapshot"
    );

    // Determine collections to snapshot
    let collection_names = if collections.is_empty() {
        db.list_collection_names()
            .await
            .map_err(|e| CdcError::Mongodb(format!("failed to list collections: {e}")))?
    } else {
        collections.to_vec()
    };

    for coll_name in &collection_names {
        if shutdown.is_cancelled() {
            tracing::info!("shutdown received, aborting snapshot");
            return Ok(SnapshotResult {
                snapshot_time,
                completed: false,
            });
        }

        tracing::info!(collection = %coll_name, "starting snapshot");
        let coll = db.collection::<mongodb::bson::Document>(coll_name);

        let mut cursor = coll
            .find(doc! {})
            .await
            .map_err(|e| CdcError::Mongodb(format!("snapshot find on {coll_name}: {e}")))?;

        let mut count: u64 = 0;
        loop {
            let next = tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(collection = %coll_name, count, "shutdown received, aborting snapshot");
                    return Ok(SnapshotResult { snapshot_time, completed: false });
                }
                next = cursor.next() => next,
            };

            let Some(result) = next else {
                break;
            };

            let doc = result
                .map_err(|e| CdcError::Mongodb(format!("snapshot cursor on {coll_name}: {e}")))?;

            let event = snapshot_doc_to_cdc_event(&doc, database, coll_name);
            if let SendOutcome::ShuttingDown =
                send_event(sender, SourceEvent::Change(event), shutdown).await?
            {
                return Ok(SnapshotResult {
                    snapshot_time,
                    completed: false,
                });
            }

            count += 1;

            if count.is_multiple_of(SNAPSHOT_CHECKPOINT_INTERVAL) {
                let offset = MongoOffset::SnapshotInProgress {
                    collection: coll_name.clone(),
                    count,
                }
                .to_json()?;
                if let SendOutcome::ShuttingDown =
                    send_event(sender, SourceEvent::Checkpoint { offset }, shutdown).await?
                {
                    return Ok(SnapshotResult {
                        snapshot_time,
                        completed: false,
                    });
                }
            }
        }

        // Per-collection checkpoint: the snapshot as a whole is still in progress
        // until every collection has been scanned.
        let offset = MongoOffset::SnapshotInProgress {
            collection: coll_name.clone(),
            count,
        }
        .to_json()?;
        if let SendOutcome::ShuttingDown =
            send_event(sender, SourceEvent::Checkpoint { offset }, shutdown).await?
        {
            return Ok(SnapshotResult {
                snapshot_time,
                completed: false,
            });
        }

        tracing::info!(collection = %coll_name, count, "snapshot complete");
    }

    // All collections scanned — record a resumable position so a restart skips
    // the snapshot and starts the change stream at the recorded cluster time.
    let offset = MongoOffset::SnapshotComplete {
        cluster_time: snapshot_time.into(),
    }
    .to_json()?;
    let completed = matches!(
        send_event(sender, SourceEvent::Checkpoint { offset }, shutdown).await?,
        SendOutcome::Sent
    );

    Ok(SnapshotResult {
        snapshot_time,
        completed,
    })
}
