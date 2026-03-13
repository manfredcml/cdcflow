use futures_util::StreamExt;
use mongodb::bson::{doc, Timestamp};
use mongodb::Client;
use tokio::sync::mpsc;

use crate::error::{CdcError, Result};
use crate::source::SourceEvent;

use super::event_converter::snapshot_doc_to_cdc_event;

/// Result of a snapshot operation.
pub struct SnapshotResult {
    /// The cluster time recorded before the snapshot started.
    /// Used to start change stream from the correct point.
    pub snapshot_time: Timestamp,
}

/// Perform initial snapshot of the specified collections.
///
/// 1. Record the current cluster time via a `ping` command.
/// 2. Scan each collection with `find({})`.
/// 3. Send Change + Checkpoint events for each document batch.
pub async fn perform_snapshot(
    client: &Client,
    database: &str,
    collections: &[String],
    sender: &mpsc::Sender<SourceEvent>,
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
        tracing::info!(collection = %coll_name, "starting snapshot");
        let coll = db.collection::<mongodb::bson::Document>(coll_name);

        let mut cursor = coll
            .find(doc! {})
            .await
            .map_err(|e| CdcError::Mongodb(format!("snapshot find on {coll_name}: {e}")))?;

        let mut count: u64 = 0;
        while let Some(result) = cursor.next().await {
            let doc = result
                .map_err(|e| CdcError::Mongodb(format!("snapshot cursor on {coll_name}: {e}")))?;

            let event = snapshot_doc_to_cdc_event(&doc, database, coll_name);
            sender
                .send(SourceEvent::Change(event))
                .await
                .map_err(|e| CdcError::Mongodb(format!("failed to send snapshot event: {e}")))?;

            count += 1;

            // Checkpoint every 1000 documents
            if count.is_multiple_of(1000) {
                let offset = format!(
                    "{{\"snapshot\":\"{}\",\"collection\":\"{}\",\"count\":{}}}",
                    coll_name, coll_name, count
                );
                sender
                    .send(SourceEvent::Checkpoint { offset })
                    .await
                    .map_err(|e| CdcError::Mongodb(format!("failed to send checkpoint: {e}")))?;
            }
        }

        // Final checkpoint for this collection
        let offset = format!(
            "{{\"snapshot_complete\":\"{}\",\"count\":{}}}",
            coll_name, count
        );
        sender
            .send(SourceEvent::Checkpoint { offset })
            .await
            .map_err(|e| CdcError::Mongodb(format!("failed to send checkpoint: {e}")))?;

        tracing::info!(collection = %coll_name, count, "snapshot complete");
    }

    Ok(SnapshotResult { snapshot_time })
}
