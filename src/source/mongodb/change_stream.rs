use std::time::Duration;

use mongodb::bson::{doc, Document, Timestamp};
use mongodb::change_stream::event::{ChangeStreamEvent, ResumeToken};
use mongodb::change_stream::ChangeStream;
use mongodb::options::FullDocumentBeforeChangeType;
use mongodb::options::FullDocumentType;
use mongodb::Client;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{CdcError, Result};
use crate::source::SourceEvent;

use super::event_converter::{change_event_to_cdc_event, ConvertResult};
use super::offset::MongoOffset;
use super::{send_event, SendOutcome};

const MAX_EVENTS_PER_COMMIT: usize = 1000;

/// Server-side wait for new events; bounds partial-batch commit latency.
const MAX_AWAIT_TIME: Duration = Duration::from_millis(500);

/// Determines where to start the change stream from.
pub enum ResumePoint {
    /// Start after a snapshot at the given cluster time.
    AfterSnapshot { snapshot_time: Timestamp },
    /// Resume from a previously saved resume token.
    Token(ResumeToken),
}

/// Uses `updateLookup` for the post-image and `whenAvailable` for the
/// pre-image; `collections` becomes a `$match` stage.
pub async fn open_change_stream(
    client: &Client,
    database: &str,
    collections: &[String],
    resume_point: &ResumePoint,
) -> Result<ChangeStream<ChangeStreamEvent<Document>>> {
    let db = client.database(database);

    let pipeline: Vec<Document> = if collections.is_empty() {
        vec![]
    } else {
        vec![doc! {
            "$match": {
                "ns.coll": { "$in": collections }
            }
        }]
    };

    let mut builder = db
        .watch()
        .pipeline(pipeline)
        .full_document(FullDocumentType::UpdateLookup)
        .full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable)
        .max_await_time(MAX_AWAIT_TIME);

    match resume_point {
        ResumePoint::AfterSnapshot { snapshot_time } => {
            builder = builder.start_at_operation_time(*snapshot_time);
        }
        ResumePoint::Token(token) => {
            builder = builder.resume_after(token.clone());
        }
    }

    builder
        .await
        .map_err(|e| CdcError::Mongodb(format!("failed to open change stream: {e}")))
}

/// True if the start position has aged out of the oplog — it can never become
/// valid again, so the only way forward is a fresh snapshot.
pub fn is_resume_point_lost(err: &CdcError) -> bool {
    let msg = err.to_string();
    msg.contains("ChangeStreamHistoryLost")
        || msg.contains("resume point may no longer be in the oplog")
        || msg.contains("as the resume point may no longer be in the oplog")
        || msg.contains("resume of change stream was not possible")
}

/// Commits the latest resume token once a server batch is drained (or after
/// `MAX_EVENTS_PER_COMMIT` events), not once per document.
pub async fn pump_change_stream(
    stream: &mut ChangeStream<ChangeStreamEvent<Document>>,
    database: &str,
    sender: &mpsc::Sender<SourceEvent>,
    shutdown: &CancellationToken,
) -> Result<()> {
    tracing::info!(database, "change stream started");

    let mut pending_offset: Option<String> = None;
    let mut pending_events: usize = 0;

    loop {
        let next = tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown received, stopping change stream");
                commit_pending(sender, &mut pending_offset, &mut pending_events, shutdown).await?;
                return Ok(());
            }
            // At most one `getMore`: an empty result means the batch is drained.
            next = stream.next_if_any() => next,
        };

        match next {
            Ok(Some(event)) => {
                // Skipped events still advance the position — except
                // `invalidate`, which the server refuses in `resumeAfter`.
                let is_invalidate = matches!(
                    event.operation_type,
                    mongodb::change_stream::event::OperationType::Invalidate
                );
                let offset = if is_invalidate {
                    None
                } else {
                    Some(MongoOffset::Token(event.id.clone()).to_json()?)
                };

                match change_event_to_cdc_event(&event, database)? {
                    ConvertResult::Event(cdc_event) => {
                        if let SendOutcome::ShuttingDown =
                            send_event(sender, SourceEvent::Change(cdc_event), shutdown).await?
                        {
                            return Ok(());
                        }
                    }
                    ConvertResult::Skip => {
                        tracing::debug!(op_type = ?event.operation_type, "skipping unsupported change event");
                    }
                }

                if let Some(offset) = offset {
                    pending_offset = Some(offset);
                }
                pending_events += 1;

                if pending_events >= MAX_EVENTS_PER_COMMIT {
                    commit_pending(sender, &mut pending_offset, &mut pending_events, shutdown)
                        .await?;
                }
            }
            Ok(None) => {
                // With no event token (idle or fully filtered stream), the
                // stream's own token keeps the offset from ageing out.
                if pending_offset.is_none() && stream.is_alive() {
                    if let Some(token) = stream.resume_token() {
                        pending_offset = Some(MongoOffset::Token(token).to_json()?);
                    }
                }

                commit_pending(sender, &mut pending_offset, &mut pending_events, shutdown).await?;

                if !stream.is_alive() {
                    return Err(CdcError::Mongodb(
                        "change stream closed by the server (the database or a watched collection \
                         was likely dropped or renamed)"
                            .into(),
                    ));
                }
            }
            Err(e) => {
                return Err(CdcError::Mongodb(format!("change stream error: {e}")));
            }
        }
    }
}

async fn commit_pending(
    sender: &mpsc::Sender<SourceEvent>,
    pending_offset: &mut Option<String>,
    pending_events: &mut usize,
    shutdown: &CancellationToken,
) -> Result<()> {
    if let Some(offset) = pending_offset.take() {
        send_event(sender, SourceEvent::Commit { offset }, shutdown).await?;
    }
    *pending_events = 0;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lost_resume_point_errors_are_recognized() {
        let cases = [
            "failed to open change stream: Kind: Command failed: Error code 286 \
             (ChangeStreamHistoryLost)",
            "failed to open change stream: resume of change stream was not possible, as the \
             resume point may no longer be in the oplog",
        ];
        for msg in cases {
            assert!(
                is_resume_point_lost(&CdcError::Mongodb(msg.into())),
                "expected a lost resume point for: {msg}"
            );
        }
    }

    #[test]
    fn test_other_errors_are_not_treated_as_lost_resume_points() {
        let cases = [
            "failed to open change stream: connection refused",
            "change stream error: not authorized on demo to execute command",
            "failed to parse stored offset",
        ];
        for msg in cases {
            assert!(
                !is_resume_point_lost(&CdcError::Mongodb(msg.into())),
                "did not expect a lost resume point for: {msg}"
            );
        }
    }
}
