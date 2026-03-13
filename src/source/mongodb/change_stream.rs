use futures_util::StreamExt;
use mongodb::bson::{doc, Document, Timestamp};
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::FullDocumentBeforeChangeType;
use mongodb::options::FullDocumentType;
use mongodb::Client;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{CdcError, Result};
use crate::source::SourceEvent;

use super::event_converter::{change_event_to_cdc_event, serialize_resume_token, ConvertResult};

/// Determines where to start the change stream from.
pub enum ResumePoint {
    /// Start after a snapshot at the given cluster time.
    AfterSnapshot { snapshot_time: Timestamp },
    /// Resume from a previously saved resume token.
    Token(ResumeToken),
}

/// Run the change stream loop, sending CDC events to the pipeline.
///
/// Supports filtering to specific collections via a `$match` pipeline stage.
/// Uses `fullDocument: "updateLookup"` to get the full document on updates,
/// and `fullDocumentBeforeChange: "whenAvailable"` to capture pre-images.
pub async fn run_change_stream(
    client: &Client,
    database: &str,
    collections: &[String],
    resume_point: ResumePoint,
    sender: &mpsc::Sender<SourceEvent>,
    shutdown: &CancellationToken,
) -> Result<()> {
    let db = client.database(database);

    // Build pipeline to filter collections if specified
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
        .full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable);

    match &resume_point {
        ResumePoint::AfterSnapshot { snapshot_time } => {
            builder = builder.start_at_operation_time(*snapshot_time);
        }
        ResumePoint::Token(token) => {
            builder = builder.resume_after(token.clone());
        }
    }

    let mut stream = builder
        .await
        .map_err(|e| CdcError::Mongodb(format!("failed to open change stream: {e}")))?;

    tracing::info!(database, "change stream started");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown received, stopping change stream");
                return Ok(());
            }
            next = stream.next() => {
                match next {
                    Some(Ok(event)) => {
                        match change_event_to_cdc_event(&event, database)? {
                            ConvertResult::Event(cdc_event) => {
                                sender
                                    .send(SourceEvent::Change(cdc_event))
                                    .await
                                    .map_err(|e| CdcError::Mongodb(format!("failed to send change event: {e}")))?;

                                // Send commit with resume token as offset
                                let offset = serialize_resume_token(&event.id)?;
                                sender
                                    .send(SourceEvent::Commit { offset })
                                    .await
                                    .map_err(|e| CdcError::Mongodb(format!("failed to send commit: {e}")))?;
                            }
                            ConvertResult::Skip => {
                                tracing::debug!(op_type = ?event.operation_type, "skipping unsupported change event");
                            }
                            ConvertResult::Commit { .. } => {}
                        }
                    }
                    Some(Err(e)) => {
                        return Err(CdcError::Mongodb(format!("change stream error: {e}")));
                    }
                    None => {
                        tracing::warn!("change stream ended unexpectedly");
                        return Ok(());
                    }
                }
            }
        }
    }
}
