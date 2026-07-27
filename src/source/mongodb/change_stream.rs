use std::time::Duration;

use mongodb::bson::{doc, Document, Timestamp};
use mongodb::change_stream::event::ResumeToken;
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

/// Max change events buffered before forcing an offset commit.
const MAX_EVENTS_PER_COMMIT: usize = 1000;

/// How long a `getMore` waits server-side for new events before returning empty.
/// Bounds the latency of the commit that flushes a partially filled batch.
const MAX_AWAIT_TIME: Duration = Duration::from_millis(500);

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
///
/// Events are drained batch-by-batch: a `Commit` carrying the latest resume
/// token is emitted once a server batch is exhausted (or after
/// `MAX_EVENTS_PER_COMMIT` events), rather than once per document.
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
        .full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable)
        .max_await_time(MAX_AWAIT_TIME);

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

    // Latest resume token not yet committed, and how many events it covers.
    let mut pending_offset: Option<String> = None;
    let mut pending_events: usize = 0;

    loop {
        let next = tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown received, stopping change stream");
                commit_pending(sender, &mut pending_offset, &mut pending_events, shutdown).await?;
                return Ok(());
            }
            // `next_if_any` performs at most one `getMore`, so an empty result
            // means the current batch is drained — the point at which the driver
            // documentation recommends persisting the resume token.
            next = stream.next_if_any() => next,
        };

        match next {
            Ok(Some(event)) => {
                // Track the token even for skipped events so the stream position
                // still advances past operations we do not forward.
                let offset = MongoOffset::Token(event.id.clone()).to_json()?;

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

                pending_offset = Some(offset);
                pending_events += 1;

                if pending_events >= MAX_EVENTS_PER_COMMIT {
                    commit_pending(sender, &mut pending_offset, &mut pending_events, shutdown)
                        .await?;
                }
            }
            Ok(None) => {
                // Batch drained: commit what we have, then check the stream is alive.
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

/// Emit a `Commit` for the pending resume token, if any.
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
