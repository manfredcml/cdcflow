pub mod mysql;
pub mod postgres;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::event::CdcEvent;

/// Events emitted by a source to the pipeline.
#[derive(Debug, Clone)]
pub enum SourceEvent {
    /// A data change event.
    Change(CdcEvent),
    /// Transaction committed — pipeline should flush batch and persist this offset.
    Commit { offset: String },
    /// Source requests the pipeline to save progress (e.g., after snapshot chunk).
    Checkpoint { offset: String },
}

/// Trait for CDC sources. Each database implementation (Postgres, MySQL, etc.)
/// implements this trait to provide snapshot and streaming capabilities.
pub trait Source: Send {
    /// Run the full CDC lifecycle: snapshot (if needed) then stream.
    /// Sends events through the provided sender.
    fn start(
        &mut self,
        sender: mpsc::Sender<SourceEvent>,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
