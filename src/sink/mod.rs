pub mod iceberg;
pub mod kafka;
pub mod postgres;
pub mod stdout;

use crate::error::Result;
use crate::event::CdcEvent;

/// Trait for CDC event sinks. Implementations deliver events to their destination
/// (stdout, Kafka, HTTP, etc.).
pub trait Sink: Send {
    /// Write a batch of events. Called between Commit/Checkpoint boundaries.
    fn write_batch(
        &mut self,
        events: &[CdcEvent],
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Flush any buffered data. Called after write_batch on commit.
    fn flush(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
}