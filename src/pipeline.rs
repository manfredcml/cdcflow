use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::event::CdcEvent;
use crate::metrics::PipelineMetrics;
use crate::offset::OffsetStore;
use crate::sink::Sink;
use crate::source::{Source, SourceEvent};

/// Max events to buffer before flushing to the sink, even mid-transaction.
/// Prevents unbounded memory growth from large transactions in WAL streaming.
/// The offset is NOT saved on these intermediate flushes — only on Commit/Checkpoint.
const MAX_BATCH_SIZE: usize = 10_000;

/// The CDC pipeline: connects a Source to a Sink with offset tracking.
///
/// Generic over Source, Sink, and OffsetStore so any combination works.
/// Events are batched between transaction boundaries (Commit/Checkpoint)
/// before being written to the sink and the offset persisted.
pub struct Pipeline<Src, Snk, Off> {
    source: Src,
    sink: Snk,
    offset_store: Off,
    channel_size: usize,
    metrics: Option<Arc<PipelineMetrics>>,
}

impl<Src, Snk, Off> Pipeline<Src, Snk, Off>
where
    Src: Source + Send + 'static,
    Snk: Sink,
    Off: OffsetStore,
{
    pub fn new(source: Src, sink: Snk, offset_store: Off) -> Self {
        Self {
            source,
            sink,
            offset_store,
            channel_size: 1024,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<PipelineMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Run the pipeline until shutdown or error.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let (tx, rx) = mpsc::channel(self.channel_size);

        // Destructure self so source can be moved into the spawned task
        let mut source = self.source;
        let mut sink = self.sink;
        let mut offset_store = self.offset_store;

        let source_shutdown = shutdown.clone();
        let source_handle = tokio::spawn(async move {
            source.start(tx, source_shutdown).await
        });

        let metrics = self.metrics;

        let result =
            Self::receive_loop(&mut sink, &mut offset_store, rx, &shutdown, &metrics).await;

        match source_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("source error: {e}");
                if result.is_ok() {
                    return Err(e);
                }
            }
            Err(e) => {
                tracing::error!("source task panicked: {e}");
            }
        }

        result
    }

    /// Process events from the source channel.
    async fn receive_loop(
        sink: &mut Snk,
        offset_store: &mut Off,
        mut rx: mpsc::Receiver<SourceEvent>,
        shutdown: &CancellationToken,
        metrics: &Option<Arc<PipelineMetrics>>,
    ) -> Result<()> {
        let mut batch: Vec<CdcEvent> = Vec::new();
        let mut last_offset: Option<String> = None;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("pipeline shutting down");
                    Self::drain_channel(sink, offset_store, &mut rx, &mut batch, &mut last_offset, metrics).await?;
                    break;
                }
                event = rx.recv() => {
                    match event {
                        Some(SourceEvent::Change(cdc_event)) => {
                            batch.push(cdc_event);
                            // Flush without saving offset to bound memory on large transactions
                            if batch.len() >= MAX_BATCH_SIZE {
                                let count = batch.len() as u64;
                                sink.write_batch(&batch).await?;
                                sink.flush().await?;
                                if let Some(m) = metrics {
                                    m.record_batch(count);
                                }
                                batch.clear();
                            }
                        }
                        Some(SourceEvent::Commit { offset }) => {
                            if let Err(e) = Self::flush_batch(sink, offset_store, &mut batch, offset.clone(), metrics).await {
                                if let Some(m) = metrics { m.record_error(); }
                                return Err(e);
                            }
                            last_offset = Some(offset);
                        }
                        Some(SourceEvent::Checkpoint { offset }) => {
                            if let Err(e) = Self::flush_batch(sink, offset_store, &mut batch, offset.clone(), metrics).await {
                                if let Some(m) = metrics { m.record_error(); }
                                return Err(e);
                            }
                            last_offset = Some(offset);
                        }
                        None => {
                            tracing::info!("source channel closed");
                            break;
                        }
                    }
                }
            }
        }

        if !batch.is_empty() {
            if let Some(offset) = last_offset.clone() {
                Self::flush_batch(sink, offset_store, &mut batch, offset, metrics).await?;
            }
        }

        sink.flush().await?;
        tracing::info!(?last_offset, "pipeline stopped");
        Ok(())
    }

    /// Drain remaining events from the channel after shutdown signal.
    async fn drain_channel(
        sink: &mut Snk,
        offset_store: &mut Off,
        rx: &mut mpsc::Receiver<SourceEvent>,
        batch: &mut Vec<CdcEvent>,
        last_offset: &mut Option<String>,
        metrics: &Option<Arc<PipelineMetrics>>,
    ) -> Result<()> {
        while let Ok(event) = rx.try_recv() {
            match event {
                SourceEvent::Change(cdc_event) => {
                    batch.push(cdc_event);
                }
                SourceEvent::Commit { offset } | SourceEvent::Checkpoint { offset } => {
                    Self::flush_batch(sink, offset_store, batch, offset.clone(), metrics).await?;
                    *last_offset = Some(offset);
                }
            }
        }
        Ok(())
    }

    /// Write the current batch to the sink and persist the offset.
    async fn flush_batch(
        sink: &mut Snk,
        offset_store: &mut Off,
        batch: &mut Vec<CdcEvent>,
        offset: String,
        metrics: &Option<Arc<PipelineMetrics>>,
    ) -> Result<()> {
        if !batch.is_empty() {
            let count = batch.len() as u64;
            sink.write_batch(batch).await?;
            sink.flush().await?;
            if let Some(m) = metrics {
                m.record_batch(count);
            }
        }
        offset_store.save(offset).await?;
        batch.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ChangeOp, ColumnValue, Lsn, TableId};
    use crate::offset::memory::MemoryOffsetStore;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    type SharedBatches = Arc<Mutex<Vec<Vec<CdcEvent>>>>;

    /// A mock sink that records batches.
    struct MockSink {
        batches: SharedBatches,
        flush_count: Arc<Mutex<usize>>,
    }

    impl MockSink {
        fn new() -> (Self, SharedBatches, Arc<Mutex<usize>>) {
            let batches = Arc::new(Mutex::new(Vec::new()));
            let flush_count = Arc::new(Mutex::new(0));
            (
                Self {
                    batches: batches.clone(),
                    flush_count: flush_count.clone(),
                },
                batches,
                flush_count,
            )
        }
    }

    impl Sink for MockSink {
        async fn write_batch(&mut self, events: &[CdcEvent]) -> Result<()> {
            self.batches.lock().unwrap().push(events.to_vec());
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            *self.flush_count.lock().unwrap() += 1;
            Ok(())
        }
    }

    /// A mock source that sends pre-defined events.
    struct MockSource {
        events: Vec<SourceEvent>,
    }

    impl Source for MockSource {
        async fn start(
            &mut self,
            sender: mpsc::Sender<SourceEvent>,
            _shutdown: CancellationToken,
        ) -> Result<()> {
            for event in self.events.drain(..) {
                sender.send(event).await.unwrap();
            }
            Ok(())
        }
    }

    fn make_event(table: &str, id: &str, lsn: u64) -> CdcEvent {
        CdcEvent {
            lsn: Lsn(lsn),
            timestamp_us: 0,
            xid: 1,
            table: TableId {
                schema: "public".into(),
                name: table.into(),
                oid: 1,
            },
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([("id".into(), ColumnValue::Text(id.into()))])),
            old: None,
            primary_key_columns: vec!["id".into()],
        }
    }

    #[tokio::test]
    async fn test_pipeline_basic_flow() {
        let source = MockSource {
            events: vec![
                SourceEvent::Change(make_event("users", "1", 0x100)),
                SourceEvent::Change(make_event("users", "2", 0x100)),
                SourceEvent::Commit { offset: Lsn(0x200).to_string() },
            ],
        };

        let (sink, batches, _flush_count) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();
        let offset_clone = offset_store.clone();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
        assert_eq!(offset_clone.current(), Some(Lsn(0x200).to_string()));
    }

    #[tokio::test]
    async fn test_pipeline_multiple_transactions() {
        let source = MockSource {
            events: vec![
                SourceEvent::Change(make_event("users", "1", 0x100)),
                SourceEvent::Commit { offset: Lsn(0x200).to_string() },
                SourceEvent::Change(make_event("users", "2", 0x300)),
                SourceEvent::Change(make_event("users", "3", 0x300)),
                SourceEvent::Commit { offset: Lsn(0x400).to_string() },
            ],
        };

        let (sink, batches, _) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();
        let offset_clone = offset_store.clone();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1); // first tx: 1 event
        assert_eq!(batches[1].len(), 2); // second tx: 2 events
        assert_eq!(offset_clone.current(), Some(Lsn(0x400).to_string()));
    }

    #[tokio::test]
    async fn test_pipeline_checkpoint_event() {
        let source = MockSource {
            events: vec![
                SourceEvent::Change(make_event("users", "1", 0x100)),
                SourceEvent::Checkpoint { offset: Lsn(0x100).to_string() },
            ],
        };

        let (sink, batches, _) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();
        let offset_clone = offset_store.clone();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(offset_clone.current(), Some(Lsn(0x100).to_string()));
    }

    #[tokio::test]
    async fn test_pipeline_empty_commit() {
        let source = MockSource {
            events: vec![SourceEvent::Commit { offset: Lsn(0x100).to_string() }],
        };

        let (sink, batches, _) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();
        let offset_clone = offset_store.clone();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        // Empty commit should still save offset but not write a batch
        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 0);
        assert_eq!(offset_clone.current(), Some(Lsn(0x100).to_string()));
    }

    #[tokio::test]
    async fn test_pipeline_shutdown() {
        let (tx, rx) = mpsc::channel(10);
        let (mut sink, _, _) = MockSink::new();
        let mut offset_store = MemoryOffsetStore::new();
        let shutdown = CancellationToken::new();

        tx.send(SourceEvent::Change(make_event("t", "1", 0x100)))
            .await
            .unwrap();
        tx.send(SourceEvent::Commit { offset: Lsn(0x200).to_string() })
            .await
            .unwrap();

        shutdown.cancel();

        Pipeline::<MockSource, MockSink, MemoryOffsetStore>::receive_loop(
            &mut sink,
            &mut offset_store,
            rx,
            &shutdown,
            &None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_pipeline_resume_from_offset() {
        // Simulate resuming — offset already saved
        let offset_store = MemoryOffsetStore::with_offset(Lsn(0x500).to_string());

        // Source sends new events starting from the saved offset
        let source = MockSource {
            events: vec![
                SourceEvent::Change(make_event("t", "10", 0x600)),
                SourceEvent::Commit { offset: Lsn(0x700).to_string() },
            ],
        };

        let (sink, batches, _) = MockSink::new();
        let offset_clone = offset_store.clone();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(offset_clone.current(), Some(Lsn(0x700).to_string()));
    }

    #[tokio::test]
    async fn test_pipeline_with_metrics() {
        let source = MockSource {
            events: vec![
                SourceEvent::Change(make_event("users", "1", 0x100)),
                SourceEvent::Change(make_event("users", "2", 0x100)),
                SourceEvent::Commit { offset: Lsn(0x200).to_string() },
                SourceEvent::Change(make_event("users", "3", 0x300)),
                SourceEvent::Commit { offset: Lsn(0x400).to_string() },
            ],
        };

        let (sink, _, _) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();
        let metrics = PipelineMetrics::new();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store)
            .with_metrics(metrics.clone());
        pipeline.run(shutdown).await.unwrap();

        let snap = metrics.snapshot();
        assert_eq!(snap.events_total, 3);
        assert_eq!(snap.batches_total, 2);
        assert_eq!(snap.last_batch_size, 1);
        assert_eq!(snap.errors_total, 0);
    }

    #[tokio::test]
    async fn test_pipeline_without_metrics() {
        // Ensure pipeline works fine without metrics (None path)
        let source = MockSource {
            events: vec![
                SourceEvent::Change(make_event("t", "1", 0x100)),
                SourceEvent::Commit { offset: Lsn(0x200).to_string() },
            ],
        };

        let (sink, batches, _) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn test_pipeline_large_transaction_intermediate_flush() {
        // A single transaction with more events than MAX_BATCH_SIZE should
        // produce intermediate flushes to bound memory, but only save the
        // offset once on Commit.
        let event_count = MAX_BATCH_SIZE + 500;
        let mut events: Vec<SourceEvent> = (0..event_count)
            .map(|i| SourceEvent::Change(make_event("t", &i.to_string(), 0x100)))
            .collect();
        events.push(SourceEvent::Commit { offset: Lsn(0x200).to_string() });

        let source = MockSource { events };
        let (sink, batches, flush_count) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();
        let offset_clone = offset_store.clone();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        let batches = batches.lock().unwrap();
        // Should have 2 write_batch calls from the intermediate flush + the Commit flush,
        // plus 1 final flush() call from pipeline shutdown
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), MAX_BATCH_SIZE);
        assert_eq!(batches[1].len(), 500);

        // Offset should only be saved once (on the Commit), not during intermediate flush
        assert_eq!(offset_clone.current(), Some(Lsn(0x200).to_string()));

        // flush() is called: once for intermediate flush, once for Commit flush_batch,
        // plus once for the final pipeline shutdown flush
        let flush_count = *flush_count.lock().unwrap();
        assert_eq!(flush_count, 3);
    }

    #[tokio::test]
    async fn test_pipeline_exactly_max_batch_size_triggers_flush() {
        // Exactly MAX_BATCH_SIZE events should trigger one intermediate flush
        let mut events: Vec<SourceEvent> = (0..MAX_BATCH_SIZE)
            .map(|i| SourceEvent::Change(make_event("t", &i.to_string(), 0x100)))
            .collect();
        events.push(SourceEvent::Commit { offset: Lsn(0x200).to_string() });

        let source = MockSource { events };
        let (sink, batches, _) = MockSink::new();
        let offset_store = MemoryOffsetStore::new();

        let shutdown = CancellationToken::new();
        let pipeline = Pipeline::new(source, sink, offset_store);
        pipeline.run(shutdown).await.unwrap();

        let batches = batches.lock().unwrap();
        // Intermediate flush at MAX_BATCH_SIZE, then empty Commit (no write_batch for empty batch)
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), MAX_BATCH_SIZE);
    }
}
