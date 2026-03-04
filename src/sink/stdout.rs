use std::io::Write;

use crate::error::Result;
use crate::event::CdcEvent;
use crate::schema::value::event_to_json;
use crate::sink::Sink;

/// A sink that writes each event as a JSON line to stdout.
pub struct StdoutSink {
    writer: Box<dyn Write + Send>,
}

impl StdoutSink {
    pub fn new() -> Self {
        Self {
            writer: Box::new(std::io::stdout()),
        }
    }

    /// Create a StdoutSink with a custom writer (for testing).
    pub fn with_writer(writer: Box<dyn Write + Send>) -> Self {
        Self { writer }
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for StdoutSink {
    async fn write_batch(&mut self, events: &[CdcEvent]) -> Result<()> {
        for event in events {
            let json = serde_json::to_string(&event_to_json(event))?;
            writeln!(self.writer, "{json}")?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ChangeOp, ColumnValue, Lsn, TableId};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    fn make_test_event(id: &str) -> CdcEvent {
        CdcEvent {
            lsn: Lsn::ZERO,
            timestamp_us: 0,
            xid: 1,
            table: TableId {
                schema: "public".into(),
                name: "users".into(),
                oid: 16384,
            },
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([("id".into(), ColumnValue::Text(id.into()))])),
            old: None,
            primary_key_columns: vec!["id".into()],
        }
    }

    /// A writer that captures output into a shared buffer.
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn make_sink_and_buffer() -> (StdoutSink, Arc<Mutex<Vec<u8>>>) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = StdoutSink::with_writer(Box::new(SharedWriter(buf.clone())));
        (sink, buf)
    }

    fn read_buffer(buf: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(buf.lock().unwrap().clone()).unwrap()
    }

    #[tokio::test]
    async fn test_stdout_sink_writes_json_lines() {
        let (mut sink, buf) = make_sink_and_buffer();

        let events = vec![make_test_event("1"), make_test_event("2")];
        sink.write_batch(&events).await.unwrap();
        sink.flush().await.unwrap();

        let output = read_buffer(&buf);
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 2);

        for line in &lines {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(parsed["metadata"]["table"], "users");
            assert_eq!(parsed["metadata"]["op"], "I");
        }
    }

    #[tokio::test]
    async fn test_stdout_sink_event_content() {
        let (mut sink, buf) = make_sink_and_buffer();

        sink.write_batch(&[make_test_event("42")]).await.unwrap();

        let output = read_buffer(&buf);
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert_eq!(parsed["metadata"]["op"], "I");
        assert_eq!(parsed["metadata"]["schema"], "public");
        assert_eq!(parsed["metadata"]["table"], "users");
        // Row values at top level: new.values
        assert_eq!(parsed["new"]["values"]["id"], "42");
        assert_eq!(parsed["new"]["types"]["id"], "Text");
    }

    #[tokio::test]
    async fn test_stdout_sink_empty_batch() {
        let (mut sink, buf) = make_sink_and_buffer();

        sink.write_batch(&[]).await.unwrap();

        let output = read_buffer(&buf);
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn test_stdout_sink_multiple_batches() {
        let (mut sink, buf) = make_sink_and_buffer();

        sink.write_batch(&[make_test_event("1")]).await.unwrap();
        sink.write_batch(&[make_test_event("2"), make_test_event("3")])
            .await
            .unwrap();

        let output = read_buffer(&buf);
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 3);
    }
}
