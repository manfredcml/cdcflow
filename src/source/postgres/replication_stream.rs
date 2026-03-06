use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{CdcError, Result};
use crate::event::Lsn;
use crate::source::SourceEvent;

use super::event_converter::{ConvertResult, EventConverter};
use super::protocol::parser;
use super::replication_client::{ReplicationClient, ReplicationEvent};

/// Streams WAL changes from PostgreSQL via logical replication.
///
/// Connects the replication client (TCP), pgoutput parser, and event converter
/// into a continuous processing loop.
pub struct ReplicationStream {
    client: ReplicationClient,
    converter: EventConverter,
    last_received_lsn: u64,
}

impl ReplicationStream {
    pub fn new(
        client: ReplicationClient,
        pk_cache: HashMap<(String, String), Vec<String>>,
    ) -> Self {
        Self {
            client,
            converter: EventConverter::new(pk_cache),
            last_received_lsn: 0,
        }
    }

    /// Start the replication slot streaming.
    pub async fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: Lsn,
        publication_name: &str,
    ) -> Result<()> {
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            slot_name, start_lsn, publication_name,
        );
        self.client.simple_query(&sql).await?;
        tracing::info!(%start_lsn, slot_name, publication_name, "replication started");
        Ok(())
    }

    /// Run the replication stream event loop.
    ///
    /// Reads WAL events, parses pgoutput, converts to CdcEvents, and sends
    /// them through the channel. Handles keepalives and shutdown.
    pub async fn run(
        &mut self,
        sender: &mpsc::Sender<SourceEvent>,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("replication stream shutting down");
                    return Ok(());
                }
                event = self.client.next_event() => {
                    match event? {
                        Some(ReplicationEvent::XLogData { wal_start, data, .. }) => {
                            self.last_received_lsn = wal_start;
                            self.process_wal_data(&data, sender).await?;
                        }
                        Some(ReplicationEvent::Keepalive { wal_end, reply_requested, .. }) => {
                            if reply_requested {
                                self.send_status_update(wal_end).await?;
                            }
                        }
                        None => {
                            tracing::info!("replication stream ended (CopyDone)");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Process a single WAL data payload (pgoutput message).
    async fn process_wal_data(
        &mut self,
        data: &[u8],
        sender: &mpsc::Sender<SourceEvent>,
    ) -> Result<()> {
        let msg = parser::parse(data)?;
        let results = self.converter.convert(msg)?;

        for result in results {
            match result {
                ConvertResult::Event(event) => {
                    sender
                        .send(SourceEvent::Change(event))
                        .await
                        .map_err(|e| CdcError::Protocol(format!("send event: {e}")))?;
                }
                ConvertResult::Commit(lsn) => {
                    sender
                        .send(SourceEvent::Commit {
                            offset: lsn.to_string(),
                        })
                        .await
                        .map_err(|e| CdcError::Protocol(format!("send commit: {e}")))?;

                    // Acknowledge the commit to PostgreSQL
                    self.send_status_update(lsn.as_u64()).await?;
                }
                ConvertResult::Begin(_) | ConvertResult::Skip => {}
            }
        }

        Ok(())
    }

    /// Send a standby status update to PostgreSQL.
    async fn send_status_update(&mut self, lsn: u64) -> Result<()> {
        self.client
            .send_standby_status_update(lsn, lsn, lsn, 0, false)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build raw pgoutput message bytes
    fn make_begin_bytes(final_lsn: u64, timestamp: i64, xid: u32) -> Vec<u8> {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&final_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&xid.to_be_bytes());
        buf
    }

    fn make_commit_bytes(end_lsn: u64) -> Vec<u8> {
        let mut buf = vec![b'C', 0]; // flags
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&0i64.to_be_bytes());
        buf
    }

    fn make_relation_bytes(rel_id: u32, namespace: &str, name: &str, cols: &[&str]) -> Vec<u8> {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&rel_id.to_be_bytes());
        buf.extend_from_slice(namespace.as_bytes());
        buf.push(0);
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.push(b'd'); // replica identity
        buf.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for &col in cols {
            buf.push(0); // flags
            buf.extend_from_slice(col.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&25u32.to_be_bytes()); // text OID
            buf.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        }
        buf
    }

    fn make_insert_bytes(rel_id: u32, values: &[&str]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&rel_id.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for &val in values {
            buf.push(b't');
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes());
            buf.extend_from_slice(val.as_bytes());
        }
        buf
    }

    #[tokio::test]
    async fn test_process_wal_data_sends_events() {
        // Test that process_wal_data correctly sends events through the channel
        // We can't create a real ReplicationStream without a TCP connection,
        // but we can test the converter + parser integration
        let (tx, mut rx) = mpsc::channel(100);
        let mut converter = EventConverter::new(HashMap::new());

        // Helper to simulate process_wal_data
        async fn process(
            converter: &mut EventConverter,
            data: &[u8],
            sender: &mpsc::Sender<SourceEvent>,
        ) -> Result<()> {
            let msg = parser::parse(data)?;
            let results = converter.convert(msg)?;
            for result in results {
                match result {
                    ConvertResult::Event(event) => {
                        sender.send(SourceEvent::Change(event)).await.unwrap();
                    }
                    ConvertResult::Commit(lsn) => {
                        sender
                            .send(SourceEvent::Commit {
                                offset: lsn.to_string(),
                            })
                            .await
                            .unwrap();
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        // Send a full transaction
        process(&mut converter, &make_begin_bytes(0x100, 0, 1), &tx)
            .await
            .unwrap();
        process(
            &mut converter,
            &make_relation_bytes(1, "public", "t", &["id"]),
            &tx,
        )
        .await
        .unwrap();
        process(&mut converter, &make_insert_bytes(1, &["42"]), &tx)
            .await
            .unwrap();
        process(&mut converter, &make_commit_bytes(0x200), &tx)
            .await
            .unwrap();

        drop(tx);

        // Verify events
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }

        assert_eq!(events.len(), 2); // 1 Change + 1 Commit
        assert!(matches!(&events[0], SourceEvent::Change(_)));
        assert!(
            matches!(&events[1], SourceEvent::Commit { offset } if *offset == Lsn(0x200).to_string())
        );
    }
}
