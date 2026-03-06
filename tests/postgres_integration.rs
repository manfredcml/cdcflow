//! Integration tests for the full CDC pipeline with PostgreSQL.
//!
//! These tests require Docker to be running. Run with:
//!   cargo test -- --ignored
//!
//! Each test spins up a PostgreSQL container with wal_level=logical.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::time::timeout;
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;

use cdcflow::config::PostgresSourceConfig;
use cdcflow::error::Result;
use cdcflow::event::{CdcEvent, ChangeOp, ColumnValue};
use cdcflow::offset::memory::MemoryOffsetStore;
use cdcflow::pipeline::Pipeline;
use cdcflow::sink::Sink;
use cdcflow::source::postgres::PostgresSource;

/// A sink that collects events into a shared Vec for assertions.
struct CollectorSink {
    events: Arc<Mutex<Vec<CdcEvent>>>,
}

impl CollectorSink {
    fn new() -> (Self, Arc<Mutex<Vec<CdcEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

impl Sink for CollectorSink {
    async fn write_batch(&mut self, events: &[CdcEvent]) -> Result<()> {
        for event in events {
            let json = serde_json::to_string_pretty(event).unwrap();
            println!("\n--- CDC Event ---\n{json}");
        }
        self.events.lock().unwrap().extend_from_slice(events);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Start a PostgreSQL container with wal_level=logical.
async fn start_postgres() -> (ContainerAsync<Postgres>, String, u16) {
    let container = Postgres::default()
        .with_tag("18-alpine")
        .with_cmd(vec![
            "postgres".to_string(),
            "-c".to_string(),
            "wal_level=logical".to_string(),
            "-c".to_string(),
            "max_replication_slots=4".to_string(),
            "-c".to_string(),
            "max_wal_senders=4".to_string(),
        ])
        .start()
        .await
        .expect("failed to start postgres container");

    let host_port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get host port");

    let host = container
        .get_host()
        .await
        .expect("failed to get host")
        .to_string();

    (container, host, host_port)
}

/// Connect a regular tokio-postgres client.
async fn connect_client(host: &str, port: u16) -> Client {
    let conn_str = format!(
        "host={} port={} user=postgres password=postgres dbname=postgres",
        host, port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("failed to connect");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    client
}

/// Set up a test table and publication.
async fn setup_test_schema(client: &Client) {
    client
        .batch_execute(
            "CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            );
            ALTER TABLE test_users REPLICA IDENTITY FULL;
            CREATE PUBLICATION test_pub FOR TABLE test_users;",
        )
        .await
        .expect("failed to create test schema");
}

fn make_pg_config(host: &str, port: u16) -> PostgresSourceConfig {
    PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        database: "postgres".to_string(),
        slot_name: "test_cdc_slot".to_string(),
        publication_name: "test_pub".to_string(),
        create_publication: true,
        tables: vec!["public.test_users".to_string()],
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_full_pipeline_snapshot_and_stream() {
    let (_container, host, port) = start_postgres().await;

    // Wait for PostgreSQL to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = connect_client(&host, port).await;
    setup_test_schema(&client).await;

    // Insert initial data (will be captured in snapshot)
    client
        .execute(
            "INSERT INTO test_users (name, email) VALUES ($1, $2)",
            &[&"Alice", &"alice@example.com"],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO test_users (name, email) VALUES ($1, $2)",
            &[&"Bob", &Option::<String>::None],
        )
        .await
        .unwrap();

    // Set up the pipeline
    let config = make_pg_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let source = PostgresSource::new(config, offset_store.clone());

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let pipeline = Pipeline::new(source, sink, offset_store.clone());

    // Run pipeline in background
    let pipeline_handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Wait for snapshot to complete
    let snapshot_complete = timeout(Duration::from_secs(15), async {
        loop {
            let count = events.lock().unwrap().len();
            if count >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(
        snapshot_complete.is_ok(),
        "snapshot did not complete in time"
    );

    // Verify snapshot events
    {
        let captured = events.lock().unwrap();
        assert!(captured.len() >= 2, "expected at least 2 snapshot events");

        // All snapshot events should have snapshot=true
        for event in captured.iter().filter(|e| e.is_snapshot()) {
            assert_eq!(event.table.name, "test_users");
            assert!(event.op == ChangeOp::Snapshot);
        }
    }

    // Now insert more data (should be captured via streaming)
    client
        .execute(
            "INSERT INTO test_users (name, email) VALUES ($1, $2)",
            &[&"Charlie", &"charlie@example.com"],
        )
        .await
        .unwrap();

    // Wait for stream event
    let stream_received = timeout(Duration::from_secs(10), async {
        loop {
            let count = events.lock().unwrap().len();
            if count >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(stream_received.is_ok(), "stream event not received in time");

    // Verify streamed insert
    {
        let captured = events.lock().unwrap();
        let stream_events: Vec<_> = captured.iter().filter(|e| !e.is_snapshot()).collect();
        assert!(!stream_events.is_empty(), "no stream events captured");

        let insert = &stream_events[0];
        assert_eq!(insert.table.name, "test_users");
        assert_eq!(insert.op, ChangeOp::Insert);
        let new = insert.new.as_ref().unwrap();
        assert_eq!(new.get("name"), Some(&ColumnValue::Text("Charlie".into())));
    }

    // Verify offset was saved
    assert!(offset_store.current().is_some());

    // Shutdown
    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), pipeline_handle).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_update_and_delete_events() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = connect_client(&host, port).await;
    setup_test_schema(&client).await;

    // Insert initial data
    client
        .execute(
            "INSERT INTO test_users (name, email) VALUES ($1, $2)",
            &[&"Alice", &"alice@example.com"],
        )
        .await
        .unwrap();

    let config = make_pg_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let source = PostgresSource::new(config, offset_store.clone());

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let pipeline = Pipeline::new(source, sink, offset_store);

    let pipeline_handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Wait for snapshot
    timeout(Duration::from_secs(15), async {
        loop {
            if !events.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("snapshot timeout");

    // Perform UPDATE
    client
        .execute(
            "UPDATE test_users SET name = $1 WHERE name = $2",
            &[&"Alice Updated", &"Alice"],
        )
        .await
        .unwrap();

    // Wait for update event
    timeout(Duration::from_secs(10), async {
        loop {
            let has_update = events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Update);
            if has_update {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("update event timeout");

    // Perform DELETE
    client
        .execute(
            "DELETE FROM test_users WHERE name = $1",
            &[&"Alice Updated"],
        )
        .await
        .unwrap();

    // Wait for delete event
    timeout(Duration::from_secs(10), async {
        loop {
            let has_delete = events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Delete);
            if has_delete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("delete event timeout");

    // Verify events
    {
        let captured = events.lock().unwrap();

        let updates: Vec<_> = captured
            .iter()
            .filter(|e| e.op == ChangeOp::Update)
            .collect();
        assert!(!updates.is_empty(), "no update events");

        let deletes: Vec<_> = captured
            .iter()
            .filter(|e| e.op == ChangeOp::Delete)
            .collect();
        assert!(!deletes.is_empty(), "no delete events");
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), pipeline_handle).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_resume_from_offset() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = connect_client(&host, port).await;
    setup_test_schema(&client).await;

    // Insert initial data
    client
        .execute(
            "INSERT INTO test_users (name, email) VALUES ($1, $2)",
            &[&"Alice", &"alice@example.com"],
        )
        .await
        .unwrap();

    // First run: snapshot + initial stream
    let config = make_pg_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink1, events1) = CollectorSink::new();
    let source1 = PostgresSource::new(config.clone(), offset_store.clone());

    let shutdown1 = CancellationToken::new();
    let shutdown1_clone = shutdown1.clone();
    let pipeline1 = Pipeline::new(source1, sink1, offset_store.clone());

    let handle1 = tokio::spawn(async move { pipeline1.run(shutdown1_clone).await });

    // Wait for snapshot
    timeout(Duration::from_secs(15), async {
        loop {
            if !events1.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("first run snapshot timeout");

    // Stop first run
    shutdown1.cancel();
    let _ = timeout(Duration::from_secs(5), handle1).await;

    let saved_offset = offset_store.current();
    assert!(
        saved_offset.is_some(),
        "offset should be saved after first run"
    );

    // Insert more data while pipeline is stopped
    client
        .execute(
            "INSERT INTO test_users (name, email) VALUES ($1, $2)",
            &[&"Bob", &"bob@example.com"],
        )
        .await
        .unwrap();

    // Second run: should resume from saved offset (no snapshot)
    let (sink2, events2) = CollectorSink::new();
    let source2 = PostgresSource::new(config, offset_store.clone());

    let shutdown2 = CancellationToken::new();
    let shutdown2_clone = shutdown2.clone();
    let pipeline2 = Pipeline::new(source2, sink2, offset_store.clone());

    let handle2 = tokio::spawn(async move { pipeline2.run(shutdown2_clone).await });

    // Wait for the streamed insert (Bob)
    let resume_ok = timeout(Duration::from_secs(15), async {
        loop {
            let has_bob = events2.lock().unwrap().iter().any(|e| {
                e.op == ChangeOp::Insert
                    && e.new.as_ref().is_some_and(|new| {
                        new.get("name") == Some(&ColumnValue::Text("Bob".into()))
                    })
            });
            if has_bob {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(resume_ok.is_ok(), "Bob's insert not received after resume");

    // The second run should NOT have snapshot events
    {
        let captured = events2.lock().unwrap();
        let snapshot_events: Vec<_> = captured.iter().filter(|e| e.is_snapshot()).collect();
        assert!(
            snapshot_events.is_empty(),
            "should not have snapshot events on resume, got {}",
            snapshot_events.len()
        );
    }

    shutdown2.cancel();
    let _ = timeout(Duration::from_secs(5), handle2).await;
}
