//! Integration tests for the full CDC pipeline with MySQL.
//!
//! These tests require Docker to be running. Run with:
//!   cargo test -- --ignored
//!
//! Each test spins up a MySQL container with binlog enabled.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mysql_async::prelude::*;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use cdcflow::config::MySqlSourceConfig;
use cdcflow::error::Result;
use cdcflow::event::{CdcEvent, ChangeOp, ColumnValue};
use cdcflow::offset::memory::MemoryOffsetStore;
use cdcflow::pipeline::Pipeline;
use cdcflow::sink::Sink;
use cdcflow::source::mysql::MySqlSource;

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

/// Start a MySQL container with binlog enabled in ROW format.
async fn start_mysql() -> (ContainerAsync<Mysql>, String, u16) {
    let container = Mysql::default()
        .with_tag("8")
        .with_env_var("MYSQL_ROOT_PASSWORD", "root")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_cmd(vec![
            "mysqld".to_string(),
            "--server-id=1".to_string(),
            "--log-bin=mysql-bin".to_string(),
            "--binlog-format=ROW".to_string(),
            "--binlog-row-image=FULL".to_string(),
        ])
        .start()
        .await
        .expect("failed to start mysql container");

    let host_port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("failed to get host port");

    let host = container
        .get_host()
        .await
        .expect("failed to get host")
        .to_string();

    (container, host, host_port)
}

/// Create a mysql_async pool for test operations.
fn connect_pool(host: &str, port: u16) -> mysql_async::Pool {
    let url = format!("mysql://root:root@{host}:{port}/testdb");
    let opts = mysql_async::Opts::from_url(&url).expect("invalid MySQL URL");
    mysql_async::Pool::new(opts)
}

/// Set up a test table.
async fn setup_test_schema(pool: &mysql_async::Pool) {
    let mut conn = pool.get_conn().await.expect("failed to get connection");
    conn.query_drop(
        "CREATE TABLE test_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255)
        )",
    )
    .await
    .expect("failed to create test table");
}

fn make_mysql_config(host: &str, port: u16) -> MySqlSourceConfig {
    MySqlSourceConfig {
        host: host.to_string(),
        port,
        user: "root".to_string(),
        password: Some("root".to_string()),
        database: "testdb".to_string(),
        server_id: 1000,
        tables: vec!["testdb.test_users".to_string()],
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mysql_snapshot_and_stream() {
    let (_container, host, port) = start_mysql().await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let pool = connect_pool(&host, port);
    setup_test_schema(&pool).await;

    // Insert initial data (will be captured in snapshot)
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(
            "INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com')",
        )
        .await
        .unwrap();
        conn.query_drop("INSERT INTO test_users (name, email) VALUES ('Bob', NULL)")
            .await
            .unwrap();
    }

    // Set up the pipeline
    let config = make_mysql_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let source = MySqlSource::new(config, offset_store.clone());

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let pipeline = Pipeline::new(source, sink, offset_store.clone());

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

        for event in captured.iter().filter(|e| e.is_snapshot()) {
            assert_eq!(event.table.name, "test_users");
            assert!(event.op == ChangeOp::Snapshot);
        }
    }

    // Now insert more data (should be captured via streaming)
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(
            "INSERT INTO test_users (name, email) VALUES ('Charlie', 'charlie@example.com')",
        )
        .await
        .unwrap();
    }

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

    pool.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mysql_update_and_delete() {
    let (_container, host, port) = start_mysql().await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let pool = connect_pool(&host, port);
    setup_test_schema(&pool).await;

    // Insert initial data
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(
            "INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com')",
        )
        .await
        .unwrap();
    }

    let config = make_mysql_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let source = MySqlSource::new(config, offset_store.clone());

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
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop("UPDATE test_users SET name = 'Alice Updated' WHERE name = 'Alice'")
            .await
            .unwrap();
    }

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
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop("DELETE FROM test_users WHERE name = 'Alice Updated'")
            .await
            .unwrap();
    }

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

    pool.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mysql_resume_from_offset() {
    let (_container, host, port) = start_mysql().await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let pool = connect_pool(&host, port);
    setup_test_schema(&pool).await;

    // Insert initial data
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(
            "INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com')",
        )
        .await
        .unwrap();
    }

    // First run: snapshot
    let config = make_mysql_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink1, events1) = CollectorSink::new();
    let source1 = MySqlSource::new(config.clone(), offset_store.clone());

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

    // Give time for the offset to be persisted
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop first run
    shutdown1.cancel();
    let _ = timeout(Duration::from_secs(5), handle1).await;

    let saved_offset = offset_store.current();
    assert!(
        saved_offset.is_some(),
        "offset should be saved after first run"
    );

    // Insert more data while pipeline is stopped
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop("INSERT INTO test_users (name, email) VALUES ('Bob', 'bob@example.com')")
            .await
            .unwrap();
    }

    // Second run: should resume from saved offset (no snapshot)
    let (sink2, events2) = CollectorSink::new();
    let source2 = MySqlSource::new(config, offset_store.clone());

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

    pool.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mysql_truncate_event() {
    let (_container, host, port) = start_mysql().await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let pool = connect_pool(&host, port);
    setup_test_schema(&pool).await;

    // Insert initial data
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(
            "INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com')",
        )
        .await
        .unwrap();
    }

    let config = make_mysql_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let source = MySqlSource::new(config, offset_store.clone());

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

    // Perform TRUNCATE
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop("TRUNCATE TABLE test_users").await.unwrap();
    }

    // Wait for truncate event
    timeout(Duration::from_secs(10), async {
        loop {
            let has_truncate = events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Truncate);
            if has_truncate {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("truncate event timeout");

    // Verify truncate event
    {
        let captured = events.lock().unwrap();
        let truncates: Vec<_> = captured
            .iter()
            .filter(|e| e.op == ChangeOp::Truncate)
            .collect();
        assert!(!truncates.is_empty(), "no truncate events");
        assert_eq!(truncates[0].table.name, "test_users");
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), pipeline_handle).await;

    pool.disconnect().await.unwrap();
}

/// Start a MySQL container with GTID mode and full metadata (matching user's Docker config).
async fn start_mysql_gtid() -> (ContainerAsync<Mysql>, String, u16) {
    let container = Mysql::default()
        .with_tag("8")
        .with_env_var("MYSQL_ROOT_PASSWORD", "root")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_cmd(vec![
            "mysqld".to_string(),
            "--server-id=1".to_string(),
            "--log-bin=mysql-bin".to_string(),
            "--binlog-format=ROW".to_string(),
            "--binlog-row-image=FULL".to_string(),
            "--binlog-row-metadata=FULL".to_string(),
            "--enforce-gtid-consistency=ON".to_string(),
            "--gtid-mode=ON".to_string(),
        ])
        .start()
        .await
        .expect("failed to start mysql container with GTID mode");

    let host_port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("failed to get host port");

    let host = container
        .get_host()
        .await
        .expect("failed to get host")
        .to_string();

    (container, host, host_port)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mysql_truncate_event_gtid_mode() {
    let (_container, host, port) = start_mysql_gtid().await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let pool = connect_pool(&host, port);
    setup_test_schema(&pool).await;

    // Insert initial data
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(
            "INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com')",
        )
        .await
        .unwrap();
    }

    let config = make_mysql_config(&host, port);
    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let source = MySqlSource::new(config, offset_store.clone());

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

    // Perform TRUNCATE
    {
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop("TRUNCATE TABLE test_users").await.unwrap();
    }

    // Wait for truncate event
    timeout(Duration::from_secs(10), async {
        loop {
            let has_truncate = events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Truncate);
            if has_truncate {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("truncate event timeout — GTID mode may prevent TRUNCATE QueryEvent delivery");

    // Verify truncate event
    {
        let captured = events.lock().unwrap();
        let truncates: Vec<_> = captured
            .iter()
            .filter(|e| e.op == ChangeOp::Truncate)
            .collect();
        assert!(!truncates.is_empty(), "no truncate events in GTID mode");
        assert_eq!(truncates[0].table.name, "test_users");
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), pipeline_handle).await;

    pool.disconnect().await.unwrap();
}
