//! Integration tests for the full CDC pipeline with MongoDB.
//!
//! These tests require Docker to be running. Run with:
//!   cargo test -- --ignored
//!
//! Each test spins up a MongoDB container configured as a single-node replica set.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongodb::bson::doc;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::mongo::Mongo;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use cdcflow::config::MongodbSourceConfig;
use cdcflow::error::Result;
use cdcflow::event::{CdcEvent, ChangeOp, ColumnValue};
use cdcflow::offset::memory::MemoryOffsetStore;
use cdcflow::offset::OffsetStore;
use cdcflow::pipeline::Pipeline;
use cdcflow::sink::Sink;
use cdcflow::source::mongodb::MongodbSource;

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

/// Start a MongoDB container configured as a single-node replica set.
async fn start_mongodb() -> (ContainerAsync<Mongo>, String, u16) {
    let container = Mongo::default()
        .with_tag("7")
        .with_cmd(vec![
            "mongod".to_string(),
            "--replSet".to_string(),
            "rs0".to_string(),
            "--bind_ip_all".to_string(),
        ])
        .start()
        .await
        .expect("failed to start mongodb container");

    let host_port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("failed to get host port");

    let host = container
        .get_host()
        .await
        .expect("failed to get host")
        .to_string();

    // Initialize replica set — use 127.0.0.1:{mapped_port} as the member host
    // so MongoDB recognizes itself. The container binds to 0.0.0.0 and the
    // mapped port is forwarded from the host, making this work with directConnection.
    let conn_str = format!("mongodb://{}:{}/?directConnection=true", host, host_port);
    let client = mongodb::Client::with_uri_str(&conn_str)
        .await
        .expect("failed to connect to mongodb");

    let admin_db = client.database("admin");

    // Use replSetInitiate with "localhost:27017" — the address MongoDB sees
    // internally. We connect from the host via the mapped port using
    // directConnection=true, but the replica set member must use the address
    // visible inside the container.
    admin_db
        .run_command(doc! {
            "replSetInitiate": {
                "_id": "rs0",
                "members": [{ "_id": 0, "host": "localhost:27017" }]
            }
        })
        .await
        .expect("failed to initiate replica set");

    // Wait for primary election
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Ok(status) = admin_db.run_command(doc! { "replSetGetStatus": 1 }).await {
            if let Some(members) = status.get_array("members").ok() {
                for member in members {
                    if let Some(doc) = member.as_document() {
                        if doc.get_str("stateStr").ok() == Some("PRIMARY") {
                            // Primary elected, ready to go
                            return (container, host.to_string(), host_port);
                        }
                    }
                }
            }
        }
    }

    panic!("MongoDB replica set did not elect a primary within 30 seconds");
}

/// Build a connection string for the test container.
fn connection_string(host: &str, port: u16) -> String {
    format!(
        "mongodb://{}:{}/?replicaSet=rs0&directConnection=true",
        host, port
    )
}

/// Get a mongodb::Client connected to the test container.
async fn get_client(host: &str, port: u16) -> mongodb::Client {
    let conn = connection_string(host, port);
    mongodb::Client::with_uri_str(&conn)
        .await
        .expect("failed to create mongodb client")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_snapshot_and_stream() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    // Insert initial documents
    let db = client.database("testdb");
    let coll = db.collection::<mongodb::bson::Document>("test_coll");

    // Enable pre/post images
    db.run_command(doc! {
        "collMod": "test_coll",
        "changeStreamPreAndPostImages": { "enabled": true }
    })
    .await
    .ok(); // May fail if collection doesn't exist yet, that's fine

    // Drop and recreate with pre/post images enabled
    coll.drop().await.ok();
    db.create_collection("test_coll")
        .await
        .expect("failed to create collection");
    db.run_command(doc! {
        "collMod": "test_coll",
        "changeStreamPreAndPostImages": { "enabled": true }
    })
    .await
    .expect("failed to enable change stream images");

    coll.insert_many(vec![
        doc! { "name": "Alice", "age": 30 },
        doc! { "name": "Bob", "age": 25 },
    ])
    .await
    .expect("failed to insert test data");

    // Set up pipeline
    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "testdb".into(),
        collections: vec!["test_coll".into()],
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);

    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Wait for snapshot to complete
    let snapshot_received = timeout(Duration::from_secs(30), async {
        loop {
            {
                let evts = events.lock().unwrap();
                let snapshot_count = evts.iter().filter(|e| e.op == ChangeOp::Snapshot).count();
                if snapshot_count >= 2 {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        snapshot_received.is_ok(),
        "timed out waiting for snapshot events"
    );

    // Verify snapshot events
    {
        let evts = events.lock().unwrap();
        let snapshots: Vec<&CdcEvent> =
            evts.iter().filter(|e| e.op == ChangeOp::Snapshot).collect();
        assert_eq!(snapshots.len(), 2, "expected 2 snapshot events");
        for snap in &snapshots {
            assert_eq!(snap.table.schema, "testdb");
            assert_eq!(snap.table.name, "test_coll");
            assert_eq!(snap.primary_key_columns, vec!["_id"]);
            assert!(snap.new.is_some());
        }
    }

    // Now insert a new document to test streaming
    coll.insert_one(doc! { "name": "Charlie", "age": 35 })
        .await
        .expect("failed to insert streaming doc");

    // Wait for the "Charlie" insert event specifically
    let insert_received = timeout(Duration::from_secs(15), async {
        loop {
            {
                let evts = events.lock().unwrap();
                let has_charlie = evts.iter().any(|e| {
                    e.op == ChangeOp::Insert
                        && e.new
                            .as_ref()
                            .and_then(|r| r.get("name"))
                            .map(|v| matches!(v, ColumnValue::Text(s) if s == "Charlie"))
                            .unwrap_or(false)
                });
                if has_charlie {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        insert_received.is_ok(),
        "timed out waiting for Charlie insert event"
    );

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_update_and_delete() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("testdb2");

    // Create collection with pre/post images
    db.create_collection("items")
        .await
        .expect("failed to create collection");
    db.run_command(doc! {
        "collMod": "items",
        "changeStreamPreAndPostImages": { "enabled": true }
    })
    .await
    .expect("failed to enable change stream images");

    let coll = db.collection::<mongodb::bson::Document>("items");

    // Insert initial doc
    coll.insert_one(doc! { "_id": "item1", "name": "Widget", "price": 10.0 })
        .await
        .expect("failed to insert");

    // Set up pipeline
    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "testdb2".into(),
        collections: vec!["items".into()],
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);

    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Wait for snapshot
    timeout(Duration::from_secs(15), async {
        loop {
            {
                let evts = events.lock().unwrap();
                if evts.iter().any(|e| e.op == ChangeOp::Snapshot) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timed out waiting for snapshot");

    // Update the document
    coll.update_one(doc! { "_id": "item1" }, doc! { "$set": { "price": 15.0 } })
        .await
        .expect("failed to update");

    // Wait for update event
    timeout(Duration::from_secs(15), async {
        loop {
            {
                let evts = events.lock().unwrap();
                if evts.iter().any(|e| e.op == ChangeOp::Update) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timed out waiting for update event");

    // Verify update event
    {
        let evts = events.lock().unwrap();
        let updates: Vec<&CdcEvent> = evts.iter().filter(|e| e.op == ChangeOp::Update).collect();
        assert!(!updates.is_empty(), "expected update event");
        let update = updates.last().unwrap();
        // With updateLookup, full_document should have the updated doc
        let new_row = update.new.as_ref().expect("update should have new row");
        assert_eq!(
            new_row.get("name"),
            Some(&ColumnValue::Text("Widget".into()))
        );
    }

    // Delete the document
    coll.delete_one(doc! { "_id": "item1" })
        .await
        .expect("failed to delete");

    // Wait for delete event
    timeout(Duration::from_secs(15), async {
        loop {
            {
                let evts = events.lock().unwrap();
                if evts.iter().any(|e| e.op == ChangeOp::Delete) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timed out waiting for delete event");

    // Verify delete event
    {
        let evts = events.lock().unwrap();
        let deletes: Vec<&CdcEvent> = evts.iter().filter(|e| e.op == ChangeOp::Delete).collect();
        assert!(!deletes.is_empty(), "expected delete event");
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_resume_from_offset() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("testdb3");
    db.create_collection("resume_coll")
        .await
        .expect("failed to create collection");
    db.run_command(doc! {
        "collMod": "resume_coll",
        "changeStreamPreAndPostImages": { "enabled": true }
    })
    .await
    .expect("failed to enable change stream images");

    let coll = db.collection::<mongodb::bson::Document>("resume_coll");

    // Insert initial doc
    coll.insert_one(doc! { "name": "First" })
        .await
        .expect("failed to insert");

    let conn_str = connection_string(&host, port);

    // Run 1: Snapshot + stream, then stop
    let offset_store = MemoryOffsetStore::new();
    {
        let config = MongodbSourceConfig {
            connection_string: conn_str.clone(),
            database: "testdb3".into(),
            collections: vec!["resume_coll".into()],
        };

        let (sink, events) = CollectorSink::new();
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        let source = MongodbSource::new(config, offset_store.clone());
        let pipeline = Pipeline::new(source, sink, offset_store.clone());

        let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

        // Wait for snapshot
        timeout(Duration::from_secs(15), async {
            loop {
                {
                    let evts = events.lock().unwrap();
                    if evts.iter().any(|e| e.op == ChangeOp::Snapshot) {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for snapshot");

        // Insert while streaming to generate an offset
        coll.insert_one(doc! { "name": "During" })
            .await
            .expect("failed to insert");

        timeout(Duration::from_secs(15), async {
            loop {
                {
                    let evts = events.lock().unwrap();
                    if evts.iter().any(|e| e.op == ChangeOp::Insert) {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for insert");

        shutdown.cancel();
        let _ = timeout(Duration::from_secs(5), handle).await;
    }

    // Verify offset was saved
    let saved_offset = offset_store.load().await.expect("failed to load offset");
    assert!(saved_offset.is_some(), "offset should have been saved");

    // Insert a doc while pipeline is down
    coll.insert_one(doc! { "name": "WhileDown" })
        .await
        .expect("failed to insert while down");

    // Run 2: Resume from offset
    {
        let config = MongodbSourceConfig {
            connection_string: conn_str,
            database: "testdb3".into(),
            collections: vec!["resume_coll".into()],
        };

        let (sink, events) = CollectorSink::new();
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        let source = MongodbSource::new(config, offset_store.clone());
        let pipeline = Pipeline::new(source, sink, offset_store);

        let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

        // Should receive the "WhileDown" insert without re-snapshotting
        timeout(Duration::from_secs(15), async {
            loop {
                {
                    let evts = events.lock().unwrap();
                    let insert_names: Vec<String> = evts
                        .iter()
                        .filter(|e| e.op == ChangeOp::Insert)
                        .filter_map(|e| {
                            e.new
                                .as_ref()
                                .and_then(|r| r.get("name"))
                                .and_then(|v| match v {
                                    ColumnValue::Text(s) => Some(s.clone()),
                                    _ => None,
                                })
                        })
                        .collect();
                    if insert_names.contains(&"WhileDown".to_string()) {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for WhileDown insert after resume");

        // Verify no snapshot events (we resumed, not re-snapshotted)
        {
            let evts = events.lock().unwrap();
            let snapshot_count = evts.iter().filter(|e| e.op == ChangeOp::Snapshot).count();
            assert_eq!(
                snapshot_count, 0,
                "should not have snapshot events after resume"
            );
        }

        shutdown.cancel();
        let _ = timeout(Duration::from_secs(5), handle).await;
    }
}
