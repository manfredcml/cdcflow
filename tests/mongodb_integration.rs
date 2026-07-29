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

use cdcflow::config::{MongodbSourceConfig, PostgresSinkConfig, SinkMode, SourceConnectionConfig};
use cdcflow::error::Result;
use cdcflow::event::{CdcEvent, ChangeOp, ColumnValue};
use cdcflow::offset::memory::MemoryOffsetStore;
use cdcflow::offset::OffsetStore;
use cdcflow::pipeline::Pipeline;
use cdcflow::sink::postgres::PostgresSink;
use cdcflow::sink::Sink;
use cdcflow::source::mongodb::offset::{ClusterTime, MongoOffset};
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
        .with_tag("8")
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
            if let Ok(members) = status.get_array("members") {
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

/// Poll `check` until it returns true or the timeout expires.
async fn wait_until<F>(secs: u64, label: &str, mut check: F)
where
    F: FnMut() -> bool,
{
    let result = timeout(Duration::from_secs(secs), async {
        loop {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "timed out waiting for {label}");
}

/// Without pre-images (the MongoDB default), update and delete must still
/// carry `_id` in `old` so sinks can target the row.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_update_and_delete_without_pre_images() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("no_preimage_db");
    // Deliberately no collMod: pre-images stay disabled.
    db.create_collection("items")
        .await
        .expect("failed to create collection");
    let coll = db.collection::<mongodb::bson::Document>("items");
    coll.insert_one(doc! { "_id": "item1", "name": "Widget", "price": 10.0 })
        .await
        .expect("failed to insert");

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "no_preimage_db".into(),
        collections: vec!["items".into()],
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    {
        let events = events.clone();
        wait_until(30, "snapshot", move || {
            events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Snapshot)
        })
        .await;
    }

    coll.update_one(doc! { "_id": "item1" }, doc! { "$set": { "price": 15.0 } })
        .await
        .expect("failed to update");

    {
        let events = events.clone();
        wait_until(20, "update event", move || {
            events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Update)
        })
        .await;
    }

    {
        let evts = events.lock().unwrap();
        let update = evts.iter().find(|e| e.op == ChangeOp::Update).unwrap();
        let old = update
            .old
            .as_ref()
            .expect("update must carry an old row built from documentKey");
        assert_eq!(old.get("_id"), Some(&ColumnValue::Text("item1".into())));
    }

    coll.delete_one(doc! { "_id": "item1" })
        .await
        .expect("failed to delete");

    {
        let events = events.clone();
        wait_until(20, "delete event", move || {
            events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Delete)
        })
        .await;
    }

    {
        let evts = events.lock().unwrap();
        let delete = evts.iter().find(|e| e.op == ChangeOp::Delete).unwrap();
        let old = delete
            .old
            .as_ref()
            .expect("delete must carry an old row built from documentKey");
        assert_eq!(old.get("_id"), Some(&ColumnValue::Text("item1".into())));
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), handle).await;
}

/// Current cluster time — what a completed snapshot records in its offset.
async fn current_cluster_time(client: &mongodb::Client, database: &str) -> ClusterTime {
    let ping = client
        .database(database)
        .run_command(doc! { "ping": 1 })
        .await
        .expect("ping failed");
    let ts = ping
        .get("operationTime")
        .and_then(|v| v.as_timestamp())
        .expect("ping response missing operationTime");
    ClusterTime::from(ts)
}

/// A `snapshot_complete` offset resumes from its cluster time, no re-snapshot.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_resumes_from_snapshot_complete_offset() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("snapshot_only_db");
    db.create_collection("docs")
        .await
        .expect("failed to create collection");
    let coll = db.collection::<mongodb::bson::Document>("docs");
    coll.insert_one(doc! { "name": "BeforeOffset" })
        .await
        .expect("failed to insert");

    // Exactly what the source persists once a snapshot finishes with no change
    // events behind it.
    let cluster_time = current_cluster_time(&client, "snapshot_only_db").await;
    let offset_store = MemoryOffsetStore::with_offset(
        MongoOffset::SnapshotComplete { cluster_time }
            .to_json()
            .unwrap(),
    );

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "snapshot_only_db".into(),
        collections: vec!["docs".into()],
    };
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Written after the recorded cluster time, so it must arrive on the stream.
    tokio::time::sleep(Duration::from_secs(2)).await;
    coll.insert_one(doc! { "name": "AfterOffset" })
        .await
        .expect("failed to insert");

    {
        let events = events.clone();
        wait_until(30, "AfterOffset insert", move || {
            events.lock().unwrap().iter().any(|e| {
                e.op == ChangeOp::Insert
                    && e.new
                        .as_ref()
                        .and_then(|r| r.get("name"))
                        .map(|v| matches!(v, ColumnValue::Text(s) if s == "AfterOffset"))
                        .unwrap_or(false)
            })
        })
        .await;
    }

    {
        let evts = events.lock().unwrap();
        assert_eq!(
            evts.iter().filter(|e| e.op == ChangeOp::Snapshot).count(),
            0,
            "a completed snapshot must not be re-run on restart"
        );
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), handle).await;
}

/// Snapshots are not resumable mid-way, so `snapshot_in_progress` re-runs it.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_reruns_interrupted_snapshot() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("interrupted_db");
    db.create_collection("docs")
        .await
        .expect("failed to create collection");
    db.collection::<mongodb::bson::Document>("docs")
        .insert_many(vec![doc! { "name": "A" }, doc! { "name": "B" }])
        .await
        .expect("failed to insert");

    let offset_store = MemoryOffsetStore::with_offset(
        MongoOffset::SnapshotInProgress {
            collection: "docs".into(),
            count: 1,
        }
        .to_json()
        .unwrap(),
    );

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "interrupted_db".into(),
        collections: vec!["docs".into()],
    };
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    {
        let events = events.clone();
        wait_until(30, "re-run snapshot of both documents", move || {
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.op == ChangeOp::Snapshot)
                .count()
                >= 2
        })
        .await;
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), handle).await;
}

/// An uninterpretable offset must fail loudly, not become a bogus token.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_rejects_unrecognized_offset() {
    let (_container, host, port) = start_mongodb().await;

    let offset_store =
        MemoryOffsetStore::with_offset(r#"{"snapshot_complete":"docs","count":42}"#.to_string());
    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "whatever_db".into(),
        collections: vec![],
    };
    let (sink, _events) = CollectorSink::new();
    let shutdown = CancellationToken::new();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);

    let result = timeout(Duration::from_secs(30), pipeline.run(shutdown))
        .await
        .expect("pipeline should fail fast on an unreadable offset");

    let err = result.expect_err("an unreadable offset must be an error");
    assert!(
        err.to_string().contains("failed to parse stored offset"),
        "unexpected error: {err}"
    );
}

/// An empty `collections` list snapshots and streams every collection in the database.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_all_collections_when_filter_empty() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("all_colls_db");
    for name in ["alpha", "beta"] {
        db.create_collection(name)
            .await
            .expect("failed to create collection");
        db.collection::<mongodb::bson::Document>(name)
            .insert_one(doc! { "name": name })
            .await
            .expect("failed to insert");
    }

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "all_colls_db".into(),
        collections: vec![], // all collections
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    {
        let events = events.clone();
        wait_until(30, "snapshot of both collections", move || {
            let evts = events.lock().unwrap();
            let tables: std::collections::HashSet<String> = evts
                .iter()
                .filter(|e| e.op == ChangeOp::Snapshot)
                .map(|e| e.table.name.clone())
                .collect();
            tables.contains("alpha") && tables.contains("beta")
        })
        .await;
    }

    // A change in either collection must stream through too.
    db.collection::<mongodb::bson::Document>("beta")
        .insert_one(doc! { "name": "beta2" })
        .await
        .expect("failed to insert");

    {
        let events = events.clone();
        wait_until(20, "streamed insert on beta", move || {
            events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Insert && e.table.name == "beta")
        })
        .await;
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), handle).await;
}

/// A non-empty `collections` list must exclude changes from other collections.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_collection_filter_excludes_others() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("filter_db");
    for name in ["watched", "ignored"] {
        db.create_collection(name)
            .await
            .expect("failed to create collection");
    }

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "filter_db".into(),
        collections: vec!["watched".into()],
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Give the change stream time to open before writing.
    tokio::time::sleep(Duration::from_secs(3)).await;

    db.collection::<mongodb::bson::Document>("ignored")
        .insert_one(doc! { "name": "nope" })
        .await
        .expect("failed to insert");
    db.collection::<mongodb::bson::Document>("watched")
        .insert_one(doc! { "name": "yes" })
        .await
        .expect("failed to insert");

    {
        let events = events.clone();
        wait_until(20, "insert on watched collection", move || {
            events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.op == ChangeOp::Insert && e.table.name == "watched")
        })
        .await;
    }

    {
        let evts = events.lock().unwrap();
        assert!(
            evts.iter().all(|e| e.table.name != "ignored"),
            "events from unwatched collections must be filtered out"
        );
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), handle).await;
}

/// Shutdown during a snapshot must stop it promptly, mid-scan.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_shutdown_during_snapshot() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("big_snapshot_db");
    let coll = db.collection::<mongodb::bson::Document>("many");
    let docs: Vec<mongodb::bson::Document> = (0..20_000)
        .map(|i| doc! { "i": i, "payload": "x".repeat(200) })
        .collect();
    coll.insert_many(docs).await.expect("failed to seed docs");

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "big_snapshot_db".into(),
        collections: vec!["many".into()],
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Wait until the snapshot is clearly underway, then cancel mid-scan.
    {
        let events = events.clone();
        wait_until(30, "snapshot to start", move || {
            !events.lock().unwrap().is_empty()
        })
        .await;
    }
    shutdown.cancel();

    let result = timeout(Duration::from_secs(10), handle)
        .await
        .expect("pipeline did not stop within 10s of cancellation");
    result
        .expect("pipeline task panicked")
        .expect("pipeline returned an error");

    let seen = events.lock().unwrap().len();
    assert!(
        seen < 20_000,
        "snapshot should have been interrupted, but emitted all {seen} documents"
    );
}

/// End-to-end replication into PostgreSQL with pre-images disabled.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_to_postgres_replication() {
    use testcontainers_modules::postgres::Postgres;
    use tokio_postgres::NoTls;

    let (_mongo, mongo_host, mongo_port) = start_mongodb().await;
    let pg_container = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("failed to start postgres container");
    let pg_port = pg_container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get pg port");
    let pg_host = pg_container
        .get_host()
        .await
        .expect("failed to get pg host")
        .to_string();

    let mongo_client = get_client(&mongo_host, mongo_port).await;
    let db = mongo_client.database("repl_db");
    db.create_collection("users")
        .await
        .expect("failed to create collection");
    let coll = db.collection::<mongodb::bson::Document>("users");
    coll.insert_many(vec![
        doc! { "_id": "u1", "name": "Alice", "age": 30 },
        doc! { "_id": "u2", "name": "Bob", "age": 25 },
    ])
    .await
    .expect("failed to seed docs");

    let mongo_conn = connection_string(&mongo_host, mongo_port);
    let sink = PostgresSink::new(
        PostgresSinkConfig {
            host: pg_host.clone(),
            port: pg_port,
            user: "postgres".into(),
            password: Some("postgres".into()),
            database: "postgres".into(),
            schema: "public".into(),
            table_prefix: "".into(),
        },
        SinkMode::Replication,
        Some(SourceConnectionConfig::Mongodb {
            url: mongo_conn.clone(),
        }),
    )
    .await
    .expect("failed to create postgres sink");

    let config = MongodbSourceConfig {
        connection_string: mongo_conn,
        database: "repl_db".into(),
        collections: vec!["users".into()],
    };

    let offset_store = MemoryOffsetStore::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Verification client.
    let (pg, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            pg_host, pg_port
        ),
        NoTls,
    )
    .await
    .expect("failed to connect to postgres");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("pg connection error: {e}");
        }
    });

    async fn count_users(pg: &tokio_postgres::Client) -> i64 {
        pg.query_one("SELECT count(*)::bigint FROM public.users", &[])
            .await
            .map(|row| row.get::<_, i64>(0))
            .unwrap_or(-1)
    }

    // Snapshot replicated.
    timeout(Duration::from_secs(60), async {
        loop {
            if count_users(&pg).await == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("timed out waiting for snapshot rows in postgres");

    // Update (no pre-image available) must update the existing row, not add one.
    coll.update_one(doc! { "_id": "u1" }, doc! { "$set": { "age": 31 } })
        .await
        .expect("failed to update");

    timeout(Duration::from_secs(60), async {
        loop {
            let row = pg
                .query_opt("SELECT age FROM public.users WHERE _id = 'u1'", &[])
                .await
                .expect("query failed");
            if let Some(row) = row {
                let age: i64 = match row.try_get::<_, i64>(0) {
                    Ok(v) => v,
                    Err(_) => row.get::<_, i32>(0) as i64,
                };
                if age == 31 && count_users(&pg).await == 2 {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("timed out waiting for the update to replicate");

    // Delete (no pre-image available) must remove the row by its `_id`.
    coll.delete_one(doc! { "_id": "u2" })
        .await
        .expect("failed to delete");

    timeout(Duration::from_secs(60), async {
        loop {
            if count_users(&pg).await == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("timed out waiting for the delete to replicate");

    let remaining: String = pg
        .query_one("SELECT _id FROM public.users", &[])
        .await
        .expect("query failed")
        .get(0);
    assert_eq!(remaining, "u1", "the wrong row was deleted");

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(10), handle).await;
}

/// A missing collection must fail fast, not run forever replicating nothing.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_rejects_unknown_collection() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("typo_db");
    db.create_collection("users")
        .await
        .expect("failed to create collection");

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "typo_db".into(),
        collections: vec!["Users".into()], // wrong case
    };

    let (sink, _events) = CollectorSink::new();
    let offset_store = MemoryOffsetStore::new();
    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store);

    let result = timeout(
        Duration::from_secs(30),
        pipeline.run(CancellationToken::new()),
    )
    .await
    .expect("pipeline should fail fast on an unknown collection");

    let err = result.expect_err("an unknown collection must be an error");
    assert!(
        err.to_string().contains("not found in database typo_db"),
        "unexpected error: {err}"
    );
}

/// Writes to filtered-out collections must still advance the stored offset,
/// otherwise it ages out of the oplog while the watched collection is idle.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_offset_advances_while_watched_collection_idle() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("idle_db");
    for name in ["watched", "busy"] {
        db.create_collection(name)
            .await
            .expect("failed to create collection");
    }

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "idle_db".into(),
        collections: vec!["watched".into()],
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, _events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store.clone());
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    // Let the snapshot finish and the stream open.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let baseline = offset_store.load().await.unwrap();

    // Traffic only on the collection that is filtered out.
    for i in 0..5 {
        db.collection::<mongodb::bson::Document>("busy")
            .insert_one(doc! { "i": i })
            .await
            .expect("failed to insert");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    timeout(Duration::from_secs(30), async {
        loop {
            let current = offset_store.load().await.unwrap();
            if current.is_some() && current != baseline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timed out waiting for the offset to advance past the filtered writes");

    let advanced = offset_store.load().await.unwrap().unwrap();
    assert!(
        matches!(MongoOffset::from_json(&advanced), Ok(MongoOffset::Token(_))),
        "expected a resume token offset, got {advanced}"
    );

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(10), handle).await;
}

/// Dropping the database ends the stream with an `invalidate` event, whose
/// token must never be stored: the server refuses it in `resumeAfter`.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_mongodb_invalidate_token_is_not_persisted() {
    let (_container, host, port) = start_mongodb().await;
    let client = get_client(&host, port).await;

    let db = client.database("dropped_db");
    db.create_collection("docs")
        .await
        .expect("failed to create collection");
    let coll = db.collection::<mongodb::bson::Document>("docs");
    coll.insert_one(doc! { "name": "First" })
        .await
        .expect("failed to insert");

    let config = MongodbSourceConfig {
        connection_string: connection_string(&host, port),
        database: "dropped_db".into(),
        collections: vec![],
    };

    let offset_store = MemoryOffsetStore::new();
    let (sink, events) = CollectorSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let source = MongodbSource::new(config, offset_store.clone());
    let pipeline = Pipeline::new(source, sink, offset_store.clone());
    let handle = tokio::spawn(async move { pipeline.run(shutdown_clone).await });

    {
        let events = events.clone();
        wait_until(30, "snapshot", move || !events.lock().unwrap().is_empty()).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    db.drop().await.expect("failed to drop database");

    let result = timeout(Duration::from_secs(30), handle)
        .await
        .expect("pipeline should stop after the stream is invalidated")
        .expect("pipeline task panicked");
    assert!(
        result.is_err(),
        "a server-closed change stream must surface as an error"
    );
    shutdown.cancel();

    // Whatever was stored must still be usable as a resume point.
    let saved = offset_store
        .load()
        .await
        .unwrap()
        .expect("an offset should have been stored");
    match MongoOffset::from_json(&saved).expect("stored offset must parse") {
        MongoOffset::Token(token) => {
            client
                .database("dropped_db")
                .watch()
                .resume_after(token)
                .await
                .expect("stored resume token must be accepted by the server");
        }
        MongoOffset::SnapshotComplete { .. } | MongoOffset::SnapshotInProgress { .. } => {}
    }
}
