//! Integration tests for the Iceberg sink.
//!
//! These tests require Docker to be running. Run with:
//!   cargo test -- --ignored
//!
//! Each test spins up an Iceberg REST catalog and a PostgreSQL container
//! via testcontainers.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use iceberg::spec::ManifestContentType;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls};

use cdcflow::config::{
    IcebergCatalogConfig, IcebergSinkConfig, RestCatalogConfig, SinkMode, SourceConnectionConfig,
};
use cdcflow::event::{CdcEvent, ChangeOp, ColumnValue, Lsn, TableId};
use cdcflow::sink::iceberg::IcebergSink;
use cdcflow::sink::Sink;

// ---------------------------------------------------------------------------
// Container helpers
// ---------------------------------------------------------------------------

async fn start_iceberg_rest() -> (ContainerAsync<GenericImage>, String, u16) {
    let container = GenericImage::new("apache/iceberg-rest-fixture", "latest")
        .with_exposed_port(ContainerPort::Tcp(8181))
        .with_env_var("CATALOG_WAREHOUSE", "file:///tmp/warehouse")
        .start()
        .await
        .expect("failed to start iceberg-rest-fixture container");

    // Wait for the Jetty server inside the container to be ready.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let port = container
        .get_host_port_ipv4(8181)
        .await
        .expect("failed to get iceberg port");

    let host = container
        .get_host()
        .await
        .expect("failed to get host")
        .to_string();

    (container, host, port)
}

async fn start_postgres() -> (ContainerAsync<Postgres>, String, u16) {
    let container = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("failed to start postgres container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get postgres port");

    let host = container
        .get_host()
        .await
        .expect("failed to get host")
        .to_string();

    (container, host, port)
}

async fn connect_client(host: &str, port: u16) -> Client {
    let conn_str =
        format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("failed to connect to postgres");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    client
}

/// Create a namespace in the REST catalog via its HTTP API.
async fn create_namespace(host: &str, port: u16, namespace: &str) {
    let url = format!("http://{host}:{port}/v1/namespaces");
    let body = serde_json::json!({ "namespace": [namespace] });

    reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .expect("failed to create namespace");
}

// ---------------------------------------------------------------------------
// Config / event helpers
// ---------------------------------------------------------------------------

fn make_config(iceberg_host: &str, iceberg_port: u16) -> IcebergSinkConfig {
    IcebergSinkConfig {
        catalog: IcebergCatalogConfig::Rest(RestCatalogConfig {
            uri: format!("http://{iceberg_host}:{iceberg_port}"),
            warehouse: None,
            properties: HashMap::new(),
            timeout_secs: 30,
        }),
        namespace: vec!["default".into()],
        table_prefix: "cdc_test_".into(),
    }
}

fn make_table_id(schema: &str, name: &str) -> TableId {
    TableId {
        schema: schema.into(),
        name: name.into(),
        oid: 0,
    }
}

fn make_insert(table_id: TableId, lsn: u64, columns: Vec<(&str, &str)>) -> CdcEvent {
    CdcEvent {
        lsn: Lsn(lsn),
        timestamp_us: 1_700_000_000_000_000,
        xid: 1,
        table: table_id,
        op: ChangeOp::Insert,
        new: Some(
            columns
                .into_iter()
                .map(|(k, v)| (k.into(), ColumnValue::Text(v.into())))
                .collect(),
        ),
        old: None,
        primary_key_columns: vec![],
    }
}

/// Shared setup: starts both containers, creates source table(s), creates namespace.
struct TestEnv {
    _iceberg: ContainerAsync<GenericImage>,
    _postgres: ContainerAsync<Postgres>,
    iceberg_host: String,
    iceberg_port: u16,
    pg_host: String,
    pg_port: u16,
}

impl TestEnv {
    async fn start(source_tables_ddl: &[&str]) -> Self {
        let (iceberg, iceberg_host, iceberg_port) = start_iceberg_rest().await;
        let (postgres, pg_host, pg_port) = start_postgres().await;

        // Give Postgres a moment to accept connections.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let client = connect_client(&pg_host, pg_port).await;
        for ddl in source_tables_ddl {
            client
                .batch_execute(ddl)
                .await
                .expect("failed to create source table");
        }

        create_namespace(&iceberg_host, iceberg_port, "default").await;

        Self {
            _iceberg: iceberg,
            _postgres: postgres,
            iceberg_host,
            iceberg_port,
            pg_host,
            pg_port,
        }
    }

    fn sink_config(&self) -> IcebergSinkConfig {
        make_config(&self.iceberg_host, self.iceberg_port)
    }

    fn source_connection(&self) -> SourceConnectionConfig {
        SourceConnectionConfig::Postgres {
            url: format!(
                "postgres://postgres:postgres@{}:{}/postgres",
                self.pg_host, self.pg_port
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test that the Iceberg sink handles all change kinds: Insert, Update, Delete, Truncate.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_iceberg_sink_all_change_kinds() {
    let env = TestEnv::start(&["CREATE TABLE public.events_table (id INT, data TEXT)"]).await;

    let config = env.sink_config();
    let source_conn = env.source_connection();
    let mut sink = IcebergSink::new(config, SinkMode::Cdc, source_conn)
        .await
        .unwrap();

    let table_id = make_table_id("public", "events_table");

    let events = vec![
        CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_700_000_000_000_000,
            xid: 1,
            table: table_id.clone(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("data".into(), ColumnValue::Text("initial".into())),
            ])),
            old: None,
            primary_key_columns: vec![],
        },
        CdcEvent {
            lsn: Lsn(200),
            timestamp_us: 1_700_000_000_000_001,
            xid: 2,
            table: table_id.clone(),
            op: ChangeOp::Update,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("data".into(), ColumnValue::Text("updated".into())),
            ])),
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("data".into(), ColumnValue::Text("initial".into())),
            ])),
            primary_key_columns: vec![],
        },
        CdcEvent {
            lsn: Lsn(300),
            timestamp_us: 1_700_000_000_000_002,
            xid: 3,
            table: table_id.clone(),
            op: ChangeOp::Delete,
            new: None,
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("data".into(), ColumnValue::Text("updated".into())),
            ])),
            primary_key_columns: vec![],
        },
        CdcEvent {
            lsn: Lsn(400),
            timestamp_us: 1_700_000_000_000_003,
            xid: 4,
            table: table_id.clone(),
            op: ChangeOp::Truncate,
            new: None,
            old: None,
            primary_key_columns: vec![],
        },
    ];

    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();
}

// ---------------------------------------------------------------------------
// Replication mode helpers
// ---------------------------------------------------------------------------

async fn build_test_catalog(env: &TestEnv) -> impl Catalog {
    let mut props = HashMap::new();
    props.insert(
        "uri".to_string(),
        format!("http://{}:{}", env.iceberg_host, env.iceberg_port),
    );
    RestCatalogBuilder::default()
        .load("rest", props)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Replication mode integration tests
// ---------------------------------------------------------------------------

/// Test replication mode with insert-only events (fast_append path).
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_iceberg_replication_insert_only() {
    let env = TestEnv::start(&[
        "CREATE TABLE public.rep_insert (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    ])
    .await;

    let config = env.sink_config();
    let source_conn = env.source_connection();
    let mut sink = IcebergSink::new(config, SinkMode::Replication, source_conn)
        .await
        .unwrap();

    let table_id = make_table_id("public", "rep_insert");
    let events = vec![
        make_insert(table_id.clone(), 100, vec![("id", "1"), ("name", "Alice")]),
        make_insert(table_id.clone(), 200, vec![("id", "2"), ("name", "Bob")]),
    ];

    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Verify: load table via catalog and check snapshot exists
    let catalog = build_test_catalog(&env).await;
    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    let ident = TableIdent::new(ns, "cdc_test_rep_insert".to_string());
    let table = catalog.load_table(&ident).await.unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .unwrap();
    // Insert-only: should have data manifests only, no delete manifests
    assert!(manifest_list
        .entries()
        .iter()
        .all(|m| m.content == ManifestContentType::Data));
    assert!(!manifest_list.entries().is_empty());
}

/// Test replication mode: insert rows, then delete some via row_delta commit.
/// Verifies that delete manifests are committed in the snapshot.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_iceberg_replication_insert_then_delete() {
    let env = TestEnv::start(&[
        "CREATE TABLE public.rep_del (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    ])
    .await;

    let config = env.sink_config();
    let source_conn = env.source_connection();
    let mut sink = IcebergSink::new(config, SinkMode::Replication, source_conn)
        .await
        .unwrap();

    let table_id = make_table_id("public", "rep_del");

    // Batch 1: inserts
    let inserts = vec![
        make_insert(table_id.clone(), 100, vec![("id", "1"), ("name", "Alice")]),
        make_insert(table_id.clone(), 200, vec![("id", "2"), ("name", "Bob")]),
        make_insert(table_id.clone(), 300, vec![("id", "3"), ("name", "Carol")]),
    ];
    sink.write_batch(&inserts).await.unwrap();

    // Batch 2: delete id=2
    let deletes = vec![CdcEvent {
        lsn: Lsn(400),
        timestamp_us: 1_700_000_000_000_000,
        xid: 2,
        table: table_id.clone(),
        op: ChangeOp::Delete,
        new: None,
        old: Some(BTreeMap::from([
            ("id".into(), ColumnValue::Text("2".into())),
            ("name".into(), ColumnValue::Text("Bob".into())),
        ])),
        primary_key_columns: vec![],
    }];
    sink.write_batch(&deletes).await.unwrap();
    sink.flush().await.unwrap();

    // Verify: snapshot should contain both data and delete manifests
    let catalog = build_test_catalog(&env).await;
    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    let ident = TableIdent::new(ns, "cdc_test_rep_del".to_string());
    let table = catalog.load_table(&ident).await.unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .unwrap();

    let has_data = manifest_list
        .entries()
        .iter()
        .any(|m| m.content == ManifestContentType::Data);
    let has_deletes = manifest_list
        .entries()
        .iter()
        .any(|m| m.content == ManifestContentType::Deletes);
    assert!(has_data, "should have data manifests");
    assert!(has_deletes, "should have delete manifests");
}

/// Test replication mode: update events produce both data files (new values)
/// and equality delete files (old PKs) in a single atomic commit.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_iceberg_replication_update() {
    let env = TestEnv::start(&[
        "CREATE TABLE public.rep_upd (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    ])
    .await;

    let config = env.sink_config();
    let source_conn = env.source_connection();
    let mut sink = IcebergSink::new(config, SinkMode::Replication, source_conn)
        .await
        .unwrap();

    let table_id = make_table_id("public", "rep_upd");

    // Batch 1: insert
    let inserts = vec![make_insert(
        table_id.clone(),
        100,
        vec![("id", "1"), ("name", "Alice")],
    )];
    sink.write_batch(&inserts).await.unwrap();

    // Batch 2: update name "Alice" → "Alicia"
    let updates = vec![CdcEvent {
        lsn: Lsn(200),
        timestamp_us: 1_700_000_000_000_001,
        xid: 2,
        table: table_id.clone(),
        op: ChangeOp::Update,
        new: Some(BTreeMap::from([
            ("id".into(), ColumnValue::Text("1".into())),
            ("name".into(), ColumnValue::Text("Alicia".into())),
        ])),
        old: Some(BTreeMap::from([
            ("id".into(), ColumnValue::Text("1".into())),
            ("name".into(), ColumnValue::Text("Alice".into())),
        ])),
        primary_key_columns: vec![],
    }];
    sink.write_batch(&updates).await.unwrap();
    sink.flush().await.unwrap();

    // Verify: latest snapshot has both data and delete manifests
    let catalog = build_test_catalog(&env).await;
    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    let ident = TableIdent::new(ns, "cdc_test_rep_upd".to_string());
    let table = catalog.load_table(&ident).await.unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .unwrap();

    let has_data = manifest_list
        .entries()
        .iter()
        .any(|m| m.content == ManifestContentType::Data);
    let has_deletes = manifest_list
        .entries()
        .iter()
        .any(|m| m.content == ManifestContentType::Deletes);
    assert!(has_data, "update should produce data manifest (new values)");
    assert!(
        has_deletes,
        "update should produce delete manifest (old PKs)"
    );
}

/// Test replication mode: mixed batch with inserts, updates, and deletes.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_iceberg_replication_mixed_batch() {
    let env =
        TestEnv::start(&["CREATE TABLE public.rep_mix (id SERIAL PRIMARY KEY, val TEXT NOT NULL)"])
            .await;

    let config = env.sink_config();
    let source_conn = env.source_connection();
    let mut sink = IcebergSink::new(config, SinkMode::Replication, source_conn)
        .await
        .unwrap();

    let table_id = make_table_id("public", "rep_mix");

    // Single batch: insert id=1, insert id=2, update id=1, delete id=2
    let events = vec![
        make_insert(table_id.clone(), 100, vec![("id", "1"), ("val", "a")]),
        make_insert(table_id.clone(), 200, vec![("id", "2"), ("val", "b")]),
        CdcEvent {
            lsn: Lsn(300),
            timestamp_us: 1_700_000_000_000_002,
            xid: 3,
            table: table_id.clone(),
            op: ChangeOp::Update,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("val".into(), ColumnValue::Text("a_updated".into())),
            ])),
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("val".into(), ColumnValue::Text("a".into())),
            ])),
            primary_key_columns: vec![],
        },
        CdcEvent {
            lsn: Lsn(400),
            timestamp_us: 1_700_000_000_000_003,
            xid: 4,
            table: table_id.clone(),
            op: ChangeOp::Delete,
            new: None,
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("2".into())),
                ("val".into(), ColumnValue::Text("b".into())),
            ])),
            primary_key_columns: vec![],
        },
    ];

    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Verify
    let catalog = build_test_catalog(&env).await;
    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    let ident = TableIdent::new(ns, "cdc_test_rep_mix".to_string());
    let table = catalog.load_table(&ident).await.unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .unwrap();

    let has_data = manifest_list
        .entries()
        .iter()
        .any(|m| m.content == ManifestContentType::Data);
    let has_deletes = manifest_list
        .entries()
        .iter()
        .any(|m| m.content == ManifestContentType::Deletes);
    assert!(has_data, "mixed batch should have data manifests");
    assert!(has_deletes, "mixed batch should have delete manifests");
}

/// Test replication mode: multiple sequential batches, verify manifests accumulate.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_iceberg_replication_multi_batch() {
    let env = TestEnv::start(&[
        "CREATE TABLE public.rep_multi (id SERIAL PRIMARY KEY, data TEXT NOT NULL)",
    ])
    .await;

    let config = env.sink_config();
    let source_conn = env.source_connection();
    let mut sink = IcebergSink::new(config, SinkMode::Replication, source_conn)
        .await
        .unwrap();

    let table_id = make_table_id("public", "rep_multi");

    // Batch 1: insert
    sink.write_batch(&[make_insert(
        table_id.clone(),
        100,
        vec![("id", "1"), ("data", "first")],
    )])
    .await
    .unwrap();

    // Batch 2: insert more
    sink.write_batch(&[make_insert(
        table_id.clone(),
        200,
        vec![("id", "2"), ("data", "second")],
    )])
    .await
    .unwrap();

    // Batch 3: update id=1 (triggers row_delta path)
    sink.write_batch(&[CdcEvent {
        lsn: Lsn(300),
        timestamp_us: 1_700_000_000_000_002,
        xid: 3,
        table: table_id.clone(),
        op: ChangeOp::Update,
        new: Some(BTreeMap::from([
            ("id".into(), ColumnValue::Text("1".into())),
            ("data".into(), ColumnValue::Text("updated".into())),
        ])),
        old: Some(BTreeMap::from([
            ("id".into(), ColumnValue::Text("1".into())),
            ("data".into(), ColumnValue::Text("first".into())),
        ])),
        primary_key_columns: vec![],
    }])
    .await
    .unwrap();

    sink.flush().await.unwrap();

    // Verify: table should have 3 snapshots, latest should include all prior manifests
    let catalog = build_test_catalog(&env).await;
    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    let ident = TableIdent::new(ns, "cdc_test_rep_multi".to_string());
    let table = catalog.load_table(&ident).await.unwrap();

    // Should have at least 3 snapshots
    let snapshot_count = table.metadata().snapshots().count();
    assert!(
        snapshot_count >= 3,
        "expected at least 3 snapshots, got {snapshot_count}"
    );

    // Latest snapshot manifest list should carry forward all previous manifests
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .unwrap();

    // Should have: 2 data manifests from batch 1+2, 1 data + 1 delete from batch 3
    // = at least 3 data manifests and 1 delete manifest
    let data_count = manifest_list
        .entries()
        .iter()
        .filter(|m| m.content == ManifestContentType::Data)
        .count();
    let delete_count = manifest_list
        .entries()
        .iter()
        .filter(|m| m.content == ManifestContentType::Deletes)
        .count();
    assert!(
        data_count >= 3,
        "expected at least 3 data manifests, got {data_count}"
    );
    assert!(
        delete_count >= 1,
        "expected at least 1 delete manifest, got {delete_count}"
    );
}
