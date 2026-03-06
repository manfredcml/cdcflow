//! Integration tests for the PostgreSQL sink.
//!
//! These tests require Docker to be running. Run with:
//!   cargo test -- --ignored
//!
//! Each test spins up a PostgreSQL container via testcontainers.

use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls};

use cdcflow::config::{PostgresSinkConfig, SinkMode, SourceConnectionConfig};
use cdcflow::event::{CdcEvent, ChangeOp, ColumnValue, Lsn, TableId};
use cdcflow::sink::postgres::PostgresSink;
use cdcflow::sink::Sink;

/// Start a plain PostgreSQL container (no wal_level=logical needed for sink).
async fn start_postgres() -> (ContainerAsync<Postgres>, String, u16) {
    let container = Postgres::default()
        .with_tag("18-alpine")
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

/// Connect a regular tokio-postgres client for verification queries.
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

fn make_sink_config(host: &str, port: u16) -> PostgresSinkConfig {
    PostgresSinkConfig {
        host: host.into(),
        port,
        user: "postgres".into(),
        password: Some("postgres".into()),
        database: "postgres".into(),
        schema: "public".into(),
        table_prefix: "".into(),
    }
}

fn make_insert_event(schema: &str, table: &str, columns: Vec<(&str, ColumnValue)>) -> CdcEvent {
    CdcEvent {
        lsn: Lsn(100),
        timestamp_us: 1_700_000_000_000_000,
        xid: 1,
        table: TableId {
            schema: schema.into(),
            name: table.into(),
            oid: 0,
        },
        op: ChangeOp::Insert,
        new: Some(
            columns
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        ),
        old: None,
        primary_key_columns: vec![],
    }
}

fn make_update_event(
    schema: &str,
    table: &str,
    new_columns: Vec<(&str, ColumnValue)>,
    old_columns: Vec<(&str, ColumnValue)>,
) -> CdcEvent {
    CdcEvent {
        lsn: Lsn(200),
        timestamp_us: 1_700_000_000_000_001,
        xid: 2,
        table: TableId {
            schema: schema.into(),
            name: table.into(),
            oid: 0,
        },
        op: ChangeOp::Update,
        new: Some(
            new_columns
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        ),
        old: Some(
            old_columns
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        ),
        primary_key_columns: vec![],
    }
}

fn make_delete_event(schema: &str, table: &str, old_columns: Vec<(&str, ColumnValue)>) -> CdcEvent {
    CdcEvent {
        lsn: Lsn(300),
        timestamp_us: 1_700_000_000_000_002,
        xid: 3,
        table: TableId {
            schema: schema.into(),
            name: table.into(),
            oid: 0,
        },
        op: ChangeOp::Delete,
        new: None,
        old: Some(
            old_columns
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        ),
        primary_key_columns: vec![],
    }
}

fn make_truncate_event(schema: &str, table: &str) -> CdcEvent {
    CdcEvent {
        lsn: Lsn(400),
        timestamp_us: 1_700_000_000_000_003,
        xid: 4,
        table: TableId {
            schema: schema.into(),
            name: table.into(),
            oid: 0,
        },
        op: ChangeOp::Truncate,
        new: None,
        old: None,
        primary_key_columns: vec![],
    }
}

// ─────────────────────────────────────────────────
// CDC Mode Tests
// ─────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_cdc_mode_insert_batch() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pre-create the CDC table with 3-column JSONB schema.
    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"users\" (metadata JSONB NOT NULL, \"new\" JSONB, \"old\" JSONB)",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let source_conn = SourceConnectionConfig::Postgres {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
    };
    let mut sink = PostgresSink::new(config, SinkMode::Cdc, Some(source_conn))
        .await
        .unwrap();

    let events = vec![
        make_insert_event(
            "public",
            "users",
            vec![
                ("id", ColumnValue::Int(1)),
                ("name", ColumnValue::Text("Alice".into())),
            ],
        ),
        make_insert_event(
            "public",
            "users",
            vec![
                ("id", ColumnValue::Int(2)),
                ("name", ColumnValue::Text("Bob".into())),
            ],
        ),
    ];

    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Verify via direct query — metadata and row are JSONB columns.
    let rows = client
        .query(
            "SELECT metadata, \"new\", \"old\" FROM \"public\".\"users\" ORDER BY (metadata->>'lsn')::bigint",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);

    let metadata: serde_json::Value = rows[0].get("metadata");
    assert_eq!(metadata["op"], "I");
    assert_eq!(metadata["lsn"], 100);
    assert_eq!(metadata["snapshot"], false);

    let new_json: serde_json::Value = rows[0].get("new");
    assert_eq!(new_json["values"]["id"], 1);
    assert_eq!(new_json["values"]["name"], "Alice");
    assert_eq!(new_json["types"]["id"], "Int");
    assert_eq!(new_json["types"]["name"], "Text");

    let old_json: Option<serde_json::Value> = rows[0].get("old");
    assert!(old_json.is_none());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_cdc_mode_mixed_operations() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pre-create the CDC table with 3-column JSONB schema.
    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"orders\" (metadata JSONB NOT NULL, \"new\" JSONB, \"old\" JSONB)",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let source_conn = SourceConnectionConfig::Postgres {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
    };
    let mut sink = PostgresSink::new(config, SinkMode::Cdc, Some(source_conn))
        .await
        .unwrap();

    let events = vec![
        make_insert_event("public", "orders", vec![("id", ColumnValue::Int(1))]),
        make_delete_event("public", "orders", vec![("id", ColumnValue::Int(1))]),
    ];

    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    let rows = client
        .query(
            "SELECT metadata FROM \"public\".\"orders\" ORDER BY (metadata->>'lsn')::bigint",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    let meta0: serde_json::Value = rows[0].get("metadata");
    let meta1: serde_json::Value = rows[1].get("metadata");
    assert_eq!(meta0["op"], "I");
    assert_eq!(meta1["op"], "D");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_cdc_mode_snapshot_flag() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pre-create the CDC table with 3-column JSONB schema.
    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"snap_table\" (metadata JSONB NOT NULL, \"new\" JSONB, \"old\" JSONB)",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let source_conn = SourceConnectionConfig::Postgres {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
    };
    let mut sink = PostgresSink::new(config, SinkMode::Cdc, Some(source_conn))
        .await
        .unwrap();

    let mut event = make_insert_event("public", "snap_table", vec![("id", ColumnValue::Int(1))]);
    event.op = ChangeOp::Snapshot;

    sink.write_batch(&[event]).await.unwrap();
    sink.flush().await.unwrap();

    let row = client
        .query_one("SELECT metadata FROM \"public\".\"snap_table\"", &[])
        .await
        .unwrap();

    let metadata: serde_json::Value = row.get("metadata");
    assert_eq!(metadata["op"], "S");
    assert_eq!(metadata["snapshot"], true);
}

// ─────────────────────────────────────────────────
// Replication Mode Tests
// ─────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_replication_mode_upsert() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pre-create the target table with a PK.
    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"users\" (
                \"id\" TEXT NOT NULL,
                \"name\" TEXT,
                \"email\" TEXT,
                PRIMARY KEY (\"id\")
            )",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let mut sink = PostgresSink::new(config, SinkMode::Replication, None)
        .await
        .unwrap();

    // Insert a row
    let insert_event = make_insert_event(
        "public",
        "users",
        vec![
            ("id", ColumnValue::Int(1)),
            ("name", ColumnValue::Text("Alice".into())),
            ("email", ColumnValue::Text("alice@example.com".into())),
        ],
    );
    sink.write_batch(&[insert_event]).await.unwrap();
    sink.flush().await.unwrap();

    // Verify insert
    let rows = client
        .query("SELECT * FROM \"public\".\"users\"", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: Option<&str> = rows[0].get("name");
    assert_eq!(name, Some("Alice"));

    // Update the row (upsert)
    let update_event = make_update_event(
        "public",
        "users",
        vec![
            ("id", ColumnValue::Int(1)),
            ("name", ColumnValue::Text("Alice Updated".into())),
            ("email", ColumnValue::Text("alice-new@example.com".into())),
        ],
        vec![("id", ColumnValue::Int(1))],
    );
    sink.write_batch(&[update_event]).await.unwrap();
    sink.flush().await.unwrap();

    // Verify upsert — still 1 row, but with updated name
    let rows = client
        .query("SELECT * FROM \"public\".\"users\"", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: Option<&str> = rows[0].get("name");
    assert_eq!(name, Some("Alice Updated"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_replication_mode_delete() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"products\" (
                \"id\" TEXT NOT NULL,
                \"name\" TEXT,
                PRIMARY KEY (\"id\")
            )",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let mut sink = PostgresSink::new(config, SinkMode::Replication, None)
        .await
        .unwrap();

    // Insert two rows
    let events = vec![
        make_insert_event(
            "public",
            "products",
            vec![
                ("id", ColumnValue::Int(1)),
                ("name", ColumnValue::Text("Widget".into())),
            ],
        ),
        make_insert_event(
            "public",
            "products",
            vec![
                ("id", ColumnValue::Int(2)),
                ("name", ColumnValue::Text("Gadget".into())),
            ],
        ),
    ];
    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Verify 2 rows
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"products\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);

    // Delete one row
    let delete_event = make_delete_event("public", "products", vec![("id", ColumnValue::Int(1))]);
    sink.write_batch(&[delete_event]).await.unwrap();
    sink.flush().await.unwrap();

    // Verify 1 row remaining
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"products\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);

    let remaining = client
        .query_one("SELECT \"name\" FROM \"public\".\"products\"", &[])
        .await
        .unwrap();
    let name: Option<&str> = remaining.get("name");
    assert_eq!(name, Some("Gadget"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_replication_mode_truncate() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"logs\" (
                \"id\" TEXT NOT NULL,
                \"msg\" TEXT,
                PRIMARY KEY (\"id\")
            )",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let mut sink = PostgresSink::new(config, SinkMode::Replication, None)
        .await
        .unwrap();

    // Insert rows
    let events = vec![
        make_insert_event(
            "public",
            "logs",
            vec![
                ("id", ColumnValue::Int(1)),
                ("msg", ColumnValue::Text("a".into())),
            ],
        ),
        make_insert_event(
            "public",
            "logs",
            vec![
                ("id", ColumnValue::Int(2)),
                ("msg", ColumnValue::Text("b".into())),
            ],
        ),
    ];
    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Verify 2 rows
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"logs\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);

    // Truncate
    sink.write_batch(&[make_truncate_event("public", "logs")])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    // Verify 0 rows
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"logs\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_cdc_mode_multi_table_batch() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pre-create both CDC tables with 3-column JSONB schema.
    let client = connect_client(&host, port).await;
    let cdc_ddl = "(metadata JSONB NOT NULL, \"new\" JSONB, \"old\" JSONB)";
    client
        .execute(
            &format!("CREATE TABLE \"public\".\"table_a\" {cdc_ddl}"),
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            &format!("CREATE TABLE \"public\".\"table_b\" {cdc_ddl}"),
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let source_conn = SourceConnectionConfig::Postgres {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
    };
    let mut sink = PostgresSink::new(config, SinkMode::Cdc, Some(source_conn))
        .await
        .unwrap();

    // Single batch with events for two different tables
    let events = vec![
        make_insert_event("public", "table_a", vec![("id", ColumnValue::Int(1))]),
        make_insert_event("public", "table_b", vec![("id", ColumnValue::Int(2))]),
        make_insert_event("public", "table_a", vec![("id", ColumnValue::Int(3))]),
    ];
    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    let count_a: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"table_a\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count_a, 2);

    let count_b: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"table_b\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count_b, 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_replication_mode_composite_pk() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"tenant_users\" (
                \"tenant_id\" TEXT NOT NULL,
                \"user_id\" TEXT NOT NULL,
                \"name\" TEXT,
                PRIMARY KEY (\"tenant_id\", \"user_id\")
            )",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let mut sink = PostgresSink::new(config, SinkMode::Replication, None)
        .await
        .unwrap();

    // Insert rows with composite PK
    let events = vec![
        make_insert_event(
            "public",
            "tenant_users",
            vec![
                ("tenant_id", ColumnValue::Text("t1".into())),
                ("user_id", ColumnValue::Text("u1".into())),
                ("name", ColumnValue::Text("Alice".into())),
            ],
        ),
        make_insert_event(
            "public",
            "tenant_users",
            vec![
                ("tenant_id", ColumnValue::Text("t1".into())),
                ("user_id", ColumnValue::Text("u2".into())),
                ("name", ColumnValue::Text("Bob".into())),
            ],
        ),
    ];
    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Verify 2 rows
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"tenant_users\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);

    // Upsert row with same composite PK
    let update_event = make_update_event(
        "public",
        "tenant_users",
        vec![
            ("tenant_id", ColumnValue::Text("t1".into())),
            ("user_id", ColumnValue::Text("u1".into())),
            ("name", ColumnValue::Text("Alice Updated".into())),
        ],
        vec![
            ("tenant_id", ColumnValue::Text("t1".into())),
            ("user_id", ColumnValue::Text("u1".into())),
        ],
    );
    sink.write_batch(&[update_event]).await.unwrap();
    sink.flush().await.unwrap();

    // Still 2 rows, but Alice is updated
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"tenant_users\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);

    let row = client
        .query_one(
            "SELECT \"name\" FROM \"public\".\"tenant_users\" WHERE \"tenant_id\" = 't1' AND \"user_id\" = 'u1'",
            &[],
        )
        .await
        .unwrap();
    let name: Option<&str> = row.get("name");
    assert_eq!(name, Some("Alice Updated"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_cdc_mode_table_prefix() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pre-create the CDC table with the prefixed name and 3-column JSONB schema.
    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"cdc_events\" (metadata JSONB NOT NULL, \"new\" JSONB, \"old\" JSONB)",
            &[],
        )
        .await
        .unwrap();

    let mut config = make_sink_config(&host, port);
    config.table_prefix = "cdc_".into();
    let source_conn = SourceConnectionConfig::Postgres {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
    };
    let mut sink = PostgresSink::new(config, SinkMode::Cdc, Some(source_conn))
        .await
        .unwrap();

    let events = vec![make_insert_event(
        "public",
        "events",
        vec![("id", ColumnValue::Int(1))],
    )];
    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Table should be named "cdc_events", not "events"
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM \"public\".\"cdc_events\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);

    // Original table name should not exist
    let exists = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'events')",
            &[],
        )
        .await
        .unwrap();
    let exists: bool = exists.get(0);
    assert!(!exists);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_replication_mode_schema_evolution() {
    let (_container, host, port) = start_postgres().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pre-create table with (id, name) only.
    let client = connect_client(&host, port).await;
    client
        .execute(
            "CREATE TABLE \"public\".\"users\" (
                \"id\" TEXT NOT NULL,
                \"name\" TEXT,
                PRIMARY KEY (\"id\")
            )",
            &[],
        )
        .await
        .unwrap();

    let config = make_sink_config(&host, port);
    let source_conn = SourceConnectionConfig::Postgres {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
    };
    let mut sink = PostgresSink::new(config, SinkMode::Replication, Some(source_conn))
        .await
        .unwrap();

    // 1. Insert with (id, name) — works as before.
    let e1 = make_insert_event(
        "public",
        "users",
        vec![
            ("id", ColumnValue::Text("1".into())),
            ("name", ColumnValue::Text("Alice".into())),
        ],
    );
    sink.write_batch(&[e1]).await.unwrap();
    sink.flush().await.unwrap();

    let rows = client
        .query("SELECT * FROM \"public\".\"users\"", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: Option<&str> = rows[0].get("name");
    assert_eq!(name, Some("Alice"));

    // 2. Insert with (id, name, email) — triggers ALTER TABLE ADD COLUMN "email".
    let e2 = make_insert_event(
        "public",
        "users",
        vec![
            ("id", ColumnValue::Text("2".into())),
            ("name", ColumnValue::Text("Bob".into())),
            ("email", ColumnValue::Text("bob@example.com".into())),
        ],
    );
    sink.write_batch(&[e2]).await.unwrap();
    sink.flush().await.unwrap();

    // Verify: table now has 3 columns, new row has email, old row has NULL email.
    let col_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'users'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(col_count, 3);

    let rows = client
        .query(
            "SELECT \"id\", \"name\", \"email\" FROM \"public\".\"users\" ORDER BY \"id\"",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // Row 1 (Alice): email should be NULL.
    let email1: Option<&str> = rows[0].get("email");
    assert_eq!(email1, None);
    // Row 2 (Bob): email should be present.
    let email2: Option<&str> = rows[1].get("email");
    assert_eq!(email2, Some("bob@example.com"));

    // 3. Update with (id, name, email, phone) — triggers ADD COLUMN "phone".
    let e3 = make_update_event(
        "public",
        "users",
        vec![
            ("id", ColumnValue::Text("2".into())),
            ("name", ColumnValue::Text("Bob Updated".into())),
            ("email", ColumnValue::Text("bob-new@example.com".into())),
            ("phone", ColumnValue::Text("555-1234".into())),
        ],
        vec![("id", ColumnValue::Text("2".into()))],
    );
    sink.write_batch(&[e3]).await.unwrap();
    sink.flush().await.unwrap();

    // Verify: table now has 4 columns, values correct.
    let col_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'users'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(col_count, 4);

    let rows = client
        .query(
            "SELECT \"id\", \"name\", \"email\", \"phone\" FROM \"public\".\"users\" ORDER BY \"id\"",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // Row 1 (Alice): phone should be NULL.
    let phone1: Option<&str> = rows[0].get("phone");
    assert_eq!(phone1, None);
    // Row 2 (Bob): updated values.
    let name2: Option<&str> = rows[1].get("name");
    assert_eq!(name2, Some("Bob Updated"));
    let email2: Option<&str> = rows[1].get("email");
    assert_eq!(email2, Some("bob-new@example.com"));
    let phone2: Option<&str> = rows[1].get("phone");
    assert_eq!(phone2, Some("555-1234"));
}
