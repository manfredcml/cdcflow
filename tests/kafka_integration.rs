//! Integration tests for the Kafka sink.
//!
//! These tests require Docker to be running. Run with:
//!   cargo test -- --ignored
//!
//! Each test spins up a Kafka container via testcontainers.

use std::collections::BTreeMap;
use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::kafka::apache::Kafka;
use tokio::time::timeout;

use cdcflow::config::KafkaSinkConfig;
use cdcflow::event::{CdcEvent, ChangeOp, ColumnValue, Lsn, TableId};
use cdcflow::sink::kafka::KafkaSink;
use cdcflow::sink::Sink;

/// Start a Kafka container and return (container, bootstrap_servers).
async fn start_kafka() -> (ContainerAsync<Kafka>, String) {
    let container = Kafka::default()
        .with_jvm_image()
        .with_tag("4.1.1")
        .start()
        .await
        .expect("failed to start kafka container");

    let port = container
        .get_host_port_ipv4(9092)
        .await
        .expect("failed to get kafka port");

    let bootstrap_servers = format!("127.0.0.1:{port}");

    // Give Kafka a moment to fully start
    tokio::time::sleep(Duration::from_secs(2)).await;

    (container, bootstrap_servers)
}

fn make_test_event(schema: &str, table_name: &str, id: &str) -> CdcEvent {
    CdcEvent {
        lsn: Lsn::ZERO,
        timestamp_us: 1_700_000_000_000_000,
        xid: 1,
        table: TableId {
            schema: schema.into(),
            name: table_name.into(),
            oid: 0,
        },
        op: ChangeOp::Insert,
        new: Some(BTreeMap::from([(
            "id".into(),
            ColumnValue::Text(id.into()),
        )])),
        old: None,
        primary_key_columns: vec![],
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_kafka_sink_produces_to_correct_topics() {
    let (_container, brokers) = start_kafka().await;

    // Create sink and write events for two different tables
    let config = KafkaSinkConfig {
        brokers: brokers.clone(),
        topic_prefix: "cdc".into(),
        properties: Default::default(),
    };
    let mut sink = KafkaSink::new(config).unwrap();

    let events = vec![
        make_test_event("public", "users", "1"),
        make_test_event("public", "users", "2"),
        make_test_event("public", "orders", "100"),
    ];
    sink.write_batch(&events).await.unwrap();
    sink.flush().await.unwrap();

    // Consume from both topics and verify
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "test-verify-topics")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();

    consumer
        .subscribe(&["cdc.public.users", "cdc.public.orders"])
        .unwrap();

    let mut users_messages = Vec::new();
    let mut orders_messages = Vec::new();

    let consume_result = timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(msg) = tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
                let msg = msg.unwrap();
                let topic = msg.topic().to_string();
                let payload = msg.payload_view::<str>().unwrap().unwrap().to_string();
                if topic == "cdc.public.users" {
                    users_messages.push(payload);
                } else if topic == "cdc.public.orders" {
                    orders_messages.push(payload);
                }
                if users_messages.len() >= 2 && !orders_messages.is_empty() {
                    break;
                }
            }
        }
    })
    .await;

    assert!(consume_result.is_ok(), "failed to consume all messages");
    assert_eq!(users_messages.len(), 2);
    assert_eq!(orders_messages.len(), 1);

    // Verify JSON content — messages are now nested {metadata, new, old}
    let user_json: serde_json::Value = serde_json::from_str(&users_messages[0]).unwrap();
    assert_eq!(user_json["metadata"]["table"], "users");
    assert_eq!(user_json["metadata"]["schema"], "public");
    assert_eq!(user_json["metadata"]["op"], "I");
    assert!(user_json["new"].is_object());
    assert!(user_json["old"].is_null());

    let order_json: serde_json::Value = serde_json::from_str(&orders_messages[0]).unwrap();
    assert_eq!(order_json["metadata"]["table"], "orders");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_kafka_sink_message_key() {
    let (_container, brokers) = start_kafka().await;

    let config = KafkaSinkConfig {
        brokers: brokers.clone(),
        topic_prefix: "keytest".into(),
        properties: Default::default(),
    };
    let mut sink = KafkaSink::new(config).unwrap();

    sink.write_batch(&[make_test_event("myschema", "mytable", "1")])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    // Consume and verify key
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "test-verify-key")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();

    consumer.subscribe(&["keytest.myschema.mytable"]).unwrap();

    let msg = timeout(Duration::from_secs(15), consumer.recv())
        .await
        .expect("timeout consuming message")
        .expect("kafka error");

    let key = msg.key_view::<str>().unwrap().unwrap();
    assert_eq!(key, "myschema.mytable");
}
