use std::time::Duration;

use futures_util::future::join_all;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;

use crate::config::KafkaSinkConfig;
use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, TableId};
use crate::schema::value::event_to_json;
use crate::sink::Sink;

/// A sink that publishes CDC events to Kafka topics.
///
/// Each event is routed to a topic named `{topic_prefix}.{schema}.{table_name}`.
/// Events are serialized as JSON, with the message key set to `{schema}.{table_name}`
/// to ensure all events for a given table land on the same partition (preserving ordering).
pub struct KafkaSink {
    producer: FutureProducer,
    topic_prefix: String,
}

impl KafkaSink {
    pub fn new(config: KafkaSinkConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);

        for (key, value) in &config.properties {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| CdcError::Kafka(format!("failed to create producer: {e}")))?;

        Ok(Self {
            producer,
            topic_prefix: config.topic_prefix,
        })
    }
}

/// Build the topic name for a given table.
fn topic_name(prefix: &str, table: &TableId) -> String {
    format!("{}.{}.{}", prefix, table.schema, table.name)
}

/// Build the message key for a given table.
fn message_key(table: &TableId) -> String {
    format!("{}.{}", table.schema, table.name)
}

impl Sink for KafkaSink {
    async fn write_batch(&mut self, events: &[CdcEvent]) -> Result<()> {
        // Pre-serialize all events into owned data so borrows don't conflict with async
        let prepared: Vec<(String, String, String)> = events
            .iter()
            .map(|event| {
                let topic = topic_name(&self.topic_prefix, &event.table);
                let key = message_key(&event.table);
                let json = serde_json::to_string(&event_to_json(event))?;
                Ok((topic, key, json))
            })
            .collect::<Result<Vec<_>>>()?;

        // Fire all sends — each enqueues to librdkafka's internal buffer on first poll
        let futs: Vec<_> = prepared
            .iter()
            .map(|(topic, key, json)| {
                let record = FutureRecord::to(topic)
                    .key(key.as_str())
                    .payload(json.as_str());
                self.producer.send(record, Duration::from_secs(5))
            })
            .collect();

        let results = join_all(futs).await;
        for result in results {
            result.map_err(|(e, _)| CdcError::Kafka(format!("delivery failed: {e}")))?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.producer
            .flush(Duration::from_secs(30))
            .map_err(|e| CdcError::Kafka(format!("flush failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_table(schema: &str, name: &str) -> TableId {
        TableId {
            schema: schema.into(),
            name: name.into(),
            oid: 0,
        }
    }

    #[test]
    fn test_topic_name_formatting() {
        let table = make_test_table("public", "users");
        assert_eq!(topic_name("cdc", &table), "cdc.public.users");
    }

    #[test]
    fn test_topic_name_special_characters() {
        let table = make_test_table("my-schema", "my_table.v2");
        assert_eq!(topic_name("prefix", &table), "prefix.my-schema.my_table.v2");
    }

    #[test]
    fn test_message_key_formatting() {
        let table = make_test_table("public", "users");
        assert_eq!(message_key(&table), "public.users");
    }
}
