use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Kafka sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    pub brokers: String,
    #[serde(default = "default_kafka_topic_prefix")]
    pub topic_prefix: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

fn default_kafka_topic_prefix() -> String {
    "cdc".to_string()
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, SinkConfig};

    #[test]
    fn test_kafka_sink_config_deserialization() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "kafka",
                "brokers": "localhost:9092",
                "topic_prefix": "myapp"
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Kafka(k) => {
                assert_eq!(k.brokers, "localhost:9092");
                assert_eq!(k.topic_prefix, "myapp");
                assert!(k.properties.is_empty());
            }
            _ => panic!("expected Kafka sink config"),
        }
    }

    #[test]
    fn test_kafka_sink_config_with_extra_properties() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "kafka",
                "brokers": "localhost:9092",
                "properties": {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "PLAIN"
                }
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Kafka(k) => {
                assert_eq!(k.properties.len(), 2);
                assert_eq!(
                    k.properties.get("security.protocol").map(String::as_str),
                    Some("SASL_SSL")
                );
                assert_eq!(
                    k.properties.get("sasl.mechanism").map(String::as_str),
                    Some("PLAIN")
                );
            }
            _ => panic!("expected Kafka sink config"),
        }
    }

    #[test]
    fn test_kafka_sink_config_default_prefix() {
        let json = r#"{
            "source": {
                "type": "postgres",
                "host": "localhost",
                "user": "replicator",
                "database": "mydb",
                "slot_name": "cdc_slot",
                "publication_name": "cdc_pub"
            },
            "sink": {
                "type": "kafka",
                "brokers": "localhost:9092"
            },
            "offset": { "type": "memory" }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        match &config.sink {
            SinkConfig::Kafka(k) => {
                assert_eq!(k.topic_prefix, "cdc");
            }
            _ => panic!("expected Kafka sink config"),
        }
    }
}
