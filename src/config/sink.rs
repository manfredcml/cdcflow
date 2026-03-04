use serde::{Deserialize, Serialize};

use super::iceberg_sink::IcebergSinkConfig;
use super::kafka_sink::KafkaSinkConfig;
use super::postgres_sink::PostgresSinkConfig;

/// Sink configuration — tagged enum for future sink types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    #[serde(rename = "stdout")]
    Stdout,
    #[serde(rename = "iceberg")]
    Iceberg(IcebergSinkConfig),
    #[serde(rename = "kafka")]
    Kafka(KafkaSinkConfig),
    #[serde(rename = "postgres")]
    Postgres(PostgresSinkConfig),
}
