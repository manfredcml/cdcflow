//! # cdcflow
//!
//! A Change Data Capture (CDC) pipeline that captures row-level changes
//! from PostgreSQL and MySQL, streaming them to Kafka, PostgreSQL,
//! Apache Iceberg, or stdout.
//!
//! The pipeline reads events from a [`source`], batches them by transaction
//! boundaries, writes each batch to a [`sink`], and checkpoints progress
//! in an [`offset`] store. On restart, the pipeline resumes from the last
//! checkpoint.
//!
//! ## Modes
//!
//! - **CDC** — append-only changelog of row changes
//! - **Replication** — live replica of source tables (Iceberg and Postgres sinks)

pub mod admin;
pub mod config;
pub mod error;
pub mod event;
pub mod metrics;
pub mod offset;
pub mod pipeline;
pub mod schema;
pub mod sink;
pub mod source;
pub mod worker_http;
