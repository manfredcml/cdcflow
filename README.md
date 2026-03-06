# cdcflow

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)

A Change Data Capture (CDC) pipeline in Rust. Captures row-level changes from databases and streams them to various
destinations.

## Features

- **Database sources**: PostgreSQL and MySQL
- **Sink destinations**: Stdout, Kafka, PostgreSQL, Apache Iceberg
- **Offset stores**: SQLite, Memoryfor resumable streaming after restarts
- **Sync modes**: CDC (append-only changelog) and Replication (live replica of source tables)
- **Admin server**: REST API for job management - not completely implemented yet.

## Architecture

```
┌────────────┐     ┌────────────┐     ┌──────────────┐
│   Source   │────▶│  Pipeline  │────▶│     Sink     │
│ PG / MySQL │     │ (batch +   │     │ Kafka / PG / │
│            │     │  flush)    │     │ Iceberg / Out│
└────────────┘     └─────┬──────┘     └──────────────┘
                         │
                  ┌──────▼──────┐
                  │ OffsetStore │
                  └─────────────┘
```

The pipeline reads events from a source, batches them by transaction boundaries, writes each batch to the sink, and
checkpoints progress in the offset store. On restart, the pipeline resumes from the last checkpoint.

## Requirements

- **Platform**: Linux and macOS only
- **CMake**: Required for building the bundled librdkafka (Kafka dependency)

## Quick Start - Standalone Mode

The standalone mode runs a single pipeline without the admin server. This is ideal for local testing and development.

```bash
# Start local infrastructure (PostgreSQL, MySQL, Kafka, MinIO, Iceberg, Trino)
# You can optionally start just the components you need for testing a specific source/sink combo
cd example && docker compose up -d && cd ..

# Build
cargo build

# Run a pipeline
RUST_LOG=info ./target/debug/cdcflow job run --standalone -c example/configs/pg-to-stdout.json
```

## Quick Start - Server Mode

The admin server provides centralized job management via a REST API. Workers connect to the admin server, pull their
configuration, and report health via heartbeats — instead of reading a local config file.

```bash
# 1. Start the admin server
RUST_LOG=info ./target/debug/cdcflow admin start --port 8090

# 2. Create a job (registers config in the admin server)
RUST_LOG=info ./target/debug/cdcflow job create \
  --admin-url http://localhost:8090 \
  --name pg-stdout \
  --config example/configs/pg-to-stdout.json \
  --description "PostgreSQL CDC to stdout"

# 3. Run a worker that pulls config from the admin server
RUST_LOG=info ./target/debug/cdcflow job run \
  --admin-url http://localhost:8090 \
  --job pg-stdout \
  --http-port 9091

# 4. List jobs / workers
RUST_LOG=info ./target/debug/cdcflow job list --admin-url http://localhost:8090
```

## Example Configs

The [`example/configs/`](example/configs/) directory contains example configuration files for various source/sink
combinations. You can use these as templates for your own pipelines.

## Testing

```bash
# Unit tests
cargo test

# Integration tests (requires Docker)
cargo test -- --ignored
```

## License

This project is licensed under the [MIT License](LICENSE).
