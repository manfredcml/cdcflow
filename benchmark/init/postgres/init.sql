-- Benchmark PostgreSQL init script
-- Creates replication user, benchmark database, and table
-- Note: publication is auto-created by cdcflow on first run

CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';

CREATE DATABASE benchmark OWNER cdc_user;

\c benchmark

GRANT ALL ON SCHEMA public TO cdc_user;

CREATE TABLE bench_events (
    id SERIAL PRIMARY KEY,
    payload TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE bench_events REPLICA IDENTITY FULL;
ALTER TABLE bench_events OWNER TO cdc_user;