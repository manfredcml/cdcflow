-- PostgreSQL CDC init script
-- Creates replication user, demo database, and tables
-- Note: publication is auto-created by cdcflow on first run

-- Create replication user
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';

-- Create demo database
CREATE DATABASE demo OWNER cdc_user;

-- Connect to demo database and set up tables
\c demo

-- Grant schema usage to cdc_user
GRANT ALL ON SCHEMA public TO cdc_user;

-- Create tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status TEXT NOT NULL
);

-- Set replica identity to FULL for CDC (captures before-images)
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Grant table ownership to cdc_user
ALTER TABLE users OWNER TO cdc_user;
ALTER TABLE orders OWNER TO cdc_user;

-- Insert sample data
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.com');

INSERT INTO orders (user_id, amount, status) VALUES
    (1, 99.99, 'completed'),
    (2, 149.50, 'pending'),
    (3, 75.00, 'shipped');
