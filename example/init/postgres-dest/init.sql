-- PostgreSQL destination init script
-- Creates CDC user and target database (tables are auto-created by the CDC sink)

-- Create CDC user
CREATE ROLE cdc_user WITH LOGIN PASSWORD 'cdc_password';

-- Create destination database
CREATE DATABASE demo_dest OWNER cdc_user;

-- Connect to destination database and grant permissions
\c demo_dest

-- Grant schema usage to cdc_user
GRANT ALL ON SCHEMA public TO cdc_user;
