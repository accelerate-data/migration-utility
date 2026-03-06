-- Migration 004: replace sa_password/port with technology
--
-- Adds technology column (e.g. 'sql_server', 'fabric_warehouse', 'fabric_lakehouse', 'snowflake').
-- Drops sa_password and port which are no longer stored in the projects table.
-- SQLite DROP COLUMN requires version 3.35+; rusqlite 0.38 bundles 3.47+.

ALTER TABLE projects ADD COLUMN technology TEXT NOT NULL DEFAULT 'sql_server';
ALTER TABLE projects DROP COLUMN sa_password;
ALTER TABLE projects DROP COLUMN port;
