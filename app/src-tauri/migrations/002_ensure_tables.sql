-- Forward-compatibility migration: ensures settings and projects tables exist
-- for databases created before 001_initial_schema.sql was updated.
CREATE TABLE IF NOT EXISTS settings (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
  id          TEXT PRIMARY KEY,
  slug        TEXT NOT NULL UNIQUE,
  name        TEXT NOT NULL,
  sa_password TEXT NOT NULL,
  created_at  TEXT NOT NULL
);
