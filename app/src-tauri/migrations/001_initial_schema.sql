-- Settings key-value store (GitHub OAuth, Anthropic API key, app preferences)
CREATE TABLE settings (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Projects: one row per SQL Server migration project
CREATE TABLE projects (
  id          TEXT PRIMARY KEY,  -- UUID
  slug        TEXT NOT NULL UNIQUE,
  name        TEXT NOT NULL,
  sa_password TEXT NOT NULL,
  created_at  TEXT NOT NULL
);

-- Agent runs: one row per workflow_dispatch submission
CREATE TABLE agent_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id    TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  run_id        TEXT NOT NULL UNIQUE,  -- UUID
  action        TEXT NOT NULL,
  submitted_ts  TEXT NOT NULL,
  github_run_id TEXT,
  status        TEXT NOT NULL CHECK(status IN ('in-progress', 'success', 'failed'))
);

-- Stage status: one row per (project, table, stage) — consolidation target
CREATE TABLE stage_status (
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  table_id   TEXT NOT NULL,
  stage      TEXT NOT NULL,
  status     TEXT NOT NULL CHECK(status IN ('in-progress', 'success', 'failed')),
  dirty      INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (project_id, table_id, stage)
);

-- FDE overrides: local edits to agent output before re-submission
CREATE TABLE fde_overrides (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id          TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  table_id            TEXT NOT NULL,
  stage               TEXT NOT NULL,
  field               TEXT NOT NULL,
  fde_value           TEXT NOT NULL,
  source_run_id       TEXT NOT NULL,
  source_submitted_ts TEXT NOT NULL,
  UNIQUE (project_id, table_id, stage, field)
);
