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
