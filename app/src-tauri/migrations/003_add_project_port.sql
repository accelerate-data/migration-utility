-- Each project gets its own SQL Server container on a unique host port.
-- Existing rows default to 1433 (legacy behaviour); new rows get a free port assigned at creation.
ALTER TABLE projects ADD COLUMN port INTEGER NOT NULL DEFAULT 1433;
