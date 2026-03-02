-- Add approval workflow columns to table_config
-- Following DB Schema Change Protocol (.claude/rules/db-schema-change.md)
--
-- Data ownership: table_config owns approval metadata (1:1 with selected_tables)
-- Workspace scope: inherited via FK chain to workspaces
-- FK preservation: existing ON DELETE CASCADE behavior maintained

ALTER TABLE table_config ADD COLUMN analysis_metadata_json TEXT;
ALTER TABLE table_config ADD COLUMN approval_status TEXT 
  CHECK(approval_status IN ('pending','approved','needs_review'));
ALTER TABLE table_config ADD COLUMN approved_at TEXT;
ALTER TABLE table_config ADD COLUMN manual_overrides_json TEXT;

-- Set default approval_status for existing rows
UPDATE table_config SET approval_status = 'pending' WHERE approval_status IS NULL;
