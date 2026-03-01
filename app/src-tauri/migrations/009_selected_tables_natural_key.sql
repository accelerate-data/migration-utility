-- Enforce natural-key uniqueness for selected tables to prevent duplicate scope rows.
DELETE FROM selected_tables
WHERE rowid NOT IN (
  SELECT MIN(rowid)
  FROM selected_tables
  GROUP BY workspace_id, warehouse_item_id, schema_name, table_name
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_selected_tables_natural
  ON selected_tables(workspace_id, warehouse_item_id, schema_name, table_name);

