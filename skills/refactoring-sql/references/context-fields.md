# Refactor Context Fields

Use this reference after `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" refactor context --table <table_fqn>`.

## Table Path

Expected context fields:

- `object_type: "table"`
- `proc_body`
- `writer`
- `statements`
- `columns`
- `source_tables`
- `profile`
- `test_spec`
- `sandbox`
- `writer_ddl_slice` when the writer updates multiple targets

Sub-agent inputs:

- Sub-agent A: `proc_body`, `statements`, `columns`
- Sub-agent B: `proc_body`, `statements`, `columns`, `source_tables`, `profile`

If `writer_ddl_slice` is present, use it instead of the full `proc_body` for both sub-agents.

## View and Materialized View Path

Expected context fields:

- `object_type: "view"` or `"mv"`
- `view_sql`
- `columns`
- `source_tables`
- `profile`
- `test_spec`
- `sandbox`

Sub-agent inputs:

- Sub-agent A: `view_sql`, `columns`
- Sub-agent B: `view_sql`, `columns`, `source_tables`, `profile`

Views do not provide `writer`, `proc_body`, or `statements`. `refactor write` auto-detects the view path and writes to the view catalog.
