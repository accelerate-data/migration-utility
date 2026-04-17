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
- `selected_writer_ddl_slice` when the writer updates multiple targets

Sub-agent inputs:

- Sub-agent A: selected SQL, `statements`, `columns`
- Sub-agent B: selected SQL, `statements`, `columns`, `source_tables`, `profile`

Use `selected_writer_ddl_slice` as selected SQL when present. Otherwise use `proc_body`. If both are empty, stop with a context error.

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
