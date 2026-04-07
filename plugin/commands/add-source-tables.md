---
name: add-source-tables
description: >
  Mark one or more catalog tables as dbt sources (is_source: true).
  Guard: scoping (analyze) must be complete for each table.
  Source-confirmed tables are excluded from the migration pipeline and included in sources.yml.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Add Source Tables

Mark tables as dbt sources. Source-confirmed tables are excluded from `/status`, all batch pipeline commands, and included in `sources.yml` during `/init-dbt`.

Use this command when:

- A table has no writer (writerless) and should be a dbt source reference
- A table is written by another team's pipeline and this project treats it as an external source

## Arguments

`$ARGUMENTS` is one or more space-separated fully-qualified table names (e.g. `silver.AuditLog silver.LookupCurrency`). Ask the user if missing.

## Guards

- `manifest.json` must exist. If missing, tell the user to run `/setup-ddl` first.

## Pipeline

### Step 1 — Validate each table

For each FQN in `$ARGUMENTS`:

1. Run the scope guard:

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <fqn> scope
   ```

   If `passed` is `false`, report the error and skip this table. Continue with remaining tables.

2. Read `catalog/tables/<fqn>.json` and show the current scoping status to the user:

   ```text
   silver.AuditLog — no_writer_found
   ```

### Step 2 — Mark each validated table as a source

For each table that passed the guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-source \
  --name <fqn> --value
```

If the command exits non-zero, report the error and continue with remaining tables.

On success, confirm:

```text
✓ silver.AuditLog — marked as dbt source (is_source: true)
```

### Step 3 — Summary and commit

Present a summary:

```text
add-source-tables complete — N tables marked

  ✓ silver.AuditLog       is_source: true
  ✓ silver.LookupCurrency is_source: true
  ✗ silver.DimCustomer    skipped (scope guard failed: scope not complete)
```

If at least one table was successfully marked, commit the updated catalog files:

```bash
git add catalog/tables/<fqn1>.json catalog/tables/<fqn2>.json ...
git commit -m "feat: mark <fqns> as dbt sources"
```

Tell the user:

```text
Source tables will be included in sources.yml on the next /init-dbt run.
Run /status to see the updated pipeline view.
```

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `migrate-util guard` | non-zero | Scope not complete for this table. Skip and report. |
| `discover write-source` | 1 | Catalog missing or table not analyzed. Report and skip. |
| `discover write-source` | 2 | IO error. Report and stop. |
| `git commit` | non-zero | Not a git repo or nothing to commit. Skip silently. |
