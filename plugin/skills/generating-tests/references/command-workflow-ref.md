# Command Workflow Reference

Exact command flow for `plugin/skills/generating-tests`.

## Guard

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> test-gen
```

Stop if readiness fails.

## Context discovery

For tables:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context --table <fqn>
```

Use the selected writer from catalog scoping when the command output does not already provide it.

For views and materialized views:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show --name <fqn>
```

Also read `catalog/views/<fqn>.json` for:

- `profile`
- `scoping.logic_summary`
- `references.tables.in_scope`
- `is_materialized_view`

For all source tables, read `catalog/tables/<schema>.<table>.json` to get column nullability, identity, types, foreign keys, and `auto_increment_columns`.

## Catalog summary write

Run only after the spec is valid:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness write \
  --table <fqn> \
  --branches <branch_count> \
  --unit-tests <scenario_count> \
  --coverage <complete|partial|none>
```

Pass warnings and errors as JSON arrays when present. If the write command rejects the artifact, fix the spec and retry.
