# Table vs View Context

## Tables

Use:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <item_id> \
  [--writer <proc_fqn>]
```

Pass `--writer` when:

- the caller already provided the intended writer
- the table may have multiple candidate writers
- disambiguation matters for the review

Read from the context output:

- `proc_body`
- `statements`
- `profile`
- `columns`
- `source_tables`

## Views

Use:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <item_id>
```

Also read `catalog/views/<fqn>.json` for:

- `profile`
- `scoping.logic_summary`
- `references.tables.in_scope`

Treat the view SELECT body as a single `action: migrate` statement.

## Both

Read:

```text
test-specs/<item_id>.json
```

Use `unit_tests[]` as the scenarios to review.
