---
name: profiling-table
description: >
  Profile a single table, view, or materialized view for migration. For tables: assembles context from catalog and DDL, reasons over six profiling questions (classification, PK, watermark, FKs, PII). For views/MVs: classifies as stg or mart using sql_elements, logic_summary, and dependency signals. Auto-detects object type from catalog presence.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Profiling

Profile a single table, view, or materialized view for migration.

## Arguments

`$ARGUMENTS` is the fully-qualified name. Ask the user if missing.

## Schema discipline

Whenever this skill writes structured JSON back to the catalog, treat the schemas in `../../lib/shared/schemas/` as the contract:

- table profile: `table_catalog.json#/$defs/profile_section`
- view profile: `view_catalog.json#/properties/profile`

Do not invent field names or omit required fields. The examples and enum lists in this skill are minimum valid shapes, not loose suggestions. If `profile write` returns a schema validation error, fix the JSON to match the schema and retry the command.

Use the canonical `/profile` surfaced code list in `../../lib/shared/profile_error_codes.md`. Do not define a competing public error-code list in this skill.

## Before invoking

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> profile
```

The `ready` command auto-detects whether the FQN is a table or view — no separate guard set needed.

If `ready` is `false`, report the failing `code` and `reason` to the user and stop. If `code` is absent, report the `reason`.

## Object type detection

Check whether `catalog/views/<fqn>.json` exists:

- **If yes** → this is a **view or MV**. Follow the **View Profile Pipeline** below.
- **If no** → this is a **table**. Follow the **Table Profile Pipeline** below.

---

## View Profile Pipeline

### Step V1 -- Assemble Context (Deterministic)

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile view-context \
  --view <view_fqn>
```

Output is a JSON matching `lib/shared/schemas/view_profile_context.json`. It contains:

- `view` — normalized FQN
- `is_materialized_view` — true for Oracle MVs and SQL Server indexed views
- `sql_elements` — SQLglot-extracted SQL features (join, aggregation, window_function, case, subquery, cte, group_by); null if DDL parse failed
- `logic_summary` — plain-language description of what the view computes
- `columns` — present only for materialized views
- `references` — outbound refs (tables, views, functions) with `object_type` on each in_scope entry
- `referenced_by` — inbound refs (procedures, views, functions) with `object_type` on each in_scope entry

If exit code is non-zero, stop and report the error.

### Step V2 -- LLM Classification (Reasoning)

Read the context JSON and apply the signal table in [view-classification-signals.md](references/view-classification-signals.md).

Answer one question: **Is this view `stg` or `mart`?**

Steps:

1. Check `references.views.in_scope`. For each, call `discover show --name <fqn>` to inspect its `profile.classification`. If any dependency is `mart`, inherit `mart`.
2. Apply the signal table to `sql_elements`. Aggregation, group_by, window_function → `mart`. Single-source with no aggregation → `stg`.
3. Use `logic_summary` as tiebreaker when `sql_elements` is empty or null.
4. For materialized views: aggregation signals → `mart`; lookup/pass-through → `stg`.
5. When signals conflict: default to `mart`.

Write a 1–2 sentence rationale citing the specific signals that drove the decision.

### Step V3 -- Write to Catalog (Deterministic)

Persist the view profile as soon as the JSON is ready. Do not ask for approval before writing — this is a write-through workflow.

Write the profile JSON to a temp file:

```bash
mkdir -p .staging
# Write profile JSON to .staging/view_profile.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile write \
  --table <view_fqn> \
  --profile-file .staging/view_profile.json; rm -rf .staging
```

Do not include `status` in the profile JSON — the CLI determines it from the content.

The profile JSON must match `../../lib/shared/schemas/view_catalog.json#/properties/profile`. Required fields: `classification`, `rationale`, `source`.

| Field | Valid values |
|---|---|
| `classification` | `stg`, `mart` |
| `source` | `llm` |

If the write exits non-zero, report the validation errors and retry with corrected JSON.

### Step V4 -- Present Persisted Result

After `profile write` succeeds, present the classification summary:

- Classification (`stg` or `mart`)
- Rationale (which signals drove the decision)
- Any dependency views inspected and their classifications
- Confirmation that the profile was written to the catalog

### View References

- [references/view-classification-signals.md](references/view-classification-signals.md) — signal table and tie-breaking rules for stg vs mart classification

---

## Table Profile Pipeline

### Step 1 -- Assemble Context (Deterministic)

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile context \
  --table <table>
```

The CLI reads the selected writer from the table's catalog scoping section — no `--writer` argument needed.

This reads catalog signals, writer references, proc body, column list, and related procedure context. Output is a JSON matching `lib/shared/schemas/profile_context.json`.

If exit code is non-zero, stop and report the error.

**Multi-table-writer:** If `writer_ddl_slice` is present in the context, the writer is a multi-table proc. Focus your analysis on `writer_ddl_slice` as the primary SQL — it contains only the portion of the proc relevant to this table. The full `proc_body` is provided for reference only and may contain logic for other tables.

### Step 2 -- LLM Profiling (Reasoning)

Read the context JSON and the signal tables in [profiling-signals.md](references/profiling-signals.md). Answer the six profiling questions (Q1–Q6) defined there. Follow all signal tables and pattern matching rules — do not abbreviate. If any signal tentatively points to `fact_accumulating_snapshot`, also read [accumulating-snapshot-classification.md](references/accumulating-snapshot-classification.md) and apply its decision guide before confirming. If any signal tentatively points to `fact_periodic_snapshot`, also read [periodic-snapshot-classification.md](references/periodic-snapshot-classification.md) and apply its decision guide before confirming.

### Step 3 -- Write to Catalog (Deterministic)

Persist the table profile as soon as the JSON is ready. Do not ask for approval before writing — this is a write-through workflow.

Write the profile JSON to a temp file to avoid shell escaping issues:

```bash
mkdir -p .staging
# Write profile JSON to .staging/profile.json (avoids shell quoting breakage from apostrophes in rationale text)
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile write \
  --table <table> \
  --profile-file .staging/profile.json; rm -rf .staging
```

Do not include `status` in the profile JSON — the CLI determines it from the content.

The profile JSON must match `../../lib/shared/schemas/table_catalog.json#/$defs/profile_section`. Required fields: `writer`. Each decision point must include a `rationale` field (1–2 sentences): `classification.rationale`, `primary_key.rationale`, `natural_key.rationale`, `watermark.rationale`, and per-entry `rationale` in `foreign_keys[]` and `pii_actions[]`.

All enum values must be from the allowed sets below (canonical source: `lib/shared/profile.py`):

| Field | Valid values |
|---|---|
| `status` | `ok`, `partial`, `error` |
| `classification.resolved_kind` | `dim_non_scd`, `dim_scd1`, `dim_scd2`, `dim_junk`, `fact_transaction`, `fact_periodic_snapshot`, `fact_accumulating_snapshot`, `fact_aggregate` |
| `classification.source` | `catalog`, `llm`, `catalog+llm` |
| `primary_key.primary_key_type` | `surrogate`, `natural`, `composite`, `unknown` |
| `primary_key.source` | `catalog`, `llm`, `catalog+llm` |
| `natural_key.source` | `catalog`, `llm`, `catalog+llm` |
| `watermark.source` | `catalog`, `llm`, `catalog+llm` |
| `foreign_keys[*].fk_type` | `standard`, `role_playing`, `degenerate` |
| `foreign_keys[*].source` | `catalog`, `llm`, `catalog+llm` |
| `pii_actions[*].suggested_action` | `mask`, `drop`, `tokenize`, `keep` |
| `pii_actions[*].source` | `catalog`, `llm`, `catalog+llm` |

If the write exits non-zero, report the validation errors and retry with corrected JSON.

### Step 4 -- Present Persisted Result

After `profile write` succeeds, present the profile as a structured summary. Include:

- Classification with rationale
- Primary key with source
- Foreign keys with types
- Natural key vs surrogate key determination
- Watermark column
- PII actions
- Confirmation that the profile was written to the catalog

## Output Schema

The `profile` section written to `catalog/tables/<table>.json` follows `table_catalog.json#/$defs/profile_section`.

## References

- [references/profiling-signals.md](references/profiling-signals.md) — six profiling questions (Q1–Q6), signal tables, and pattern matching rules for classification, keys, watermark, and PII
- [references/accumulating-snapshot-classification.md](references/accumulating-snapshot-classification.md) — decision guide for confirming `fact_accumulating_snapshot`: primary write-pattern signals, negative signals, and role-playing FK date key disambiguation
- [references/periodic-snapshot-classification.md](references/periodic-snapshot-classification.md) — decision guide for confirming `fact_periodic_snapshot`: snapshot date vs. role-playing FK date keys, semi-additive vs. fully additive measures, calendar join and GROUP BY grain signals
- [`../../lib/shared/profile_error_codes.md`](../../lib/shared/profile_error_codes.md) — canonical `/profile` statuses and surfaced error/warning codes

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `profile context` | 1 | Catalog file missing for table or writer. Report which prerequisite is missing |
| `profile context` | 2 | IO/parse error. Surface the error message |
| `profile write` | 1 | Validation failure (invalid JSON, missing fields, bad enums). Report errors, ask user to correct |
| `profile write` | 2 | IO error (catalog unreadable, write failure). Report and stop |
