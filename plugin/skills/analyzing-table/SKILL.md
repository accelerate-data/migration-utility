---
name: analyzing-table
description: >
  Use when scoping a single table, view, or materialized view for migration and the next step depends on identifying its writer, SQL structure, or dependency call tree from catalog-backed DDL context.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Analyzing Table

Analyze a table, view, or materialized view — discover writer candidates, evaluate them, and persist the scoping decision to the catalog.

## When to Use

Use this skill when:

- a table needs its writer procedure selected before profiling or model generation
- a view or materialized view needs SQL elements, call tree, and logic summary written to catalog
- `/scope` or downstream readiness depends on catalog-backed scoping, not ad-hoc inspection

Do not use this skill when:

- the object is already confirmed as a dbt source and no writer analysis is needed
- the prerequisite catalog files are missing; fix readiness failures first

## Arguments

`$ARGUMENTS` is the fully-qualified name (e.g. `silver.DimCustomer`, `silver.vw_CustomerSales`). Ask the user if missing.

## Schema discipline

Use the canonical `/scope` surfaced code list in `../../lib/shared/scope_error_codes.md`. If `discover write-scoping` or `discover write-statements` returns a validation error, fix the JSON and retry.

Diagnostics written to `warnings` or `errors` must use canonical entries from that file. Include at least:

- `code`
- `severity`
- `message`

## Before invoking

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> scope
```

If `ready` is `false`, report the failing check's `code` and `reason` to the user and stop.

All temp payloads for this skill must live under `.staging/` in the current project root. In eval fixtures, the project root is the fixture root passed via `--project-root`. Do not use `/tmp` or any temp path outside the active project root.

## Object type detection

Check whether `catalog/views/<fqn>.json` exists:

- **If yes** → this is a **view or MV**. Follow the **View Pipeline** below.
- **If no** → this is a **table**. Follow the **Table Pipeline** below.

## Quick Reference

| Object | Read command | Persist command | Success shape |
|---|---|---|---|
| table | `discover refs --name <table>` | `discover write-scoping --name <table> --scoping-file .staging/scoping.json` | `selected_writer` plus rationale and candidate context |
| view or MV | `discover show --name <view_fqn>` | `discover write-scoping --name <view_fqn> --scoping-file .staging/scoping.json` | `sql_elements`, `call_tree`, `logic_summary`, `rationale` |

---

## View Pipeline

### Step V1 -- Show view from catalog

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <view_fqn>
```

Read `catalog/views/<view_fqn>.json` to get `is_materialized_view` and `references.views.in_scope`.

Present the object type and, for materialized views, column count:

```text
silver.vw_CustomerSales (view)
```

If `errors` contains `DDL_PARSE_ERROR` (i.e. `sql_elements` is null), note that SQLglot could not parse the DDL and proceed using `raw_ddl` directly for Steps V2-V4. Preserve the canonical diagnostic entry in the persisted scoping output.

### Step V2 -- Build call tree

Resolve sources from `refs.reads_from` (source tables) and `references.views.in_scope` from the view catalog (source views).

```text
Call tree for silver.vw_CustomerSales:

  Reads tables:  bronze.Customer, bronze.Person
  Reads views:   silver.vw_AddressBase
```

If `references.views.in_scope` is non-empty, add a warning entry to the scoping output noting that the view depends on other in-scope views. Use a canonical diagnostic entry with `severity: "warning"` and preserve the dependency detail in `message`.

### Step V3 -- Identify SQL elements

If `sql_elements` is populated, present them directly:

```text
SQL elements:
  - join: INNER JOIN bronze.Person
  - join: LEFT JOIN bronze.Address
  - group_by: GROUP BY
  - aggregation: SUM, COUNT
```

If `sql_elements` is null (parse error), read `raw_ddl` and identify SQL features manually: JOINs (type and target), GROUP BY, aggregation functions, window functions (OVER), CASE expressions, subqueries, CTEs. Present the same format as above.

### Step V4 -- Logic summary

Read `raw_ddl` and write a plain-language description of what the view computes (2-4 sentences).

### Step V5 -- Persist scoping to catalog

Persist the view analysis:

Write the scoping JSON to a temp file:

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <view_fqn> --scoping-file .staging/scoping.json && rm -rf .staging
```

The `.staging/` directory must be created under the current project root before writing `scoping.json`.

Do not include `status` in the scoping dict.

Required fields: `sql_elements`, `call_tree`, `logic_summary`, `rationale`, `warnings`, `errors`.

If there was a parse failure, keep the existing `DDL_PARSE_ERROR` entry in `errors`. If the view depends on in-scope views, include a warning entry describing that dependency. Only use canonical `/scope` codes and severities from `../../lib/shared/scope_error_codes.md`.

### Step V6 -- Present persisted result

Present the persisted result: call tree, SQL elements, logic summary. Show `VIEW_DEPENDS_ON_VIEWS` warning prominently if applicable.

---

## Table Pipeline

### Step 1 -- Show columns from catalog

Read `catalog/tables/<table>.json` and present the column list:

```text
silver.DimCustomer (table, 3 columns)

  CustomerKey   INTEGER      NOT NULL
  FirstName     VARCHAR(50)  NULL
  Region        VARCHAR(50)  NULL
```

### Step 2 -- Discover writer candidates

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover refs \
  --name <table>
```

Extract the `writers` array from the output. If no writers are found, persist `no_writer_found` to catalog:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping '{"selected_writer": null, "selected_writer_rationale": "No procedures found that write to this table."}'
```

Then ask the user:

> No writer found for `<table>`. Mark as a dbt source? (y/n)

If **y**, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-source \
  --name <table> --value
```

Confirm: "Marked `<table>` as a dbt source (`is_source: true`)."

If **n**, skip — the table will appear in the "pending source confirmation" section of `/status` until confirmed.

Stop here (no further steps for `no_writer_found` tables).

#### Multi-table-writer handling

If a candidate proc has a `MULTI_TABLE_WRITE` warning, do **not** disqualify it. Instead, assess whether the logic is separable or truly interleaved:

**Truly interleaved** — a single MERGE/INSERT block writes to both tables simultaneously, or the logic uses shared variables/transaction semantics that cannot be cleanly attributed to one table:

- Do not write `status` manually.
- Persist a scoping payload with no `selected_writer`, the candidate context you gathered, and an `errors` entry using canonical `/scope` fields:

  ```json
  {
    "code": "SCOPING_FAILED",
    "severity": "error",
    "message": "Writer logic is interleaved across multiple target tables and cannot be attributed to a table-specific slice."
  }
  ```

- Stop evaluating this candidate.

**Separable** — distinct MERGE/INSERT/UPDATE blocks handle each target table (shared upstream CTEs or temp table declarations are fine):

1. Identify the DDL block(s) that write to **this target table only**, including any shared setup (CTEs, temp table declarations) that those blocks depend on.
2. Write the slice to the proc catalog:

   ```bash
   mkdir -p .staging
   # Write the slice DDL to .staging/slice.sql, then:
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-slice \
     --proc <proc_fqn> --table <target_table_fqn> --slice-file .staging/slice.sql
   rm -rf .staging
   ```

   Create `.staging/` under the current project root before writing `slice.sql`.

3. Proceed to evaluate this candidate normally. If it is the best writer, persist it through the standard table scoping payload and let `discover write-scoping` derive the final `status`.
4. In `selected_writer_rationale`, note that this is a multi-table-writer proc and name the other tables it writes to.

### Step 3 -- Analyze each writer candidate

For each writer candidate, read and follow the [procedure analysis reference](references/procedure-analysis.md). Run all 6 steps (fetch, classify, resolve call graph, logic summary, migration guidance, persist) for each candidate before moving to Step 4.

Every candidate must complete statement persistence before final writer selection, even if the candidate is later rejected. Rejected candidates still need a persisted procedure catalog showing the resolved `migrate`/`skip` decisions that supported the rejection.

If there are multiple candidates, analyze them sequentially — each candidate's analysis must complete before starting the next.

### Step 4 -- Present writer candidates

After all candidates are analyzed, present a summary:

```text
Writer candidates for silver.DimCustomer:

  1. dbo.usp_load_dimcustomer_full (direct writer)
     Reads: bronze.Customer, bronze.Person
     Writes: silver.DimCustomer
     Statements: 1 migrate, 1 skip

  2. dbo.usp_load_dimcustomer_delta (direct writer)
     Reads: bronze.Customer, silver.DimCustomer
     Writes: silver.DimCustomer
     Statements: 1 migrate (MERGE)
```

Include rationale (direct writer, transitive writer), dependencies (reads/writes), and statement summary for each candidate.

### Step 5 -- Resolution

Use the decision table below as the primary writer-selection rule set. The bullets that follow are only reminders of the common outcomes.

Use this decision table when selecting the final writer:

| Situation | Select proc? | Persisted outcome |
|---|---|---|
| Single defensible local writer | yes | persist `selected_writer` with rationale |
| Multiple defensible writers, one clearly primary | yes | persist the best-supported `selected_writer` with rationale |
| Multiple writers, no defensible tie-break | no | persist candidate context and let status resolve to `ambiguous_multi_writer` |
| Multi-table writer with a clean table-specific slice | yes | write the slice, then evaluate and select normally if it is the best writer |
| Multi-table writer with interleaved target logic | no | persist candidate context plus canonical error entry |
| Remote or linked-server `EXEC` is ancillary and local target-table writes are sufficient | yes | keep the proc selectable; persist the remote statement as `skip` and mention the skipped out-of-scope behavior in rationale or warnings |
| Remote or linked-server `EXEC` is the only meaningful write path for the target table | no | persist canonical `REMOTE_EXEC_UNSUPPORTED` error |
| Dynamic or opaque write path leaves the target-table transformation materially unresolved | no | persist canonical `SCOPING_FAILED` error and do not select the proc |
| No writers found | no | `no_writer_found` |

Common outcomes:

- **1 writer** -- auto-select and persist when it remains defensible under the decision table
- **2+ writers** -- present candidates, choose the best-supported writer, and persist with clear rationale
- **0 writers** -- report `no_writer_found` (already handled in Step 2)

If all discovered candidates are unsupported external delegates, persist table scoping without `selected_writer`. In that case:

- omit `selected_writer`
- explain in `selected_writer_rationale` that the apparent writer delegates to an out-of-scope external procedure and cannot be migrated from this project
- include an `errors` entry with canonical fields, for example:

  ```json
  {
    "code": "REMOTE_EXEC_UNSUPPORTED",
    "severity": "error",
    "message": "The apparent writer delegates through a cross-database or linked-server EXEC, so the writer cannot be resolved from this project."
  }
  ```

### Step 6 -- Persist scoping to catalog

Treat any existing `scoping` section as non-authoritative on reruns. Recompute scoping from the current catalog evidence, then overwrite `scoping` with the newly derived canonical payload. Do not preserve stale writer decisions, candidate lists, warnings, or errors just because they already exist in the catalog.

Write the scoping JSON to a temp file:

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping-file .staging/scoping.json && rm -rf .staging
```

The `.staging/` directory must be created under the current project root before writing `scoping.json`.

Do not include `status` in the scoping dict.

The scoping JSON must include a `selected_writer_rationale` field (1–2 sentences explaining why this writer was chosen over alternatives, or why no writer / ambiguous). If the write exits non-zero, report the error and ask the user to correct.

The table scoping JSON shape:

```json
{
  "selected_writer": "silver.usp_load_dimcustomer_full",
  "selected_writer_rationale": "Full loader is the primary writer because it independently rebuilds the target table from source data.",
  "candidates": [
    {
      "procedure_name": "silver.usp_load_dimcustomer_full",
      "rationale": "Direct full-load writer for the target table.",
      "dependencies": {
        "tables": ["bronze.customer", "bronze.person"],
        "views": [],
        "functions": []
      }
    }
  ],
  "warnings": [],
  "errors": []
}
```

## Common Mistakes

- Do not put `status` in the scoping JSON. `discover write-scoping` derives it from the payload.
- Do not write diagnostics as code-only strings. Use canonical entries with `code`, `severity`, and `message`.
- Do not reject every `MULTI_TABLE_WRITE` candidate. Separable writers stay valid after slicing.
- Do not skip readiness checks or fixture-local `--project-root` overrides when running eval scenarios.
- Do not write temp payloads to `/tmp` or another external directory. Use `.staging/` under the active project root only.

For multi-writer cases, every entry in `candidates` must use `procedure_name` and `rationale`. `dependencies` is optional. Do not use legacy fields such as `procedure`, `write_type`, or `selected`.

For unsupported external delegate cases: omit `selected_writer`, explain in `selected_writer_rationale`, add `REMOTE_EXEC_UNSUPPORTED` to `errors[]`.

After `discover write-scoping` succeeds, present the persisted result to the user.

## References

- [references/procedure-analysis.md](references/procedure-analysis.md) — six-step deep-dive pipeline: fetch, classify, call graph, logic summary, migration guidance, persist
- [references/statement-classification.md](references/statement-classification.md) — dialect-routed statement classification
- [`../../lib/shared/scope_error_codes.md`](../../lib/shared/scope_error_codes.md) — canonical `/scope` statuses and surfaced error/warning codes

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `discover refs` | 1 | Object not found or catalog file missing. Report and stop |
| `discover refs` | 2 | Catalog directory unreadable (IO error). Report and stop |
| procedure analysis | reference failure | Log failure, mark candidate `BLOCKED`, continue with remaining |
| `discover write-scoping` | 1 | Validation failure. Report errors, ask user to correct |
| `discover write-scoping` | 2 | Invalid JSON or IO error. Report and stop |
| `discover write-source` | 1 | Catalog file missing or table not analyzed. Report and stop |
