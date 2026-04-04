# Scoping Skill Contract

The scoping skill maps a target table to one or more candidate writer procedures for T-SQL sources (SQL Server, Fabric Warehouse). It is the prerequisite step for profiler input.

## Philosophy and Boundary

- Analysis is responsible only for writer discovery and writer selection.
- Scoping writes results to `catalog/tables/<table>.json` (scoping section), not a separate output file. Downstream skills read `selected_writer` from catalog.
- Keep analysis payload minimal for clear handoff.
- Exception: `reads_from` is included on candidate writers to support downstream context without requiring re-analysis.

## Goal

Given a target table, identify candidate writer procedures and select one writer when resolvable. Write the scoping decision to the table's catalog file.

## Discovery Strategy

> **Catalog-first analysis.** The scoping skill calls `uv run discover refs` per table for catalog-based writer identification. Catalog files (from `setup-ddl`) carry `is_updated`/`is_selected` flags from `sys.dm_sql_referenced_entities` — writers are procs with `is_updated=true`. The skill then calls `discover show` on each candidate writer for statement-level analysis and dependency resolution. Procs flagged `needs_llm: true` in their catalog file (EXEC(@var), TRY/CATCH, WHILE, IF) require LLM reasoning from the raw DDL.

The skill's role is: read the input, run `discover refs` per item, run `discover show` per candidate writer for statement analysis and dependency resolution, apply resolution rules, validate, and write scoping results to catalog.

### Resolution Rules

- Return one of: `resolved`, `ambiguous_multi_writer`, `no_writer_found`, `error`.
- Status rules:
  - `resolved`: exactly one writer proc has `is_updated=true` in catalog.
  - `ambiguous_multi_writer`: two or more procs have `is_updated=true`.
  - `no_writer_found`: no proc has `is_updated=true`.
  - cross-database reference detected: return `error` with issue code `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE`.
  - `error`: execution/parsing/runtime failure prevented completion.

### `dependencies` Enrichment

For each `resolved` item, the skill populates `dependencies` on the selected writer's candidate entry. This contains the transitively resolved base tables, views, and functions the writer procedure depends on. Views and functions are resolved down to their underlying base tables. The data is obtained from:

- `discover show` on the selected writer, then recursive `discover show` on referenced views/functions/procedures to resolve down to base tables
- LLM judgment + `discover show` lookups on referenced objects for `needs_llm: true` candidates

This field is consumed by downstream wave planning to build inter-table dependency graphs without re-analysing source DDL.

### Validation

- Run internal consistency/contract checks on the output item (for example selected writer consistency and required fields). Runtime failures must be reported in `errors`.
- Validation checklist:
  - `item_id` is present.
  - `status` is one of: `resolved|ambiguous_multi_writer|no_writer_found|error`.
  - `candidates` is structurally valid.
  - every candidate includes `procedure_name` and `rationale`.
  - if `status == "resolved"`:
    - `selected_writer` is present
    - `selected_writer` exists in `candidates`
    - the selected writer candidate has `dependencies` populated
  - if `status == "ambiguous_multi_writer"`:
    - at least two candidates are present
    - `selected_writer` is absent
  - if `status == "no_writer_found"`:
    - `candidates` is empty
    - `selected_writer` is absent
  - if `status == "error"`:
    - `errors` is non-empty
  - `validation.passed` is `false` when any validation issue exists.

## Output: Catalog Scoping Section

Scoping results are written to `catalog/tables/<item_id>.json` under a `scoping` key:

```json
{
  "scoping": {
    "status": "resolved",
    "selected_writer": "dbo.usp_load_fact_sales",
    "candidates": [
      {
        "procedure_name": "dbo.usp_load_fact_sales",
        "dependencies": {
          "tables": ["bronze.salesraw", "dbo.dimcustomer"],
          "views": [],
          "functions": []
        },
        "rationale": "Catalog referenced_by shows is_updated=true for this procedure."
      }
    ],
    "warnings": [],
    "validation": {
      "passed": true,
      "issues": []
    },
    "errors": []
  }
}
```

## Resolution Rules

- `resolved`: exactly one writer proc has `is_updated=true` in catalog.
- `ambiguous_multi_writer`: two or more procs have `is_updated=true`.
- `no_writer_found`: no proc has `is_updated=true` for the target table.
- `error`: analysis execution failed for the table.

## Known Limitations

- Dynamic SQL (`sp_executesql`, `EXEC(@sql)`) may hide dependencies from metadata.
- Synonyms/views may mask base-table writers.
- `TRUNCATE`-only writers may be missed by dependency metadata and require body-parse detection.
- `dependencies` for `needs_llm: true` candidates depends on LLM judgment from reading raw DDL combined with `discover show` lookups — accuracy varies with procedure complexity.

## Assumptions

- Orchestrator traversal is always enabled through call-graph resolution.
- Cross-database access is out of scope for this flow.

## Step 5 — Persist Resolved Statements to Catalog

After writing the scoping results, the skill persists resolved statements for each `resolved` item to `catalog/procedures/<selected_writer>.json`.

Only persist for procs not already in catalog (idempotent) — if the proc already has `statements`, skip.

For each resolved item:

1. If `discover show` returned `needs_llm: false` — all statements already have `action: migrate|skip`. Write them to catalog with `source: "ast"`.

2. If `discover show` returned `needs_llm: true` — the LLM analysis in Step 2 resolved all `needs_llm` actions to `migrate` or `skip`. Write the resolved statements to catalog with `source: "llm"`.

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-statements \
  --name <selected_writer> --statements '<json>'
```

The `write-statements` subcommand validates that every statement has `action` in `[migrate, skip]` and `source` in `[ast, llm]`, then merges the `statements` array into the existing procedure catalog file.

No `claude` actions are persisted to catalog — all are resolved before writing.

Downstream stages (profiler, migrator) read resolved statements from `catalog/procedures/<writer>.json` and `selected_writer` from `catalog/tables/<table>.json`.
