---
name: analyzing-table
description: >
  Use when scoping a single table, view, or materialized view for migration and the next step depends on identifying its writer, SQL structure, or dependency call tree from catalog-backed DDL context.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Analyzing Table

Scope one table, view, or materialized view from catalog-backed DDL context.

## When to Use

Use this skill when:

- a table needs its writer selected before downstream work
- a view or materialized view needs SQL elements, call tree, and logic summary written to catalog
- `/scope` depends on catalog-backed scoping, not ad-hoc inspection

Do not use this skill when:

- the object is already confirmed as a dbt source and no writer analysis is needed
- the prerequisite catalog files are missing; fix readiness failures first

## Arguments

`$ARGUMENTS` is the fully-qualified name (e.g. `silver.DimCustomer`, `silver.vw_CustomerSales`). Ask the user if missing.

## Guardrails

- Use canonical `/scope` codes from `../../lib/shared/scope_error_codes.md`.
- Diagnostics in `warnings` or `errors` must include `code`, `severity`, and `message`.
- Treat existing `scoping` and `statements` as non-authoritative on reruns. Recompute and overwrite.
- Use `.staging/` under the active project root only. Do not use `/tmp`.
- Do not include `status` in persisted scoping payloads.

## Before invoking

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> scope
```

If `ready` is `false`, report the failing check's `code` and `reason` to the user and stop.

## Object type detection

Check whether `catalog/views/<fqn>.json` exists:

- **If yes** → this is a **view or MV**. Follow the **View Pipeline** below.
- **If no** → this is a **table**. Follow the **Table Pipeline** below.

## Quick Reference

| Object | Normal path | Open reference when | Persisted outcome |
|---|---|---|---|
| table | run `references/table-pipeline.md` | per-candidate procedure analysis: `references/procedure-analysis.md`; non-trivial writer selection: `references/table-writer-resolution.md` | `selected_writer` plus rationale and candidate context |
| view or MV | run `references/view-pipeline.md` | manual SQL feature inspection needs dialect help: `references/statement-classification.md` | `sql_elements`, `call_tree`, `logic_summary`, `rationale` |

---

## Table Pipeline

Read [references/table-pipeline.md](references/table-pipeline.md) and execute it.

Use [references/procedure-analysis.md](references/procedure-analysis.md) for each candidate procedure.

Use [references/table-writer-resolution.md](references/table-writer-resolution.md) only when writer selection is not trivial.

## View Pipeline

Read [references/view-pipeline.md](references/view-pipeline.md) and execute it.

Use [references/statement-classification.md](references/statement-classification.md) only when manual SQL feature inspection needs dialect-specific help.

## Common Mistakes

- Do not skip the readiness check.
- Do not bypass the top-level guardrails just because a reference has the details.
- Do not open every reference up front. Open the workflow reference first, then the exception-specific reference only when needed.

## Error handling

Use [references/error-handling.md](references/error-handling.md) for command exits and reference-level failures.
