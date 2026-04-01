---
name: scoping
description: Identifies writer procedures from catalog data and writes scoping results to catalog/tables/<table>.json. Use when scoping a migration item.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Scoping Agent

Given a batch of target tables, identify which procedures write to each and select the single writer when resolvable.

Use `uv run discover` directly for all analysis — do not invoke the discover skill. Do not read catalog files directly — use `discover refs` and `discover show` as your interface.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input:** items list with `item_id` per table (from orchestrator)
- **Output:** scoping results written to `catalog/tables/<table>.json` (scoping section) + lightweight `scoping_summary.json` to the output file path

---

## Prerequisites

Before processing items:

- Read `manifest.json` from the current working directory for `technology` and `dialect`. If missing or unreadable, fail **all** items with code `MANIFEST_NOT_FOUND` and write summary output immediately.

Per item, before Step 1:

- Check `catalog/tables/<item_id>.json` exists. If missing, skip this item with `CATALOG_FILE_MISSING` in `errors[]`.

---

## Pipeline

### Step 1 — Discover Refs

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover refs \
  --name <item_id>
```

The output contains `writers` and `readers`. Each writer is a candidate.

If discover fails (exit code 1 or 2), record `error` with code `DISCOVER_EXECUTION_FAILED`.

### Step 2 — Enrich Each Candidate

For each candidate writer:

1. **Check catalog first** (idempotent): read `catalog/procedures/<proc>.json`. If `statements` already exists, reuse — the proc was already fully analysed. Collect `dependencies` from the existing catalog data and skip to step 6.

2. Run `discover show --name <writer>` to get `refs`, `statements`, `classification`, and `raw_ddl`.

3. **Resolve call graph**: extract `refs` from the output. For every ref that is a view, function, or procedure (not a base table), run `discover show` on it and follow the chain until you reach base tables. Assemble the fully resolved `dependencies: { tables, views, functions }`.

4. **Classify statements**: check `classification`:
   - `deterministic` with `statements` populated — statements are pre-classified, no further action needed.
   - `claude_assisted` or `statements` is null — read `raw_ddl` and classify each statement as `migrate` or `skip`. See `../skills/analyzing-object/references/tsql-parse-classification.md` for the classification guide. If the proc calls other procs (EXEC), run `discover show` on each and follow recursively. Add `LLM_ANALYSIS_REQUIRED` warning.

5. **Persist statements**: run `discover write-statements --name <writer> --statements '<json>'` to write resolved statements to catalog. Deterministic statements get `source: "ast"`, LLM-resolved statements get `source: "llm"`. No `claude` actions are persisted — all must be resolved.

6. **Collect candidate data**:
   - `dependencies: { tables, views, functions }` — fully resolved base tables from the call graph
   - `rationale` — why this procedure was identified as a writer (e.g. "Catalog referenced_by shows is_updated=true")

### Step 3 — Apply Resolution Rules

| Condition | Status |
|---|---|
| One writer | `resolved` — set `selected_writer` |
| Two or more writers | `ambiguous_multi_writer` |
| No writers | `no_writer_found` |
| Discover failed | `error` |

### Step 4 — Write Scoping Results to Catalog

For each item, assemble the scoping JSON from Steps 1–3:

```json
{
  "status": "resolved",
  "selected_writer": "dbo.usp_load_fact_sales",
  "candidates": [
    {
      "procedure_name": "dbo.usp_load_fact_sales",
      "dependencies": {
        "tables": ["bronze.salesraw", "bronze.customer"],
        "views": [],
        "functions": []
      },
      "rationale": "Catalog referenced_by shows is_updated=true for this procedure."
    }
  ],
  "warnings": [],
  "errors": []
}
```

Each candidate is built from Step 1 (writer list from `discover refs`) and Step 2 (resolved `dependencies` from `discover show` chain). The `rationale` explains why the procedure was identified as a writer.

Then run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover write-scoping \
  --name <item_id> --scoping '<json>'
```

Non-obvious cross-field checks before writing:

- `resolved` → `selected_writer` present and matches a `procedure_name` in `candidates`.
- `resolved` → selected writer candidate has `dependencies` populated (from Step 2 refs).
- `ambiguous_multi_writer` → at least two candidates, no `selected_writer`.

Set `validation.passed = false` and populate `validation.issues[]` on failure.

### Step 5 — Write Summary Output

Write a lightweight summary JSON (`scoping_summary.json` schema) to the output file path:

```json
{
  "schema_version": "1.0",
  "run_id": "<from input>",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "resolved|ambiguous_multi_writer|no_writer_found|error",
      "catalog_path": "catalog/tables/dbo.fact_sales.json"
    }
  ],
  "summary": {
    "total": 5,
    "resolved": 3,
    "ambiguous_multi_writer": 1,
    "no_writer_found": 0,
    "error": 1
  }
}
```

The full scoping data lives in the catalog files, not duplicated in the summary output. The summary is for orchestrator routing and status tracking.

---

## Error and Warning Codes

Every entry in `errors[]` or `warnings[]` uses this format:

```json
{
  "item_id": "silver.dimcustomer",
  "code": "CATALOG_FILE_MISSING",
  "message": "catalog/tables/silver.dimcustomer.json not found.",
  "severity": "error"
}
```

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/<item_id>.json not found — skip item |
| `DISCOVER_EXECUTION_FAILED` | error | `discover refs` or `discover show` CLI failed — skip item |
| `LLM_ANALYSIS_REQUIRED` | warning | claude_assisted proc required LLM resolution — item proceeds |
