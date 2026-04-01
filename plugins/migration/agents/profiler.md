---
name: profiler
description: Batch profiling agent — produces migration profiles for each table by delegating to /profiling-table skill. No approval gates.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Profiler Agent

Given a batch of target tables, produce migration profile candidates for each table and write them into the table catalog files. Delegates per-item profiling to the `/profiling-table` skill.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input schema:** `../lib/shared/schemas/profiler_input.json`
- **Output schema:** See Batch Output section below.

---

## Prerequisites

Before processing items:

1. Read `manifest.json` from the current working directory for `technology` and `dialect`. If missing or unreadable, fail **all** items with code `MANIFEST_NOT_FOUND` and write output immediately.

Per item, before Step 1:

- Check `catalog/tables/<item_id>.json` exists. If missing, skip this item with `CATALOG_FILE_MISSING` in `errors[]`.
- Check `scoping.selected_writer` is set. If scoping section is missing or `selected_writer` is null, skip this item with `SCOPING_NOT_COMPLETED` in `errors[]`.

---

## Pipeline

### Step 1 — Profile Table (Skill Delegation)

For each item in `items[]`, follow the `/profiling-table` skill pipeline. The skill handles context assembly (via `profile context`), LLM profiling (Q1-Q6 from `profiling-signals.md`), and catalog persistence (via `profile write`).

**Batch overrides — do not use `AskUserQuestion`:**

- Make all profiling decisions deterministically
- If a required question (Q1 classification, Q2 primary key, Q5 watermark) cannot be answered, set `status: "partial"` and record the unresolved questions as `PARTIAL_PROFILE` warnings — do not stop
- Auto-approve all profiles — write directly without confirmation
- On `profile write` failure, record `PROFILE_WRITE_FAILED` and continue

If any step in the skill pipeline fails, record `status: "error"` with the appropriate error code and continue to the next item.

### Step 2 — Record Result

For each item, record:

- `item_id` — the table FQN
- `status` — `ok`, `partial`, or `error`
- `catalog_path` — path to the catalog file

---

## `source` Field Semantics

- `"catalog"` — fact from setup-ddl catalog data. Not inferred.
- `"llm"` — inferred by LLM from proc body / column patterns / reference tables.
- `"catalog+llm"` — catalog provided the base fact, LLM added classification.

## `status` Field

- `ok` — required questions answered (classification, primary_key, watermark).
- `partial` — one or more required questions unanswered.
- `error` — runtime failure prevented profiling.

---

## Batch Output

After processing all items, write a summary to the output file path:

```json
{
  "schema_version": "1.0",
  "run_id": "<from input>",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "ok",
      "catalog_path": "catalog/tables/dbo.fact_sales.json"
    }
  ],
  "summary": {
    "total": 1,
    "ok": 1,
    "partial": 0,
    "error": 0
  }
}
```

The actual profile data lives in the catalog file, not duplicated in the batch output.

---

## Error and Warning Codes

Every entry in `errors[]` or `warnings[]` uses this format:

```json
{
  "item_id": "silver.dimcustomer",
  "code": "SCOPING_NOT_COMPLETED",
  "message": "scoping section missing or no selected_writer in catalog for silver.dimcustomer.",
  "severity": "error"
}
```

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILING_FAILED` | error | `/profiling-table` skill pipeline failed — skip item |
| `PROFILE_WRITE_FAILED` | error | `profile write` CLI failed — skip item |
| `PARTIAL_PROFILE` | warning | LLM could not answer a required question — item proceeds with partial |
