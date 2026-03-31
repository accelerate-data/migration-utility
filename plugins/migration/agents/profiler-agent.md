---
name: profiler-agent
description: Batch profiling agent that produces migration profile candidates for each table. Runs profile.py for context, applies LLM reasoning for the six profiling questions, and writes results into catalog files.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Profiler Agent

Given a batch of target tables, produce migration profile candidates for each table and write them into the table catalog files. The selected writer for each table is read from the `scoping` section in `catalog/tables/<table>.json`.

Use `uv run profile` directly for context assembly and catalog writes. Do not read or write catalog files directly -- use `profile context` and `profile write` as your interface.

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

### Step 1 -- Assemble Context

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" profile context \
  --table <item_id>
```

The command reads `selected_writer` from the catalog scoping section — no `--writer` argument needed.

If the command fails (exit code 1 or 2), record `status: "error"` with the failure message in `errors[]` and continue to the next item.

### Step 2 -- LLM Profiling

Using the context JSON, answer the six profiling questions (Q1–Q6) defined in [profiling-signals.md](../skills/profile-table/references/profiling-signals.md). Follow the signal tables and pattern matching rules in that reference — do not abbreviate.

**Catalog facts are answers, not candidates.** If the catalog declares a PK, that is the PK. If the catalog has declared FKs, those are confirmed FKs. The LLM fills in what the catalog does not answer.

### Step 3 -- Write to Catalog

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" profile write \
  --table <item_id> \
  --profile '<json>'
```

The profile JSON must include `status`, `writer`, and the profiling answers. All enum values must be from the allowed sets defined in `docs/design/agent-contract/profiler-agent.md`.

No approval gates in batch mode -- write directly after reasoning.

### Step 4 -- Handle Errors

- If `profile context` fails: set `status: "error"`, record in `errors[]`, continue to next item.
- If LLM cannot answer a required question (classification, primary_key, watermark): set `status: "partial"`, record which questions are unresolved in `warnings[]`, continue to next item.
- If `profile write` fails: set `status: "error"`, record in `errors[]`, continue to next item.
- Do not stop the batch on individual item failures.

---

## `source` Field Semantics

- `"catalog"` -- fact from setup-ddl catalog data. Not inferred.
- `"llm"` -- inferred by LLM from proc body / column patterns / reference tables.
- `"catalog+llm"` -- catalog provided the base fact, LLM added classification.

## `status` Field

- `ok` -- required questions answered (classification, primary_key, watermark).
- `partial` -- one or more required questions unanswered.
- `error` -- runtime failure prevented profiling.

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

All codes use the shared diagnostics schema (`code`, `message`, `severity`, `details`). Recorded in the item's `errors[]` or `warnings[]`.

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `CONTEXT_ASSEMBLY_FAILED` | error | `profile context` CLI failed — skip item |
| `PROFILE_WRITE_FAILED` | error | `profile write` CLI failed — skip item |
| `PARTIAL_PROFILE` | warning | LLM could not answer a required question — item proceeds with partial |
