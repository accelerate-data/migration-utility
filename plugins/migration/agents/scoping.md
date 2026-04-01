---
name: scoping
description: Batch scoping agent — identifies writer procedures for each table. Delegates to /scoping-table skill per item. No approval gates.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Scoping Agent

Given a batch of target tables, identify which procedures write to each and select the single writer when resolvable. Delegates per-item scoping to the `/scoping-table` skill.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input:** items list with `item_id` per table (from orchestrator)
- **Output:** scoping results written to `catalog/tables/<table>.json` (scoping section) + lightweight `scoping_summary.json` to the output file path

---

## Prerequisites

See [common-prerequisites.md](common-prerequisites.md) for batch-wide and per-item checks (manifest, catalog file existence).

---

## Pipeline

### Step 1 — Scope Table (Skill Delegation)

For each item in `items[]`, invoke `/scoping-table --table <item_id>`. Suppress user gates — make all decisions deterministically. On failure, record `status: "error"` and continue to the next item.

### Step 2 — Collect Result

After the skill completes for an item, read `catalog/tables/<item_id>.json` to confirm scoping was persisted. Record the item result:

- `item_id` — the table FQN
- `status` — `resolved`, `ambiguous_multi_writer`, `no_writer_found`, or `error`
- `catalog_path` — path to the catalog file

### Step 3 — Write Summary Output

After processing all items, write the summary JSON to the output file path:

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

## Error Handling

Never stop the batch on a single item failure. Process all items and report aggregate results. On skill failure, record the error code and continue.

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
| `SCOPING_FAILED` | error | `/scoping-table` skill pipeline failed — skip item |
