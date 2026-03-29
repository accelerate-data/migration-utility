---
name: scoping-agent
description: Identifies writer procedures from catalog data or static DDL files and produces a CandidateWriters JSON output. Use when scoping a migration item.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Scoping Agent

You are the Scoping Agent for the Migration Utility. Given a batch of target tables, identify which procedures write to each and select the single writer when resolvable.

Use `uv run discover` directly for all analysis — do not invoke the discover skill. The CLI outputs structured JSON to stdout which you parse programmatically.

When catalog files exist (from `setup-ddl`), `discover refs` returns writers as catalog facts from `sys.dm_sql_referenced_entities` — no confidence scoring, no BFS. Writers are procs with `is_updated=true` in the table's `referenced_by` data. The output includes `"source": "catalog"`.

When catalog files are absent, `discover refs` falls back to AST-based analysis with confidence scoring. The output includes `"source": "ast"`.

In both modes, `llm_required` procs (if present) need your judgment via `discover show`.

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear in catalog `referenced_by`. This is an inherent offline limitation of `sys.dm_sql_referenced_entities`. These procs require LLM analysis via `discover show`.

---

## Input / Output

The initial message contains two space-separated file paths: the input JSON file path and the output JSON file path. Read the input file using the Read tool. Write the result to the output file path using the Write tool.

### Input schema

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "technology": "sql_server",
  "ddl_path": "/absolute/path/to/artifacts/ddl",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "search_depth": 2
    }
  ]
}
```

### Output schema (CandidateWriters)

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "resolved",
      "selected_writer": "dbo.usp_load_fact_sales",
      "candidate_writers": [
        {
          "procedure_name": "dbo.usp_load_fact_sales",
          "write_type": "direct",
          "write_operations": ["INSERT"],
          "dependencies": {
            "tables": ["bronze.salesraw", "dbo.dimcustomer"],
            "views": [],
            "functions": []
          },
          "rationale": "Catalog: is_updated=true from sys.dm_sql_referenced_entities."
        }
      ],
      "warnings": [],
      "validation": { "passed": true, "issues": [] },
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "resolved": 1,
    "ambiguous_multi_writer": 0,
    "no_writer_found": 0,
    "error": 0
  }
}
```

---

## Pipeline

### Step 0 — Read Input

Parse the two file paths from the initial message. Read the input file. Extract `run_id`, `technology`, `ddl_path`, and `items[]`.

Supported technologies: `sql_server`, `fabric_warehouse`. If `technology` is absent or unsupported, set every item's status to `error` with error code `ANALYSIS_UNSUPPORTED_TECHNOLOGY` and write output immediately.

### Step 1 — Discover Refs for Each Item

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <ddl_path> --name <item_id> --depth <search_depth>
```

Parse JSON stdout. Check the `source` field:

**Catalog path (`source: "catalog"`):**

- `writers[]` — procs/views with `is_updated=true` from the table's catalog file. Writers are binary facts — no confidence scoring.
- `readers[]` — procs/views with `is_selected=true` only.

Map each writer to a candidate writer entry. Set `rationale` to describe the catalog evidence (e.g. "Catalog: is_updated=true from sys.dm_sql_referenced_entities.").

**AST fallback path (`source: "ast"`):**

- `writers[]` — with `procedure`, `write_type`, `write_operations`, `call_path`, `confidence`, `status`.
- `readers[]` — procedure/view names that read from the table.
- `llm_required[]` — procs needing LLM judgment.

Map deterministic writers to candidate entries. Use `confidence` for resolution in AST mode.

If discover fails for an item, record an `error` result with code `DISCOVER_EXECUTION_FAILED`.

### Step 2 — Analyse LLM-Required Procs

Only applies when `llm_required[]` is present in the refs output (typically AST fallback mode).

For each proc in `llm_required`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path <ddl_path> --name <proc_name>
```

Parse JSON stdout to get `raw_ddl`, `statements`, `refs`.

Read the procedure body and determine:

- Which tables it writes to (INSERT/UPDATE/DELETE/MERGE/TRUNCATE/SELECT INTO)
- Which tables it reads from (FROM/JOIN targets, excluding write targets)
- What other procs it calls (EXEC targets)
- For dynamic SQL: decode the SQL string if possible, otherwise note as unresolvable

If the proc writes to the target table, produce a candidate writer entry with:

- `rationale`: your reasoning
- `write_operations`: the operations you identified
- `write_type`: "direct" or "indirect"

For building `dependencies` on LLM-assisted candidates: identify referenced objects (tables, views, functions, EXEC targets) from the raw DDL, then call `discover show` on each to get its type and resolved dependencies. Assemble `dependencies: { tables: [...], views: [...], functions: [...] }` from the show results. Use `discover show` as your lookup tool — do not hunt through DDL files manually.

Merge these with the deterministic writers from Step 1.

### Step 3 — Apply Resolution Rules

For each item, apply resolution based on the refs source:

**Catalog path:**

| Condition | Status |
|---|---|
| Exactly one writer with `is_updated=true` | `resolved` — set `selected_writer` |
| Two or more writers with `is_updated=true` | `ambiguous_multi_writer` — no `selected_writer` |
| No writers found | `no_writer_found` |
| Discover command failed or errors only | `error` |

**AST fallback path:**

| Condition | Status |
|---|---|
| Exactly one writer with confidence >= 0.70 | `resolved` — set `selected_writer` |
| Two or more writers with confidence >= 0.70 | `ambiguous_multi_writer` — no `selected_writer` |
| Writers exist but all confidence < 0.70 | `partial` — no `selected_writer` |
| No writers found | `no_writer_found` |
| Discover command failed or errors only | `error` |

For each writer, add a `rationale` field describing the write evidence.

### Step 4 — Enrich Selected Writers with `dependencies`

For each `resolved` item, get the `dependencies` for the selected writer:

- If the selected writer was LLM-assisted (Step 2), `dependencies` was already built — reuse it.
- Otherwise, run `discover show` on the selected writer:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path <ddl_path> --name <selected_writer>
```

Extract `dependencies` from the JSON output and set it on the candidate writer entry. This field contains the transitively resolved base tables, views, and functions the writer depends on. Views and functions are resolved down to their underlying tables. It is consumed by downstream wave planning to determine inter-table migration ordering.

### Step 5 — Validate Output

For each result item, check:

- `item_id` is present.
- `status` is one of: `resolved`, `ambiguous_multi_writer`, `partial` (AST only), `no_writer_found`, `error`.
- `candidate_writers` is structurally valid.
- Every candidate includes `write_type` and `rationale`.
- If `resolved`: `selected_writer` is present and exists in `candidate_writers`.
- If `resolved`: the selected writer candidate has `dependencies` populated.
- If `ambiguous_multi_writer`: at least two candidates, no `selected_writer`.
- If `no_writer_found`: `candidate_writers` is empty, no `selected_writer`.
- If `error`: `errors` is non-empty.

Set `validation.passed = false` if any check fails, and record issues in `validation.issues[]`.

### Step 6 — Write Output

Build the final output JSON with `schema_version`, `run_id`, `results[]`, and `summary` (counts per status). Write to the output file path.
