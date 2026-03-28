# Scoping Agent Contract

The scoping agent maps a target table to one or more candidate writer procedures for T-SQL sources (SQL Server, Fabric Warehouse).
It is the prerequisite step for profiler input.

## Philosophy and Boundary

- Analysis is responsible only for writer discovery and writer selection.
- Analysis should not output data that profiler can derive reliably from the selected writer.
- Keep analysis payload minimal for clear handoff.
- Exception: `reads_from` is included on candidate writers to support downstream wave planning without requiring re-analysis.

## Goal

Given a target table, identify candidate writer procedures and select one writer when resolvable.

## Required Input

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

## Input Semantics

- `technology` — source technology family; determines which analysis patterns to apply. Valid values: `sql_server`, `fabric_warehouse`, `fabric_lakehouse`, `snowflake`. The agent emits `ANALYSIS_UNSUPPORTED_TECHNOLOGY` for unsupported values.
- `ddl_path` — absolute path to the DDL artifacts directory. Passed to every CLI invocation; no `DDL_PATH` environment variable required.
- `search_depth` is the maximum call-graph traversal depth from discovered candidate procedures.
- Units: procedure-call hops (`0` = candidate procedure body only, `1` = direct callees, etc.).
- Valid range: integer `0..5`.
- Default: `2`.

## Discovery Strategy

> **Two-tier analysis.** The scoping agent calls `uv run discover refs` per table for deterministic AST-based writer detection and call-graph resolution. Procedures that need LLM assistance (control flow, EXEC, dynamic SQL) are flagged in `llm_required` — the agent calls `discover show` on those and reasons about writes/reads from the raw DDL. No regex fallback is used.

The agent's role is batch orchestration: read the input, run `discover refs` per item, analyse LLM-required procs via `discover show`, apply resolution rules, enrich selected writers with `reads_from`, validate, and write the output.

### Resolution Rules

- Return one of: `resolved`, `ambiguous_multi_writer`, `partial`, `no_writer_found`, `error`.
- Status rules:
  - `resolved`: exactly one high-confidence candidate with direct or well-supported indirect evidence.
  - `ambiguous_multi_writer`: two or more high-confidence candidates remain after scoring.
  - `partial`: at least one candidate exists, but evidence is incomplete/low-confidence
    (for example only dynamic SQL evidence or unresolved call paths).
  - `no_writer_found`: no candidates found.
  - cross-database reference detected: return `error` with issue code
    `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE`.
  - `error`: execution/parsing/runtime failure prevented completion.

### `dependencies` Enrichment

For each `resolved` item, the agent populates `dependencies` on the selected writer's candidate entry. This contains the transitively resolved base tables, views, and functions the writer procedure depends on. Views and functions are resolved down to their underlying base tables. The data is obtained from:

- `discover show` on the selected writer → `dependencies` field (deterministic candidates — resolution is automatic)
- Agent judgment + `discover show` lookups on referenced objects (LLM-assisted candidates)

This field is consumed by downstream wave planning to build inter-table dependency graphs without re-analysing source DDL.

### Validation

- Run internal consistency/contract checks on the output item (for example selected writer
  consistency and required fields).
  Runtime failures must be reported in `errors`.
- Validation checklist:
  - `item_id` is present.
  - `status` is one of: `resolved|ambiguous_multi_writer|partial|no_writer_found|error`.
  - `candidate_writers` is structurally valid.
  - every candidate `confidence` is within `[0,1]`.
  - every candidate includes `write_type`, `call_path`, and `rationale`.
  - if `status == "resolved"`:
    - `selected_writer` is present
    - `selected_writer` exists in `candidate_writers`
    - the selected writer candidate has `dependencies` populated
  - if `status == "ambiguous_multi_writer"`:
    - at least two candidates are present
    - `selected_writer` is absent
  - if `status == "partial"`:
    - `candidate_writers` is non-empty
  - if `status == "no_writer_found"`:
    - `candidate_writers` is empty
    - `selected_writer` is absent
  - if `status == "error"`:
    - `errors` is non-empty
  - `validation.passed` is `false` when any validation issue exists.
  - summary counts match item-level statuses.

## Output Schema (CandidateWriters)

```json
{
  "schema_version": "",
  "run_id": "",
  "results": [
    {
      "item_id": "",
      "status": "",
      "analysis": "",
      "selected_writer": "",
      "candidate_writers": []
      "warnings": [],
      "validation": {..}
      "errors": []
    }
  ],
  "summary": {
    "total": 0,
    "resolved": 0,
    "ambiguous_multi_writer": 0,
    "no_writer_found": 0,
    "partial": 0,
    "error": 0
  }
}
```

**`analysis` field:** `"deterministic"` when all candidates came from AST analysis (high trust); `"claude_assisted"` when any candidate required LLM reasoning (control flow, EXEC, dynamic SQL). Present on both per-item and per-candidate levels.

**Example**

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "resolved|ambiguous_multi_writer|no_writer_found|partial|error",
      "analysis": "deterministic|claude_assisted",
      "selected_writer": "dbo.usp_load_fact_sales",
      "candidate_writers": [
        {
          "procedure_name": "dbo.usp_load_fact_sales",
          "write_type": "direct|indirect|read_only",
          "write_operations": ["INSERT"],
          "call_path": ["dbo.usp_load_fact_sales"],
          "dependencies": {
            "tables": ["bronze.salesraw", "dbo.dimcustomer"],
            "views": [],
            "functions": []
          },
          "rationale": "Direct write operation detected in procedure body.",
          "confidence": 0.98,
          "analysis": "deterministic"
        }
      ],
      "warnings": [],
      "validation": {
        "passed": true,
        "issues": []
      },
      "errors": []
    }
  ],
  "summary": {...}
}
```

## Resolution Rules

- `resolved`: exactly one high-confidence writer exists.
- `ambiguous_multi_writer`: multiple high-confidence writers exist.
- `partial`: candidates exist but evidence is incomplete/insufficient for deterministic selection.
- `no_writer_found`: no writer candidate found.
- `error`: analysis execution failed for the table.

## Known Limitations

- Dynamic SQL (`sp_executesql`, `EXEC(@sql)`) may hide dependencies from metadata.
- Synonyms/views may mask base-table writers.
- `TRUNCATE`-only writers may be missed by dependency metadata and require body-parse detection.
- `dependencies` for LLM-assisted candidates depends on agent judgment from reading raw DDL combined with `discover show` lookups — accuracy varies with procedure complexity.

## Assumptions

- Orchestrator traversal is always enabled through call-graph resolution.
- Cross-database access is out of scope for this flow.
