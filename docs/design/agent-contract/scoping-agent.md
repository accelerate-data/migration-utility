# Scoping Agent Contract

The scoping agent maps a target table to one or more candidate writer procedures in SQL Server.
It is the prerequisite step for profiler input.

## Philosophy and Boundary

- Analysis is responsible only for writer discovery and writer selection.
- Analysis should not output data that profiler can derive reliably from the selected writer.
- Keep analysis payload minimal for clear handoff.

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

- `technology` — source technology family; determines which analysis patterns to apply.
- `ddl_path` — absolute path to the DDL artifacts directory. Passed to every MCP tool call; no `DDL_PATH` environment variable required.
- `search_depth` is the maximum call-graph traversal depth from discovered candidate procedures.
- Units: procedure-call hops (`0` = candidate procedure body only, `1` = direct callees, etc.).
- Valid range: integer `0..5`.
- Default: `2`.

## Discovery Strategy

### 1. DiscoverCandidates

- Use SQL Server dependency metadata to find procedures that reference the target table.
- Note: metadata-only discovery can miss `TRUNCATE`-only writers and dynamic SQL writers.

### 2. ResolveCallGraph

- Parse procedure bodies and extract `EXEC` calls.
- Build bounded call graph up to `search_depth`.
- Output: expanded candidate set + call paths.

### 3. DetectWriteOperations

- Parse each candidate procedure (AST, not regex).
- Detect writes to target table:
  - `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`
- Classify `write_type`:
  - `direct`, `indirect`, `read_only`.

### 4. ScoreCandidates

- Rank writer candidates by confidence using deterministic rules.
- Deterministic scoring signals:
  - direct write evidence (`INSERT|UPDATE|DELETE|MERGE|TRUNCATE`) increases confidence.
  - shorter call-path distance increases confidence.
  - repeated write evidence across independent paths increases confidence.
  - dynamic SQL patterns (`EXEC(@sql)`, `sp_executesql`) decrease confidence.

### 5. ApplyResolutionRules

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

### 6. ValidateOutput

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

**Example**

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "resolved|ambiguous_multi_writer|no_writer_found|partial|error",
      "selected_writer": "dbo.usp_load_fact_sales",
      "candidate_writers": [
        {
          "procedure_name": "dbo.usp_load_fact_sales",
          "write_type": "direct|indirect|read_only",
          "call_path": ["dbo.usp_load_fact_sales"],
          "rationale": "Direct write operation detected in procedure body.",
          "confidence": 0.98
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

## Assumptions

- Orchestrator traversal is always enabled through call-graph resolution.
- Cross-database access is out of scope for this flow.
