---
name: scope
description: >
  This skill should be used when the user asks to "find what writes to [table]", "which procedures populate [table]", "scope out writers for [table]", "identify the writer for [table]", or needs to determine which stored procedure is responsible for loading a specific target table.
user-invocable: true
argument-hint: "[ddl-path] [table-name]"
---

# Scope

Identify stored procedures that write to a target table. Used by the scoping-agent for batch processing. For individual proc analysis, `discover show` provides the same data (`refs`, `write_operations`, `classification`).

## Arguments

Parse `$ARGUMENTS`:

- `ddl-path` (required): path to the directory containing `.sql` files
- `table-name` (required): fully-qualified target table (e.g. `dbo.FactSales`)

If either is missing from `$ARGUMENTS`, ask the user before proceeding. Do not assume any default path.

### Options

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--table` | yes | fully-qualified target table name |
| `--dialect` | no | sqlglot dialect (default: `tsql`) |
| `--depth` | no | maximum call-graph depth (default: `3`) |

Invocation examples are in [`rules/workflow.md`](rules/workflow.md).

### Output shape

```json
{
  "table": "dbo.FactSales",
  "writers": [
    {
      "procedure_name": "dbo.usp_LoadFactSales",
      "write_type": "direct",
      "write_operations": ["TRUNCATE", "INSERT"],
      "call_path": ["dbo.usp_LoadFactSales"],
      "confidence": 0.90,
      "status": "confirmed"
    }
  ],
  "errors": [
    {
      "procedure": "dbo.usp_CrossDbSync",
      "code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE",
      "message": "Procedure references a cross-database name."
    }
  ],
  "llm_required": ["dbo.usp_ConditionalLoad"]
}
```

`llm_required` is present when procedures with unparseable control flow or EXEC/dynamic SQL are found. These procs need LLM analysis — see the workflow for handling.
```

## Workflow

Follow the step sequence in [`rules/workflow.md`](rules/workflow.md).

## Confidence scoring

Each writer entry has a `confidence` score in [0.0, 1.0] and a derived `status`:

| Status | Condition | Action |
|---|---|---|
| `confirmed` | confidence ≥ 0.70 | Treat as a definite writer |
| `suspected` | confidence < 0.70 | Escalate — do not proceed automatically |

Scoring signals (computed deterministically):

| Signal | Effect |
|---|---|
| Direct write evidence (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`, `SELECT_INTO`) | base 0.90 |
| Indirect write (callee is a confirmed direct writer) | base 0.75 |
| Shorter call path (per hop shorter than deepest path in candidate set) | +0.02 |
| Multiple independent paths with write evidence | +0.05 |

## Relationship with discover

When scope returns procs in `llm_required`, run `discover show` on each to get `raw_ddl` and `statements`. Read the procedure body, identify writes/reads/calls, and produce a writer entry with `analysis: "claude_assisted"` and a confidence score reflecting your certainty.

- **`needs_llm: false`** → scope handled it deterministically. Use the scope output directly.
- **`needs_llm: true`** → the proc has unparseable control flow or EXEC. Scope placed it in `llm_required`. Read `raw_ddl` from discover show to complete the analysis.

## Escalation rule

When a writer has `"status": "suspected"`:

1. Run `discover show` on the procedure to get its `raw_ddl` and `refs`.
2. Read the procedure body to determine whether it actually writes to the target table.
3. Confirm or reject:
   - **Confirmed:** include in the migration plan.
   - **Rejected:** exclude and note the reason.
4. Do not proceed to migration steps until every `suspected` entry has a decision.

Present to the user before inspecting:

```text
dbo.usp_StageLoad has confidence 0.65 (suspected).
Inspecting procedure body to verify whether it writes to dbo.FactSales...
```

## Error codes

| Code | Meaning | Action |
|---|---|---|
| `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE` | Proc references a cross-database name (3+ part) | Exclude from migration plan. Cross-database writes need a separate pipeline. |
| `PARSE_FAILED` | Entire CREATE PROCEDURE block failed to parse (block-level failure) | Read `raw_ddl` via discover show. Analyse manually — same as `llm_required` procs. |
