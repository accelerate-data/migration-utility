# Skill: Scoping Table

## Purpose

Discovers which stored procedures write to a target table, analyzes each candidate via the procedure analysis reference (call graph resolution, statement classification, persistence), resolves which writer owns the table, and persists the scoping decision to the table catalog file. This skill determines the `selected_writer` that all downstream skills (profiling, test generation, model generation) depend on.

## Invocation

```text
/scoping-table <schema.table>
```

Argument is the fully-qualified table name (e.g., `silver.DimCustomer`, `[dbo].[FactSales]`). The skill asks if missing.

## Prerequisites

- `manifest.json` must exist in the project root. If missing, run [[Skill Setup DDL]] first.
- `catalog/tables/<table>.json` must exist. If missing, run `/listing-objects list tables` to see available tables.

## Pipeline

### 1. Show columns from catalog

Reads `catalog/tables/<table>.json` and presents the column list with types and nullability.

### 2. Discover writer candidates

```bash
uv run --project <shared-path> discover refs --name <table>
```

Extracts the `writers` array from the output. If no writers are found, persists `no_writer_found` to catalog and stops.

### 3. Analyze each writer candidate

For each writer candidate, follows the procedure analysis reference (`references/procedure-analysis.md`). This runs a 6-step pipeline per candidate:

1. **Fetch object data** — `discover show --name <proc>` returns refs, statements, classification, raw_ddl
2. **Classify statements** — deterministic (AST) or claude_assisted (LLM-based from raw_ddl)
3. **Resolve call graph** — follow refs to base tables via recursive `discover show`
4. **Logic summary** — plain-language explanation of what the procedure does
5. **Migration guidance** — tag each statement as `migrate` or `skip`
6. **Persist resolved statements** — write to `catalog/procedures/<proc>.json`

If there are multiple candidates, they are analyzed sequentially.

### 4. Present writer candidates

Displays a summary of each analyzed candidate:

```text
Writer candidates for silver.DimCustomer:

  1. dbo.usp_load_dimcustomer_full (direct writer)
     Reads: bronze.Customer, bronze.Person
     Writes: silver.DimCustomer
     Statements: 1 migrate, 1 skip

  2. dbo.usp_load_dimcustomer_delta (direct writer)
     Reads: bronze.Customer, silver.DimCustomer
     Writes: silver.DimCustomer
     Statements: 1 migrate (MERGE)
```

### 5. Resolution

| Condition | Action |
|---|---|
| 1 writer | Auto-select, confirm with user before persisting |
| 2+ writers | Present candidates and ask the user to pick |
| 0 writers | Report `no_writer_found` (handled in Step 2) |

The skill waits for explicit user confirmation before proceeding.

### 6. Persist scoping to catalog

Writes scoping JSON via a temp file to avoid shell quoting issues:

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project <shared-path> discover write-scoping \
  --name <table> --scoping-file .staging/scoping.json; rm -rf .staging
```

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<table>.json` | Column list and existing catalog state |
| `catalog/procedures/<writer>.json` | Read during procedure analysis (Step 3) |

## Writes

### `scoping` section in `catalog/tables/<table>.json`

| Field | Type | Required | Description |
|---|---|---|---|
| `status` | string | yes | Enum: `resolved`, `ambiguous_multi_writer`, `no_writer_found`, `error` |
| `selected_writer` | string | no | Normalized FQN of the selected writer procedure. Present only when `status` is `resolved` |
| `selected_writer_rationale` | string | yes | 1-2 sentences explaining why this writer was chosen over alternatives, or why no writer / ambiguous |
| `candidates` | array | no | All analyzed writer candidates |
| `candidates[].procedure_name` | string | yes | Normalized FQN of the candidate procedure |
| `candidates[].dependencies` | object | no | Dependencies: `tables[]`, `views[]`, `functions[]` |
| `candidates[].rationale` | string | yes | Why this candidate was considered |
| `warnings` | array | no | Diagnostics entries (code, message, severity) |
| `errors` | array | no | Diagnostics entries (code, message, severity) |

### `statements` in `catalog/procedures/<writer>.json`

Written during procedure analysis (Step 3). Each statement has `action` (migrate/skip), `source` (ast/llm), `sql`, and optionally `rationale`.

### `scoping` status values

| Status | Meaning |
|---|---|
| `resolved` | Exactly one writer confirmed. `selected_writer` is set. |
| `ambiguous_multi_writer` | Multiple writers found, user did not resolve. `selected_writer` is null. |
| `no_writer_found` | No procedures write to this table in the catalog. `selected_writer` is null. |
| `error` | Analysis failed for all candidates. `selected_writer` is null. |

## JSON Format

### Scoping section example (resolved)

```json
{
  "scoping": {
    "status": "resolved",
    "selected_writer": "silver.usp_load_dimcustomer",
    "selected_writer_rationale": "Only writer found for silver.DimCustomer. Performs TRUNCATE + INSERT from bronze sources.",
    "candidates": [
      {
        "procedure_name": "silver.usp_load_dimcustomer",
        "dependencies": {
          "tables": ["bronze.customer", "bronze.person"],
          "views": [],
          "functions": []
        },
        "rationale": "Direct writer: INSERT INTO silver.DimCustomer"
      }
    ]
  }
}
```

### Scoping section example (no writer found)

```json
{
  "scoping": {
    "status": "no_writer_found",
    "selected_writer": null,
    "selected_writer_rationale": "No procedures found that write to this table."
  }
}
```

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `discover refs` exit code 1 | Object not found or catalog file missing | Verify the table name with `/listing-objects list tables` |
| `discover refs` exit code 2 | Catalog directory unreadable (IO error) | Check file permissions on `catalog/` |
| Procedure analysis failure | Could not analyze a candidate procedure | Candidate is marked `BLOCKED`; remaining candidates continue. If all candidates fail, `status` is set to `error` |
| `discover write-scoping` exit code 1 | Validation failure (invalid JSON, missing fields) | Check scoping JSON structure against the schema above |
| `discover write-scoping` exit code 2 | Invalid JSON or IO error | Verify the `.staging/scoping.json` file was written correctly |
| No writers detected but proc clearly writes | Writer uses dynamic SQL that catalog queries cannot resolve | Known limitation of `sys.dm_sql_referenced_entities`. Run `/scoping-table` on the suspected table and manually scope |
