# Stage 1 -- Scoping

The `/scope` command identifies which stored procedures write to each target table. It launches one sub-agent per table in parallel, each running the `/analyzing-table` skill.

## Prerequisites

- `manifest.json` must exist (if missing, all items fail with `MANIFEST_NOT_FOUND`)
- `catalog/tables/<item_id>.json` must exist for each table (if missing, the item is skipped with `CATALOG_FILE_MISSING`)

## Invocation

Pass one or more fully-qualified table names:

```text
/scope silver.DimCustomer silver.DimProduct silver.FactInternetSales
```

Single-table invocation works the same way -- it is just a batch of one.

## How It Works

### Step 1 -- Worktree setup

The command creates a git worktree to isolate the batch:

- Branch name: `feature/scope-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters after `feature/`)
- Worktree path: `../worktrees/feature/scope-<slug>`
- Clears `.migration-runs/` and writes `meta.json` with command metadata

If the worktree and branch already exist, they are reused.

### Step 2 -- Per-table scoping

One sub-agent per table runs in parallel. Each sub-agent follows the `/analyzing-table` skill pipeline:

1. Reads catalog signals (`referenced_by` in the table's catalog file) to identify candidate writer procedures
2. For each candidate, follows the procedure analysis reference to analyze the procedure's code (call graph, statement classification, persistence) and determine whether it actually writes to the target table
3. Resolves the `selected_writer` from the analysis results

### Step 3 -- Revert errored items

For any table that errored, the command reverts partially modified catalog files:

```bash
git checkout -- catalog/tables/<item_id>.json
```

### Step 4 -- Summary and PR

The command collects per-table results and presents a summary:

```text
scope complete -- 3 tables processed

  ok silver.DimCustomer    resolved
  ok silver.DimProduct     resolved
  !! silver.DimDate        error (CATALOG_FILE_MISSING)

  resolved: 2 | error: 1
```

If at least one item succeeded, the command asks whether to commit and open a PR. Only catalog JSON files from successful items are staged. The PR body includes a table of results with writer procedure names.

## Output

The scoping result is written directly to the table's catalog file as `scoping.selected_writer`:

```json
{
  "scoping": {
    "selected_writer": "dbo.usp_load_dimcustomer",
    "candidates": [...],
    "resolution": "..."
  }
}
```

## Item Result Statuses

| Status | Meaning |
|---|---|
| `resolved` | A single writer procedure was identified |
| `ambiguous_multi_writer` | Multiple candidate writers found; needs manual resolution |
| `no_writer_found` | No procedure writes to this table |
| `error` | Runtime failure prevented scoping |

## Error Codes

| Code | When |
|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` missing -- all items fail |
| `CATALOG_FILE_MISSING` | Catalog file not found for a table -- item skipped |
| `SCOPING_FAILED` | `/analyzing-table` skill pipeline failed -- item skipped |

## Next Step

Proceed to [[Stage 2 Profiling]] to classify tables and identify keys, watermarks, and PII.
