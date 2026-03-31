# Procedure Analysis Flow

Standard flow for analysing a stored procedure. Used by:

- **Interactive skill:** `show <proc>` (standalone) and `show <table>` (per writer candidate) — present results to user for review.
- **Batch agent:** scoping agent Step 2 — resolve and write without user interaction.

Steps describe what to *produce*. The caller decides whether to present for confirmation (interactive) or write directly (batch).

## Prerequisites

Run `discover show --name <proc>` to get the procedure's `refs`, `statements`, `classification`, and `raw_ddl`.

## Step 1 — Classify

Check `classification` to decide how much help you get:

1. `deterministic` with `statements` populated — `refs` and `statements` are pre-classified, use them alongside the body as the authoritative source of truth.
2. `claude_assisted` or `statements` is null — classify each statement yourself from the body. See [`tsql-parse-classification.md`](tsql-parse-classification.md) for classification guidance.

## Step 2 — Call Graph

Read/write targets from `refs`. Resolve to base tables: if a ref is a view, function, or procedure (not a base table), run `discover show` on it to get its refs, and follow the chain until you reach base tables. Produce the full resolved lineage.

```text
silver.usp_load_DimCustomer  (direct writer)
  ├── reads: silver.vw_ProductCatalog (view)
  │     ├── reads: bronze.Customer        ← resolved via discover show
  │     └── reads: bronze.Product         ← resolved via discover show
  ├── reads: bronze.Person
  └── writes: silver.DimCustomer
```

## Step 3 — Logic Summary

Always produced by reading `raw_ddl`. Plain-language description of what the procedure does, step by step. No tags, no classification — just explain the logic.

## Step 4 — Migration Guidance

Tag each statement as `migrate` or `skip`:

| Action | Meaning |
|---|---|
| `migrate` | Core transformation (INSERT, UPDATE, DELETE, MERGE, SELECT INTO) — becomes the dbt model |
| `skip` | Operational overhead (SET, TRUNCATE, DROP/CREATE INDEX) — dbt handles or ignores |

```text
Migration Guidance
  1. [skip]    TRUNCATE TABLE silver.DimCustomer
  2. [migrate] INSERT INTO silver.DimCustomer from vw_ProductCatalog JOIN bronze.Person
  3. [migrate] Computes DateFirstPurchase via OUTER APPLY on bronze.SalesOrderHeader
```

## Step 5 — Persist Resolved Statements

After analysis, persist resolved statements to catalog.

**For deterministic procedures** (`classification: deterministic`, no `claude` actions in statements):

All statements are already classified by the AST. Persist immediately — no additional user confirmation needed:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover write-statements \
  --name <procedure_name> --statements '<json>'
```

All statements get `source: "ast"`.

**For claude-assisted procedures** (`classification: claude_assisted` or statements containing `action: "claude"`):

1. Read `raw_ddl` and analyse each `claude` statement — follow the call graph, resolve dynamic SQL, and classify as `migrate` or `skip`.
2. Produce the full resolved statement list with each statement's proposed action and rationale.
3. **Interactive path:** present for user confirmation before persisting. **Batch path:** persist directly.
4. Run `discover write-statements` to persist. All resolved statements get `source: "llm"`.

No `claude` actions are written to catalog — all must be resolved before persisting.
