---
name: analyzing-object
description: >
  Analyse a stored procedure, view, or function for migration. Resolves call graphs, classifies statements, produces a logic summary and migration guidance, and persists results to catalog.
user-invocable: true
argument-hint: "--name <fqn>"
---

# Analyzing Object

Deep-dive analysis of a single stored procedure, view, or function. Produces call graph, statement classification, logic summary, migration guidance, and persists resolved statements to catalog.

## Arguments

Parse `$ARGUMENTS` for the required option:

| Option | Required | Values |
|---|---|---|
| `--name` | yes | Fully-qualified object name (e.g. `dbo.usp_load_DimCustomer`, `silver.vw_CustomerSales`) |

If `--name` is missing, use `AskUserQuestion` to prompt for it.

## Before invoking

Read `manifest.json` from the current working directory to confirm it is a valid project root and to understand the source technology and dialect. If the manifest is missing, stop and tell the user to run `setup-ddl` first.

## Procedures

Follow these steps in order. Do not abbreviate — every step must complete before moving to the next.

### Step 1 — Fetch object data

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover show \
  --name <proc>
```

This returns `refs`, `statements`, `classification`, and `raw_ddl`.

### Step 2 — Classify statements

Check the `classification` field:

- **`deterministic`** with `statements` populated — `refs` and `statements` are pre-classified by the AST. Use them alongside the body as the authoritative source of truth.
- **`claude_assisted`** or `statements` is null — classify each statement yourself from `raw_ddl`. See [`references/tsql-parse-classification.md`](references/tsql-parse-classification.md) for classification guidance.

### Step 3 — Resolve call graph

Read/write targets come from `refs`. Resolve to base tables: if a ref is a view, function, or procedure (not a base table), run `discover show` on it to get its refs and follow the chain until you reach base tables. Present the full lineage:

```text
silver.usp_load_DimCustomer  (direct writer)
  +-- reads: silver.vw_ProductCatalog (view)
  |     +-- reads: bronze.Customer        <- resolved via discover show
  |     +-- reads: bronze.Product         <- resolved via discover show
  +-- reads: bronze.Person
  +-- writes: silver.DimCustomer
```

### Step 4 — Logic summary

Read `raw_ddl` and produce a plain-language description of what the procedure does, step by step. No tags, no classification — just explain the logic.

### Step 5 — Migration guidance

Tag each statement as `migrate` or `skip`:

| Action | Meaning |
|---|---|
| `migrate` | Core transformation (INSERT, UPDATE, DELETE, MERGE, SELECT INTO) — becomes the dbt model |
| `skip` | Operational overhead (SET, TRUNCATE, DROP/CREATE INDEX) — dbt handles or ignores |

Present the tagged list:

```text
Migration Guidance
  1. [skip]    TRUNCATE TABLE silver.DimCustomer
  2. [migrate] INSERT INTO silver.DimCustomer from vw_ProductCatalog JOIN bronze.Person
  3. [migrate] Computes DateFirstPurchase via OUTER APPLY on bronze.SalesOrderHeader
```

### Step 6 — Persist resolved statements

After presenting the analysis to the user, persist resolved statements to catalog.

**Deterministic procedures** (`classification: deterministic`, no `claude` actions in statements): all statements are already classified by the AST. Persist immediately after presenting Migration Guidance — no additional user confirmation needed. All statements get `source: "ast"`.

**Claude-assisted procedures** (`classification: claude_assisted` or statements containing `action: "claude"`):

1. Read `raw_ddl` and analyse each `claude` statement — follow the call graph, resolve dynamic SQL, and classify as `migrate` or `skip`.
2. Present the full resolved statement list for confirmation. Show each statement with its proposed action and rationale.
3. After confirmation (with any edits), persist. All resolved statements get `source: "llm"`. Each statement must include a `rationale` field (1–2 sentences) explaining why it is `migrate` or `skip`.

No `claude` actions are written to catalog — all must be resolved before persisting.

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover write-statements \
  --name <procedure_name> --statements '<json>'
```

See [`references/procedure-analysis-flow.md`](references/procedure-analysis-flow.md) for the full canonical flow.

## Views

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover show \
  --name <view>
```

Show refs and the view definition:

```text
silver.vw_CustomerSales (view)

  Reads from: silver.DimCustomer, silver.FactSales

  Definition:
    SELECT c.FirstName, SUM(f.Amount) AS TotalSales
    FROM silver.DimCustomer c
    JOIN silver.FactSales f ON c.CustomerKey = f.CustomerKey
    GROUP BY c.FirstName
```

## Functions

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover show \
  --name <function>
```

Show refs and the function definition. Present the return type and any referenced objects.

## Error handling

- If `discover show` exits with code 2, the catalog directory could not be read (missing path, IO error, no catalog). Report the error and stop.
- Procedures with `parse_error` set are still loaded — not skipped. Their `raw_ddl` is preserved for inspection. Report the parse error to the user and proceed with `raw_ddl`-based analysis.
- If call graph resolution hits a circular reference, stop recursion and report the cycle.
- If dynamic SQL cannot be reconstructed (variable target table, external input), report as unresolvable and note what can be determined.
