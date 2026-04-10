# Procedure Analysis

Deep-dive analysis of a single stored procedure. Produces call graph, statement classification, logic summary, migration guidance, and persists resolved statements to catalog.

The procedure name is the candidate writer identified by the parent analyzing-table skill.

## Pipeline

Follow these steps in order. Do not abbreviate — every step must complete before moving to the next.

### Step 1 — Fetch object data

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <proc>
```

### Step 2 — Classify statements

Check the `needs_llm` field and `statements` array:

- **`needs_llm: false`** with `statements` populated and no `action: "needs_llm"` entries — `refs` and `statements` are pre-classified by the AST. Use them alongside the body as the authoritative source of truth.
- **`needs_llm: false`** but `statements` is null, empty, or contains `action: "needs_llm"` entries — safety-net fallback. Treat as needs_llm: classify each statement yourself from `raw_ddl`. See the dialect-appropriate statement classification via [`tsql-parse-classification.md`](tsql-parse-classification.md).
- **`needs_llm: true`** or `statements` is null — classify each statement yourself from `raw_ddl`. See the dialect-appropriate statement classification via [`tsql-parse-classification.md`](tsql-parse-classification.md).

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

If the procedure delegates through a cross-database or linked-server `EXEC`, classify that statement as unsupported/out-of-scope in the rationale. Treat the procedure as a non-migratable candidate for writer selection. The parent skill decides the final table scoping outcome.

### Step 6 — Persist resolved statements

After presenting the analysis, persist resolved statements to catalog.

**Deterministic procedures** (`needs_llm: false`, no `action: "needs_llm"` entries in statements): all statements are already classified by the AST. Persist immediately after presenting Migration Guidance — no additional user confirmation needed. All statements get `source: "ast"`.

**LLM-assisted procedures** (`needs_llm: true` or statements containing `action: "needs_llm"`):

1. Read `raw_ddl` and analyse each `needs_llm` statement — follow the call graph, resolve dynamic SQL, and classify as `migrate` or `skip`.
2. Build the final resolved statement list and persist it immediately. Do not ask for confirmation before writing — this is a write-through workflow.
3. After the write succeeds, present the resolved statement list and the persisted outcome to the user. All resolved statements get `source: "llm"`. Each statement must include a `rationale` field (1-2 sentences) explaining why it is `migrate` or `skip`.

No `needs_llm` actions are written to catalog — all must be resolved before persisting.

Write the statements JSON to a temp file:

```bash
mkdir -p .staging
# Write statements JSON to .staging/statements.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-statements \
  --name <procedure_name> --statements-file .staging/statements.json; rm -rf .staging
```

After `discover write-statements` succeeds, report that the statements were persisted and summarize the migrate/skip decisions.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `discover show` | 1 | Object not found or catalog file missing. Report and stop |
| `discover show` | 2 | Catalog directory unreadable (IO error). Report and stop |
| `discover show` | 0 + `parse_error` set | Still loaded — `raw_ddl` preserved. Report parse error, proceed with `raw_ddl`-based analysis |
| `discover write-statements` | 1 | Procedure not found or invalid statements. Report validation error |
| `discover write-statements` | 2 | Invalid JSON input. Report and stop |
| call graph resolution | — | Circular reference: stop recursion and report the cycle |
| dynamic SQL reconstruction | — | Unresolvable (variable target, external input): report as unresolvable |
