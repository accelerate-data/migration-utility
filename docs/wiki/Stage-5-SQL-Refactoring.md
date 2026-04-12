# Stage 5 -- SQL Refactoring

This stage restructures raw stored procedure SQL into a clean import/logical/final CTE pattern and proves the refactored SQL produces identical results via sandbox execution. The output stays in T-SQL -- dbt Jinja conversion happens in the downstream model generation stage.

## Table Refactoring (`/refactor`)

Restructures stored procedure SQL into CTE-based SELECT statements with a self-correcting equivalence audit loop. Launches one sub-agent per table in parallel, each running the `/refactoring-sql` skill.

### Prerequisites

- `manifest.json` with `sandbox.database` (run `/setup-sandbox` if missing)
- Sandbox database must exist (checked via `test-harness sandbox-status`)
- Per table: catalog file, `scoping.selected_writer`, `profile` with `status: "ok"`, and `test-specs/<item_id>.json` must all be present

### Invocation

```text
/refactor silver.DimCustomer silver.FactInternetSales
```

### Pipeline

**Step 1 -- Setup.** Creates a worktree for the batch and generates a run epoch for artifact filenames.

**Step 2 -- Refactor per table.** Each sub-agent runs `/refactoring-sql`, which launches two isolated sub-agents in parallel:

- **Sub-agent A** extracts the core SELECT from the stored procedure (ground truth)
- **Sub-agent B** restructures the SQL into import/logical/final CTEs

Both outputs are compared in the sandbox via `test-harness compare-sql` using the test spec fixtures.

**Step 3 -- Self-correction loop.** If any scenario fails equivalence (refactored output differs from extracted output), the refactored CTE SQL is revised and re-tested. Maximum 3 iterations. After 3 failed iterations the item is marked `partial`.

**Step 4 -- Write to catalog.** The refactored SQL and extracted SQL are written to the table's catalog file under the `refactor` section via `refactor write`.

**Step 5 -- Summary and PR.** The command presents per-table results:

```text
refactor complete -- 2 tables processed

  ok silver.DimCustomer    3 CTEs, all scenarios passed
  ~  silver.FactInternetSales  partial (2/5 scenarios passed)

  ok: 1 | partial: 1
```

Successfully refactored items are committed and pushed automatically. You are offered a PR at the end.

### What gets produced

| File | Section | Purpose |
|---|---|---|
| `catalog/tables/<item_id>.json` | `refactor.refactored_sql` | CTE-structured SQL (import/logical/final) |
| `catalog/tables/<item_id>.json` | `refactor.extracted_sql` | Core SELECT extracted from the stored procedure |
| `catalog/tables/<item_id>.json` | `refactor.status` | `ok`, `partial`, or `error` |

### Error Codes

| Code | When |
|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` missing -- all items fail |
| `SANDBOX_NOT_CONFIGURED` | No `sandbox.database` in manifest |
| `SANDBOX_NOT_RUNNING` | Sandbox database not found |
| `CATALOG_FILE_MISSING` | Catalog file not found -- item skipped |
| `SCOPING_NOT_COMPLETED` | No `selected_writer` -- item skipped |
| `PROFILE_NOT_COMPLETED` | Profile missing or not `ok` -- item skipped |
| `TEST_SPEC_NOT_FOUND` | Test spec not found -- item skipped |
| `REFACTOR_FAILED` | Refactoring skill pipeline failed -- item skipped |
| `EQUIVALENCE_PARTIAL` | Not all scenarios passed after max iterations -- item proceeds as partial |

## View Refactoring (`/refactor-view`)

Refactors SQL Server views into import/logical/final CTE SQL and persists the proof-backed refactor for downstream model generation. Resolves dependent views in topological order and checks equivalence against catalog column metadata.

### Prerequisites

- `manifest.json` must exist
- `dbt/dbt_project.yml` must exist (run `/init-dbt` first)
- View catalog file `catalog/views/<schema.view_name>.json` must exist (run `/setup-ddl` first)

### Invocation

```text
/refactor-view silver.vw_customer_dim
```

### Pipeline

**Step 1 -- Dependency resolution.** Walks `references.views.in_scope` recursively to build the full transitive dependency set. Leaf views (no dependencies) are processed first, the requested view last.

**Step 2 -- Refactor per view.** Each view is converted into refactored SQL with import/logical/final CTE structure. T-SQL syntax is converted to standard SQL (COALESCE instead of ISNULL, CAST instead of CONVERT).

**Step 3 -- Equivalence check.** Catalog column metadata is compared against the generated model's SELECT list. Missing columns are flagged as warnings.

**Step 4 -- dbt compile and test.** Runs `dbt compile` and `dbt test` on the generated staging models.

### What gets produced

| File | Purpose |
|---|---|
| Refactored SQL persisted in the view catalog | Proof-backed CTE-structured SQL used by downstream generation |

### Error Codes

| Code | When |
|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` missing |
| `DBT_PROJECT_MISSING` | `dbt_project.yml` not found |
| `VIEW_CATALOG_MISSING` | View catalog file not found |
| `VIEW_REFACTOR_FAILED` | Refactoring skill pipeline failed |
| `DBT_COMPILE_FAILED` | `dbt compile` returned non-zero exit |
| `DBT_TEST_FAILED` | `dbt test` returned non-zero exit |
| `COLUMN_MISSING` | Column from original view not found in generated model (warning) |

## Next Step

Proceed to [[Stage 4 Model Generation]] to generate dbt models from the refactored SQL.
