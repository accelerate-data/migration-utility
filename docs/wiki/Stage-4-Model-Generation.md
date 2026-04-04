# Stage 4 -- Model Generation

The `/generate-model` command generates dbt models from stored procedures using the profile and test spec data. It includes a self-correction loop for `dbt test` failures and a code review loop for quality. This is the final migration stage before PR review and merge.

## Prerequisites

- `manifest.json` must exist
- `dbt/dbt_project.yml` must exist (run `/init-dbt` first; if missing, all items fail with `DBT_PROJECT_MISSING`)
- Per table: catalog file, `scoping.selected_writer`, `profile` with `status: "ok"`, and `test-specs/<item_id>.json` must all be present

## Invocation

```text
/generate-model silver.DimCustomer silver.FactInternetSales
```

## Pipeline

### Step 1 -- Worktree setup

Creates a git worktree with branch `feature/generate-model-<table1>-<table2>-...` (truncated to 60 characters after `feature/`). Before creating a new worktree, the command scans for existing worktrees and offers to continue on one of them. New worktrees clear `.migration-runs/` and write `meta.json`.

### Step 2 -- Model generation

One sub-agent per table runs in parallel, each following the `/generating-model` skill:

1. Reads the table's catalog file (profile, scoping, columns, keys, FKs)
2. Reads the test spec with ground truth expectations
3. Reads the writer procedure's source code
4. Generates the dbt model SQL and schema YAML

**CTE pattern.** Generated models follow a CTE-based structure: source CTEs reference `{{ source() }}` macros, transformation CTEs implement the procedure's logic, and a final SELECT assembles the output.

**Materialization mapping.** The profiling classification drives the materialization:

| Classification | Materialization | Strategy |
|---|---|---|
| `fact_transaction` | `incremental` | Based on watermark type |
| `dim_scd1` | `table` or `incremental` | Based on volume |
| `dim_scd2` | `incremental` | Snapshot pattern |
| `dim_non_scd` | `table` | Full refresh |
| `fact_periodic_snapshot` | `table` | Full refresh |
| `fact_accumulating_snapshot` | `incremental` | Merge on PK |
| `fact_aggregate` | `table` | Full refresh |
| `dim_junk` | `table` | Full refresh |

### Step 3 -- Self-correction loop (dbt test)

After generating the model, the sub-agent runs `dbt test` to validate the model against the unit tests from the test spec. If tests fail, the sub-agent analyzes the error output and self-corrects the model.

- Maximum 3 self-correction iterations
- Each iteration modifies the model SQL and/or schema YAML, then re-runs `dbt test`
- If tests still fail after 3 iterations, the model is written as-is with a `DBT_TEST_FAILED` warning

### Step 4 -- Code review loop

For each item where `dbt test` passes, a `/reviewing-model` sub-agent evaluates:

- Code quality and dbt best practices
- Correctness of the translation from T-SQL to dbt SQL
- Test integration and schema YAML completeness
- Materialization appropriateness

Review outcomes:

| Verdict | Action |
|---|---|
| `approved` | Proceed to summary |
| `approved_with_warnings` | Proceed with noted issues |
| `revision_requested` | Feedback sent back to generator; model is revised and `dbt test` re-run |

Maximum 2 review iterations per item. After the maximum, the item is approved with warnings and proceeds.

### Step 5 -- Equivalence checking

The generation skill performs a semantic equivalence analysis between the original stored procedure and the generated dbt model. Any gaps are recorded as `EQUIVALENCE_GAP` warnings. These do not block the model from being written -- they flag areas for manual review.

### Step 6 -- Summary and PR

```text
generate-model complete -- 2 tables processed

  ok silver.DimCustomer    ok
  ~  silver.FactInternetSales  partial (EQUIVALENCE_GAP)

  ok: 1 | partial: 1
```

Only model SQL and schema YAML files from successful items are staged. The PR body includes materialization type and `dbt compile` status for each table.

## What Gets Produced

| File | Location | Purpose |
|---|---|---|
| Model SQL | `dbt/models/staging/<model_name>.sql` | dbt model implementing the stored procedure logic |
| Schema YAML | `dbt/models/staging/_<model_name>.yml` | Model description, schema tests, and `unit_tests:` rendered from the test spec |

The model name follows the pattern `stg_<table>` (e.g., `stg_dimcustomer` for `silver.DimCustomer`).

## Error Codes

| Code | When |
|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` missing -- all items fail |
| `DBT_PROJECT_MISSING` | `dbt_project.yml` not found -- all items fail |
| `CATALOG_FILE_MISSING` | Catalog file not found -- item skipped |
| `SCOPING_NOT_COMPLETED` | No `selected_writer` -- item skipped |
| `PROFILE_NOT_COMPLETED` | Profile missing or not `ok` -- item skipped |
| `TEST_SPEC_NOT_FOUND` | Test spec not found -- item skipped |
| `GENERATION_FAILED` | `/generating-model` skill failed -- item skipped |
| `EQUIVALENCE_GAP` | Semantic gap between proc and generated model -- item proceeds as partial |
| `DBT_COMPILE_FAILED` | `dbt compile` failed after retries -- item proceeds as partial |
| `DBT_TEST_FAILED` | `dbt test` failed after 3 self-corrections -- item proceeds as partial |
| `REVIEW_KICKED_BACK` | Reviewer requested revision -- item retried |
| `REVIEW_APPROVED_WITH_WARNINGS` | Reviewer approved with remaining issues after max iterations |

## After Model Generation

- Review and merge the PR opened by the command
- Run `/cleanup-worktrees` to remove the worktree after the PR is merged
- Repeat for additional tables in your migration scope
- Use the [[Status Dashboard]] to track overall migration progress across all tables
