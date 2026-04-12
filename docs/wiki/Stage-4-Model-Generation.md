# Stage 4 -- Model Generation

The `/generate-model` command generates dbt models from stored procedures using the refactored SQL, profile, and test spec data. It includes a self-correction loop for `dbt test` failures and a code review loop for quality. This is the final migration stage before PR review and merge.

## Prerequisites

- `manifest.json` must exist
- `dbt/dbt_project.yml` must exist (run `/init-dbt` first; if missing, all items fail with `DBT_PROJECT_MISSING`)
- Per table: catalog file, `scoping.selected_writer`, `profile` with `status: "ok"`, and `test-specs/<item_id>.json` must all be present.
- `refactored_sql` must exist in the writer procedure's catalog (produced by `/refactor`). This is a hard prerequisite -- model generation will fail without it.

## Invocation

```text
/generate-model silver.DimCustomer silver.FactInternetSales
```

## Pipeline

### Step 1 -- Worktree setup

Sets up an isolated worktree for the batch. If on `main`, the command warns and offers to create a worktree. If existing worktrees are found, the FDE can choose to continue on one of them instead of creating a new one.

### Step 2 -- Model generation

One sub-agent per table runs in parallel, each following the `/generating-model` skill:

1. Reads `refactored_sql` from the table's catalog refactor section — this is the CTE-structured SQL produced by `/refactor`. Raw proc SQL is not used.
2. Reads the test spec with ground truth expectations
3. Reads the table's profile (materialization, primary key, watermark, etc.)
4. Generates one reviewable dbt artifact set per target from the refactored SQL. Import CTEs use `{{ source(...) }}` directly inside the model; helper staging models are not generated.
5. Writes the model SQL and paired schema YAML for the reviewed target

**Generated model.** The model keeps the import/logical/final CTE structure from `refactored_sql` in a single file. Import CTEs read from `{{ source('<schema>', '<table>') }}` directly, logical CTEs preserve the transformed logic, and the config block uses the profile-derived materialization.

**Materialization mapping.** The profiling classification drives the mart materialization:

| Classification | Materialization | Strategy |
|---|---|---|
| `fact_transaction` | `incremental` | Based on watermark type |
| `dim_scd1` | `table` or `incremental` | Based on volume |
| `dim_scd2` | `snapshot` | Snapshot pattern |
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

For each item where `dbt test` passes, a `/reviewing-model` sub-agent evaluates correctness, standards compliance, test integration, and materialization appropriateness against structured reference guidelines.

Review feedback uses three severity tiers:

| Tier | Meaning | Model-generator response required |
|---|---|---|
| `error` | Must fix before approval | Yes — `fixed` or `ignored: <reason>` |
| `warning` | Should fix; ack required | Yes — `fixed` or `ignored: <reason>` |
| `info` | Advisory only | No |

Review outcomes:

| Verdict | Action |
|---|---|
| `approved` | Proceed to commit |
| `approved_with_warnings` | Proceed with noted issues |
| `revision_requested` | Feedback (with stable codes) sent back to generator; model is revised and `dbt test` re-run |

Maximum 2 review iterations per item. After the maximum, the item is approved with warnings and proceeds.

### Step 5 -- Equivalence checking

The generation skill performs a semantic equivalence analysis between the refactored SQL and the generated dbt model. Any gaps are recorded as `EQUIVALENCE_GAP` warnings. These do not block the model from being written -- they flag areas for manual review.

### Step 6 -- Per-table commit and summary

After review completes for each item, the result is committed automatically:

- Items with status `ok` or `partial` are committed immediately after review (no batch approval step)
- Items with status `error` have their files reverted inline: `git checkout -- <files>`
- The `/commit` command handles staging, commit message, and push

At the end of the run:

```text
generate-model complete -- 2 tables processed

  ok silver.DimCustomer    ok
  ~  silver.FactInternetSales  partial (EQUIVALENCE_GAP)

  ok: 1 | partial: 1
```

You are offered a PR. Run `/status` to check overall pipeline progress.

## What Gets Produced

| File | Location | Purpose |
|---|---|---|
| Model SQL | `dbt/models/staging/<model_name>.sql` | Reviewable dbt model implementing the target logic with inline `source()` import CTEs |
| Schema YAML | `dbt/models/staging/_<model_name>.yml` | Model description, schema tests, and `unit_tests:` rendered from the test spec |
| Snapshot SQL | `dbt/snapshots/` | Snapshot artifact for `dim_scd2` targets |

The model name follows the target naming contract (for example, `stg_dimcustomer` for `silver.DimCustomer` when written under `models/staging/`).

## Error Codes

| Code | When |
|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` missing -- all items fail |
| `DBT_PROJECT_MISSING` | `dbt_project.yml` not found -- all items fail |
| `CATALOG_FILE_MISSING` | Catalog file not found -- item skipped |
| `SCOPING_NOT_COMPLETED` | No `selected_writer` -- item skipped |
| `PROFILE_NOT_COMPLETED` | Profile missing or not `ok` -- item skipped |
| `TEST_SPEC_NOT_FOUND` | Test spec not found -- item skipped |
| `REFACTOR_NOT_COMPLETED` | Refactor section missing or no `refactored_sql` -- item skipped |
| `GENERATION_FAILED` | `/generating-model` skill failed -- item skipped |
| `EQUIVALENCE_GAP` | Semantic gap between refactored SQL and generated model -- item proceeds as partial |
| `DBT_COMPILE_FAILED` | `dbt compile` failed after retries -- item proceeds as partial |
| `DBT_TEST_FAILED` | `dbt test` failed after 3 self-corrections -- item proceeds as partial |
| `REVIEW_KICKED_BACK` | Reviewer requested revision -- item retried |
| `REVIEW_APPROVED_WITH_WARNINGS` | Reviewer approved with remaining issues after max iterations |

## After Model Generation

- Review and merge the PR opened by the command
- Run `/cleanup-worktrees` to remove the worktree after the PR is merged
- Repeat for additional tables in your migration scope
- Use the [[Status Dashboard]] to track overall migration progress across all tables
