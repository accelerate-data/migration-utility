---
name: status
description: >
  Migration status dashboard — shows per-object readiness across all pipeline stages.
  In batch mode, also builds a transitive dependency graph and computes
  maximally-parallel execution batches, surfaces catalog diagnostics with
  LLM-generated triage. In single-table mode, shows a detailed per-stage breakdown.
user-invocable: true
argument-hint: "[schema.table]"
---

# Status

Show migration progress for one table (detailed) or all tables (summary). Calls the `migrate-util dry-run` CLI for deterministic prerequisite checks and content, then applies LLM reasoning to interpret patterns and recommend next steps.

## Guards

- `manifest.json` must exist. If missing, tell the user to run `/setup-ddl` first.
- `catalog/tables/` must contain at least one `.json` file. If empty, tell the user to run `/setup-ddl` first.

## Pipeline — No table argument (batch summary)

### Step 1 — Enumerate objects

List all catalog object files from both tables and views:

```bash
ls catalog/tables/*.json catalog/views/*.json 2>/dev/null
```

Extract item IDs from filenames (strip `.json` suffix). Track which FQNs came from `catalog/tables/` (type = `table`) vs `catalog/views/` (type = `view` or `mv`). For view FQNs, read the catalog file and check `is_materialized_view` — if true, type = `mv`.

### Step 2 — Collect status per object

For each object, iterate stages in order: `scope`, `profile`, `test-gen`, `refactor`, `migrate`.

For each stage, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util dry-run <fqn> <stage>
```

Parse the JSON output:

- If `not_applicable` is `true`: record "N/A" for this stage only, then continue to the next stage.
- If `guards_passed` is `true` and content shows the stage is complete (e.g. scoping has `selected_writer`, profile has `status: ok|partial`, test-gen has `test_spec_status`, refactor has `refactor_status` or `dbt_model_exists: true` for views, migrate has `dbt_model_exists`): record the stage as complete and continue.
- If `guards_passed` is `false` (and `not_applicable` is absent/false): record the stage as blocked at that stage. Stop iterating — subsequent stages are implicitly blocked.

### Step 3 — Build summary table

Present a human-readable summary:

```text
migration status — 6 objects (4 tables, 2 views)

  Object                        type    scope      profile    test-gen   refactor   migrate
  ─────────────────────────────────────────────────────────────────────────────────────────
  silver.DimCustomer            table   resolved   ok         ok         ok         pending
  silver.DimProduct             table   resolved   partial    blocked    blocked    blocked
  silver.DimDate                table   resolved   pending    blocked    blocked    blocked
  silver.FactSales              table   pending    blocked    blocked    blocked    blocked
  silver.RefCurrency            table   resolved   N/A        N/A        N/A        N/A
  silver.vDimSalesTerritory     view    resolved   ok         N/A        pending    N/A
  silver.vwFactPromo            mv      pending    blocked    N/A        blocked    N/A

  scope: 4/6 | profile: 2/5 (1 N/A) | test-gen: 1/4 (2 N/A) | refactor: 1/5 (1 N/A) | migrate: 0/4 (2 N/A)
```

Stage status values:

- Stage complete: show the stage's status value (`resolved`, `ok`, `partial`, etc.)
- Stage is the first incomplete: `pending`
- Stage blocked by prior incomplete stage: `blocked`
- Stage does not apply to this object (writerless table): `N/A`

Object type values: `table`, `view`, `mv`.

Header object count: show total objects plus a breakdown e.g. `(4 tables, 2 views)`. If MVs are present, count them separately: `(3 tables, 1 view, 2 mvs)`.

Summary counts: the denominator excludes N/A objects. Show the N/A count in parentheses if any exist, e.g. `profile: 2/4 (1 N/A)`. Views are included in the denominator for `scope`, `profile`, and `refactor`. Views are excluded from the denominator for `test-gen` and `migrate` because those stages return `not_applicable` for views — treat them as N/A for counting purposes.

### Step 4 — Sync exclusion warnings and build dependency schedule

First, sync EXCLUDED_DEP catalog warnings so that any active objects depending on excluded objects have up-to-date warnings, and any stale warnings are cleared:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util sync-excluded-warnings
```

Then run the batch planner to get the full dependency-aware execution plan:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util batch-plan
```

Parse the JSON output and present two new sections.

#### Section A — Dependency Schedule

Show the recommended execution order. Each phase or batch can be worked in parallel within it.

```text
dependency schedule

  Phase 0 — Scope / Analyze  (run in parallel — no dependencies between them)
    silver.DimDate          table   scope_needed
    silver.vDimSalesTerritory  view  scope_needed

  Phase 1 — Profile  (run in parallel after scope is resolved)
    silver.DimGeography     table   profile_needed

  Migration Batch 1  (no unresolved in-scope dependencies)
    silver.DimProduct       table   test_gen_needed
    silver.vwFactPromo      view    migrate_needed

  Migration Batch 2  (depends on Batch 1)
    silver.FactSales        table   migrate_needed
      blocked by: silver.DimProduct (test_gen_needed), silver.DimDate (scope_needed)
```

Rules for the schedule output:

- Show scope_phase as "Phase 0 — Scope / Analyze" if any objects need scope work.
- Show profile_phase as "Phase 1 — Profile" if any objects need profiling. Note that scope and profile phases are independent across objects (different objects can be scoped and profiled in parallel) but a given table must complete scope before it can be profiled.
- Show migrate_batches as "Migration Batch N" (1-indexed for readability). Objects in the same batch are independent and can be processed in parallel.
- For objects with `blocking_deps`, list them with their pipeline_status in a "blocked by" line. Call out when a blocking dep is in an earlier pipeline phase (scope/profile) vs another migration batch.
- Omit completed_objects from the schedule (they are done).
- Show n_a_objects as a brief note: "N writerless tables (source tables, no migration needed)".
- If circular_refs is non-empty, flag them: "N objects excluded — circular dependency detected. Review CIRCULAR_REFERENCE diagnostics."
- If `summary.excluded_count > 0`, show at the bottom of the status output: "N objects excluded from pipeline — edit catalog JSON to re-include"

#### Section B — Catalog Diagnostics

If `catalog_diagnostics.total_errors > 0` or `catalog_diagnostics.total_warnings > 0`, present a triage section:

```text
catalog diagnostics  (3 errors, 5 warnings)

  Errors
    silver.FactSales      MULTI_TABLE_WRITE   Writer proc writes to 3 tables — only one can be the dbt model target
    silver.vwFactPromo    DDL_PARSE_ERROR     View DDL failed to parse: near "PIVOT": syntax error
    dbo.usp_helper_prep   CROSS_DB_EXEC       Procedure executes cross-database call — dynamic SQL cannot be statically analyzed

  Warnings
    silver.DimDate        STALE_OBJECT        Object was present in prior extraction but absent in latest
    ...
```

After listing the diagnostics, provide LLM-generated triage: for each unique error code present, generate one concise remediation action (1–2 sentences). Group by code, not by object. Examples:

- "MULTI_TABLE_WRITE (1 table): The writer proc targets multiple tables. Use `/scope` to re-select a single-table writer, or split the proc."
- "DDL_PARSE_ERROR (1 view): The view DDL has unsupported syntax. Review the view definition and simplify before running `/analyzing-view`."
- "STALE_OBJECT (1 table): Object was removed from the source. Verify it is no longer needed and remove its catalog file if so."

If there are no diagnostics, omit this section entirely.

#### Section C — Sources staleness check

If `dbt/models/staging/sources.yml` exists and any table has `scope_needed` status, show: "sources.yml may be stale — N tables have incomplete scoping. Re-run `/init-dbt` after scoping is complete."

#### Section D — init-dbt readiness hint

If `dbt/dbt_project.yml` does **not** exist AND the batch-plan `scope_phase` is empty (all in-scope objects have completed scope), show:

```text
ready to initialise dbt  — all tables are scoped. Run /init-dbt to scaffold your dbt project.
```

If there are still tables in the scope phase, omit this hint entirely. Do not show the hint if `dbt/dbt_project.yml` already exists.

#### Section E — Stale catalog cleanup

After presenting all sections above, check whether any `STALE_OBJECT` warnings appeared in the catalog diagnostics (Section B). This step applies only when running in batch mode (no table argument).

If one or more `STALE_OBJECT` entries are present:

1. Collect the catalog file path for each affected FQN. Check each of these paths and use whichever exists:
   - `catalog/tables/<fqn>.json`
   - `catalog/procedures/<fqn>.json`
   - `catalog/views/<fqn>.json`
   - `catalog/functions/<fqn>.json`

2. Present the list:

   ```text
   The following N catalog file(s) are marked stale (from a prior extraction):
     catalog/tables/silver.dimcustomer.json
     catalog/procedures/dbo.usp_load.json
   ```

3. Ask: **"Delete these N stale catalog files?"**

4. If the user confirms: delete each file. Then show:

   ```text
   Deleted N file(s). Run `git add -u && git commit -m "remove stale catalog objects"` to record the cleanup.
   ```

5. If the user declines: leave the files intact and continue normally.

6. If no `STALE_OBJECT` entries exist: omit this section entirely.

## Pipeline — With table argument (detailed)

### Step 1 — Collect detailed status

For each stage in order (`scope`, `profile`, `test-gen`, `refactor`, `migrate`), run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util dry-run <table> <stage> --detail
```

Parse the JSON output.

### Step 2 — Present per-stage breakdown

For each stage, present the guard results and content:

```text
status for silver.DimCustomer

  scope ✓
    selected_writer: dbo.usp_load_dimcustomer
    candidates: 1
    statements: 3 migrate, 1 skip, 0 unresolved

  profile ✓
    status: ok
    classification: dim_scd1
    primary_key: surrogate (CustomerKey)
    watermark: ModifiedDate
    foreign_keys: 2
    pii_actions: 1
    questions: 6/6 answered

  test-gen ✓
    status: ok, coverage: complete
    branches: 4, tests: 6
    sandbox: __test_abc123

  refactor ✓
    status: ok
    has_refactored_sql: true

  migrate ✗ — pending
    guard failed: REFACTOR_NOT_COMPLETED
    → Run /refactoring-sql silver.DimCustomer
```

For the first failing stage, explain what prerequisite is missing and suggest the specific command to run.

If `not_applicable` is `true` for a stage, show it as `N/A` and continue to the next stage.

For completed stages, show the key signals from the `--detail` content:

- **scope (table):** selected_writer, candidate count, statement resolution counts
- **scope (view):** scoping_status, is_materialized_view
- **profile (table):** status, resolved_kind, primary_key type, watermark column, FK count, PII count, questions answered/total
- **profile (view):** profile_status, classification, source
- **test-gen:** status, coverage, branch count, test count, sandbox database
- **refactor (table):** status, has_refactored_sql
- **refactor (view):** dbt_model_exists, model_name
- **migrate:** dbt model exists, schema YAML has unit_tests, compiled, test results

### Step 3 — Recommend next action

Based on the first incomplete stage, recommend the specific command to run next for this table.

## Error handling

| Situation | Action |
|---|---|
| `migrate-util dry-run` returns exit code 1 | Report the domain error from JSON output |
| `migrate-util dry-run` returns exit code 2 | Report IO error, suggest checking project setup |
| `migrate-util sync-excluded-warnings` returns exit code 2 | Log warning to stderr, continue — exclusion warnings may be stale |
| `migrate-util batch-plan` returns exit code 2 | Report IO error, suggest checking project setup |
| `migrate-util batch-plan` returns `{"error": ...}` | Report the error and suggest running `/setup-ddl` |
| No catalog files found | Tell user to run `/setup-ddl` first |
| `CLAUDE_PLUGIN_ROOT` not set | Tell user to load the plugin with `claude --plugin-dir <path>` |
