---
name: status
description: >
  Migration status dashboard — shows per-object readiness across all pipeline stages.
  In batch mode, also builds a transitive dependency graph and computes
  maximally-parallel execution batches, surfaces catalog diagnostics with
  LLM-generated triage and a single actionable "What to do next" section. In single-table mode, shows a detailed per-stage breakdown.
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

### Step 3 — Sync exclusion warnings and run batch planner

First, sync EXCLUDED_DEP catalog warnings so that any active objects depending on excluded objects have up-to-date warnings, and any stale warnings are cleared:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util sync-excluded-warnings
```

Then run the batch planner to get the full dependency-aware execution plan and per-object diagnostics:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util batch-plan
```

Parse the JSON output. You will use it for: the diagnostic overlay in Step 4, the "What to do next" section in Step 5, and the catalog diagnostics section in Step 6.

Build a lookup map from `fqn` → node for all objects across `scope_phase`, `profile_phase`, `migrate_batches`, and `completed_objects`.

### Step 4 — Build summary table with diagnostic overlay

Present a human-readable summary:

```text
migration status — 6 objects (4 tables, 2 views)

  Object                        type    scope        profile    test-gen   refactor    migrate
  ──────────────────────────────────────────────────────────────────────────────────────────────
  silver.DimCustomer            table   resolved     ok         ok         ok          pending
  silver.DimProduct             table   resolved~    partial    blocked    blocked!    blocked!
  silver.DimDate                table   pending      blocked    blocked    blocked     blocked
  silver.FactSales              table   pending!     blocked!   blocked!   blocked!    blocked!
  silver.RefCurrency            table   resolved     N/A        N/A        N/A         N/A
  silver.vDimSalesTerritory     view    resolved     ok         N/A        pending     N/A
  silver.vwFactPromo            mv      pending      blocked    N/A        blocked     N/A

  scope: 4/6 | profile: 2/5 (1 N/A) | test-gen: 1/4 (2 N/A) | refactor: 1/5 (1 N/A) | migrate: 0/4 (2 N/A)

  ! error diagnostic present   ~ warning diagnostic present
```

Stage status values:

- Stage complete: show the stage's status value (`resolved`, `ok`, `partial`, etc.)
- Stage is the first incomplete: `pending`
- Stage blocked by prior incomplete stage: `blocked`
- Stage does not apply to this object (writerless table): `N/A`

**Diagnostic overlay** — for each object, look up its node in the batch-plan output and read `diagnostic_stage_flags`:

- If the node has `diagnostic_stage_flags.refactor == "error"`: the refactor stage cell shows its normal value with `!` appended (e.g. `pending!`, `ok!`). All subsequent stage cells that are not `N/A` show `blocked!`.
- If the node has `diagnostic_stage_flags.scope == "warning"`: the scope stage cell shows its normal value with `~` appended (e.g. `resolved~`). Subsequent stages are not affected.
- If the node has `diagnostic_stage_flags.refactor == "error"` AND `diagnostic_stage_flags.scope == "warning"`: apply both — scope cell gets `~`, refactor cell gets `!`, all post-refactor cells get `blocked!`.
- Objects with no `diagnostic_stage_flags` entries display exactly as before.

**Legend** (always show below the table):

```text
  ! error diagnostic present (this stage will likely fail — fix before proceeding)
  ~ warning diagnostic present (review before proceeding)
```

Object type values: `table`, `view`, `mv`.

Header object count: show total objects plus a breakdown e.g. `(4 tables, 2 views)`. If MVs are present, count them separately: `(3 tables, 1 view, 2 mvs)`.

Summary counts: the denominator excludes N/A objects and source/pending tables (they are not in the pipeline). Show the N/A count in parentheses if any exist, e.g. `profile: 2/4 (1 N/A)`. Views are included in the denominator for scope (they can be pending/complete for scope), but views are excluded from the denominator for profile/test-gen/refactor/migrate because those stages return `VIEW_STAGE_NOT_SUPPORTED` — treat them as implicit N/A for counting purposes. Source-confirmed tables (`is_source: true`) are excluded entirely from the status table and counts.

### Step 5 — What to do next

Using the batch-plan output from Step 3, build a single prioritised action list. Present at most 3 actions in this order:

1. **Fix diagnostic errors** (if `catalog_diagnostics.total_errors > 0`)
2. **Current pipeline phase** (the phase with the most objects needing work right now)
3. **Next pipeline phase** (the phase that unlocks after action 2 completes)

Skip any action if there is nothing in that category.

**Format:**

```text
What to do next

  1. Fix 2 error diagnostic(s) before proceeding:
       PARSE_ERROR on silver.FactSales — DDL failed to parse. Simplify the view DDL
         and re-run /setup-ddl, then /analyzing-view silver.FactSales.
       MULTI_TABLE_WRITE on silver.DimProduct — writer proc targets multiple tables.
         Use /scope to re-select a single-table writer, or split the proc.

  2. /scope silver.DimDate silver.vDimSalesTerritory silver.vwFactPromo  [1 excluded — CIRCULAR_REFERENCE]

  3. /profile silver.DimGeography  (unlocks after scope is complete)
```

**Rules for each action:**

- **Action 1 — Diagnostic errors**: For each error-severity diagnostic, state the code, the object FQN, and a concise fix (1–2 sentences). This is informational — no run offer. If there are no error diagnostics, skip this action and start from action 2.
- **Action 2 — Current phase command**: Determine the immediate phase from the batch-plan:
  - If `scope_phase` is non-empty: current command is `/scope <fqn1> <fqn2> ...` for all scope-phase FQNs.
  - Else if `profile_phase` is non-empty: current command is `/profile <fqn1> <fqn2> ...`.
  - Else if `migrate_batches` is non-empty: use the first batch's `pipeline_status` to pick the command:
    - `test_gen_needed` → `/generate-tests <fqn1> ...`
    - `refactor_needed` → `/refactor <fqn1> ...`
    - `migrate_needed` → `/generate-model <fqn1> ...`
  - If `circular_refs` is non-empty, append inline: `[N excluded — CIRCULAR_REFERENCE]`
  - Max 10 FQNs listed; if more, append `and N more` (all still execute).
- **Action 3 — Next phase command**: The phase that will become unblocked after action 2 completes. Use the same command format. Omit if there is no obvious next phase.

After the "What to do next" section, show these notes if applicable:

- If `source_pending` list is non-empty, show a "Pending source confirmation" section:

  ```text
  pending source confirmation (N tables)
    Run /add-source-tables <fqn> to confirm, or confirm during /init-dbt.
    silver.AuditLog
    silver.TempStaging
  ```

- If `summary.excluded_count > 0`, show: "N objects excluded from pipeline — edit catalog JSON to re-include"

**Run offer**: After presenting the actions, if action 1 is **not** a diagnostic error (i.e. the first actionable item is a runnable plugin command), ask:

```text
Run the first command now? (y/n)
```

If action 1 IS a diagnostic error, do not show the run offer. The user must fix the errors first.

If the user confirms: execute the command inline in the same session (no sub-agent). Proceed to run the relevant plugin command directly.

### Step 6 — Catalog Diagnostics

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

### Step 7 — Sources staleness check

If `dbt/models/staging/sources.yml` exists and any table has `scope_needed` status, show: "sources.yml may be stale — N tables have incomplete scoping. Re-run `/init-dbt` after scoping is complete."

### Step 8 — Source tables note

If `summary.source_tables > 0`, show at the bottom of the status output:

```text
N source tables hidden — see sources.yml
```

If `source_pending` is non-empty AND `summary.source_tables == 0`, instead show:

```text
No source tables confirmed yet. Run /add-source-tables or confirm during /init-dbt.
```

### Step 9 — init-dbt readiness hint

If `dbt/dbt_project.yml` does **not** exist AND the batch-plan `scope_phase` is empty (all in-scope objects have completed scope), show:

```text
ready to initialise dbt  — all tables are scoped. Run /init-dbt to scaffold your dbt project.
```

If there are still tables in the scope phase, omit this hint entirely. Do not show the hint if `dbt/dbt_project.yml` already exists.

### Step 10 — Stale catalog cleanup

After presenting all sections above, check whether any `STALE_OBJECT` warnings appeared in the catalog diagnostics (Step 6). This step applies only when running in batch mode (no table argument).

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

Run the batch planner to get the node for this specific table (for diagnostics):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util batch-plan
```

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

  refactor ✗ — pending  ⚠ PARSE_ERROR: DDL failed to parse — fix before running /refactoring-sql
    guard failed: TEST_SPEC_NOT_REVIEWED

  migrate ✗ — blocked
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

**Diagnostic callout per stage**: look up this table's node in the batch-plan output and check `diagnostic_stage_flags`. For any stage that has a flag, add an inline callout:

- For a stage with `"error"` severity flag: append `⚠ <CODE>: <short message> — <one-sentence fix>` on the stage line.
- For a stage with `"warning"` severity flag: append `~ <CODE>: <short message>` on the stage line.

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
