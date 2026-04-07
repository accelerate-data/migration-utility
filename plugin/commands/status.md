---
name: status
description: >
  Migration status dashboard — shows per-table readiness across all pipeline stages.
  Calls migrate-util dry-run to check prerequisites and gather catalog/dbt evidence,
  then interprets patterns and recommends next actions.
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

- If `not_applicable` is `true`: record "N/A" for this stage and all remaining stages. Stop iterating.
- If `guards_passed` is `true` and content shows the stage is complete (e.g. scoping has `selected_writer`, profile has `status: ok|partial`, test-gen has `test_spec_status`, refactor has `refactor_status`, migrate has `dbt_model_exists`): record the stage as complete and continue.
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
  silver.vDimSalesTerritory     view    pending    blocked    blocked    blocked    blocked
  silver.vwFactPromo            mv      pending    blocked    blocked    blocked    blocked

  scope: 4/5 | profile: 2/4 (1 N/A) | test-gen: 1/4 (1 N/A) | refactor: 1/4 (1 N/A) | migrate: 0/4 (1 N/A)
```

Stage status values:

- Stage complete: show the stage's status value (`resolved`, `ok`, `partial`, etc.)
- Stage is the first incomplete: `pending`
- Stage blocked by prior incomplete stage: `blocked`
- Stage does not apply to this object (writerless table): `N/A`

Object type values: `table`, `view`, `mv`.

Header object count: show total objects plus a breakdown e.g. `(4 tables, 2 views)`. If MVs are present, count them separately: `(3 tables, 1 view, 2 mvs)`.

Summary counts: the denominator excludes N/A objects. Show the N/A count in parentheses if any exist, e.g. `profile: 2/4 (1 N/A)`. Views are included in the denominator for scope (they can be pending/complete for scope), but views are excluded from the denominator for profile/test-gen/refactor/migrate because those stages return `VIEW_STAGE_NOT_SUPPORTED` — treat them as implicit N/A for counting purposes.

### Step 4 — Interpret and recommend

After the summary table, provide LLM analysis:

1. **Patterns:** flag cross-object patterns. Examples:
   - "5 tables are blocked at profiling — all missing watermark column"
   - "All scoped tables have resolved writers, profiling is the bottleneck"
   - "3 tables have partial profiles — consider re-running /profile for them"
   - "2 views are present but have no scoping — view migration is not yet supported"
   - "N tables are writerless (no_writer_found) and are treated as source tables"

2. **Sources staleness check:** if `dbt/models/staging/sources.yml` exists and any table has incomplete scoping (not `resolved` or `no_writer_found`), show a note: "sources.yml may be stale — N tables have incomplete scoping. Re-run `/init-dbt` after scoping is complete."

3. **Next action:** recommend the single most impactful next step. Examples:
   - "Run `/profile silver.DimDate silver.FactSales` to unblock 2 tables"
   - "Run `/setup-sandbox` then `/generate-tests` for the 3 profiled tables"
   - "All tables are ready for migration — run `/generate-model` on the batch"

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

For completed stages, show the key signals from the `--detail` content:

- **scope:** selected_writer, candidate count, statement resolution counts
- **profile:** status, resolved_kind, primary_key type, watermark column, FK count, PII count, questions answered/total
- **test-gen:** status, coverage, branch count, test count, sandbox database
- **refactor:** status, has_refactored_sql
- **migrate:** dbt model exists, schema YAML has unit_tests, compiled, test results

### Step 3 — Recommend next action

Based on the first incomplete stage, recommend the specific command to run next for this table.

## Error handling

| Situation | Action |
|---|---|
| `migrate-util dry-run` returns exit code 1 | Report the domain error from JSON output |
| `migrate-util dry-run` returns exit code 2 | Report IO error, suggest checking project setup |
| No catalog files found | Tell user to run `/setup-ddl` first |
| `CLAUDE_PLUGIN_ROOT` not set | Tell user to load the plugin with `claude --plugin-dir <path>` |
