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

### Step 1 — Enumerate tables

List all catalog table files:

```bash
ls catalog/tables/*.json
```

Extract item IDs from filenames (strip `.json` suffix).

### Step 2 — Collect status per table

For each table, iterate stages in order: `scope`, `profile`, `test-gen`, `migrate`.

For each stage, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util dry-run <table> <stage>
```

Parse the JSON output. If `guards_passed` is `true` and the content shows the stage is complete (has meaningful output — e.g. scoping has `selected_writer`, profile has `status: ok|partial`, test-gen has `test_spec_status`, migrate has `dbt_model_exists`), record the stage as complete and continue to the next stage.

If `guards_passed` is `false`, record the table as blocked at that stage. Stop iterating stages for that table — subsequent stages are implicitly blocked.

### Step 3 — Build summary table

Present a human-readable summary:

```text
migration status — N tables

  Table                   scope      profile    test-gen   migrate
  ────────────────────────────────────────────────────────────────
  silver.DimCustomer      resolved   ok         ok         pending
  silver.DimProduct       resolved   partial    blocked    blocked
  silver.DimDate          resolved   pending    blocked    blocked
  silver.FactSales        pending    blocked    blocked    blocked

  scope: 3/4 | profile: 2/4 | test-gen: 1/4 | migrate: 0/4
```

Stage status values:

- Stage complete: show the stage's status value (`resolved`, `ok`, `partial`, etc.)
- Stage is the first incomplete: `pending`
- Stage blocked by prior incomplete stage: `blocked`

### Step 4 — Interpret and recommend

After the summary table, provide LLM analysis:

1. **Patterns:** flag cross-table patterns. Examples:
   - "5 tables are blocked at profiling — all missing watermark column"
   - "All scoped tables have resolved writers, profiling is the bottleneck"
   - "3 tables have partial profiles — consider re-running /profile for them"

2. **Sources staleness check:** if `dbt/models/staging/sources.yml` exists and any table has incomplete scoping (not `resolved` or `no_writer_found`), show a note: "sources.yml may be stale — N tables have incomplete scoping. Re-run `/init-dbt` after scoping is complete."

3. **Next action:** recommend the single most impactful next step. Examples:
   - "Run `/profile silver.DimDate silver.FactSales` to unblock 2 tables"
   - "Run `/setup-sandbox` then `/generate-tests` for the 3 profiled tables"
   - "All tables are ready for migration — run `/generate-model` on the batch"

## Pipeline — With table argument (detailed)

### Step 1 — Collect detailed status

For each stage in order (`scope`, `profile`, `test-gen`, `migrate`), run:

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

  migrate ✗ — pending
    guard failed: TEST_SPEC_NOT_FOUND
    → Run /generating-tests silver.DimCustomer
```

For the first failing stage, explain what prerequisite is missing and suggest the specific command to run.

For completed stages, show the key signals from the `--detail` content:

- **scope:** selected_writer, candidate count, statement resolution counts
- **profile:** status, resolved_kind, primary_key type, watermark column, FK count, PII count, questions answered/total
- **test-gen:** status, coverage, branch count, test count, sandbox database
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
