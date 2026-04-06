# Status Dashboard

The `/status` command shows migration progress across all pipeline stages. It runs deterministic prerequisite checks via the `migrate-util dry-run` CLI, then applies LLM reasoning to interpret patterns and recommend next actions.

## Modes

### Batch summary (no arguments)

Running `/status` with no arguments enumerates every table in `catalog/tables/` and checks each one against the five pipeline stages in order: `scope`, `profile`, `test-gen`, `refactor`, `migrate`.

The output is a summary table:

```text
migration status -- 4 tables

  Table                   scope      profile    test-gen   refactor   migrate
  --------------------------------------------------------------------------
  silver.DimCustomer      resolved   ok         ok         ok         pending
  silver.DimProduct       resolved   partial    blocked    blocked    blocked
  silver.DimDate          resolved   pending    blocked    blocked    blocked
  silver.FactSales        pending    blocked    blocked    blocked    blocked

  scope: 3/4 | profile: 2/4 | test-gen: 1/4 | refactor: 1/4 | migrate: 0/4
```

For each table, stages are checked in order. When a stage fails its guards, all subsequent stages are marked `blocked` and no further checks run for that table.

### Single-table detail (`/status silver.DimCustomer`)

When given a table name, `/status` runs `migrate-util dry-run` with the `--detail` flag for each stage and presents a per-stage breakdown:

```text
status for silver.DimCustomer

  scope >
    selected_writer: dbo.usp_load_dimcustomer
    candidates: 1
    statements: 3 migrate, 1 skip, 0 unresolved

  profile >
    status: ok
    classification: dim_scd1
    primary_key: surrogate (CustomerKey)
    watermark: ModifiedDate
    foreign_keys: 2
    pii_actions: 1
    questions: 6/6 answered

  test-gen >
    status: ok, coverage: complete
    branches: 4, tests: 6
    sandbox: __test_abc123

  refactor ✓
    status: ok
    has_refactored_sql: true

  migrate x -- pending
    guard failed: REFACTOR_NOT_COMPLETED
    -> Run /refactor silver.DimCustomer
```

For completed stages, key signals are shown. For the first failing stage, the failing guard is displayed with a suggested command to fix it.

## Stage status values

| Value | Meaning |
|---|---|
| `resolved` | Scoping complete -- selected writer and all statements resolved |
| `ok` | Profile, test-gen, or refactor completed successfully |
| `partial` | Profile completed but some questions unanswered |
| `pending` | This is the next stage to work on |
| `blocked` | A prior stage has not been completed |

## Recommendations engine

After the summary table, `/status` provides LLM-driven analysis:

- **Patterns** -- cross-table observations like "5 tables blocked at profiling, all missing watermark column" or "all scoped tables have resolved writers, profiling is the bottleneck"
- **Next action** -- the single most impactful command to run next, e.g. "Run `/refactor silver.DimDate silver.FactSales` to unblock 2 tables"

## Dry-run guard checks

Each stage has an ordered set of prerequisite guards. Guards run in sequence and short-circuit on the first failure.

| Stage | Guards checked (in order) |
|---|---|
| `scope` | manifest exists, table catalog exists |
| `profile` | manifest exists, table catalog exists, selected writer set, statements resolved |
| `test-gen` | manifest exists, table catalog exists, selected writer set, statements resolved, profile completed, sandbox configured |
| `refactor` | manifest exists, table catalog exists, selected writer set, statements resolved, profile completed, sandbox configured, test spec exists |
| `migrate` | manifest exists, table catalog exists, selected writer set, statements resolved, profile completed, sandbox configured, test spec exists, refactor completed |

## Error codes

These are the error codes returned by `migrate-util dry-run` when a guard fails:

| Code | Guard | Meaning | Fix |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | `manifest_exists` | `manifest.json` not found in project root | Run `/setup-ddl` to extract DDL and create the manifest |
| `MANIFEST_CORRUPT` | `manifest_exists` | `manifest.json` is not valid JSON | Re-run `/setup-ddl` or manually fix the file |
| `CATALOG_FILE_MISSING` | `table_catalog_exists` | `catalog/tables/<table>.json` not found | Run `/setup-ddl` to populate the catalog |
| `CATALOG_FILE_CORRUPT` | `table_catalog_exists` | Table catalog file is not valid JSON | Re-run `/setup-ddl` or manually fix the file |
| `SCOPING_NOT_COMPLETED` | `selected_writer_set` | No `selected_writer` in the table's scoping section | Run `/scope <table>` or `/analyzing-table <table>` |
| `STATEMENTS_NOT_RESOLVED` | `statements_resolved` | One or more statements not resolved to `migrate` or `skip` | Run `/scope <table>` to resolve remaining statements |
| `PROFILE_NOT_COMPLETED` | `profile_completed` | Profile section missing or status is not `ok`/`partial` | Run `/profile <table>` or `/profiling-table <table>` |
| `SANDBOX_NOT_CONFIGURED` | `sandbox_configured` | Sandbox metadata (`database`) missing from manifest | Run `/setup-sandbox` to create the test database |
| `TEST_SPEC_NOT_FOUND` | `test_spec_exists` | `test-specs/<table>.json` not found | Run `/generate-tests <table>` or `/generating-tests <table>` |
| `REFACTOR_NOT_COMPLETED` | `refactor_completed` | Refactor section missing or no `refactored_sql` in catalog | Run `/refactor <table>` or `/refactoring-sql <table>` |

## Exit codes

The `migrate-util dry-run` CLI uses these exit codes:

| Exit code | Meaning |
|---|---|
| 0 | Success (check `guards_passed` field for pass/fail) |
| 1 | Domain failure (invalid stage name, bad table FQN) |
| 2 | IO or parse error |

## Related pages

- [[Stage 1 Project Init]] -- fixing `MANIFEST_NOT_FOUND` and `CATALOG_FILE_MISSING`
- [[Stage 1 Scoping]] -- fixing `SCOPING_NOT_COMPLETED` and `STATEMENTS_NOT_RESOLVED`
- [[Stage 2 Profiling]] -- fixing `PROFILE_NOT_COMPLETED`
- [[Stage 3 Test Generation]] -- fixing `SANDBOX_NOT_CONFIGURED` and `TEST_SPEC_NOT_FOUND`
- [[Stage 5 SQL Refactoring]] -- fixing `REFACTOR_NOT_COMPLETED`
- [[Stage 4 Model Generation]] -- dbt model generation
- [[Troubleshooting and Error Codes]] -- full error code index
