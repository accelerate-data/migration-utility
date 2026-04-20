# Troubleshooting and Error Codes

Cross-reference index of the error codes you may encounter while using the migration utility.

## Setup and status

| Code | Usually seen from | Typical meaning | Fix |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | `/status`, `/scope-tables`, `/profile-tables`, `/generate-tests`, `/refactor-query`, `/generate-model` | `manifest.json` is missing | Run `/init-ad-migration`, then `ad-migration setup-source` |
| `CATALOG_NOT_FOUND` | `/status` | no catalog files exist yet | Run `ad-migration setup-source` |
| `CATALOG_FILE_MISSING` | `/scope-tables`, `/profile-tables`, downstream commands | required object catalog file is missing | Re-run `ad-migration setup-source` for the relevant objects |
| `STATUS_FAILED` | `/status` | status collection failed and no more specific canonical code applied | Inspect the surfaced detail and repair the underlying catalog or manifest problem |
| `BATCH_PLAN_FAILED` | `/status` | dependency-aware batch planning failed | Inspect the surfaced detail and fix the reported catalog or graph issue |
| `SYNC_EXCLUDED_WARNINGS_FAILED` | `/status` | exclusion-diagnostic refresh failed before batch planning | Re-run after fixing the reported catalog error |

## Scoping and source confirmation

| Code | Usually seen from | Typical meaning | Fix |
|---|---|---|---|
| `SCOPING_NOT_COMPLETED` | `/status`, `/profile-tables`, `/generate-tests`, `/generate-model` | no resolved writer or analyzed view state yet | Run `/scope-tables <object>` or [[Analyzing Table]] |
| `SOURCE_TABLE` | `/scope-tables`, `/profile-tables`, `/generate-tests`, `/refactor-query`, `/generate-model` | object is already confirmed as a source and is not a migration target | Skip migration for that object or remove the source designation intentionally |
| `EXCLUDED` | `/scope-tables`, `/profile-tables`, `/status`, downstream commands | object is excluded from the migration pipeline | Leave it excluded or remove the exclusion intentionally |
| `WRITERLESS_TABLE` | `/status`, readiness checks before target setup | table has `scoping.status == "no_writer_found"` and is not yet confirmed as a source | Mark it with `ad-migration add-source-table <fqn>` or revisit scoping |
| `MULTI_TABLE_WRITE` | `/scope-tables` | writer candidate updates multiple tables | Review the candidate writer and disambiguate |
| `REMOTE_EXEC_UNSUPPORTED` | `/scope-tables` | cross-database or linked-server delegation blocks supported analysis | Handle manually or reduce scope |
| `PARSE_ERROR` | `/scope-tables`, `/profile-tables` | procedure parsing failed | Inspect the object with `/listing-objects show <proc>` |
| `DDL_PARSE_ERROR` | `/scope-tables`, `/profile-tables` | view DDL parsing failed | Inspect the view definition directly and re-run after cleanup if possible |
| `MISSING_REFERENCE` | `/scope-tables` | referenced object has no catalog entry | Re-extract if it should be in scope, otherwise treat as an external dependency |
| `OUT_OF_SCOPE_REFERENCE` | `/scope-tables` | referenced object is outside migration scope | Decide whether to expand scope or keep it external |

## Profiling

| Code | Usually seen from | Typical meaning | Fix |
|---|---|---|---|
| `PROFILE_NOT_COMPLETED` | `/status`, `/generate-tests`, `/refactor-query`, `/generate-model` | no usable profile has been written yet | Run `/profile-tables <object>` or [[Profiling Table]] |
| `VIEW_SCOPING_NOT_COMPLETED` | `/profile-tables` for views | view analysis did not complete | Re-run `/scope-tables <view>` first |
| `PROFILING_FAILED` | `/profile-tables` | profiling failed and no more specific canonical code applied | Inspect the surfaced detail and rerun |
| `PARTIAL_PROFILE` | `/profile-tables`, `/status` | profiling completed with unresolved ambiguity | Review the object and re-run if needed |

## Sandbox, tests, and refactor

| Code | Usually seen from | Typical meaning | Fix |
|---|---|---|---|
| `SANDBOX_NOT_CONFIGURED` | `/status`, `/generate-tests`, `/refactor-query`, `/generate-model` | sandbox runtime is missing from `manifest.json` | Run `ad-migration setup-sandbox` |
| `SANDBOX_NOT_RUNNING` | `/generate-tests`, `/refactor-query` | active sandbox cannot be reached | Recreate it with `ad-migration setup-sandbox` |
| `TEST_SPEC_MISSING` | `/status`, `/generate-model`, readiness checks | approved test spec is missing | Run `/generate-tests <object>` |
| `TEST_SPEC_NOT_FOUND` | `/generate-model` | generation or review expected an approved spec but none was found | Run `/generate-tests <object>` |
| `TEST_GENERATION_FAILED` | `/generate-tests` | test generation failed and no more specific canonical code applied | Inspect the surfaced detail and rerun |
| `SCENARIO_EXECUTION_FAILED` | `/generate-tests` | one or more sandbox scenarios failed during ground-truth capture | Fix the failing scenario or sandbox state and rerun |
| `REFACTOR_WRITE_FAILED` | `/refactor-query` | refactor output could not be persisted | Inspect the write payload and rerun |
| `EQUIVALENCE_PARTIAL` | `/refactor-query`, `/status` | refactor exists but semantic or executable proof was partial | Re-run `/refactor-query` with a working sandbox if full proof is required |
| `COMPARE_SQL_FAILED` | `/refactor-query` | executable compare failed at least one scenario | Inspect the compare output, tighten the spec, then rerun |
| `SANDBOX_DOWN_FAILED` | `ad-migration teardown-sandbox` | sandbox teardown failed | Check connectivity, permissions, and sandbox existence |

## Target and model generation

| Code | Usually seen from | Typical meaning | Fix |
|---|---|---|---|
| `TARGET_NOT_CONFIGURED` | `/status`, readiness before `/generate-model` | target runtime is missing from `manifest.json` | Run `ad-migration setup-target` |
| `DBT_PROJECT_MISSING` | `/status`, `/generate-model` | dbt project files are missing | Run `ad-migration setup-target` |
| `DBT_PROFILE_MISSING` | `/status`, `/generate-model` | dbt profile configuration is missing | Re-run `ad-migration setup-target` or restore `dbt/profiles.yml` |
| `DBT_CONNECTION_FAILED` | `/generate-model` | `dbt debug` failed | Fix the target connection variables or dbt profile |
| `DBT_COMPILE_FAILED` | `/generate-model` | generated model did not compile after retries | Review the generated SQL and rerun |
| `DBT_TEST_FAILED` | `/generate-model` | generated model still failed `dbt build` or tests after retries | Inspect the artifact and validation output, then rerun |
| `EQUIVALENCE_GAP` | `/generate-model` | semantic gap remains between proof-backed refactor and generated model | Review the differences before accepting the result |
| `GENERATION_FAILED` | `/generate-model` | model generation failed and no more specific canonical code applied | Inspect the surfaced detail and rerun |

## Status as the first diagnostic tool

When in doubt, run:

```text
/status
/status <schema.object>
```

`/status` is the fastest way to see:

- the first failing stage
- whether an object is blocked versus pending
- whether a writerless table still needs source confirmation
- which batch command should run next

## Related pages

- [[Quickstart]]
- [[Status Dashboard]]
- [[Handling Diagnostic Errors and Warnings]]
- [[Scoping]]
- [[Profiling]]
- [[Test Generation]]
- [[SQL Refactoring]]
- [[Model Generation]]
- [[Sandbox Operations]]
