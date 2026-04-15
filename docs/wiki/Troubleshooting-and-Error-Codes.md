# Troubleshooting and Error Codes

Cross-reference index of common pipeline failures and the user-facing command that usually fixes them.

## Setup and catalog

| Code | Typical meaning | Usually seen from | Fix |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` is missing | `/status` or any downstream batch command | Run `ad-migration setup-source` |
| `MANIFEST_CORRUPT` | `manifest.json` is invalid JSON | `/status` or downstream commands | Repair the file or re-run `ad-migration setup-source` |
| `CATALOG_FILE_MISSING` | Expected catalog file is missing | `/status`, `/scope`, `/profile`, downstream commands | Re-run `ad-migration setup-source` for the relevant source objects |
| `CATALOG_FILE_CORRUPT` | Catalog JSON is invalid | `/status` or downstream commands | Repair the file or re-run `ad-migration setup-source` |
| `TECHNOLOGY_NOT_SET` | Source technology was not initialized | `ad-migration setup-source` | Run `/init-ad-migration` again if the scaffold is incomplete |
| `TECHNOLOGY_UNKNOWN` | Unsupported or misspelled technology value | `ad-migration setup-source` | Fix `manifest.json` to a supported technology |

### Common setup issues

**Toolbox not found**

Live SQL Server extraction requires `toolbox` on `PATH`.

**MSSQL bootstrap environment variables missing**

`ad-migration setup-source` needs these before it can persist `runtime.source`:

- `SOURCE_MSSQL_HOST`
- `SOURCE_MSSQL_PORT`
- `SOURCE_MSSQL_DB`
- `SOURCE_MSSQL_PASSWORD`

`ad-migration setup-sandbox` and `/generate-model` instead rely on the env vars referenced by `runtime.sandbox` and `runtime.target`.

**Plugin root not configured**

If `CLAUDE_PLUGIN_ROOT` is missing, the plugin was not loaded correctly. Launch Claude Code with the plugin enabled.

## Scoping and source confirmation

| Code | Typical meaning | Usually seen from | Fix |
|---|---|---|---|
| `SCOPING_NOT_COMPLETED` | No selected writer yet | `/status`, `/profile`, later stages | Run `/scope <object>` or `/analyzing-table <object>` |
| `STATEMENTS_NOT_RESOLVED` | Not all writer statements are classified | `/status`, later stages | Re-run `/scope <object>` and inspect the writer with `/listing-objects show <proc>` |
| `SOURCE_TABLE` | The object is already marked `is_source: true` | `/scope`, `/profile`, `/generate-tests`, `/refactor`, `/generate-model` | Skip migration for that object or remove the source designation intentionally |

### Common scoping issues

**No writer found**

If the object is truly an external source, mark it with `ad-migration add-source-table`. If it should be migrated, inspect references with `/listing-objects refs <object>`.

**Multiple writers**

Use `/listing-objects show <proc>` on each candidate before choosing a writer.

## Profiling

| Code | Typical meaning | Usually seen from | Fix |
|---|---|---|---|
| `PROFILE_NOT_COMPLETED` | No usable profile has been written yet | `/status`, `/generate-tests`, `/refactor`, `/generate-model` | Run `/profile <object>` or `/profiling-table <object>` |
| `PARTIAL_PROFILE` | The profile was written but some evidence stayed ambiguous | `/profile` summary or `/status` | Review the object and re-run `/profile` if needed |

## Sandbox and test generation

| Code | Typical meaning | Usually seen from | Fix |
|---|---|---|---|
| `SANDBOX_NOT_CONFIGURED` | No sandbox metadata in `manifest.json` | `/status`, `/generate-tests`, `/refactor`, `/generate-model` | Run `ad-migration setup-sandbox` |
| `SANDBOX_NOT_RUNNING` | Sandbox database is missing or unreachable | `/generate-tests`, `/refactor` | Recreate it with `ad-migration setup-sandbox` |
| `TEST_SPEC_NOT_FOUND` | The approved test spec is missing | `/status`, `/refactor`, `/generate-model` | Run `/generate-tests <object>` |
| `SANDBOX_DOWN_FAILED` | Sandbox teardown failed | `ad-migration teardown-sandbox` | Check connectivity, permissions, and whether the DB still exists |

## Refactor

| Code | Typical meaning | Usually seen from | Fix |
|---|---|---|---|
| `REFACTOR_NOT_COMPLETED` | No usable persisted refactor exists yet | `/status`, `/generate-model` | Run `/refactor <object>` |
| `EQUIVALENCE_PARTIAL` | Semantic refactor exists but executable proof was partial or skipped | `/refactor` summary or `/status` | Re-run `/refactor` with a working sandbox if full proof is required |

### Common refactor issue

**Audit loop does not converge**

This usually points to dynamic SQL, side effects, or behavior that is hard to isolate with current fixtures. Review the compare output, tighten the test spec, then re-run `/refactor`.

## dbt and model generation

| Code | Typical meaning | Usually seen from | Fix |
|---|---|---|---|
| `DBT_PROJECT_MISSING` | `dbt/` has not been scaffolded | `/generate-model` | Run `ad-migration setup-target` |
| `DBT_PROFILE_MISSING` | `profiles.yml` is missing | `/generate-model` | Re-run `ad-migration setup-target` or restore the file |
| `DBT_CONNECTION_FAILED` | `dbt debug` failed | `/generate-model` | Fix `profiles.yml` or the env-bound credentials referenced by `runtime.target` |
| `DBT_COMPILE_FAILED` | Generated model did not compile | `/generate-model` | Review the generated SQL and rerun the command |
| `DBT_TEST_FAILED` | Generated model still failed tests after retries | `/generate-model` | Inspect the generated artifact and test output, then rerun |
| `EQUIVALENCE_GAP` | Semantic gap remains between proof-backed refactor and dbt output | `/generate-model` | Review the flagged differences before accepting the result |

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
- [[Stage 1 Scoping]]
- [[Stage 2 Profiling]]
- [[Stage 3 Test Generation]]
- [[Stage 5 SQL Refactoring]]
- [[Stage 4 Model Generation]]
- [[Cleanup and Teardown]]
