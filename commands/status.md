---
name: status
description: >
  Use when checking migration pipeline status for all objects or one object.
user-invocable: true
argument-hint: "[schema.table]"
---

# Status

## Authority

| Rule | Instruction |
|---|---|
| Fresh data | Run the required CLI commands every time. Never reuse earlier `/status` output. |
| Source of truth | Use CLI JSON only. Do not infer readiness or stage state from files on disk. |
| Summary scope | Read only `batch-plan.summary` and `batch-plan.status_summary`. |
| Detail scope | Use the matching `status_summary.pipeline_rows` row for stage cells and the matching batch-plan object for first incomplete stage. |
| Test-gen readiness | Use only `migrate-util ready test-gen` JSON. |
| Workflow-exempt tables | Source and seed tables stay out of summary pipeline rows. |
| Detail for exempt tables | If requested object is source or seed, report it as workflow-exempt and do not recommend pipeline commands. |
| Commands | Stage work uses slash commands. Never print `!ad-migration generate-tests`. |
| Harness | Do not print command plans, fixture analysis, or harness-mode notes. |

## Mode Selection

| Invocation | Mode |
|---|---|
| `/status` with no object argument | Summary mode |
| `/status <schema.table>` with an object argument | Detail mode |

## Run Commands

| Mode | Required commands |
|---|---|
| `/status` | `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util sync-excluded-warnings`<br>`uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan` |
| `/status <schema.table>` | `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan`<br>`uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util status <schema.table>` |
| detail + `pipeline_status == "test_gen_needed"` | Also run `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util ready test-gen --project-root <project-root> --object <schema.table>` |
| detail + `pipeline_row.test_gen in ["pending", "setup-blocked"]` | Also run `migrate-util ready test-gen` as above. |
| `sync-excluded-warnings` exits 2 | Continue and use fresh `batch-plan` output. |

## Summary Render

| Section | Source | Render | Do not render |
|---|---|---|---|
| Pipeline | `status_summary.pipeline_rows` | Columns: `Object`, `type`, `scope`, `profile`, `test-gen`, `refactor`, `migrate`. Print cells as-is. | symbols, explanations, source tables, seed tables, selected writers, branch counts, dependencies, table evidence |
| Diagnostics | `status_summary.diagnostic_rows` | Columns: `Object`, `errors`, `warnings`, `details`. `details` is `row.details_command`. | diagnostic codes, messages, rationales, likely causes, review paths, fix instructions |
| What to do next | `status_summary.next_action` | Use the Summary Next Action table below. | any other command or explanation |

Render sections in this order:

| Order | Section |
|---|---|
| 1 | Pipeline |
| 2 | Diagnostics, only when `diagnostic_rows` is non-empty |
| 3 | What to do next |

Summary diagnostic count cells:

| Counts | Cell |
|---|---|
| no unresolved and no resolved | `0` |
| unresolved only | `N unresolved` |
| resolved only | `N resolved` |
| both | `N unresolved, M resolved` |

Summary next action:

| `next_action.kind` | Output | Stop rule |
|---|---|---|
| `command` | Print `next_action.command`, then ask `Run this command now? (y/n)` | If user answers yes, run it inline. |
| `diagnostics` | `Resolve unresolved errors listed in diagnostics, then rerun /status.` | Stop. Do not add detail commands or diagnostic explanation. |
| `none` | `No action is currently available.` | Stop. |

## Detail Render

| Step | Source | Output |
|---|---|---|
| Find row | `status_summary.pipeline_rows[]` | Match requested object case-insensitively. This row controls displayed stage cells. |
| Find object | batch-plan object collections | Match requested object case-insensitively. This object controls `pipeline_status`. |
| Source or seed object | `batch-plan.source_tables[]` or `batch-plan.seed_tables[]` | Use the Workflow-Exempt Detail table, then stop. Do not recommend pipeline commands. |
| Stage rows | Stage Display table | One row each for `scope`, `profile`, `test-gen`, `refactor`, `migrate`. |
| Diagnostics | Diagnostic Callout table | Append callouts only to the matching stage line. |
| Next action | Detail Next Action table | Print exactly one next action. |

Workflow-exempt detail:

| Field | Output |
|---|---|
| Status token | Print literal `workflow-exempt`. |
| Stage rows | `scope: N/A`, `profile: N/A`, `test-gen: N/A`, `refactor: N/A`, `migrate: N/A`. |
| Next action | `No action is currently available.` |
| Forbidden output | `/scope-tables`, `/profile-tables`, `/generate-tests`, `/refactor-query`, `/generate-model`, `Run this command now?` |

Stage display:

| Stage cell | Evidence |
|---|---|
| `blocked` | Do not show compact evidence. |
| `setup-blocked` | Do not show compact evidence. |
| `N/A` | Do not show compact evidence. |
| any other value | Add compact evidence from `migrate-util status <schema.table>` using the Compact Evidence table. |

Compact evidence:

| Stage | Evidence |
|---|---|
| `scope` table | status, selected writer, candidate count, statement counts |
| `scope` view | status, materialized-view flag, references summary |
| `profile` table | status, kind, primary key, watermark, FK count, PII count |
| `profile` view | status, classification, source |
| `test-gen` | status, coverage, branch count, test count, sandbox endpoint |
| `refactor` table | status, `has_refactored_sql` |
| `refactor` view | dbt model exists, model name |
| `migrate` | dbt model exists, schema YAML unit tests, compiled, test results |

Diagnostic callouts:

| Severity | Detail mode stage-line callout |
|---|---|
| `error` | `ERROR <CODE>: <short message> - <one-sentence fix>` |
| `warning` | `<CODE>: <short message>` |

Detail next action:

| Condition | Next action |
|---|---|
| `pipeline_status == "scope_needed"` | `/scope-tables <schema.table>` |
| `pipeline_status == "profile_needed"` | `/profile-tables <schema.table>` |
| `pipeline_status == "test_gen_needed"` | Use Test-Gen Next Action table. |
| `pipeline_status == "refactor_needed"` | `/refactor-query <schema.table>` |
| `pipeline_status == "migrate_needed"` | `/generate-model <schema.table>` |
| otherwise | `No action is currently available.` |

Test-gen next action:

| Condition | Stage display | Next action | Forbidden output |
|---|---|---|---|
| `pipeline_row.test_gen == "setup-blocked"` and `ready.project.code == "TARGET_NOT_CONFIGURED"` | `test-gen: setup-blocked` | `!ad-migration setup-target` | `/generate-tests`, `ready`, `ready to proceed`, `ready for generation` |
| `pipeline_row.test_gen == "setup-blocked"` and `ready.project.code == "SANDBOX_NOT_CONFIGURED"` | `test-gen: setup-blocked` | `!ad-migration setup-sandbox` | `/generate-tests`, `ready`, `ready to proceed`, `ready for generation` |
| `ready.ready == true` | `test-gen: pending` | `/generate-tests <schema.table>` | `!ad-migration generate-tests` |
| any other readiness failure | keep CLI stage cell | Report the readiness error briefly. | `/generate-tests` |

## Forbidden Inferences

| Do not use | Use instead |
|---|---|
| dbt files, `profiles.yml`, test specs, refactor artifacts, or manifest sections to decide test-gen readiness | `migrate-util ready test-gen` JSON |
| diagnostic review files in summary mode | `status_summary.diagnostic_rows` counts |
| catalog files in summary mode | `status_summary.pipeline_rows` |
| source or seed table notes | `status_summary.pipeline_rows` only |
| prior conversation output | fresh CLI output |

## Error Handling

| Situation | Action |
|---|---|
| missing `manifest.json` | Tell the user to run `ad-migration setup-source` first. |
| no `catalog/tables/*.json` files | Tell the user to run `ad-migration setup-source` first. |
| `migrate-util status` exits 1 | Report the domain error from JSON output. |
| `migrate-util status` exits 2 | Report IO error and suggest checking project setup. |
| `migrate-util batch-plan` exits 2 | Report IO error and suggest checking project setup. |
| `migrate-util batch-plan` returns `{"error": ...}` | Report the error and suggest `ad-migration setup-source`. |
| `CLAUDE_PLUGIN_ROOT` is unset | Tell the user to load the plugin with `claude --plugin-dir <path>`. |
