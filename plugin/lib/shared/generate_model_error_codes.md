# Generate Model Error Codes

Use only these statuses and surfaced codes for `/generate-model` and
`/reviewing-model`.

This file is the shared source of truth for the `/generate-model` command and
the `reviewing-model` skill.

## Statuses

`/generate-model` item results may use only these statuses:

- `ok`
- `partial`
- `error`

`/reviewing-model` results may use only these statuses:

- `approved`
- `approved_with_warnings`
- `revision_requested`
- `error`

## Codes

| Code | Severity | Use when | Status |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | error | `manifest.json` is missing before `/generate-model` starts | `error` |
| `DBT_PROJECT_MISSING` | error | `dbt/dbt_project.yml` or required dbt project files are missing | `error` |
| `DBT_PROFILE_MISSING` | error | dbt profile configuration is missing | `error` |
| `DBT_CONNECTION_FAILED` | error | `dbt debug` connection test failed | `error` |
| `SANDBOX_NOT_CONFIGURED` | error | sandbox not configured in manifest — run `/setup-sandbox` first | `error` |
| `CATALOG_FILE_MISSING` | error | required table or view catalog file is missing | `error` |
| `SCOPING_NOT_COMPLETED` | error | scoping is missing or unresolved for the target object | `error` |
| `PROFILE_NOT_COMPLETED` | error | profile is missing or not complete enough for model generation | `error` |
| `TEST_SPEC_NOT_FOUND` | error | approved test spec is missing when review or generation depends on it | `error` |
| `TEST_SPEC_MISSING` | error | test spec not found for the table — run `/generate-tests` before `/generate-model` | `error` |
| `CONTEXT_PREREQUISITE_MISSING` | error | `migrate context` could not assemble because a prerequisite is missing | `error` |
| `CONTEXT_IO_ERROR` | error | `migrate context` failed with an IO or parse error | `error` |
| `MODEL_NOT_FOUND` | error | generated model SQL or YAML files required for review are missing | `error` |
| `GENERATION_FAILED` | error | `/generating-model` could not produce a usable model artifact | `error` |
| `EQUIVALENCE_GAP` | warning | semantic gap remains between proc logic and the generated model | `partial` or `approved_with_warnings` |
| `DBT_COMPILE_FAILED` | warning | `dbt compile` failed after self-correction attempts | `partial` |
| `DBT_TEST_FAILED` | warning | `dbt build` still failed its validation or test phase after self-correction attempts | `partial` |
| `REVIEW_KICKED_BACK` | warning | reviewer requested another generation pass | `revision_requested` |
| `REVIEW_APPROVED_WITH_WARNINGS` | warning | reviewer approved after max iterations with issues remaining | `approved_with_warnings` |

## Rules

- Do not invent new surfaced codes in the command or skill.
- If a lower-level failure has no canonical code here, use `GENERATION_FAILED`
  for `/generate-model` and preserve the raw detail in `message`.
- Use `severity: "error"` only for conditions that must stop the workflow.
- Use `severity: "warning"` for conditions that still allow review, partial
  completion, or approval with warnings.
- Keep the command and skill docs aligned to this file.
