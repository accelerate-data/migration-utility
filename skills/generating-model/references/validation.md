# Validation

Validate generated artifacts with the manifest runtime roles.

## Runtime Roles

Read `manifest.json` at the project root:

- `runtime.target` is the dbt validation target.
- `runtime.sandbox` is the source-relation execution endpoint when live
  source-backed validation is needed.

Do not read flat fields such as `sandbox.database`. Do not derive `target` from
`source` or `sandbox`.

## dbt Commands

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt compile --select <model_name>
```

Record the compile result in `execution.dbt_compile_passed`. Do not run `dbt build` or `dbt test` — unit-test execution is owned by the `/generate-model` unit-test repair stage.

## Outcomes

| Outcome | Action |
|---|---|
| Compile passes | Return `ok` if invariants also pass. |
| Model SQL compile failure | Revise, re-write, and retry up to 3 total attempts. |
| Warehouse unavailable | Run `dbt parse`, warn, and skip execution. |
| Target environment failure independent of SQL | Run `dbt parse`, warn, and do not rewrite business SQL. |
| Invariants fail | Do not return `ok` or `partial`. |

Before returning `ok` or `partial`, verify
`../../_shared/references/model-artifact-invariants.md`.
