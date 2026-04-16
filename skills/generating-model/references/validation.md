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
cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt build --select <model_name>
```

Use `dbt build`, not `dbt test` alone. Record the build result in
`execution.dbt_test_passed`.

## Outcomes

| Outcome | Action |
|---|---|
| Build passes | Return `ok` if invariants also pass. |
| Model SQL compile/build failure | Revise, re-write, and retry up to 3 total attempts. |
| Warehouse unavailable | Run `dbt parse`, warn, and skip execution. |
| Target environment failure independent of SQL | Run `dbt parse`, warn, and do not rewrite business SQL. |
| Invariants fail | Do not return `ok` or `partial`. |

Before returning `ok` or `partial`, verify
`../../_shared/references/model-artifact-invariants.md`.
