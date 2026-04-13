# Stage 4 -- Model Generation

`/generate-model` turns the proof-backed refactor, profile, and approved test spec into dbt artifacts.

## Prerequisites

- `manifest.json`
- `dbt/` scaffolded by `/setup-target`
- completed scoping
- completed profiling
- approved test spec
- completed refactor state for the object

## Invocation

```text
/generate-model silver.DimCustomer silver.FactInternetSales
```

## Pipeline

1. generate dbt SQL and schema YAML
2. run `dbt test`
3. self-correct up to the command limits
4. run the independent `/reviewing-model` loop
5. commit successful items and optionally raise a PR

The generator uses the proof-backed refactor persisted by `/refactor`; it does not go back to raw procedure SQL as the primary migration input.

## What gets written

- model SQL under `dbt/models/`
- schema YAML with tests and `unit_tests:`
- snapshot artifacts when the target pattern requires them

## Notes

- Successful items are committed as they finish.
- Items marked `is_source: true` are skipped because they are not migration targets.
- `/status` is the best way to see which objects are now ready versus still blocked.

## Next step

Use [[Status Dashboard]] to plan the next batch or merge the PR opened by the run.
