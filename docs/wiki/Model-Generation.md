# Model Generation

`/generate-model` turns the proof-backed refactor, profile, and approved test spec into dbt artifacts.

## Prerequisites

- `manifest.json`
- `dbt/` scaffolded by `ad-migration setup-target`
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
2. run `dbt compile` against `runtime.target`
3. materialize direct parents with an empty run when unit tests need them
4. run scoped dbt unit tests for the generated model
5. self-correct up to the command limits
6. run the independent model review loop
7. commit successful items and optionally raise a PR

The generator uses the proof-backed refactor persisted by `/refactor-query`; it does not go back to raw procedure SQL as the primary migration input.

`/generate-model` does not run a broad `dbt build` for each generated mart model. Target setup uses `dbt build` for generated staging/source setup artifacts, and `/refactor-mart` uses candidate-scoped validation from its approved plan.

## What gets written

- model SQL under `dbt/models/`
- schema YAML with tests and `unit_tests:`
- snapshot artifacts when the target pattern requires them

## Notes

- Successful items are committed as they finish.
- Items marked `is_source: true` are skipped because they are not migration targets.
- `/status` is the best way to see which objects are now ready versus still blocked.
- `runtime.target` and `runtime.sandbox` are both required: target for dbt validation, sandbox for any live source-backed checks the workflow performs.
- See [[Target Setup]] for the source-facing dbt validation created before model generation.

## Next step

Use [[Status Dashboard]] to plan the next batch or merge the PR opened by the run.
