# Stage 5 -- SQL Refactoring

`/refactor` restructures source SQL into import/logical/final CTE form and persists only proof-backed results.

## Prerequisites

- `manifest.json`
- completed scoping
- completed profiling
- approved test spec
- sandbox available if you want executable compare proof

If the sandbox is unavailable or you explicitly skip executable compare, the command can still persist semantic-review-backed results as `partial`.

## Invocation

```text
/refactor silver.DimCustomer silver.FactInternetSales
```

## Pipeline

1. gather refactor context
2. extract the ground-truth SQL
3. restructure it into import/logical/final CTEs
4. run semantic review and, when available, executable compare
5. persist the refactor result
6. commit successful items and optionally raise a PR

## Where the result is stored

For table migrations, the proof-backed refactor is persisted on the selected writer procedure catalog entry:

- `catalog/procedures/<selected_writer>.json`

For views and materialized views, the refactor is persisted on the view catalog entry:

- `catalog/views/<item_id>.json`

That persisted refactor state is what `/generate-model` consumes later.

## Status values

- `ok` for proof-backed success
- `partial` when semantic proof succeeded but executable proof was skipped or incomplete
- `error` when refactor could not be completed

## Next step

Proceed to [[Stage 4 Model Generation]].
