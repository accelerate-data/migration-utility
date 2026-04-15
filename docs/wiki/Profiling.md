# Profiling

`/profile` produces the migration profile for each table, view, or materialized view.

## What it answers

For tables, the profile captures the downstream signals needed for generation and testing, including:

- classification
- keying
- watermark strategy
- foreign-key structure
- PII handling

For views and materialized views, the profile classifies them for downstream migration behavior, for example `stg` versus `mart`.

## Invocation

```text
/profile silver.DimCustomer silver.FactInternetSales
```

## Git behavior

Like the other batch commands, `/profile` manages its own git workflow, commits successful items as they complete, and can open or update a PR at the end of the run.

## Status values

- `ok`
- `partial`
- `error`

`partial` means the profile was written but some required evidence was incomplete or ambiguous.

## Next step

Proceed to [[Test Generation]].
