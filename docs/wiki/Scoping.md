# Scoping

`/scope-tables` is the batch command for writer discovery and initial object analysis.

## What it does

- for tables: identifies the selected writer procedure
- for views and materialized views: analyzes the SQL structure and writes the scoped view state

Internally, the command delegates each item to `/analyzing-table`, which auto-detects whether the object is a table or a view.

## Invocation

```text
/scope-tables silver.DimCustomer silver.DimProduct
```

## Git behavior

The command checks your git state first:

- if you are already on a feature branch or worktree, it uses that
- if you are on the default branch, it prompts you to continue there or create a worktree-backed feature branch

Successful items are committed and pushed as they finish. At the end, the command can raise or update a PR for the run.

## Output

For tables, the result is written to `catalog/tables/<fqn>.json` under `scoping`.

Typical outcomes:

- `resolved`
- `ambiguous_multi_writer`
- `no_writer_found`
- `error`

If a table ends up `no_writer_found`, it is not automatically treated as a source table. Use `ad-migration add-source-table` if that table should become a dbt source.

Once source decisions are written, use `/listing-objects list sources` to list the confirmed source tables without mixing them into the migration workflow dashboard.

## Next step

Proceed to [[Profiling]] or use `ad-migration add-source-table` and `ad-migration exclude-table` to clean up remaining non-migration targets before `ad-migration setup-target`.
