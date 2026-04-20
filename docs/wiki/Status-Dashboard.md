# Status Dashboard

`/status` is the operational view of the pipeline. It reads current catalog state, computes dependency-aware readiness, and tells you what to run next.

## Invocation

```text
/status
/status silver.DimCustomer
```

## What it does

The command reads the local catalog to compute current pipeline state:

- per-object stage status across all in-scope tables, views, and materialized views
- exclusion diagnostics refreshed from the current catalog
- dependency-aware execution planning to identify the next best action

## Batch mode

With no arguments, `/status` shows:

- stage status across all in-scope objects
- object type (`table`, `view`, `mv`)
- diagnostic overlays for warnings and errors
- source-confirmed tables excluded from the active migration pipeline
- pending source confirmations for writerless tables that are not yet marked `is_source: true`
- a short "What to do next" section with the next command to run

Typical statuses are:

- `ok`
- `partial`
- `pending`
- `blocked`
- `N/A`

`N/A` is used for writerless tables that are not migration targets for downstream pipeline stages.

## Single-object mode

For one object, `/status <fqn>` shows the per-stage breakdown and the first failing or pending guard with the recommended command to fix it.

If the output shows table errors or warnings, review them one object at a time. See [[Handling Diagnostic Errors and Warnings]].

## Source-table behavior

The command distinguishes three cases:

- confirmed source tables: excluded from the pipeline table and counted as sources
- writerless but unconfirmed tables: surfaced as pending source confirmation
- excluded tables: omitted from active migration planning and counted separately

This is why `/status` is the best checkpoint before running `ad-migration setup-target`, `/generate-tests`, `/refactor-query`, or `/generate-model`. See [[Target Setup]] for what the target setup step validates.

## Typical use

- run `/status` after `ad-migration setup-source` to see what still needs scoping
- run it after `/scope-tables` to see which items are ready for `/profile-tables`
- run it before `ad-migration setup-target` to clear remaining source/exclude decisions
- run it after each batch command to choose the next highest-leverage action
