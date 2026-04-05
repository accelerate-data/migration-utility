# Modular Modeling Reference

Concise decision rules for splitting refactored CTE SQL into dbt models. Source: [dbt modular modeling techniques](https://www.getdbt.com/blog/modular-data-modeling-techniques).

## Layers

| Layer | Prefix | Materialization | Purpose |
|---|---|---|---|
| Staging | `stg_` | `ephemeral` | 1:1 with source table. Clean, rename, cast, filter deleted rows. No joins. |
| Mart | `fct_` / `dim_` | `table` or `incremental` | Business logic: joins, CASE, window functions, aggregations. |

Intermediate (`int_`) and report (`rpt_`) layers are not used in this pipeline.

## Staging model rules

- One `stg_` model per source table referenced by the proc.
- Each staging model does `select * from {{ source('<schema>', '<table>') }}` with light transforms only.
- Allowed transforms: type casting, column renaming, filtering deleted/extraneous records.
- **No joins** in staging. If two sources are always used together, they still get separate staging models — the mart joins them.
- Staging models are `materialized='ephemeral'` — they compile to inline CTEs in the mart SQL, not physical tables.

## Mart model rules

- Mart models `{{ ref('stg_<name>') }}` their staging dependencies.
- All heavy logic lives here: multi-table joins, CASE WHEN, window functions, aggregations.
- Materialization comes from the profile context (`table`, `incremental`, or `snapshot`).
- One mart model per target table.

## Decision: what becomes a staging model?

Every **import CTE** (a CTE that reads directly from a source with `select *`) becomes a `stg_` model, provided:

1. The source table appears in `source_tables` from the context.
2. The CTE does not contain joins or business logic beyond casting/renaming/filtering.

If an import CTE already applies joins or business logic, it stays as a logical CTE inside the mart — do not force it into a staging model.

## Decision: when to reuse an existing staging model?

Before creating a new `stg_` model, check `dbt/models/staging/` for an existing one on the same source table. If it exists and its column set is compatible, `{{ ref() }}` it — do not duplicate.

## File layout

```text
dbt/
  models/
    staging/
      stg_<source_table>.sql          -- ephemeral
      _stg_<source_table>.yml         -- schema + tests
    marts/
      <target_table>.sql              -- table/incremental
      _<target_table>.yml             -- schema + unit tests
```

## Mapping from refactored CTE SQL

Given refactored SQL with import/logical/final CTEs:

1. **Import CTEs** &rarr; `stg_` models (ephemeral).
2. **Logical + final CTEs** &rarr; stay inside the mart model, referencing staging via `{{ ref('stg_...') }}`.
3. The mart's import CTEs become `{{ ref('stg_...') }}` calls — no more inline `select * from {{ source() }}` in the mart.
