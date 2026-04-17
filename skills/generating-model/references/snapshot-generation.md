# Snapshot Generation

Use snapshot syntax when the profile-derived materialization is SCD2-style history, but preserve the selected transformed SQL: `selected_writer_ddl_slice` for multi-table writers, otherwise `refactored_sql`.

## Core Rules

- Snapshot config changes the wrapper, not the business logic.
- Keep the same joins, filters, projections, and CTE flow that the non-snapshot model would use.
- Snapshot SQL still follows the shared model artifact invariants, including `_dbt_run_id` and `_loaded_at`.
- Do not collapse the body to raw `select * from {{ source(...) }}` unless the selected transformed SQL is already that simple.
- Write snapshots through `migrate write`; report the CLI-returned `snapshots/<snapshot_name>.sql` and `snapshots/_snapshots__models.yml` paths.

## Strategy Selection

| Profile signal | Strategy |
|---|---|
| Watermark column present | `strategy='timestamp'` and `updated_at='<watermark_column>'` |
| No watermark column | `strategy='check'`; use `check_cols='all'` unless the profile identifies a narrower mutable set |

## Shape

Wrap the transformed query in a snapshot block:

```sql
{% snapshot <model_name>_snapshot %}

{{ config(
    unique_key='<pk_column>',
    strategy='timestamp|check',
    updated_at='<watermark_column>',  -- timestamp only
    check_cols='all'                  -- check only
) }}

with source_a as (
    select * from {{ ref('stg_bronze__source_a') }}
),
final as (
    select
        ...,
        '{{ invocation_id }}' as _dbt_run_id,
        {{ current_timestamp() }} as _loaded_at
    from source_a
)

select * from final

{% endsnapshot %}
```

Use snapshot YAML, not model YAML:

```yaml
version: 2
snapshots:
  - name: <model_name>_snapshot
    description: "<target description>"
    columns:
      - name: <pk_column>
        tests:
          - unique
          - not_null
```

## Common Mistakes

- Dropping transformed logic and snapshotting the raw source table instead.
- Using snapshot config for tables that should remain `table` or `incremental`.
- Direct-writing snapshot files instead of using `migrate write`.
