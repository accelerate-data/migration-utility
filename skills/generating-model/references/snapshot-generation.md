# Snapshot Generation

Use snapshot syntax when the profile-derived materialization is SCD2-style history, but preserve the transformed logic from `refactored_sql`.

## Core Rules

- Snapshot config changes the wrapper, not the business logic.
- Keep the same joins, filters, projections, and CTE flow that the non-snapshot model would use.
- Do not collapse the body to raw `select * from {{ source(...) }}` unless `refactored_sql` is already that simple.
- Let the runtime writer decide actual file paths. If the caller or CLI returns written paths, report those exact paths instead of assuming placement.

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
    select * from {{ source('bronze', 'source_a') }}
),
final as (
    select ...
    from source_a
)

select * from final

{% endsnapshot %}
```

## Common Mistakes

- Dropping transformed logic and snapshotting the raw source table instead.
- Using snapshot config for tables that should remain `table` or `incremental`.
- Assuming snapshots must be written to a hardcoded directory even when the caller or CLI owns path selection.
