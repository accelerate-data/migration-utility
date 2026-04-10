# Snapshot Generation

Rules for generating dbt snapshot models. Snapshots replace the `.sql` model file — do not generate both a model and a snapshot for the same table.

## File placement

Place snapshot files in `snapshots/` (not `models/staging/`). Output paths:

- SQL: `dbt/snapshots/<model_name>.sql`
- Schema: `dbt/snapshots/schema.yml`

## Timestamp strategy (watermark column present)

When the profile has a watermark column, use `strategy='timestamp'`:

```sql
{% snapshot <model_name>_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='<pk_column>',
    strategy='timestamp',
    updated_at='<watermark_column>',
) }}

select * from {{ source('<source_name>', '<table_name>') }}

{% endsnapshot %}
```

## Check strategy (no watermark column)

When the profile has no watermark column, use `strategy='check'`:

```sql
{% snapshot <model_name>_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='<pk_column>',
    strategy='check',
    check_cols='all',
) }}

select * from {{ source('<source_name>', '<table_name>') }}

{% endsnapshot %}
```

Use a specific column list for `check_cols` if the profile identifies mutable columns.
