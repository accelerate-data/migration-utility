{% snapshot dim_employee_scd2_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='NationalIDNumber',
    strategy='timestamp',
    updated_at='ValidFrom',
) }}

select
    NationalIDNumber,
    LoginID,
    JobTitle,
    MaritalStatus,
    getdate() as ValidFrom,
    {{ invocation_id }} as _dbt_run_id,
    current_timestamp() as _loaded_at
from {{ source('bronze', 'employee') }}

{% endsnapshot %}
