{% snapshot dim_employee_scd2 %}

{{ config(
    unique_key='EmployeeNaturalKey',
    strategy='timestamp',
    updated_at='ValidFrom',
    target_schema='snapshots'
) }}

with import_employee as (
    select
        NationalIDNumber,
        LoginID,
        JobTitle,
        MaritalStatus
    from {{ ref('stg_bronze__employee') }}
),

logical_new_current_rows as (
    select
        src.NationalIDNumber as EmployeeNaturalKey,
        substring(src.LoginID, charindex('\\', src.LoginID) + 1, 50) as FirstName,
        src.JobTitle as LastName,
        src.JobTitle,
        src.MaritalStatus as Department,
        getdate() as ValidFrom,
        cast('9999-12-31' as datetime2) as ValidTo,
        1 as IsCurrent
    from import_employee as src
    left join {{ source('silver', 'dimemployeescd2') }} as tgt
        on src.NationalIDNumber = tgt.EmployeeNaturalKey
       and tgt.IsCurrent = 1
    where tgt.EmployeeSCD2Key is null
),

final as (
    select
        *,
        '{{ invocation_id }}' as _dbt_run_id,
        {{ current_timestamp() }} as _loaded_at
    from logical_new_current_rows
)

select * from final

{% endsnapshot %}
