{% snapshot dim_employee_scd2_snapshot %}

{{ config(
    unique_key='EmployeeNaturalKey',
    strategy='check',
    check_cols=['JobTitle', 'Department']
) }}

with import_employee as (
    select
        NationalIDNumber,
        LoginID,
        JobTitle,
        MaritalStatus
    from {{ source('bronze', 'employee') }}
),

logical_new_current_rows as (
    select
        src.NationalIDNumber as EmployeeNaturalKey,
        substring(src.LoginID, charindex(N'\', src.LoginID) + 1, 50) as FirstName,
        src.JobTitle as LastName,
        src.JobTitle,
        src.MaritalStatus as Department,
        getdate() as ValidFrom,
        cast('9999-12-31' as datetime2) as ValidTo,
        1 as IsCurrent,
        {{ invocation_id }} as _dbt_run_id
    from import_employee as src
    left join {{ this }} as tgt
        on src.NationalIDNumber = tgt.EmployeeNaturalKey
       and tgt.IsCurrent = 1
    where tgt.EmployeeSCD2Key is null
),

final as (
    select * from logical_new_current_rows
)

select * from final

{% endsnapshot %}
