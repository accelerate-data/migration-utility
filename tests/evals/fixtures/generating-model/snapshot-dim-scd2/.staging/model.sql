{% snapshot dim_employee_scd2 %}

{{ config(
    unique_key='EmployeeSCD2Key',
    strategy='timestamp',
    updated_at='ValidFrom'
) }}

with import_employee as (
    select
        NationalIDNumber,
        LoginID,
        JobTitle,
        MaritalStatus
    from {{ source('bronze', 'Employee') }}
),

logical_transform as (
    select
        src.NationalIDNumber as EmployeeNaturalKey,
        substring(src.LoginID, charindex(N'\', src.LoginID) + 1, 50) as FirstName,
        src.JobTitle as LastName,
        src.JobTitle,
        src.MaritalStatus as Department,
        getdate() as ValidFrom,
        cast('9999-12-31' as datetime2) as ValidTo,
        1 as IsCurrent
    from import_employee as src
),

final as (
    select * from logical_transform
)

select * from final

{% endsnapshot %}
