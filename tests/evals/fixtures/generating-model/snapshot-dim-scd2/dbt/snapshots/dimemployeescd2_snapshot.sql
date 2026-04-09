{% snapshot dimemployeescd2_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='EmployeeNaturalKey',
    strategy='check',
    check_cols=['JobTitle', 'MaritalStatus'],
) }}

with import_employee as (
    select
        NationalIDNumber,
        LoginID,
        JobTitle,
        MaritalStatus
    from {{ ref('stg_employee') }}
),

logical_employee_transformed as (
    select
        src.NationalIDNumber as EmployeeNaturalKey,
        substring(src.LoginID, charindex('\', src.LoginID) + 1, 50) as FirstName,
        src.JobTitle as LastName,
        src.JobTitle,
        src.MaritalStatus as Department
    from import_employee as src
),

final as (
    select * from logical_employee_transformed
)

select * from final

{% endsnapshot %}
