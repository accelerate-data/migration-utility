{% snapshot dim_employee_scd2 %}

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

logical_transform as (
    select
        src.NationalIDNumber as EmployeeNaturalKey,
        substring(src.LoginID, charindex('\', src.LoginID) + 1, 50) as FirstName,
        src.JobTitle as LastName,
        src.JobTitle,
        src.MaritalStatus as Department
    from import_employee as src
),

final as (
    select * from logical_transform
)

select * from final

{% endsnapshot %}
