{{ config(materialized='table') }}

{#
  Source procedure usp_load_FactExecProfile is an orchestrator that
  delegates to usp_stage_FactExecProfile. The staging proc performs
  the actual INSERT with a JOIN. This model represents the leaf writer.
#}

with source_execlog as (
    select * from {{ source('bronze', 'execlog') }}
),

dim_procedure as (
    select * from {{ ref('dim_procedure') }}
),

joined as (
    select
        p.ProcedureKey,
        e.ExecutionDate,
        e.DurationMs,
        e.RowsAffected,
        e.StatusCode
    from source_execlog e
    join dim_procedure p on e.ProcedureName = p.ProcedureName
),

final as (
    select
        ProcedureKey,
        ExecutionDate,
        DurationMs,
        RowsAffected,
        StatusCode
    from joined
)

select * from final
