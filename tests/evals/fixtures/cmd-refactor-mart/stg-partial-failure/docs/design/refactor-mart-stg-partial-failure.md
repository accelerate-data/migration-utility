# Refactor Mart Staging Partial Failure Plan

## Targets

- gold.fct_sales

## Assumptions

- Each approved staging candidate is independent.

## Non-Goals

- Do not stop the staging wave after the first candidate failure.

## Candidate Summary

- STG-001 can be applied and validated.
- STG-002 has a declared consumer with an eval-only validation failure marker.

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders int_sales_orders
- Rewire consumers: dbt/models/intermediate/int_sales_orders.sql
- Execution status: planned

## Candidate: STG-002

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__returns.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__returns int_returns
- Rewire consumers: dbt/models/intermediate/int_returns.sql
- Execution status: planned

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_orders.sql
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: planned

## Execution Order

1. Run `/refactor-mart docs/design/refactor-mart-stg-partial-failure.md stg`.
2. Resolve STG-002 before running the higher-layer wave.
