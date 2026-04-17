# Refactor Mart Staging Happy Path Plan

## Targets

- gold.fct_sales

## Assumptions

- The staging candidate is a source-facing normalization with declared
  downstream consumers.

## Non-Goals

- Do not execute higher-layer candidates during staging mode.

## Candidate Summary

- STG-001 normalizes `stg_bronze__orders` and rewires multiple direct consumers.
- INT-001 and MART-001 remain planned for a later wave.

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders int_sales_orders int_sales_order_summary
- Execution status: planned

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_orders.sql
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: planned

## Candidate: MART-001

- [x] Approve: yes
- Type: mart
- Output: dbt/models/marts/fct_sales.sql
- Depends on: INT-001
- Validation: dbt build --select fct_sales
- Execution status: planned

## Execution Order

1. Run `/refactor-mart docs/design/refactor-mart-stg-happy-path.md stg`.
2. Run `/refactor-mart docs/design/refactor-mart-stg-happy-path.md int`.
