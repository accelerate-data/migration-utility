# Refactor Mart Int Validation Failure Plan

## Targets

- gold.fct_sales

## Assumptions

- Dependency gating for later candidates observes statuses written earlier in
  the same int-mode wave.

## Candidate Summary

- STG-001 is already applied.
- INT-001 is attempted but validation fails.
- MART-001 depends on INT-001 and must be blocked after INT-001 fails.

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders
- Execution status: applied
- Validation result: dbt build --select stg_bronze__orders

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_orders.sql
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: planned

Rewrite `int_sales_orders` so it references `stg_bronze__orders`.

## Candidate: MART-001

- [x] Approve: yes
- Type: mart
- Output: dbt/models/marts/fct_sales.sql
- Depends on: INT-001
- Validation: dbt build --select fct_sales
- Execution status: planned

Rewrite `fct_sales` so it references `int_sales_orders`.

## Execution Order

1. Run `/refactor-mart docs/design/refactor-mart-int-validation-failure.md int`.
