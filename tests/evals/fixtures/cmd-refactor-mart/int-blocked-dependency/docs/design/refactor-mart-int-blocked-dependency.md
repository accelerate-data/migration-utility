# Refactor Mart Int Blocked Dependency Plan

## Targets

- gold.fct_sales

## Assumptions

- Higher-layer candidates must not edit models until dependencies are applied.

## Candidate Summary

- INT-001 depends on STG-001, which is still planned.
- MART-001 depends on STG-002, which failed in the staging wave.
- INT-002 is missing dependency metadata and must block before edits.
- INT-003 has malformed dependency metadata and must block before edits.
- MART-002 has ambiguous dependency metadata and must block before edits.
- INT-004 has empty dependency metadata and must block before edits.

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders
- Execution status: planned

## Candidate: STG-002

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__returns.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__returns
- Execution status: failed
- Validation result: dbt build --select stg_bronze__returns failed

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
- Depends on: STG-002
- Validation: dbt build --select fct_sales
- Execution status: planned

Rewrite `fct_sales` so it references `int_sales_orders`.

## Candidate: INT-002

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_returns.sql
- Validation: dbt build --select int_sales_returns
- Execution status: planned

Rewrite `int_sales_returns` so it references `stg_bronze__returns`.

## Candidate: INT-003

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_adjustments.sql
- Depends on: STG-001; STG-002
- Validation: dbt build --select int_sales_adjustments
- Execution status: planned

Rewrite `int_sales_adjustments` so it references `stg_bronze__orders`.

## Candidate: MART-002

- [x] Approve: yes
- Type: mart
- Output: dbt/models/marts/fct_returns.sql
- Depends on: STG-001 or STG-002
- Validation: dbt build --select fct_returns
- Execution status: planned

Rewrite `fct_returns` so it references `int_sales_returns`.

## Candidate: INT-004

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_empty_dependency.sql
- Depends on:
- Validation: dbt build --select int_empty_dependency
- Execution status: planned

Rewrite `int_empty_dependency` so it references `stg_bronze__orders`.

## Execution Order

1. Run `/refactor-mart docs/design/refactor-mart-int-blocked-dependency.md int`.
