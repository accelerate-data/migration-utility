# Planner Agent Contract

The planner agent consumes scoping input + profiler output and returns an editable plan JSON for FDE.
Planner output is decisions/proposal, not raw profiling facts.

## Required Input

```json
{
  "request": {
    "procedure": { "name": "dbo.usp_load_fact_sales" },
    "target": {
      "name": "dbo.fact_sales",
      "intended_kind": "auto|dim_non_scd|dim_scd1|dim_scd2|dim_junk|fact_transaction|fact_periodic_snapshot|fact_accumulating_snapshot|fact_aggregate"
    },
    "constraints": {
      "business_context": "",
      "fde_overrides": []
    }
  },
  "profile": "PlannerReadyProfile"
}
```

## Planner Requery Rule

Planner should not re-query SQL Server when:

- `profile.completeness.status == "complete"`, and
- `profile.validation.passed == true`, and
- all planner-required fields are present.

Planner may do targeted fallback queries only for missing/invalid required sections.

## Output Schema (PlanAgentOutput)

```json
{
  "schema_version": "1.0",
  "table_ref": "dbo.fact_sales",
  "status": "draft",

  "source_context": {
    "procedure_name": "dbo.usp_load_fact_sales",
    "procedure_summary": "Loads fact_sales from stage + dimensions with incremental filtering.",
    "input_relations": ["dbo.sales_stage", "dbo.dim_customer", "dbo.dim_product"],
    "output_relation": "dbo.fact_sales"
  },

  "proposed": {
    "model_name": "fct_fact_sales",
    "layer": "gold",
    "materialized": "incremental",
    "description": "Transaction-level sales fact table for reporting.",
    "source_tables": ["dbo.sales_stage", "dbo.dim_customer", "dbo.dim_product"],
    "unique_key": ["sale_id"],
    "incremental_column": "load_date",
    "canonical_date_column": "sale_date",
    "sql": "with src as (...) select * from ..."
  },

  "tests": [
    {
      "name": "not_null",
      "input": "sale_id",
      "purpose": "Ensure transaction key exists."
    },
    {
      "name": "unique",
      "input": "sale_id",
      "purpose": "Enforce fact grain."
    },
    {
      "name": "relationships",
      "input": "customer_sk -> dim_customer.customer_sk",
      "purpose": "Validate dimension key integrity."
    }
  ],

  "pii_candidates": [
    {
      "column": "customer_email",
      "entity": "email",
      "confidence": 0.91,
      "source": "profile",
      "action": "mask",
      "rationale": "PII candidate detected in profiler evidence."
    }
  ],

  "current": {
    "state": "completed",
    "phase": "planning",
    "current_procedure": "dbo.usp_load_fact_sales",
    "current_sql": "select ..."
  },

  "fde_review": {
    "must_confirm": [
      "Model kind and materialization",
      "Unique key and incremental column",
      "Tests",
      "PII actions"
    ],
    "open_questions": [],
    "notes": ""
  },

  "confidence": {
    "overall": 0.84
  }
}
```

## FDE Responsibilities

- Review and edit any `proposed` field.
- Review and edit `tests` rows (`name`, `input`, `purpose`).
- Review and confirm `pii_candidates[*].action`.
- Approve by setting `status` from `draft`/`stale` to `approved` only after unresolved questions are closed.

## Planner Boundary

Planner should not copy raw profiler dumps into output. It should emit only explainable decisions and references to source evidence where needed.
