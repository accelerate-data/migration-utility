# Planner Agent Contract

The planner agent consumes scoping context plus profiler candidates and returns an editable
FDE decision manifest. Planner output is decisions and documentation intent only.

## Philosophy and Boundary

- Planner captures FDE-approved judgments.
- Planner does not emit fields the migrator can fetch reliably using tools.
- Migrator generates dbt SQL and YAML artifacts from planner decisions plus tool-fetched facts.

## Required Input

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "selected_writer": "dbo.usp_load_fact_sales",
      "candidate_profile": {
        "candidate_classifications": [
          { "resolved_kind": "fact_transaction", "confidence": 0.88 }
        ],
        "candidate_primary_keys": [
          {
            "columns": ["sale_id"],
            "primary_key_type": "surrogate",
            "confidence": 0.97
          }
        ],
        "candidate_natural_keys": [
          { "columns": ["order_id", "line_number"], "confidence": 0.78 }
        ],
        "candidate_foreign_keys": [
          {
            "column": "customer_sk",
            "references": "dim_customer.customer_sk",
            "confidence": 0.9
          }
        ],
        "candidate_watermarks": [
          { "column": "load_date", "confidence": 0.94 }
        ],
        "candidate_pii_actions": [
          { "column": "customer_email", "suggested_action": "mask", "confidence": 0.93 }
        ]
      }
    }
  ]
}
```

## Output Schema (PlannerDecisionManifest)

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "decisions": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "status": "draft|approved|rejected|needs_clarification|error",
      "decision": {
        "selected_writer": "dbo.usp_load_fact_sales",
        "selected_classification": "fact_transaction",
        "selected_materialization": "incremental",
        "selected_primary_key": ["sale_id"],
        "selected_primary_key_type": "surrogate|natural|composite|unknown",
        "selected_natural_key": ["order_id", "line_number"],
        "selected_foreign_keys": [
          { "column": "customer_sk", "references": "dim_customer.customer_sk" }
        ],
        "selected_watermark": "load_date",
        "selected_pii_actions": [
          { "column": "customer_email", "action": "mask" }
        ]
      },
      "documentation": {
        "model_name": "fct_fact_sales",
        "model_description": "Transaction-level sales fact table for reporting and analytics.",
        "column_descriptions": [
          { "column": "sale_id", "description": "Surrogate key for each sale event." },
          { "column": "customer_sk", "description": "Foreign key to dim_customer." },
          { "column": "load_date", "description": "Ingestion timestamp used for incremental loading." }
        ],
        "business_definitions": [
          {
            "term": "Sale Event",
            "definition": "A finalized transaction line captured at checkout."
          }
        ],
        "tags": ["gold", "sales"],
        "owner": "data-platform"
      },
      "approval": {
        "approved_by": "",
        "approved_at_utc": "",
        "notes": ""
      },
      "open_questions": [],
      "warnings": [],
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "approved": 0,
    "draft": 1,
    "rejected": 0,
    "needs_clarification": 0,
    "error": 0
  }
}
```

## Required Planner Decisions

Each `decisions[*]` item must contain:

- `decision.selected_classification`
- `decision.selected_primary_key`
- `decision.selected_primary_key_type`
- `decision.selected_foreign_keys`
- `decision.selected_watermark` (or explicit null when not applicable)
- `decision.selected_pii_actions`
- `documentation.model_name`
- `documentation.model_description`

## Planner Boundary

Planner must not output:

- target schema metadata
- source schema metadata
- generated dbt SQL/Jinja content
- generated dbt YAML content

These are migrator responsibilities using tool-fetched facts and planner-approved decisions.
