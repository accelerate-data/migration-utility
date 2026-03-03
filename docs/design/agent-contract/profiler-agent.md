# Profiler Agent Contract

The profiler agent proposes migration candidates for FDE review.
It should output only inferred candidates that require judgment, not direct facts planner can fetch via tools.

## Philosophy and Boundary

- Profiler is for candidate generation, not metadata transport.
- Planner retrieves direct facts (schema, constraints, object metadata) using tools.
- FDE approves profiler candidates before planner consumes them.
- Avoid duplicate derivation: if planner can fetch a fact reliably, profiler should not include it.

## Required Input

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "status": "resolved",
      "selected_writer": "dbo.usp_load_fact_sales"
    }
  ]
}
```

## Output Schema (ProfilerCandidateProfile)

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "status": "ok|partial|error",
      "profile": {
        "request": {
          "selected_writer": "dbo.usp_load_fact_sales",
          "target_table": "dbo.fact_sales"
        },
        "candidate_classifications": [
          {
            "resolved_kind": "fact_transaction",
            "confidence": 0.88,
            "rationale": ["Transaction-level insert behavior detected."]
          }
        ],
        "candidate_primary_keys": [
          {
            "columns": ["sale_id"],
            "primary_key_type": "surrogate|natural|composite|unknown",
            "confidence": 0.97,
            "rationale": "High uniqueness and stable writer behavior."
          }
        ],
        "candidate_natural_keys": [
          {
            "columns": ["order_id", "line_number"],
            "confidence": 0.78,
            "rationale": "Business-level row identity pattern."
          }
        ],
        "candidate_foreign_keys": [
          {
            "column": "customer_sk",
            "references": "dim_customer.customer_sk",
            "confidence": 0.9,
            "rationale": "Join and naming pattern evidence."
          }
        ],
        "candidate_watermarks": [
          {
            "column": "load_date",
            "confidence": 0.94,
            "rationale": "Monotonic load timestamp in writer logic."
          }
        ],
        "candidate_pii_actions": [
          {
            "column": "customer_email",
            "entity": "email",
            "suggested_action": "mask|drop|tokenize|keep",
            "confidence": 0.93,
            "rationale": "Column and value pattern evidence."
          }
        ],
        "approval_required_fields": [
          "classification",
          "primary_key",
          "primary_key_type",
          "natural_key",
          "foreign_keys",
          "watermark",
          "pii_actions"
        ],
        "validation": {
          "passed": true,
          "issues": []
        },
        "warnings": [],
        "errors": []
      },
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "ok": 1,
    "partial": 0,
    "error": 0
  }
}
```

## Classification Kinds

`candidate_classifications[*].resolved_kind` must be one of:

- `dim_non_scd`
- `dim_scd1`
- `dim_scd2`
- `dim_junk`
- `fact_transaction`
- `fact_periodic_snapshot`
- `fact_accumulating_snapshot`
- `fact_aggregate`

## What Profiler Must Not Output

- Direct metadata planner can fetch reliably (for example target schema definitions).
- Final dbt SQL or Jinja model content.
- Final materialization/test decisions.
