# Migrator Agent Contract

The migrator agent consumes planner-approved decisions and generates dbt project artifacts.
Migrator is responsible for querying direct source metadata via tools and converting decisions into executable files.

## Philosophy and Boundary

- Migrator owns artifact generation (`.sql`, `.yml`, and related dbt resources).
- Migrator fetches direct facts (schema, column types, relation metadata) using tools.
- Migrator must not invent business decisions that require FDE judgment.
- Planner decisions are authoritative for classification, keys, watermark, and PII actions.

## Required Input

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "status": "approved",
      "decision": {
        "selected_writer": "dbo.usp_load_fact_sales",
        "selected_classification": "fact_transaction",
        "selected_materialization": "incremental",
        "selected_primary_key": ["sale_id"],
        "selected_primary_key_type": "surrogate",
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
        "business_definitions": [],
        "tags": ["gold", "sales"],
        "owner": "data-platform"
      }
    }
  ]
}
```

## Output Schema (MigrationArtifactManifest)

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "ok|partial|error",
      "output": {
        "table_ref": "dbo.fact_sales",
        "model_name": "fct_fact_sales",
        "artifact_paths": {
          "model_sql": "models/gold/fct_fact_sales.sql",
          "model_yaml": "models/gold/fct_fact_sales.yml",
          "source_yaml": "models/sources/warehouse_sources.yml"
        },
        "generated": {
          "model_sql": {
            "materialized": "incremental",
            "uses_watermark": true,
            "uses_selected_writer_logic": true
          },
          "model_yaml": {
            "has_model_description": true,
            "has_column_descriptions": true,
            "has_primary_key_tests": true,
            "has_foreign_key_relationship_tests": true
          },
          "source_yaml": {
            "source_count": 1,
            "table_count": 3
          }
        },
        "execution": {
          "dbt_parse_passed": true,
          "dbt_compile_passed": true,
          "dbt_errors": []
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

## Required Migrator Guarantees

- Only `status == "approved"` decision items are executed.
- Generated dbt artifacts must reflect approved planner decisions exactly.
- Tool-fetched schema facts are used for type/column correctness.
- If planner decisions are incomplete, return `partial|error` with explicit missing fields.

## Migrator Boundary

Migrator must not:

- change approved business decisions
- request new decision candidates from profiler
- bypass planner approval state
