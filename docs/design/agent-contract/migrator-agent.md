# Migrator Agent Contract

The migrator agent consumes planner output and test generator fixtures, then generates dbt project artifacts.
Migrator is responsible for querying direct source metadata via tools and converting planning output
into executable files. The application merges planner output and FixtureManifest per `item_id` before routing to the migrator.

## Philosophy and Boundary

- Migrator owns artifact generation (`.sql`, `.yml`, and related dbt resources).
- Migrator fetches direct facts (schema, column types, relation metadata) using tools.
- Migrator must not invent business decisions that require FDE judgment.
- Planner output is authoritative for selected answers, decomposition, schema tests, and documentation.
- Test generator output (`unit_tests[]`) is authoritative for fixture-based unit tests.

## Required Input

```json
{
  "schema_version": "",
  "run_id": "",
  "items": [
    {
      "item_id": "",
      "answers": {},
      "decomposition": {...},
      "plan": {...},
      "unit_tests": []
    }
  ]
}
```

**Example**

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "answers": {
        "writer": "dbo.usp_load_fact_sales",
        "classification": "fact_transaction",
        "primary_key": ["sale_id"],
        "primary_key_type": "surrogate",
        "natural_key": ["order_id", "line_number"],
        "foreign_keys": [
          {
            "column": "customer_sk",
            "references_source_relation": "dbo.dim_customer",
            "references_column": "customer_sk",
            "fk_type": "standard"
          }
        ],
        "watermark": "load_date",
        "pii_actions": [
          { "column": "customer_email", "action": "mask" }
        ]
      },
      "decomposition": {
        "segmented_logical_blocks": [],
        "candidate_model_split_points": []
      },
      "plan": {
        "materialization": "incremental",
        "schema_tests": {
          "entity_integrity_tests": [
            { "name": "not_null", "columns": ["sale_id"], "severity": "error" },
            { "name": "unique", "columns": ["sale_id"], "severity": "error" },
            { "name": "unique_combination", "columns": ["order_id", "line_number"], "severity": "error" }
          ],
          "referential_integrity_tests": [
            {
              "name": "relationships",
              "column": "customer_sk",
              "references_source_relation": "dbo.dim_customer",
              "references_column": "customer_sk",
              "severity": "error"
            }
          ],
          "domain_validity_tests": [
            { "name": "not_null", "columns": ["customer_sk"], "severity": "error" }
          ],
          "incremental_recency_tests": [
            { "name": "not_null", "columns": ["load_date"], "severity": "error" },
            { "name": "recency", "columns": ["load_date"], "severity": "warning" }
          ],
          "classification_semantic_tests": [
            { "name": "grain_no_duplication", "columns": ["sale_id"], "classification": "fact_transaction", "severity": "error" }
          ],
          "pii_governance_checks": [
            { "name": "column_masking_applied", "column": "customer_email", "action": "mask", "severity": "error" }
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
      },
      "unit_tests": [
        {
          "name": "test_incremental_new_sale_inserted",
          "model": "fct_fact_sales",
          "given": [
            {
              "input": "source('fabric_wh', 'staging_sales')",
              "rows": [
                { "order_id": 1, "line_number": 1, "customer_sk": 101, "load_date": "2024-01-15" }
              ]
            }
          ],
          "expect": {
            "rows": [
              { "sale_id": 1001, "order_id": 1, "line_number": 1, "customer_sk": 101, "load_date": "2024-01-15" }
            ]
          }
        }
      ]
    }
  ]
}
```

## Output Schema (MigrationArtifactManifest)

## Output Structure (Short)

```json
{
  "schema_version": "",
  "run_id": "",
  "results": [
    {
      "item_id": "",
      "status": "",
      "output": {
        "table_ref": "",
        "model_name": "",
        "artifact_paths": {...},
        "generated": {...},
        "execution": {...},
        "warnings": [],
        "errors": []
      },
      "errors": []
    }
  ],
  "summary": {...}
}
```

**Example**

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
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
            "uses_writer_logic": true
          },
          "model_yaml": {
            "has_model_description": true,
            "has_column_descriptions": true,
            "schema_tests_rendered": ["entity_integrity_tests", "referential_integrity_tests", "incremental_recency_tests", "pii_governance_checks"],
            "has_unit_tests": true
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

- Generated dbt artifacts must reflect planner output exactly.
- `plan.schema_tests` is rendered into column/model-level dbt tests in `model_yaml` (not_null, unique, relationships, freshness, etc.).
- `unit_tests[]` from the test generator is rendered into `unit_tests:` blocks in `model_yaml`.
- Tool-fetched schema facts are used for type/column correctness.
- If planner output is incomplete, return `partial|error` with explicit missing fields.

## Migrator Boundary

Migrator must not:

- change planner business decisions
- request new decision candidates from profiler
- add approval gating requirements not present in planner input

`warnings[]`, `errors[]`, and `execution.dbt_errors[]` use the shared diagnostics schema in
`docs/design/agent-contract/README.md`.
