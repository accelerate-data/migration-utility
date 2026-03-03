# Planner Agent Contract

The planner agent consumes approved migration inputs and returns a design manifest for migrator
execution. Planner output is final planning intent: materialization, test plan, and
documentation.

## Philosophy and Boundary

- Planner consumes FDE decisions on the profiling output.
- Planner consumes approved decomposition input from app routing (including FDE edits/approval).
- Planner adds design decisions the profiler does not provide:
  - materialization strategy
  - explicit test plan
  - documentation payload
- Migrator generates dbt SQL and YAML artifacts from planner output plus tool-fetched facts.

## Required Input

```json
{
  "schema_version": "",
  "batch_id": "",
  "items": [
    {
      "item_id": "",
      "answers": {
        "writer": "",
        "classification": "",
        "primary_key": [],
        "primary_key_type": "",
        "natural_key": [],
        "foreign_keys": [],
        "watermark": "",
        "pii_actions": []
      },
      "decomposition": {...}
    }
  ]
}
```

**Example**

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
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
        "segmented_logical_blocks": [
          {
            "block_id": "01_extract_sales_stage",
            "purpose": "Load source rows and apply base filters.",
            "rationale": ["Statement boundary and source extraction phase boundary."],
            "source_sql_ref": {
              "statement_indices": [0],
              "line_span": { "start": 12, "end": 37 }
            },
            "confidence": 0.91
          },
          {
            "block_id": "02_enrich_customer_product",
            "purpose": "Resolve dimension surrogate keys.",
            "rationale": ["Join/enrichment phase boundary after extract block."],
            "source_sql_ref": {
              "statement_indices": [1],
              "line_span": { "start": 38, "end": 79 }
            },
            "confidence": 0.88
          }
        ],
        "candidate_model_split_points": [
          {
            "split_after_block_id": "01_extract_sales_stage",
            "proposed_model_name": "int_fact_sales_source",
            "rationale": ["Reusable filtered source layer."],
            "confidence": 0.86
          }
        ]
      }
    }
  ]
}
```

## Planning Strategy

### 1. ValidateSelectedAnswers

- Validate presence and structure of answers.
- Reject items with missing required selections as `partial|error`.

### 2. DecideMaterialization

- Choose `materialization` from answers and planning rules.
- Apply deterministic mapping using classification + watermark availability.
- Apply rules in order:
  - if `classification == "dim_scd2"`, use `snapshot`
  - else if `watermark` is missing or null, use `table`
  - else if `classification == "fact_periodic_snapshot"`, use `table`
  - else if `classification` is one of
    `fact_transaction|fact_accumulating_snapshot|fact_aggregate|dim_non_scd|dim_scd1|dim_junk`
    and `watermark` is present, use `incremental`
  - else return `partial` with issue code `PLANNER_MATERIALIZATION_UNRESOLVED`

### 3. UseApprovedDecomposition

- Consume approved `decomposition` input from app routing.
- Carry `decomposition` forward as top-level output next to `answers` (not under `plan`).
- Planner must not modify decomposition boundaries. Boundary edits are upstream
  (decomposer/FDE/app).

### 4. BuildSchemaTests

- Generate explicit test intent grouped by category:
  - `entity_integrity_tests`
  - `referential_integrity_tests`
  - `domain_validity_tests`
  - `incremental_recency_tests`
  - `classification_semantic_tests`
  - `pii_governance_checks`
- Branch-covering fixture generation is owned entirely by the test generator agent. Planner does not emit `unit_tests`.
- Deterministic rules:
  - `entity_integrity_tests`:
    - always generate `not_null` and `unique` on `answers.primary_key`.
    - if `answers.natural_key` is non-empty, generate `unique_combination`.
  - `referential_integrity_tests`:
    - generate one `relationships` test per `answers.foreign_keys[*]`.
  - `domain_validity_tests`:
    - generate `not_null` tests for business-critical columns explicitly provided in answers
      (`primary_key`, `watermark`, and `foreign_keys[*].column`).
  - `incremental_recency_tests`:
    - if `answers.watermark` exists, generate watermark `not_null`.
    - if `plan.materialization == "incremental"`, generate freshness/recency assertion on watermark.
  - `classification_semantic_tests`:
    - for `classification` that implies additive facts (`fact_transaction`, `fact_aggregate`),
      generate grain-preservation/no-duplication assertions aligned to `primary_key`.
    - for snapshot classifications (`fact_periodic_snapshot`, `fact_accumulating_snapshot`),
      generate snapshot-timeline consistency assertions.
  - `pii_governance_checks`:
    - for each `answers.pii_actions[*]`, generate one governance check aligned to action
      (`mask`, `drop`, `tokenize`, `keep`).
  - if required test inputs are missing, return `partial` with issue code
    `PLANNER_TEST_PLAN_INCOMPLETE`.

### 5. BuildDocumentation

- Generate model-level and column-level documentation payload for migrator rendering.

### 6. ValidateOutput

- Run internal consistency/contract checks.
- Runtime failures are reported in `errors`.
- Validation checklist:
  - `item_id` is present.
  - `status` is one of: `ok|partial|error`.
  - `answers` echo payload is present.
  - approved input `decomposition` is present.
  - output `decomposition` matches approved input `decomposition`.
  - `plan.materialization` is present when `status == "ok"`.
  - every `decomposition.candidate_model_split_points[*].split_after_block_id` exists in
    `decomposition.segmented_logical_blocks[*].block_id`.
  - every `decomposition.segmented_logical_blocks[*].rationale` is `string[]`.
  - every `decomposition.candidate_model_split_points[*].rationale` is `string[]`.
  - `plan.schema_tests` contains all required categories from `BuildSchemaTests` rules.
  - required categories in `plan.schema_tests`:
    - `entity_integrity_tests`
    - `referential_integrity_tests`
    - `domain_validity_tests`
    - `incremental_recency_tests`
    - `classification_semantic_tests`
    - `pii_governance_checks`
  - `plan.documentation.model_name` and `plan.documentation.model_description` are present when
    `status == "ok"`.
  - if `status == "partial"`, `validation.issues` is non-empty.
  - if `status == "error"`, `errors` is non-empty.
  - `validation.passed` is `false` when any validation issue exists.
  - summary counts match item-level statuses.

## Output Schema (PlannerDesignManifest)

```json
{
  "schema_version": "",
  "batch_id": "",
  "results": [
    {
      "item_id": "",
      "status": "",
      "answers": {},
      "decomposition": {
        "segmented_logical_blocks": [],
        "candidate_model_split_points": []
      },
      "plan": {
        "materialization": "",
        "schema_tests": {
          "entity_integrity_tests": [],
          "referential_integrity_tests": [],
          "domain_validity_tests": [],
          "incremental_recency_tests": [],
          "classification_semantic_tests": [],
          "pii_governance_checks": []
        },
        "documentation": {
          "model_name": "",
          "model_description": "",
          "column_descriptions": [],
          "business_definitions": [],
          "tags": [],
          "owner": ""
        }
      },
      "validation": {...}
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
  "batch_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "ok|partial|error",
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
        "segmented_logical_blocks": [
          {
            "block_id": "01_extract_sales_stage",
            "purpose": "Load source rows and apply base filters.",
            "rationale": ["Statement boundary and source extraction phase boundary."],
            "source_sql_ref": {
              "statement_indices": [0],
              "line_span": { "start": 12, "end": 37 }
            }
          },
          {
            "block_id": "02_enrich_customer_product",
            "purpose": "Resolve dimension surrogate keys.",
            "rationale": ["Join/enrichment phase boundary after extract block."],
            "source_sql_ref": {
              "statement_indices": [1],
              "line_span": { "start": 38, "end": 79 }
            }
          },
          {
            "block_id": "03_project_fact_sales",
            "purpose": "Project final grain and output columns.",
            "rationale": ["Final projection boundary before model output."],
            "source_sql_ref": {
              "statement_indices": [2],
              "line_span": { "start": 80, "end": 103 }
            }
          }
        ],
        "candidate_model_split_points": [
          {
            "split_after_block_id": "01_extract_sales_stage",
            "proposed_model_name": "int_fact_sales_source",
            "rationale": ["Reusable filtered source layer."]
          },
          {
            "split_after_block_id": "02_enrich_customer_product",
            "proposed_model_name": "int_fact_sales_enriched",
            "rationale": ["Dimension key resolution isolated from final projection."]
          }
        ]
      },
      "plan": {
        "materialization": "incremental",
        "schema_tests": {
          "entity_integrity_tests": [
            { "name": "not_null", "columns": ["sale_id"], "severity": "error" },
            { "name": "unique", "columns": ["sale_id"], "severity": "error" },
            {
              "name": "unique_combination",
              "columns": ["order_id", "line_number"],
              "severity": "error"
            }
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
            {
              "name": "grain_no_duplication",
              "columns": ["sale_id"],
              "classification": "fact_transaction",
              "severity": "error"
            }
          ],
          "pii_governance_checks": [
            {
              "name": "column_masking_applied",
              "column": "customer_email",
              "action": "mask",
              "severity": "error"
            }
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
        }
      },
      "validation": {
        "passed": true,
        "issues": []
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

## Required Planner Outputs

Each `results[*]` item must contain:

- `answers` echo payload
- `decomposition` (approved and unchanged)
- `plan.materialization`
- `plan.schema_tests` (all required categories)
- `plan.documentation`
- `validation`
- `errors`

## Planner Boundary

Planner must not output generated dbt SQL/Jinja or rendered dbt YAML files.
Those are migrator responsibilities.

`validation.issues[]` and `errors[]` use the shared diagnostics schema in
`docs/design/agent-contract/README.md`.
