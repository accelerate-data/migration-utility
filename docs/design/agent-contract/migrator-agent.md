# Migrator Agent Contract

The migrator agent reads approved profile and resolved statements from catalog files, then generates dbt project artifacts. Migrator is responsible for querying direct source metadata via tools and converting profile data into executable files.

## Philosophy and Boundary

- Migrator owns artifact generation (`.sql`, `.yml`, and related dbt resources).
- Migrator reads approved profile from `catalog/tables/<item_id>.json` and resolved statements from `catalog/procedures/<writer>.json`. It derives materialization deterministically from profile classification and generates schema tests from profile answers. Test generator output (`unit_tests[]`) is incorporated in a subsequent stage.
- Migrator fetches direct facts (schema, column types, relation metadata) using tools.
- Migrator must not invent business decisions that require FDE judgment.

## Required Input

```json
{
  "schema_version": "2.0",
  "run_id": "uuid",
  "ddl_path": "/absolute/path/to/artifacts/ddl",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "selected_writer": "dbo.usp_load_fact_sales"
    }
  ]
}
```

Reference schema: `../shared/shared/schemas/migrator_input.json`

## Pipeline

The migrator owns these steps internally:

### 1. ReadCatalog

Read profile from `catalog/tables/<item_id>.json` and resolved statements from `catalog/procedures/<writer>.json`.

### 2. DecideMaterialization

Deterministic rules based on profile classification:

- `dim_scd2` -> `snapshot`
- No watermark OR `fact_periodic_snapshot` -> `table`
- Fact/dimension with watermark -> `incremental`

### 3. FilterMigrateStatements

Filter resolved statements where `action == migrate`.

### 4. TranspileSQL

Use sqlglot to transpile T-SQL to target dialect, wrap in dbt model.

### 5. BuildSchemaTests

Deterministic from profile:

- PK -> `unique` + `not_null`
- FK -> `relationships`
- Watermark -> `recency` (if incremental)
- PII -> column-level `meta` tags

### 6. BuildDocumentation

Model name, description, and column descriptions from profile and tool-fetched column metadata.

### 7. WriteArtifacts

Write dbt model `.sql` and schema `.yml` files.

### 8. ValidateOutput

Validate generated artifacts for correctness and completeness.

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

- Generated dbt artifacts must reflect profile answers and resolved statements faithfully.
- Schema tests are derived deterministically from profile (PK, FK, watermark, PII) and rendered into column/model-level dbt tests in `model_yaml`.
- `unit_tests[]` from the test generator is rendered into `unit_tests:` blocks in `model_yaml` in a subsequent stage.
- Tool-fetched schema facts are used for type/column correctness.
- If catalog data is incomplete, return `partial|error` with explicit missing fields.

## Migrator Boundary

Migrator must not:

- invent business decisions that require FDE judgment
- request new decision candidates from profiler
- override approved profile answers

`warnings[]`, `errors[]`, and `execution.dbt_errors[]` use the shared diagnostics schema in
`docs/design/agent-contract/README.md`.
