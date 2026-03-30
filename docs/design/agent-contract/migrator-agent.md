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
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "selected_writer": "dbo.usp_load_fact_sales"
    }
  ]
}
```

Project root is inferred from CWD. Reference schema: `../lib/shared/schemas/migrator_input.json`

## Pipeline

For each item in `items[]`:

### 1. AssembleContext (Deterministic — `migrate.py context`)

Run `uv run migrate context --table <item_id> --writer <selected_writer>`.

Outputs: profile (classification, keys, watermark, PII), derived materialization, resolved statements, full proc body, target table columns, source tables, and deterministic schema tests.

### 2. GenerateModel (LLM)

Using the context output, the original proc body, and the resolved `migrate` statements, generate a dbt model:

1. Decide model structure from proc complexity:
   - Single INSERT from sources → one staging model
   - Multiple INSERTs to same table → one model with UNION ALL
   - Temp table chains or multi-step logic → staging + intermediate models with `ref()`
   - Nested subqueries → flatten into sequential CTE chain
2. Generate dbt SQL using the import CTE → logical CTE → final CTE pattern:
   - Import CTEs: all `{{ ref() }}` and `{{ source() }}` at the top
   - Logical CTEs: one transformation step per CTE, descriptive names
   - Final CTE: clean SELECT from the last logical CTE
3. Apply dbt patterns: `{{ config(materialized=...) }}`, `{{ var('param', default) }}` for proc parameters, `{{ ref() }}`/`{{ source() }}` for table references

### 3. LogicalEquivalenceCheck (LLM)

Compare the generated model against the original `migrate` statements:

- Same source tables read?
- Same columns selected/written?
- Same join conditions and types?
- Same filter predicates (WHERE, HAVING)?
- Same aggregation grain (GROUP BY)?
- INSERT/MERGE/UPDATE semantics preserved?

Flag discrepancies as warnings. If a semantic gap is found, revise the model before proceeding.

### 4. BuildSchemaYml

Render `schema_tests` from context into `.yml`:

- PK → `unique` + `not_null`
- FK → `relationships`
- Watermark → `recency` (if incremental)
- PII → column-level `meta` tags
- Model and column descriptions

### 5. WriteArtifacts (Deterministic — `migrate.py write`)

Run `uv run migrate write --table <item_id> --dbt-project-path <path> --model-sql '<sql>' --schema-yml '<yml>'`.

### 6. Validate

Run `dbt compile --select <model_name>` to verify the generated model compiles.

### 7. HandleErrors

- If `migrate.py` fails: set `status: "error"`, record in `errors[]`, continue to next item.
- If LLM generation fails or equivalence check finds irreconcilable gaps: set `status: "partial"`, record in `warnings[]`, continue.
- If `dbt compile` fails: set `status: "partial"`, record compile errors, continue.

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
