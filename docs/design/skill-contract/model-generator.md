# Model Generator Skill Contract

The model-generator skill reads approved profile and resolved statements from catalog files, consumes the approved test spec from `test-specs/` (mandatory — test generation runs before migration), then generates dbt project artifacts. The generator uses `refactored_sql` (CTE-structured SQL from the refactor stage) as its sole SQL input and produces a single model that references sources via `{{ source() }}` directly with the full CTE chain inline. After generating the model, the model-generator runs `dbt test` against the test spec's `unit_tests:` and self-corrects until tests pass (or max iterations reached). The model-generator may also create additional tests beyond the spec. The code reviewer then reviews the output.

## Philosophy and Boundary

- Model generator owns artifact generation (`.sql`, `.yml`, and related dbt resources).
- Model generator reads approved profile from `catalog/tables/<item_id>.json` and resolved statements from `catalog/procedures/<writer>.json`. It derives materialization deterministically from profile classification and generates schema tests from profile answers.
- Model generator uses `refactored_sql` as its sole SQL input. `proc_body` and `statements` are ignored during generation — they exist in context for the reviewer's correctness checks.
- Model generator produces a single model that references sources via `{{ source() }}` directly with the full CTE chain inline. No staging models are generated.
- Model generator reads the approved test spec from `test-specs/<item_id>.json` and renders `unit_tests[]` into the schema YAML alongside schema tests.
- After generating artifacts, the model-generator runs `dbt test` against the unit tests. If tests fail, the model-generator revises the model and re-tests. Maximum self-correction iterations: 3.
- Model generator fetches direct facts (schema, column types, relation metadata) using tools.
- Model generator must not invent business decisions that require FDE judgment.
- After the model-generator's build-and-test loop passes, the code reviewer skill reviews the output and may kick back with standards or correctness issues. See [Code Reviewer Skill](code-reviewer.md).

## Pipeline

### 1. AssembleContext (Deterministic — `migrate.py context`)

Run `uv run migrate context --table <item_id>`.

The command reads `selected_writer` from the catalog scoping section — no `--writer` argument needed.

Outputs: profile (classification, keys, watermark, PII), derived materialization, resolved statements, full proc body, target table columns, source tables, deterministic schema tests, and `refactored_sql` (cleaned, CTE-structured SQL from the refactor stage).

### 2. GenerateModel (LLM)

Using `refactored_sql` as the sole SQL input, generate a single model file (`<target_table>.sql`) that references sources via `{{ source() }}` directly. The full CTE chain (import, logical, final) stays inline in one file — no staging models are generated. Config block uses the profile-derived materialization.

Apply style guides throughout:

- [sql-style.md](../../../plugin/skills/reviewing-model/references/sql-style.md) — keywords, indentation, commas, aliases
- [cte-structure.md](../../../plugin/skills/reviewing-model/references/cte-structure.md) — import-first order, `final` naming, no nested CTEs
- [model-naming.md](../../../plugin/skills/reviewing-model/references/model-naming.md) — layer prefixes, `_dbt_run_id`, `_loaded_at` rules

### 3. LogicalEquivalenceCheck (LLM)

Compare the generated model against `refactored_sql`:

- Same source tables read?
- Same columns selected/written?
- Same join conditions and types?
- Same filter predicates (WHERE, HAVING)?
- Same aggregation grain (GROUP BY)?
- INSERT/MERGE/UPDATE semantics preserved?

Flag discrepancies as warnings. If a semantic gap is found, revise the model before proceeding.

### 4. BuildSchemaYml

Render `schema_tests` from context into `.yml`:

- PK -> `unique` + `not_null`
- FK -> `relationships`
- Watermark -> `recency` (if incremental)
- PII -> column-level `meta` tags
- Model and column descriptions

Apply [yaml-style.md](../../../plugin/skills/reviewing-model/references/yaml-style.md) (indentation, `version: 2`, required descriptions) throughout.

Merge `unit_tests[]` from `test-specs/<item_id>.json` into the schema YAML as a `unit_tests:` block. Every scenario from the test spec must be rendered — none may be dropped or modified.

### 4b. IdentifyCoverageGaps (LLM)

After rendering the test-spec's unit tests, analyze the generated model's logic for branches not covered by existing scenarios (additional JOIN conditions, CASE arms, NULL handling paths, incremental filter boundaries, empty source tables). Generate 1-3 additional unit test scenarios with the naming convention `test_gap_<description>`. Gap tests use LLM-derived expectations (not ground truth) — `dbt test` validates them. Test-spec tests are ground truth and must never be modified; gap tests may be revised during self-correction.

### 5. WriteArtifacts (Deterministic — `migrate.py write`)

Write SQL and YAML to temporary files, then call the write CLI:

```bash
mkdir -p .staging
uv run migrate write \
  --table <item_id> \
  --model-sql-file .staging/model.sql \
  --schema-yml-file .staging/schema.yml; rm -rf .staging
```

The dbt project path is resolved from `$DBT_PROJECT_PATH` or defaults to `./dbt`.

### 6. CompileAndTest

1. Run `dbt compile --select <model_name>` to verify the generated model compiles.
2. Run `dbt test --select <model_name>` to execute unit tests from the test spec.
3. If unit tests fail: analyze failures, revise the model (return to Step 2), and re-test.
4. Maximum self-correction iterations: 3. If tests still fail after 3 attempts, set `status: "partial"` and record failures.

If compile fails with a connection error, fall back to `dbt parse` for offline validation and skip unit tests.

### 7. HandleErrors

- If `migrate.py` fails: set `status: "error"`, record in `errors[]`.
- If LLM generation fails or equivalence check finds irreconcilable gaps: set `status: "partial"`, record in `warnings[]`.
- If `dbt compile` fails: set `status: "partial"`, record compile errors.
- If `dbt test` fails after max iterations: set `status: "partial"`, record failing test names and diffs.

## Output Structure

Per-item output written to `.migration-runs/`:

```json
{
  "item_id": "dbo.fact_sales",
  "status": "ok|partial|error",
  "output": {
    "table_ref": "dbo.fact_sales",
    "model_name": "fct_fact_sales",
    "artifact_paths": {
      "model_sql": "models/gold/fct_fact_sales.sql",
      "staging_sql": ["models/staging/stg_source_a.sql", "models/staging/stg_source_b.sql"],
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
      "dbt_compile_passed": true,
      "dbt_test_passed": true,
      "self_correction_iterations": 0,
      "dbt_errors": []
    },
    "review": {
      "iterations": 1,
      "verdict": "approved|approved_with_warnings"
    },
    "warnings": [],
    "errors": []
  },
  "errors": []
}
```

## Required Model Generator Guarantees

- Generated dbt artifacts must reflect profile answers and `refactored_sql` faithfully.
- Models use `{{ source() }}` directly with the full CTE chain inline — no staging models are generated.
- Schema tests are derived deterministically from profile (PK, FK, watermark, PII) and rendered into column/model-level dbt tests in `model_yaml`.
- `unit_tests[]` from `test-specs/<item_id>.json` is rendered into `unit_tests:` blocks in `model_yaml`. All scenarios must be present — none dropped.
- All unit tests must pass before the model-generator reports `status: "ok"`.
- Tool-fetched schema facts are used for type/column correctness.
- If catalog data is incomplete, return `partial|error` with explicit missing fields.

## Model Generator Boundary

Model generator must not:

- invent business decisions that require FDE judgment
- request new decision candidates from profiler
- override approved profile answers

`warnings[]`, `errors[]`, and `execution.dbt_errors[]` use the shared diagnostics schema in `README.md`.
