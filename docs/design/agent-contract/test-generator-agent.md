# Test Generator Agent Contract

The test generator agent produces branch-covering dbt unit test fixtures for a stored procedure migration. It extracts all conditional branches from the proc, generates synthetic input data that exercises each branch, captures the actual proc output as ground truth, and emits `unit_tests:` YAML blocks for the migrator to incorporate into the model schema file.

This contract is for the **batch GHA pipeline** only. For the interactive single-table path, `test_gen.py` (part of the `migrate-table` plugin) produces dbt schema tests from AST inference with no live DB required — see [SP → dbt Migration Plugin](../sp-to-dbt-plugin/README.md). The full ground-truth harness below applies when a live source DB connection is available (batch path).

See [docs/design/unit-test-strategy/](../unit-test-strategy/) for design rationale, tooling decisions, and the full harness design including the branch manifest schema.

Test generation runs AFTER migration in the pipeline (migration is stage 3, test generation is stage 4).

## Philosophy and Boundary

- Test generator owns fixture generation and ground-truth capture.
- Test generator reads profile from `catalog/tables/<item_id>.json`, resolved statements from `catalog/procedures/<writer>.json`, and migration output from the migrator's artifact directory.
- `proc_body` is read from DDL files via `discover show --ddl-path <ddl_path> --name <writer>`. `table_schemas` are read from DDL files via `discover show --ddl-path <ddl_path> --name <table>`. No live database access is needed for metadata — the live DB is only used for ground-truth execution in the ground-truth capture stage.
- Migrator consumes test generator output and incorporates `unit_tests:` blocks into model schema YAML.
- Test generator must not make or modify migration business decisions (classification, keys, materialization).

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

Reference schema: `../shared/shared/schemas/test_generator_input.json`

## Generation Strategy

### 1. FetchProcContext

- Read `proc_body` from DDL files via `discover show --ddl-path <ddl_path> --name <writer>` using `selected_writer`.
- Read `table_schemas` from DDL files via `discover show --ddl-path <ddl_path> --name <table>` for `item_id` and all join targets.

### 2. ExtractBranches

- For each statement in the resolved statements from `catalog/procedures/<writer>.json` where `action == migrate`, extract conditional branches from the SQL AST.
- sqlglot handles individual statement conditions (WHERE, CASE, MERGE clauses); LLM handles IF/ELSE/WHILE procedural control flow.
- Output: branch manifest — see [harness design](../unit-test-strategy/ground-truth-harness.md) for schema.

### 3. GenerateFixtures

- For each branch, generate minimum input rows (positive + negative case).
- Use profile `foreign_keys` from `catalog/tables/<item_id>.json` to build FK dependency graph; generate rows in topological order.
- Group correlated columns (date ranges, amount/currency) in a single LLM call.

### 4. CaptureGroundTruth

- Run dotnet-sqltest (testcontainers built-in): spins up ephemeral SQL Server container, deploys proc DDL + table schemas.
- For each scenario: `BEGIN TRANSACTION` → load fixture rows → `EXEC proc` → `SELECT * FROM output_table` → `ROLLBACK`.
- dotnet-sqltest emits Cobertura XML for statement coverage alongside proc execution.

### 5. ResolveCoverage

- Parse Cobertura XML → uncovered lines.
- Cross-reference against branch manifest `line_span` → `uncovered_branches[]`.
- If `uncovered_branches` is non-empty: re-prompt LLM with uncovered branches + existing scenarios → generate additional fixtures → repeat Stage 4. Maximum 3 iterations.

### 6. EmitFixtures

- Format `{input_rows, expected_rows}` per scenario as structured `unit_tests[]` JSON objects.
- Test name convention: `test_<load_pattern>_<scenario_description>`.
- Migrator renders `unit_tests[]` to `unit_tests:` YAML.

### 7. ValidateOutput

- Validate every `unit_tests:` block has at least one `given` row and one `expect` row.
- Set `coverage` field: `complete` when all branches covered, `partial` otherwise.
- Set item `status`: `ok | partial | error`.

## Output Schema (FixtureManifest)

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "ok|partial|error",
      "coverage": "complete|partial",
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
      ],
      "cobertura_xml_path": "artifacts/coverage/dbo.fact_sales.xml",
      "branch_manifest": {
        "branches": []
      },
      "uncovered_branches": [],
      "warnings": [],
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

Migrator renders `unit_tests[]` to `unit_tests:` YAML.

### unit_tests[] Entry Schema

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Test name — convention: `test_<load_pattern>_<scenario_description>` |
| `model` | string | yes | dbt model name from `plan.documentation.model_name` |
| `given` | object[] | yes | One entry per mocked input relation |
| `given[].input` | string | yes | dbt `ref(...)` or `source(...)` expression identifying the relation |
| `given[].rows` | object[] | yes | One or more fixture input rows as column→value maps |
| `expect.rows` | object[] | yes | Expected output rows as column→value maps (ground-truth captured from proc execution) |

## Coverage and Status Rules

| Condition | `coverage` | `status` |
|---|---|---|
| All branches covered | `complete` | `ok` |
| Max iterations reached, branches remain | `partial` | `partial` |
| Branch declared unreachable by LLM | `partial` (branch skipped) | `ok` |
| Proc execution or container failure | — | `error` |

Items with `coverage: partial` are flagged for FDE manual fixture authoring before migration sign-off.

## Validation Checklist

- `item_id` is present.
- `status` is one of: `ok|partial|error`.
- `coverage` is one of: `complete|partial`.
- Every `unit_tests[]` entry has a `name`, at least one `given` input with rows, and an `expect` with rows.
- `uncovered_branches` is empty when `coverage == "complete"`.
- `uncovered_branches` is non-empty when `coverage == "partial"` (includes unreachable branches).
- `cobertura_xml_path` is present when `status != "error"`.
- If `status == "partial"`, `validation.issues` is non-empty.
- If `status == "error"`, `errors` is non-empty.

## Test Generator Boundary

Test generator must not output:

- Generated dbt SQL model files
- YAML strings — `unit_tests[]` is structured JSON; migrator renders YAML
- Materialization or business key decisions

`validation.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in `docs/design/agent-contract/README.md`.
