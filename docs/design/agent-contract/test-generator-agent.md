# Test Generator Agent Contract

The test generator agent produces branch-covering dbt unit test fixtures for a stored procedure migration. It extracts all conditional branches from the proc, generates synthetic input data that exercises each branch, captures the actual proc output as ground truth, and emits `unit_tests:` YAML blocks for the migrator to incorporate into the model schema file.

See [docs/design/unit-test-strategy/](../unit-test-strategy/) for design rationale, tooling decisions, and the full harness design including the branch manifest schema.

## Philosophy and Boundary

- Test generator owns fixture generation and ground-truth capture.
- `proc_body` and `table_schemas` are tool-fetched at runtime from `sys.sql_modules` and `sys.columns`. They are not passed in the input.
- Migrator consumes test generator output and incorporates `unit_tests:` blocks into model schema YAML.
- Test generator must not make or modify migration business decisions (classification, keys, materialization).

## Required Input

The test generator receives the planner output unchanged.

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
        "pii_actions": []
      },
      "decomposition": {
        "segmented_logical_blocks": [
          {
            "block_id": "01_extract_sales_stage",
            "purpose": "Load source rows and apply base filters.",
            "source_sql_ref": {
              "statement_indices": [0],
              "line_span": {"start": 12, "end": 37}
            }
          }
        ],
        "candidate_model_split_points": []
      },
      "plan": {
        "materialization": "incremental",
        "documentation": {
          "model_name": "fct_fact_sales"
        }
      }
    }
  ]
}
```

## Generation Strategy

### 1. FetchProcContext

- Fetch `proc_body` from `sys.sql_modules` using `answers.writer`.
- Fetch `table_schemas` from `sys.columns` / `sys.types` for `item_id` and all join targets.

### 2. ExtractBranches

- For each block in `decomposition.segmented_logical_blocks`, prompt the LLM to extract all conditional branches within the block's `source_sql_ref.line_span`.
- sqlglot handles individual statement conditions (WHERE, CASE, MERGE clauses); LLM handles IF/ELSE/WHILE procedural control flow.
- Output: branch manifest — see [harness design](../unit-test-strategy/ground-truth-harness.md) for schema.

### 3. GenerateFixtures

- For each branch, generate minimum input rows (positive + negative case).
- Use `answers.foreign_keys` to build FK dependency graph; generate rows in topological order.
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

- Validate all `split_after_block_id` references exist in `decomposition.segmented_logical_blocks`.
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
