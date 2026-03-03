# Ground-Truth Capture Harness

Generates branch-covering dbt unit test fixtures for a stored procedure migration. Runs entirely from the Python orchestrator. Outputs `unit_tests:` YAML blocks and a Cobertura coverage report.

## Input

The harness input is the planner output — no extra fields. `proc_body` and `table_schemas` are fetched at runtime via MCP tool calls (`sys.sql_modules`, `sys.columns`) using `answers.writer` and `item_id`. Passing SQL text through the pipeline would duplicate what the MCP can retrieve directly.

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
          },
          {
            "block_id": "02_enrich_customer_product",
            "purpose": "Resolve dimension surrogate keys.",
            "source_sql_ref": {
              "statement_indices": [1],
              "line_span": {"start": 38, "end": 79}
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

| Field | Source |
|---|---|
| `item_id` | Target table — doubles as `output_table` for proc capture |
| `answers.writer` | Scoping agent — used to fetch `proc_body` from `sys.sql_modules` at runtime |
| `answers.foreign_keys` | Profiler agent — used as `fk_map` for Stage 2 topological ordering |
| `decomposition` | Decomposer agent, carried unchanged through planner — provides block `line_span` |
| `plan.documentation.model_name` | Planner — used as the dbt model name in emitted `unit_tests:` YAML |
| `proc_body` | **Tool-fetched** from `sys.sql_modules` using `answers.writer` |
| `table_schemas` | **Tool-fetched** from `sys.columns` / `sys.types` using `item_id` and join targets |

## Design

```text
orchestrator (Python)
  │
  ├─ Stage 1: LLM branch extraction
  │    · input: decomposer segmented_logical_blocks + proc_body
  │    · for each block (line_span already known from decomposer):
  │      extract conditions within the block — IF/ELSE, CASE WHEN,
  │      MERGE arms, JOIN types, NULL paths, ROW_NUMBER() ties
  │    · output: branch manifest JSON (inherits line_span from blocks)
  │
  ├─ Stage 2: LLM fixture generation
  │    · input: branch manifest + FK map (from profiler agent)
  │    · generate: minimum input rows per branch (positive + negative)
  │    · FK order: topological sort — parent rows before child rows
  │    · output: {table: [rows]} per scenario
  │
  └─ Stage 3+4: Ground-Truth Harness
       │
       ├─ dotnet-sqltest (testcontainers built-in):
       │    · spins up ephemeral SQL Server container
       │    · deploys proc DDL + table schemas
       │    · for each scenario:
       │        BEGIN TRANSACTION
       │        INSERT fixture rows into source tables
       │        EXEC proc
       │        SELECT * FROM output table → expected rows  (ground truth)
       │        ROLLBACK
       │    · emits Cobertura XML (statement coverage captured during execution)
       │
       ├─ Coverage Resolver:
       │    parse Cobertura uncovered lines
       │    cross-ref against branch manifest line_span
       │    → uncovered_branches[]
       │
       ├─ if uncovered_branches not empty (max 3 iterations):
       │    LLM gap-fill prompt:
       │      · uncovered_branches[] + existing scenarios (no duplicates)
       │      · output: new_scenarios[] (additive)
       │    → loop back with all_scenarios = existing + new
       │
       └─ Stage 4: JSON serializer
            {input_rows, expected_rows} → unit_tests[] JSON (FixtureManifest)
            returned to orchestrator; migrator renders unit_tests: YAML
```

## Branch Manifest Schema

Output of Stage 1. Each branch carries a `line_span` so the Coverage Resolver can map Cobertura uncovered lines back to manifest entries.

```json
{
  "schema_version": "1.0",
  "proc_name": "dbo.usp_load_fact_sales",
  "branches": [
    {
      "id": "merge_when_matched_update",
      "block_id": "02_enrich_customer_product",
      "branch_type": "merge_arm",
      "description": "WHEN MATCHED arm — update existing row when staging key matches target",
      "positive_condition": "staging row customer_id exists in target table",
      "negative_condition": "staging row customer_id has no match in target table",
      "line_span": { "start": 43, "end": 51 },
      "tables_needed": ["dbo.staging_customer", "dbo.dim_customer"],
      "fk_deps": [
        { "child_table": "dbo.staging_customer", "parent_table": "dbo.dim_customer", "on": "customer_id" }
      ]
    },
    {
      "id": "merge_when_not_matched_insert",
      "block_id": "02_enrich_customer_product",
      "branch_type": "merge_arm",
      "description": "WHEN NOT MATCHED arm — insert new row when staging key absent from target",
      "positive_condition": "staging row customer_id has no match in target table",
      "negative_condition": "staging row customer_id exists in target table",
      "line_span": { "start": 52, "end": 58 },
      "tables_needed": ["dbo.staging_customer", "dbo.dim_customer"],
      "fk_deps": []
    },
    {
      "id": "null_email_filter",
      "branch_type": "where_predicate",
      "description": "WHERE email IS NOT NULL filters out rows with missing email",
      "positive_condition": "row has non-null email — passes filter",
      "negative_condition": "row has null email — excluded from output",
      "line_span": { "start": 67, "end": 67 },
      "tables_needed": ["dbo.staging_customer"],
      "fk_deps": []
    }
  ]
}
```

### Field Reference

| Field | Type | Description |
|---|---|---|
| `id` | string | Stable identifier used in gap-fill prompt and coverage output |
| `block_id` | string | References `segmented_logical_blocks[*].block_id` from decomposer output |
| `branch_type` | string | One of: `if_else`, `case_when`, `merge_arm`, `where_predicate`, `join_type`, `null_check`, `row_number_tie`, `scd2_path` |
| `description` | string | What the branch does |
| `positive_condition` | string | Data condition that triggers this path |
| `negative_condition` | string | Data condition that bypasses this path |
| `line_span` | object | `{ "start": int, "end": int }` — lines in proc body, used for Cobertura cross-ref |
| `tables_needed` | string[] | Source tables that need fixture rows for this branch |
| `fk_deps` | object[] | FK relationships to respect when generating fixture rows; parent rows must be inserted first |

## Termination

| Condition | `coverage` | `status` |
|---|---|---|
| `uncovered_branches` is empty | `complete` | `ok` |
| Max 3 iterations reached | `partial` | `partial` |
| LLM returns no scenarios for a branch | `partial` | `ok` |

Items with `coverage: partial` are flagged for FDE manual review before sign-off.

## Outputs

- FixtureManifest: structured `unit_tests[]` JSON consumed by the migrator, which renders `unit_tests:` YAML
- Cobertura XML per proc stored alongside the migration artifact
- `coverage: complete | partial` and `status: ok | partial | error` per item
