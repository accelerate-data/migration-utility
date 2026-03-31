# Test Generator Agent Contract

The test generator agent produces branch-covering dbt unit test fixtures for a stored procedure migration. It analyzes the proc's conditional branches, synthesizes minimal synthetic input data that exercises each branch, executes the proc in a sandbox database via MCP to capture ground truth output, and emits `unit_tests:` JSON blocks to `test-specs/`.

Test generation runs BEFORE migration. The test spec is an independent artifact that the migrator consumes — the test generator never sees the generated dbt model.

This contract covers both paths:

- **Batch (agent):** `plugins/ground-truth-harness/agents/test-generator/` — runs in GHA pipeline, JSON in/out, no approval gates.
- **Interactive (skill):** `plugins/ground-truth-harness/skills/test-gen/SKILL.md` — user-invocable via `/test-gen`, presents results for approval.

Both paths share the Python CLI (`lib/shared/test_harness.py`) for deterministic work (sandbox lifecycle, SQL execution, result capture). LLM reasoning (branch analysis, fixture synthesis) is replicated with path-appropriate prompting.

## Philosophy and Boundary

- Test generator owns branch analysis, fixture synthesis, ground truth capture, and coverage self-assessment.
- Test generator reads the same context as the migrator: profile from `catalog/tables/<item_id>.json`, resolved statements from `catalog/procedures/<writer>.json`, proc body and columns from DDL files.
- Test generator does NOT score its own coverage — the test reviewer independently enumerates branches and scores.
- Test generator writes structured JSON to `test-specs/<item_id>.json`. It does not write dbt YAML files.
- Test generator must not make or modify migration business decisions (classification, keys, materialization).

## Required Input

```json
{
  "schema_version": "2.0",
  "run_id": "uuid",
  "items": [
    {
      "item_id": "silver.dimproduct",
      "selected_writer": "dbo.usp_load_dimproduct"
    }
  ]
}
```

Project root is inferred from CWD. Reference schema: `../lib/shared/schemas/test_generator_input.json`

## Prerequisites

The sandbox database must exist before the agent runs. Created via the `sandbox-up` command:

```bash
uv run --project lib test-harness sandbox-up --run-id <uuid>
```

If the sandbox does not exist, the agent fails with a clear error directing the user to run `sandbox-up`.

## Generation Strategy

### 1. AssembleContext (Deterministic — `migrate.py context`)

Run `uv run migrate context --table <item_id> --writer <selected_writer>`.

Same context the migrator receives: profile, materialization, statements, proc body, columns, source tables, schema tests. The test generator uses this to understand what the proc does — not to generate dbt.

### 2. ExtractBranches (LLM)

For each statement where `action == migrate`, identify all conditional branches:

| Pattern | Branches to enumerate |
|---|---|
| MERGE WHEN clauses | One per WHEN MATCHED, WHEN NOT MATCHED, WHEN NOT MATCHED BY SOURCE |
| CASE/WHEN | One per arm + ELSE |
| JOIN | Match, no-match (NULL right side for LEFT JOIN), partial multi-condition match |
| WHERE | Row that passes, row that fails |
| Subquery | EXISTS true/false, IN match/miss, correlated hit/miss |
| NULL handling | Nullable columns in filters/joins/COALESCE — NULL vs non-NULL |
| Aggregation | Single group, multiple groups, empty group |
| Type boundaries | Watermark date edges, MAX int, empty string |
| Empty source | Zero-row edge case |

Output: branch manifest — a list of branches with descriptions and the statement they belong to.

### 3. GenerateFixtures (LLM)

For each branch, generate minimum synthetic input rows (1-3 per source table):

- Each scenario is self-contained — no shared test data across scenarios.
- FK-consistent within each scenario: use the catalog's `foreign_keys` to build a dependency graph and generate rows in topological order so FK values align.
- Use column types from catalog to generate type-appropriate values.
- Parameters are ignored or flagged — rare in warehouse procs, typically orchestration concerns.

### 4. CaptureGroundTruth (Deterministic — `test_harness.py execute`)

For each scenario, execute against the sandbox database via MCP:

1. Insert synthetic fixture rows into source tables in the sandbox.
2. `EXEC` the proc.
3. `SELECT *` from the target table — capture output as ground truth.
4. Clean up inserted rows (TRUNCATE or DELETE) before the next scenario.

The sandbox contains the same schemas and table structures as production but in an isolated throwaway database (`__test_<run_id>`). The proc runs unmodified — schema references resolve naturally within the sandbox namespace.

### 5. SelfIterateOnGaps (LLM)

After capturing ground truth for all scenarios:

- Review the branch manifest against generated scenarios.
- If any branches lack a scenario, generate additional fixtures and re-run capture.
- Maximum 3 iterations. Remaining gaps are reported but not blocking.

Note: this is the generator's own internal loop. The test reviewer performs the authoritative coverage scoring independently.

### 6. EmitFixtures

Write `test-specs/<item_id>.json` with the structured output (see Output Schema below).

- Test name convention: `test_<load_pattern>_<scenario_description>`.
- Model name is deterministic from table FQN: `silver.dimproduct` → `stg_dimproduct`.
- `given[].input` uses dbt `source()` for bronze tables and `ref()` for silver/gold tables.

### 7. ValidateOutput

- Every `unit_tests[]` entry has at least one `given` input with rows and one `expect` with rows.
- Set `coverage` field: `complete` when all branches have scenarios, `partial` otherwise.
- Set item `status`: `ok | partial | error`.

## Output Schema (TestSpec)

Written to `test-specs/<item_id>.json`.

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "silver.dimproduct",
      "status": "ok|partial|error",
      "coverage": "complete|partial",
      "branch_manifest": [
        {
          "id": "merge_matched_update",
          "statement_index": 0,
          "description": "MERGE WHEN MATCHED → UPDATE existing product",
          "scenarios": ["test_merge_matched_existing_product_updated"]
        }
      ],
      "unit_tests": [
        {
          "name": "test_merge_matched_existing_product_updated",
          "model": "stg_dimproduct",
          "given": [
            {
              "input": "source('bronze', 'product')",
              "rows": [
                { "product_id": 1, "product_name": "Widget", "list_price": 99.99 }
              ]
            },
            {
              "input": "ref('stg_dimproduct')",
              "rows": [
                { "product_key": 1, "product_name": "Old Widget", "list_price": 50.00 }
              ]
            }
          ],
          "expect": {
            "rows": [
              { "product_key": 1, "product_name": "Widget", "list_price": 99.99 }
            ]
          }
        }
      ],
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

### unit_tests[] Entry Schema

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Test name — convention: `test_<load_pattern>_<scenario_description>` |
| `model` | string | yes | dbt model name, deterministic from table FQN |
| `given` | object[] | yes | One entry per mocked input relation |
| `given[].input` | string | yes | dbt `ref(...)` or `source(...)` expression |
| `given[].rows` | object[] | yes | Synthetic fixture input rows as column→value maps |
| `expect.rows` | object[] | yes | Ground truth output rows captured from proc execution |

## Coverage and Status Rules

| Condition | `coverage` | `status` |
|---|---|---|
| All branches have scenarios | `complete` | `ok` |
| Max iterations reached, branches remain | `partial` | `partial` |
| Proc execution or sandbox failure | — | `error` |

Items with `coverage: partial` proceed to the test reviewer, which may kick back with specific missing branches.

## Test Generator Boundary

Test generator must not:

- Generate dbt SQL model files
- Render YAML — `unit_tests[]` is structured JSON; the migrator renders YAML
- Make materialization or business key decisions
- Score its own coverage authoritatively — the test reviewer does that

`validation.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in `docs/design/agent-contract/README.md`.
