---
name: generating-tests
description: >
  Generates ground truth test fixtures for a stored procedure migration.
  Invoke when the user asks to "generate tests", "create test spec",
  "capture ground truth", or "test <table>". Requires catalog scoping
  and profile from prior stages. Sandbox must be running.
user-invocable: true
argument-hint: "<schema.table>"
---

# Generating Tests

Generate ground truth test fixtures for a stored procedure migration. Reads deterministic context from catalog, uses LLM to enumerate conditional branches and synthesize fixtures, executes the proc in a sandbox database to capture ground truth output, and writes structured JSON to `test-specs/`.

Test generation runs BEFORE migration. The test spec is an independent artifact that the model-generator consumes — the test generator never sees the generated dbt model.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing.

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, tell the user to run `setup-ddl` first.
2. Confirm `catalog/tables/<table>.json` exists. If missing, tell the user to run `/listing-objects list tables` to see available tables and stop.
3. Check the sandbox exists:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness sandbox-status
```

If the sandbox is not found (exit code 1), stop and tell the user to run `/setup-sandbox` first.

## Step 1: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" migrate context \
  --table <item_id>
```

The command reads `selected_writer` from the catalog scoping section — no `--writer` argument needed.

Read the output JSON. It contains:

- `profile` — classification, keys, watermark, PII answers
- `materialization` — derived from profile (snapshot/table/incremental)
- `statements` — resolved statement list with action (migrate/skip) and SQL
- `proc_body` — full original procedure SQL
- `columns` — target table column list
- `source_tables` — tables read by the writer
- `schema_tests` — deterministic test specs (entity integrity, referential integrity, recency, PII)

## Step 2: Extract branches

For each statement where `action == migrate`, identify all conditional branches. Use the proc body and statement SQL to enumerate every code path that produces different output behavior.

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

Output: branch manifest — a list of branches with IDs, descriptions, the statement index they belong to, and the pattern they exercise.

## Step 3: Generate fixtures

For each branch, generate minimum synthetic input rows (1-3 per source table):

- Each scenario is self-contained — no shared test data across scenarios.
- FK-consistent within each scenario: use the catalog's `foreign_keys` to build a dependency graph and generate rows in topological order so FK values align.
- Use column types from catalog to generate type-appropriate values.
- Parameters are ignored or flagged — rare in warehouse procs, typically orchestration concerns.

## Step 4: Capture ground truth

For each scenario, execute against the sandbox database:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness execute \
  --run-id <uuid> \
  --scenario <json_file>
```

The execution flow per scenario:

1. Insert synthetic fixture rows into source tables in the sandbox.
2. `EXEC` the proc.
3. `SELECT *` from the target table — capture output as ground truth.
4. Clean up inserted rows (TRUNCATE or DELETE) before the next scenario.

If a scenario execution fails (exit code 1), record the error in the test spec and continue to the next scenario. Do not abort the entire run.

## Step 5: Self-iterate on gaps

After capturing ground truth for all scenarios:

- Review the branch manifest against generated scenarios.
- If any branches lack a scenario, generate additional fixtures and re-run capture for those new scenarios.
- Maximum 3 iterations. Remaining gaps are reported in `uncovered_branches` but are not blocking.

This is the generator's own internal loop; the test reviewer performs authoritative coverage scoring independently.

## Step 6: Present for approval

Show the user:

1. Branch manifest (all identified branches with descriptions)
2. Generated fixtures (inputs per scenario)
3. Captured ground truth (expected outputs per scenario)
4. Any uncovered branches or warnings

Ask the user: "Approve this test spec? (y/n/edit)"

If the user requests edits, apply them and re-capture ground truth for affected scenarios only. Then present the updated spec for re-approval.

## Step 7: Emit fixtures

After approval, write `test-specs/<item_id>.json` with the TestSpec schema.

Naming conventions:

- Test name: `test_<load_pattern>_<scenario_description>`
- Model name is deterministic from table FQN: `silver.dimproduct` → `stg_dimproduct`
- `given[].input` uses `source()` for bronze tables and `ref()` for silver/gold tables

## Step 8: Validate output

- Every `unit_tests[]` entry has at least one `given` input with rows and one `expect` with rows.
- Set `coverage` field: `complete` when all branches have scenarios, `partial` otherwise.
- Set `status`: `ok | partial | error`.

## Output Schema (TestSpec)

Written to `test-specs/<item_id>.json`:

```json
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
```

### unit_tests[] Entry Schema

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Test name — convention: `test_<load_pattern>_<scenario_description>` |
| `model` | string | yes | dbt model name, deterministic from table FQN |
| `given` | object[] | yes | One entry per mocked input relation |
| `given[].input` | string | yes | dbt `ref(...)` or `source(...)` expression |
| `given[].rows` | object[] | yes | Synthetic fixture input rows as column->value maps |
| `expect.rows` | object[] | yes | Ground truth output rows captured from proc execution |

## Coverage and Status Rules

| Condition | `coverage` | `status` |
|---|---|---|
| All branches have scenarios | `complete` | `ok` |
| Max iterations reached, branches remain | `partial` | `partial` |
| Proc execution or sandbox failure | — | `error` |

Items with `coverage: partial` proceed to the test reviewer, which may kick back with specific missing branches.

## Boundary Rules

Test generator must not:

- Generate dbt SQL model files
- Render YAML — `unit_tests[]` is structured JSON; the model-generator renders YAML
- Make materialization or business key decisions
- Score its own coverage authoritatively — the test reviewer does that

## Diagnostics

`validation.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema:

```json
{
  "code": "SCOPING_NOT_COMPLETED",
  "message": "scoping section missing or no selected_writer in catalog for silver.dimcustomer.",
  "item_id": "silver.dimcustomer",
  "severity": "error",
  "details": {}
}
```

Field requirements:

- `code`: stable machine-readable identifier.
- `message`: human-readable description.
- `item_id`: fully qualified table name this entry relates to.
- `field`: optional field path associated with the issue (empty or omitted for non-field errors).
- `severity`: `error` or `warning`.
- `details`: optional structured context object.

## Error handling

| Condition | Action |
|---|---|
| `migrate context` exits 1 | No profile or no statements. Tell user to run scoping and profiling first |
| `migrate context` exits 2 | IO/parse error. Surface the error message |
| `test-harness execute` exits 1 | Scenario execution failure. Record error in test spec, continue to next scenario |
| `test-harness sandbox-status` exits 1 | Sandbox not found. Tell user to run `/setup-sandbox` |
