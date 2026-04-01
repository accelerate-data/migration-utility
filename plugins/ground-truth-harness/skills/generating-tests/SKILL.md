---
name: generating-tests
description: >
  Generates test scenarios for a stored procedure migration. Enumerates
  conditional branches and synthesizes minimal fixtures. Does not execute
  procs or capture ground truth — that is done by the /generate-tests
  command after review approval.
user-invocable: true
argument-hint: "<schema.table>"
---

# Generating Tests

Generate test scenarios for a stored procedure migration. Reads deterministic context from catalog, uses LLM to enumerate conditional branches and synthesize minimal fixtures, and writes structured JSON to `test-specs/`.

This skill produces scenarios only — no proc execution, no ground truth capture. The `/generate-tests` command bulk-executes approved scenarios after the review loop completes.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing.

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, tell the user to run `setup-ddl` first.
2. Confirm `catalog/tables/<table>.json` exists. If missing, tell the user to run `/listing-objects list tables` to see available tables and stop.

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

## Step 4: Present for approval

Show the user:

1. Branch manifest (all identified branches with descriptions)
2. Generated fixtures (inputs per scenario)
3. Any uncovered branches or warnings

Ask the user: "Approve these test scenarios? (y/n/edit)". If the user requests edits, apply them and re-present.

## Step 5: Write test spec

Write `test-specs/<item_id>.json` with the TestSpec schema. The `expect` field is omitted — ground truth is captured later by the command after review approval.

Naming conventions:

- Test name: `test_<load_pattern>_<scenario_description>`
- Model name is deterministic from table FQN: `silver.dimproduct` → `stg_dimproduct`
- `given[].input` uses `source()` for bronze tables and `ref()` for silver/gold tables

## Step 6: Validate output

- Every `unit_tests[]` entry has at least one `given` input with rows.
- Set `coverage` field: `complete` when all branches have scenarios, `partial` otherwise.
- Set `status`: `ok | partial | error`.

## Output Schema (TestSpec)

Written to `test-specs/<item_id>.json` (before ground truth capture):

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
      ]
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

The `/generate-tests` command adds `expect.rows` to each `unit_tests[]` entry after bulk-executing scenarios in the sandbox.

### unit_tests[] Entry Schema

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Test name — convention: `test_<load_pattern>_<scenario_description>` |
| `model` | string | yes | dbt model name, deterministic from table FQN |
| `given` | object[] | yes | One entry per mocked input relation |
| `given[].input` | string | yes | dbt `ref(...)` or `source(...)` expression |
| `given[].rows` | object[] | yes | Synthetic fixture input rows as column->value maps |
| `expect.rows` | object[] | no | Ground truth output rows — added by command after execution |

## Coverage and Status Rules

| Condition | `coverage` | `status` |
|---|---|---|
| All branches have scenarios | `complete` | `ok` |
| Branches remain after review loop | `partial` | `partial` |

## Boundary Rules

Test generator must not:

- Execute stored procedures or access the sandbox
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

| Command | Exit code | Action |
|---|---|---|
| `migrate context` | 1 | No profile or no statements. Tell user to run scoping and profiling first |
| `migrate context` | 2 | IO/parse error. Surface the error message |
