# Skill: Generating Tests

## Purpose

Generates test scenarios for a stored procedure migration. Enumerates conditional branches from the procedure's logic, synthesizes minimal fixture data for each branch, and writes structured JSON to `test-specs/`. This skill produces scenarios only -- it does not execute stored procedures, capture ground truth, or generate dbt YAML. The `/generate-tests` command handles bulk execution and YAML conversion after the review loop completes.

## Invocation

```text
/generating-tests <schema.table>
```

Argument is the fully-qualified table name (the `item_id`). The writer procedure is read automatically from the catalog scoping section.

## Prerequisites

- `manifest.json` must exist in the project root. If missing, run [[Skill Setup DDL]] first.
- `catalog/tables/<item_id>.json` must exist. If missing, run `/listing-objects list tables` to see available tables.
- The table must have been scoped ([[Skill Scoping Table]]) and profiled ([[Skill Profiling Table]]) before test generation.

## Pipeline

### 1. Assemble context

```bash
uv run --project <shared-path> migrate context --table <item_id>
```

Output JSON contains:

| Field | Description |
|---|---|
| `profile` | Classification, keys, watermark, PII answers |
| `materialization` | Derived from profile (snapshot/table/incremental) |
| `statements` | Resolved statement list with action (migrate/skip) and SQL |
| `proc_body` | Full original procedure SQL |
| `columns` | Target table column list |
| `source_tables` | Tables read by the writer |
| `schema_tests` | Deterministic test specs (entity integrity, referential integrity, recency, PII) |

For each table in `source_tables`, the skill reads `catalog/tables/<schema>.<table>.json` to get full column metadata (`is_nullable`, `is_identity`, types, `auto_increment_columns`).

### 2. Extract branches

For each statement where `action == migrate`, the skill identifies all conditional branches:

| Pattern | Branches to enumerate |
|---|---|
| MERGE WHEN clauses | One per WHEN MATCHED, WHEN NOT MATCHED, WHEN NOT MATCHED BY SOURCE |
| CASE/WHEN | One per arm + ELSE |
| JOIN | Match, no-match (NULL right side for LEFT JOIN), partial multi-condition match |
| WHERE | Row that passes, row that fails |
| Subquery | EXISTS true/false, IN match/miss, correlated hit/miss |
| NULL handling | Nullable columns in filters/joins/COALESCE -- NULL vs non-NULL |
| Aggregation | Single group, multiple groups, empty group |
| Type boundaries | Watermark date edges, MAX int, empty string |
| Empty source | Zero-row edge case |

Output: a branch manifest with IDs, descriptions, statement indexes, and patterns.

### 3. Generate fixtures

For each branch, generates minimum synthetic input rows (1-3 per source table):

- Each scenario is self-contained -- no shared test data across scenarios
- FK-consistent within each scenario using catalog `foreign_keys` for topological ordering
- Type-appropriate values from catalog column metadata
- Parameters are ignored or flagged (rare in warehouse procs)

**Columns excluded from fixtures:**

- Computed columns (defined with `AS <expression>` in DDL)
- Identity columns not needed by the scenario (let SQL Server auto-generate)

**NOT NULL column coverage:** Every source table fixture row includes all columns where `is_nullable == false`, except computed and unneeded identity columns. Type-appropriate defaults are used for NOT NULL columns not referenced by the procedure:

| SQL Type Pattern | Default Value |
|---|---|
| INT, BIGINT, SMALLINT, TINYINT | `0` |
| NVARCHAR, VARCHAR, CHAR, NCHAR | `""` (empty string) |
| DATETIME, DATETIME2, DATE, SMALLDATETIME | `"1900-01-01"` |
| BIT | `0` |
| DECIMAL, NUMERIC, MONEY, SMALLMONEY | `0.00` |
| FLOAT, REAL | `0.0` |
| UNIQUEIDENTIFIER | `"00000000-0000-0000-0000-000000000000"` |
| VARBINARY, BINARY | `""` (empty string) |

**CHECK constraint compliance:** Fixture values satisfy any CHECK constraints found in the DDL (range, enum, cross-column).

### 4. Present for approval (interactive)

Shows branch manifest, generated fixtures, and any uncovered branches or warnings. The user approves, requests edits, or declines.

### 5. Write test spec

Writes `test-specs/<item_id>.json` with the TestSpec schema. The `expect` field is omitted -- ground truth is captured later by the command.

### 6. Validate output

- Every `unit_tests[]` entry has at least one `given` entry with rows
- Every `given[].rows` entry includes all NOT NULL non-identity columns
- `coverage` set: `complete` when all branches have scenarios, `partial` otherwise
- `status` set: `ok`, `partial`, or `error`

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<item_id>.json` | Target table columns, scoping, profile |
| `catalog/tables/<source>.json` | Source table columns for NOT NULL coverage |
| `catalog/procedures/<writer>.json` | Writer procedure statements and refs |

## Writes

### `test-specs/<item_id>.json`

| Field | Type | Required | Description |
|---|---|---|---|
| `item_id` | string | yes | Fully qualified table name |
| `status` | string | yes | Enum: `ok`, `partial`, `error` |
| `coverage` | string | yes | Enum: `complete`, `partial` |
| `branch_manifest` | array | yes | All identified branches |
| `unit_tests` | array | yes | Test scenarios with fixtures |
| `uncovered_branches` | string[] | yes | Branch IDs that lack scenarios |
| `warnings` | array | yes | Diagnostics entries |
| `validation` | object | yes | `passed` (boolean) and `issues[]` |
| `errors` | array | yes | Diagnostics entries |

### `branch_manifest[]` entry

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | yes | Stable branch identifier (e.g., `merge_matched_update`) |
| `statement_index` | integer | yes | Index into the resolved statements array (0-based) |
| `description` | string | yes | Human-readable branch description |
| `scenarios` | string[] | yes | Test names that exercise this branch |

### `unit_tests[]` entry

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Test name: `test_<load_pattern>_<scenario_description>` |
| `target_table` | string | yes | Bracket-quoted target table identifier, e.g., `[silver].[DimProduct]` |
| `procedure` | string | yes | Bracket-quoted stored procedure identifier, e.g., `[silver].[usp_load_DimProduct]` |
| `model` | string | no | dbt model name -- optional; used during dbt YAML conversion |
| `given` | array | yes | One entry per fixture source table (minItems: 1) |
| `expect` | object | no | Ground truth output rows -- added by command after execution |

### `given[]` entry

| Field | Type | Required | Description |
|---|---|---|---|
| `table` | string | yes | Bracket-quoted SQL identifier, e.g., `[bronze].[SalesOrderHeader]` |
| `rows` | array | yes | Synthetic fixture input rows as column-value maps (minItems: 1) |

### `expect` entry (added post-execution)

| Field | Type | Required | Description |
|---|---|---|---|
| `rows` | array | yes | Ground truth output rows captured from proc execution |

### Naming conventions

- Test name: `test_<load_pattern>_<scenario_description>`
- `target_table`: bracket-quoted FQN, e.g., `[silver].[DimProduct]`
- `procedure`: bracket-quoted FQN, e.g., `[silver].[usp_load_DimProduct]`
- `given[].table`: bracket-quoted SQL identifier, e.g., `[bronze].[SalesOrderHeader]`

### Coverage and status rules

| Condition | `coverage` | `status` |
|---|---|---|
| All branches have scenarios | `complete` | `ok` |
| Branches remain after review loop | `partial` | `partial` |
| Generation failed | -- | `error` |

### Diagnostics schema

`validation.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema:

| Field | Type | Required | Description |
|---|---|---|---|
| `code` | string | yes | Stable machine-readable identifier (e.g., `SCOPING_NOT_COMPLETED`) |
| `message` | string | yes | Human-readable description |
| `item_id` | string | no | Fully qualified table name |
| `field` | string | no | Optional field path associated with the issue |
| `severity` | string | yes | Enum: `error`, `warning` |
| `details` | object | no | Optional structured context |

## JSON Format

### Test spec example

```json
{
  "item_id": "silver.dimproduct",
  "status": "ok",
  "coverage": "complete",
  "branch_manifest": [
    {
      "id": "merge_matched_update",
      "statement_index": 0,
      "description": "MERGE WHEN MATCHED -> UPDATE existing product",
      "scenarios": ["test_merge_matched_existing_product_updated"]
    },
    {
      "id": "merge_not_matched_insert",
      "statement_index": 0,
      "description": "MERGE WHEN NOT MATCHED -> INSERT new product",
      "scenarios": ["test_merge_not_matched_new_product_inserted"]
    }
  ],
  "unit_tests": [
    {
      "name": "test_merge_matched_existing_product_updated",
      "target_table": "[silver].[DimProduct]",
      "procedure": "[silver].[usp_load_DimProduct]",
      "given": [
        {
          "table": "[bronze].[Product]",
          "rows": [
            { "ProductID": 1, "Name": "Widget", "ListPrice": 99.99, "ModifiedDate": "2024-01-15", "rowguid": "A0000000-0000-0000-0000-000000000001" }
          ]
        },
        {
          "table": "[silver].[DimProduct]",
          "rows": [
            { "ProductKey": 1, "ProductName": "Old Widget", "ListPrice": 50.00 }
          ]
        }
      ]
    },
    {
      "name": "test_merge_not_matched_new_product_inserted",
      "target_table": "[silver].[DimProduct]",
      "procedure": "[silver].[usp_load_DimProduct]",
      "given": [
        {
          "table": "[bronze].[Product]",
          "rows": [
            { "ProductID": 999, "Name": "New Widget", "ListPrice": 29.99, "ModifiedDate": "2024-06-01", "rowguid": "B0000000-0000-0000-0000-000000000001" }
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

## Handling Reviewer Feedback

When invoked with a `feedback_for_generator` JSON block (from [[Skill Reviewing Tests]]):

- **`uncovered_branches`**: list of branch IDs missing coverage. New scenarios are generated for each listed branch and added to `unit_tests[]`.
- **`quality_fixes`**: per-scenario remediation instructions. Named scenarios are revised in `unit_tests[]` as directed.

Previously approved scenarios are preserved -- only additions and revisions are applied.

## Boundary Rules

The test generator must not:

- Execute stored procedures or access the sandbox
- Generate dbt SQL model files
- Render YAML -- `unit_tests[]` is structured JSON; dbt YAML conversion happens post-execution
- Make materialization or business key decisions
- Score its own coverage authoritatively -- [[Skill Reviewing Tests]] does that

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `migrate context` exit code 1 | No profile or no statements in catalog | Run [[Skill Scoping Table]] and [[Skill Profiling Table]] first |
| `migrate context` exit code 2 | IO/parse error reading catalog | Check file permissions and JSON validity in `catalog/` |
| Empty branch manifest | Procedure has no conditional logic (single straight-through INSERT) | This is valid -- generate a single scenario for the base case |
| NOT NULL violation in fixtures | Fixture row missing a required non-nullable column | Ensure all NOT NULL non-identity columns are included with type-appropriate defaults |
