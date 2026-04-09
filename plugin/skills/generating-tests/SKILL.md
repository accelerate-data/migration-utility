---
name: generating-tests
description: >
  Generates test scenarios for a stored procedure or view migration. Enumerates
  conditional branches and synthesizes minimal fixtures. Does not execute
  procs or capture ground truth — that is done by the /generate-tests
  command after review approval.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Generating Tests

Generate test scenarios for a stored procedure or view migration. Reads deterministic context from catalog, enumerates conditional branches, synthesizes minimal fixtures, and writes structured JSON to `test-specs/`.

## Arguments

`$ARGUMENTS` is the fully-qualified table or view name. Ask the user if missing.

## Contracts

Use the canonical test spec schema when writing `test-specs/<item_id>.json`:

- test spec: `../../lib/shared/schemas/test_spec.json`

Do not invent fields. If `test-harness write` rejects the payload, fix the payload and retry.

## Schema discipline

Use the canonical generating-tests surfaced code list in `../../lib/shared/generate_tests_error_codes.md`. Do not define a competing public error-code list in this skill.

## Load existing spec

Before running the stage guard, check whether `test-specs/<item_id>.json` already exists.

If it exists:

- Read the file and extract: `unit_tests[].name` list, `branch_manifest`, and any `expect` blocks keyed by scenario name.
- Set **merge_mode = true**.

If it does not exist:

- Set **merge_mode = false**. All steps below run identically to the first-run behavior.

## Before invoking

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> test-gen
```

If `passed` is `false`, report the failing check's `code` and `message` to the user and stop.

## Object type detection

Check whether `catalog/views/<fqn>.json` exists:

- **If yes** → object is a **view or MV**. Note `object_type = view` for the steps below.
- **If no** → object is a **table**. Note `object_type = table` for the steps below.

---

## Step 1: Assemble context

**For tables:**

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <item_id>
```

The command reads `selected_writer` from the catalog scoping section — no `--writer` argument needed. The output contains:

- `profile` — classification, keys, watermark, PII answers
- `materialization` — derived from profile (snapshot/table/incremental)
- `statements` — resolved statement list with action (migrate/skip) and SQL
- `proc_body` — full original procedure DDL
- `columns` — target table column list
- `source_tables` — tables read by the writer

Record the `selected_writer` procedure name from the catalog's `scoping` section — this becomes the `procedure` field in the test spec.

**For views:**

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <view_fqn>
```

This returns `raw_ddl`, `refs`, `sql_elements`, and `errors`. Also read `catalog/views/<fqn>.json` directly to get `profile`, `scoping.logic_summary`, and `references.tables.in_scope`.

Assemble the context:

- `profile` — from view catalog `profile` section
- `statements` — the view's SELECT body from `raw_ddl` (treat as a single `action: migrate` statement)
- `proc_body` — `raw_ddl` from `discover show`
- `source_tables` — from `references.tables.in_scope` in the view catalog
- If `errors` contains `DDL_PARSE_ERROR`, note it and proceed using `raw_ddl` directly for branch extraction

There is no `selected_writer` for views — the view's refactored SELECT statement becomes the `sql` field in the test spec.

**Source table catalog lookup (both):** For each table in `source_tables`, read `catalog/tables/<schema>.<table>.json` to get the full column list with `is_nullable`, `is_identity`, and type metadata. Also read `auto_increment_columns` to identify identity columns.

## Step 2: Extract branches

For each statement where `action == migrate`, identify all conditional branches. Use the proc body and statement SQL to enumerate every code path that produces different output behavior.

**For tables**, enumerate all patterns:

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

**For views**, use SELECT-level patterns only:

| Pattern | Branches to enumerate |
|---|---|
| WHERE | Row that passes, row that fails |
| JOIN | Match, no-match (NULL right side for LEFT JOIN), partial multi-condition match |
| CASE/WHEN | One per arm + ELSE |
| Subquery | EXISTS true/false, IN match/miss, correlated hit/miss |
| NULL handling | Nullable columns in filters/joins/COALESCE — NULL vs non-NULL |
| Aggregation | Single group, multiple groups, empty group (with/without HAVING) |
| Empty source | Zero-row edge case |

Output: branch manifest — a list of branches with IDs, descriptions, the statement index they belong to, and the pattern they exercise.

If **merge_mode**, compare the re-extracted branch IDs against the `branch_manifest` stored in the existing spec. For any branch IDs present in the stored manifest but absent from the re-extracted manifest, add a `STALE_BRANCH` warning (see `../../lib/shared/generate_tests_error_codes.md`).

## Step 2.5: Coverage gate (merge_mode only)

Skip this step if merge_mode is false.

Pass to the LLM:

- The full re-extracted branch manifest (all branch IDs and descriptions).
- The existing `unit_tests[]` list with each scenario's `name` and `branch_id` (or inferred branch mapping from the stored `branch_manifest[].scenarios` lists).

Ask: "For each branch in the manifest, is there an existing scenario that exercises it? Return the list of branch IDs that have no covering scenario."

- **No uncovered branches**: skip Step 3 entirely. Proceed to Step 4 carrying `new_scenarios = []`.
- **Uncovered branches found**: proceed to Step 3 scoped to those branches only. Carry `uncovered_branch_ids` into Step 3.

## Step 3: Generate fixtures

When **merge_mode**, generate fixtures only for branches in `uncovered_branch_ids` (from Step 2.5). All other branches already have coverage — do not regenerate scenarios for them.

When **not merge_mode** (first run), generate for all branches.

For each targeted branch, generate minimum synthetic input rows (1-3 per source table):

- Each scenario is self-contained — no shared test data across scenarios.
- Use column types from catalog to generate type-appropriate values.

**For tables:** build a dependency graph from the catalog's `foreign_keys` and generate rows in topological order so FK values align within each scenario. Procedure parameters are ignored or flagged — rare in warehouse procs, typically orchestration concerns.

**For views:** skip FK graph traversal — source tables are read-only inputs with no write-path FK constraints to enforce.

Apply column exclusion, NOT NULL coverage, and CHECK constraint rules from [references/fixture-synthesis-ref.md](references/fixture-synthesis-ref.md).

## Step 4: Write test spec

Write the test spec as soon as fixtures are ready. Do not ask for confirmation before writing — this skill is a write-through workflow.

Write the spec JSON directly to `test-specs/<item_id>.json`.

**First run (merge_mode = false):**

Write `test-specs/<item_id>.json` with the TestSpec schema. Omit the `expect` field.

**Re-invocation (merge_mode = true):**

Merge into the existing `test-specs/<item_id>.json`:

- **`unit_tests[]`**: append new scenario entries. Never overwrite existing entries. Preserve any `expect` blocks already present on existing scenarios.
- **`branch_manifest[]`**: for newly discovered branches, add new entries. For existing branch entries, append new scenario names to their `scenarios[]` array.
- **`uncovered_branches`**: recalculate from the merged manifest — branches with no scenarios in the final `unit_tests[]`.
- **`coverage`** and **`status`**: recalculate using the Coverage and Status Rules after the merge.
- **`warnings[]`**: append new warnings (e.g., stale branch warnings from Step 2); do not remove existing warnings.

**Table vs view output format:**

- **Tables:** each `unit_tests[]` entry includes `target_table` and `procedure` fields (bracket-quoted FQNs).
- **Views:** each `unit_tests[]` entry uses `sql` in place of `target_table` and `procedure`. The `sql` field contains the view's refactored SELECT statement.

After writing, print the result:

```text
Test spec written: test-specs/<item_id>.json
  Branches: N  (new: X)
  Scenarios: M  (new: Y)
  Coverage: complete|partial
  Warnings: (if any)
```

For merge_mode, include a **Preserved / New** summary:

```text
  Preserved: N existing scenarios unchanged
  New: M scenarios added for X branches
```

Naming conventions:

- Test name (table): `test_<load_pattern>_<scenario_description>`, e.g. `test_merge_matched_product_updated`
- Test name (view): `test_<sql_pattern>_<scenario_description>`, e.g. `test_where_filter_active_row`
- `target_table`: bracket-quoted FQN of the target table, e.g. `[silver].[DimProduct]`
- `procedure`: bracket-quoted FQN of the writer procedure from catalog scoping, e.g. `[silver].[usp_load_DimProduct]`
- `given[].table`: bracket-quoted SQL identifier, e.g. `[bronze].[SalesOrderHeader]`

## Step 5: Validate output

- Every `unit_tests[]` entry has at least one `given` entry with rows.
- Every `given[].rows` entry includes all NOT NULL non-identity columns for that source table.
- Set `coverage` field: `complete` when all branches have scenarios, `partial` otherwise.
- Set `status`: `ok | partial | error`.

## Final Step: Write test-gen status to catalog

After the test spec has been written to `test-specs/<item_id>.json`, record the summary in the catalog:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness write \
  --table <fqn> \
  --branches <number_of_branches> \
  --unit-tests <number_of_unit_tests> \
  --coverage <complete|partial|none>
```

If there are warnings or errors to report, pass them as JSON arrays:

```bash
  --warnings '[{"code": "...", "message": "..."}]' \
  --errors '[{"code": "...", "message": "..."}]'
```

---

## Handling reviewer feedback

If `$ARGUMENTS` or the invoking prompt includes a `feedback_for_generator` JSON block, apply it before running the normal pipeline:

- **`uncovered_branches`**: list of branch IDs missing coverage. Read the existing `test-specs/<item_id>.json`, then generate new scenarios targeting each listed branch and add them to `unit_tests[]`.
- **`quality_fixes`**: list of per-scenario remediation instructions. Locate the named scenario in `unit_tests[]` and revise its fixtures according to the instruction (e.g., fix unrealistic values, align FK consistency).

After applying feedback, re-run Steps 2–5 with the revised scenarios. Do not discard previously approved scenarios — only add or revise as directed.

If no `feedback_for_generator` is present, skip this section.

---

## Coverage and Status Rules

| Condition | `coverage` | `status` |
|---|---|---|
| All branches have scenarios | `complete` | `ok` |
| Branches remain after review loop | `partial` | `partial` |
| Generation failed (context assembly, branch extraction, or fixture synthesis error) | — | `error` |

---

## Boundary Rules

Test generator must not:

- Execute stored procedures or access the sandbox
- Generate dbt SQL model files
- Render YAML — `unit_tests[]` is structured JSON; dbt YAML conversion happens post-execution
- Make materialization or business key decisions
- Score its own coverage authoritatively — the test reviewer does that

---

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

- `code`: stable machine-readable identifier — use codes from `../../lib/shared/generate_tests_error_codes.md`.
- `message`: human-readable description.
- `item_id`: fully qualified table or view name this entry relates to.
- `field`: optional field path associated with the issue (empty or omitted for non-field errors).
- `severity`: `error` or `warning`.
- `details`: optional structured context object.

---

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `migrate context` | 1 | No profile or no statements. Tell user to run scoping and profiling first |
| `migrate context` | 2 | IO/parse error. Surface the error message |
| `test-harness write` | 1 | Validation failure — report field-level errors, correct payload, retry |
| `test-harness write` | 2 | IO/parse error — report and stop |

---

## References

- [`../../lib/shared/generate_tests_error_codes.md`](../../lib/shared/generate_tests_error_codes.md) — canonical generating-tests and reviewing-tests statuses and surfaced error/warning codes
- [`references/fixture-synthesis-ref.md`](references/fixture-synthesis-ref.md) — column exclusion, NOT NULL defaults, and CHECK constraint rules for fixture generation
