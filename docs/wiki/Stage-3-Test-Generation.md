# Stage 3 -- Test Generation

This stage covers generating test scenarios with ground truth capture and tearing down the sandbox when done. Test generation runs BEFORE model generation -- the model generator consumes the approved test spec and must pass `dbt test` against it.

## Test Generation (`/generate-tests`)

Generates test scenarios for each table, reviews them for coverage, then executes approved scenarios in the sandbox to capture ground truth output.

### Prerequisites

- `manifest.json` with `sandbox.database` (run `/setup-sandbox` first)
- Sandbox database must exist (checked via `test-harness sandbox-status`)
- Per table: catalog file, `scoping.selected_writer`, and `profile` with `status: "ok"` must all be present

### Invocation

```text
/generate-tests silver.DimCustomer silver.FactInternetSales
```

### Pipeline

The command creates a worktree (or reuses an existing one — see [[Git Workflow]]) and launches one sub-agent per table in parallel.

**Step 1 -- Scenario generation.** Each sub-agent runs the `/generating-tests` skill, which:

- Analyzes the stored procedure to enumerate all execution branches
- Synthesizes fixture data (INSERT statements) that exercise each branch
- Writes `test-specs/<item_id>.json` with the branch manifest and fixtures

**Step 2 -- Review loop.** For each completed item, a separate `/reviewing-tests` sub-agent independently evaluates:

- Branch coverage completeness
- Fixture data quality and realism
- Scenario isolation

Review outcomes:

| Verdict | Action |
|---|---|
| `approved` | Proceed to ground truth capture |
| `approved_with_warnings` | Proceed with noted issues |
| `revision_requested` | Feedback sent back to generator for a second attempt |

Maximum 2 review iterations per item. If the reviewer still requests revision after iteration 2, the item proceeds as `partial`.

**Step 3 -- Ground truth capture.** For each approved item:

```bash
uv run --project <shared-path> test-harness execute-spec \
  --spec test-specs/<item_id>.json
```

The CLI executes all scenarios in the sandbox, runs the stored procedure, and captures the actual output rows as `expect.rows` in the test spec file.

**Step 4 -- dbt YAML conversion.** For each item with captured ground truth:

```bash
uv run --project <shared-path> test-harness convert-dbt \
  --spec test-specs/<item_id>.json \
  --output test-specs/<item_id>.yml
```

This converts the CLI-ready JSON to dbt unit test YAML format. Bracket-quoted identifiers become `source()`/`ref()` expressions, and the target table is mapped to a dbt model name.

**Step 5 -- Summary and PR.** The command presents per-table results:

```text
generate-tests complete -- 2 tables processed

  ok silver.DimCustomer    ok
  ~  silver.FactInternetSales  partial (COVERAGE_PARTIAL)

  ok: 1 | partial: 1
```

Only dbt YAML files (`test-specs/<item_id>.yml`) from successful items are staged for commit. The intermediate JSON files are not committed.

### What gets produced

| File | Format | Purpose |
|---|---|---|
| `test-specs/<item_id>.json` | JSON | Branch manifest, fixtures, ground truth (intermediate, not committed) |
| `test-specs/<item_id>.yml` | YAML | dbt unit test format (committed artifact) |

### Error Codes

| Code | When |
|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` missing -- all items fail |
| `SANDBOX_NOT_CONFIGURED` | No `sandbox.database` in manifest -- run `/setup-sandbox` first |
| `SANDBOX_NOT_RUNNING` | Sandbox database not found -- may have been torn down |
| `CATALOG_FILE_MISSING` | Catalog file not found -- item skipped |
| `SCOPING_NOT_COMPLETED` | No `selected_writer` -- item skipped |
| `PROFILE_NOT_COMPLETED` | Profile missing or not `ok` -- item skipped |
| `TEST_GENERATION_FAILED` | `/generating-tests` skill failed -- item skipped |
| `REVIEW_KICKED_BACK` | Reviewer requested revision -- item retried (warning) |
| `COVERAGE_PARTIAL` | Not all branches covered after max iterations -- item proceeds as partial |
| `SCENARIO_EXECUTION_FAILED` | One or more scenarios failed during ground truth capture -- item proceeds with partial expectations |

## Sandbox Teardown (`/teardown-sandbox`)

Drops the throwaway sandbox database when you are finished with test generation.

### What it does

1. Reads `sandbox.database` from `manifest.json`
2. Shows which database will be dropped and asks for confirmation (this is destructive)
3. Runs `test-harness sandbox-down` to drop the database
4. Clears the `sandbox` section from `manifest.json`

If the database was already dropped, the command reports success (not an error).

### When to tear down

Tear down the sandbox after all test generation batches are complete. You do not need to keep the sandbox running for model generation -- the model generator uses the captured ground truth from the test spec files, not the live sandbox.

## Next Step

Proceed to [[Stage 4 Model Generation]] to generate dbt models from the stored procedures using the profile and test spec data.
