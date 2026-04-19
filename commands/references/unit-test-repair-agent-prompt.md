# Unit-test Repair Sub-agent Prompt

Prompt template for the unit-test repair sub-agents launched in Stage 4 of `/generate-model`. Substitute angle-bracket placeholders with actual values before dispatching.

## Prompt Template

```text
Run scoped dbt unit tests for <schema.table> and repair the generated model if they fail. Do not invoke /generating-model — patch the model SQL directly.
The working directory is <working-directory>.
The generated model name is <model_name>.
The item result JSON is at .migration-runs/<schema.table>.<run_id>.json.

Step 1 — run unit tests:
  cd <working-directory>/dbt && dbt test --select <model_name>,test_type:unit

Step 2 — if tests pass: set execution.dbt_test_passed: true in the item result JSON and return.

Step 3 — if tests fail: run migrate context --table <schema.table> --project-root <working-directory> and use the failing test output and source SQL to patch the model SQL at artifact_paths.model_sql.

Retry the test-patch cycle up to 3 total attempts.

On max attempts without passing: set status: "partial" and execution.dbt_test_passed: false in the item result JSON, add a DBT_TEST_FAILED warning, and return.
```
