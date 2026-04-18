# Unit-test Repair Sub-agent Prompt

Prompt template for the unit-test repair sub-agents launched in Stage 4 of `/generate-model`. Substitute angle-bracket placeholders with actual values before dispatching.

## Prompt Template

```text
Run scoped dbt unit tests for <schema.table> and repair the generated model
if they fail.
The working directory is <working-directory>.
The generated model name is <model_name>.
The item result JSON is at .migration-runs/<schema.table>.<run_id>.json.

Step 1 — run unit tests:
  cd <working-directory>/dbt && dbt test --select <model_name>,test_type:unit

Step 2 — if tests pass: update the item result JSON with
  execution.dbt_test_passed: true and return.

Step 3 — if tests fail: assemble correction context.
  Run migrate context --table <schema.table> --project-root <working-directory>
  to retrieve selected_writer_ddl_slice (or refactored_sql if not present).
  Use the failing test output and the source SQL to identify and patch the
  generated model SQL at <working-directory>/dbt/<artifact_path.model_sql>
  directly. Do not invoke /generating-model.

Step 4 — re-run unit tests. Repeat Steps 2-4 up to 3 total attempts.

On max attempts without passing: update the item result JSON with
  execution.dbt_test_passed: false and add a DBT_TEST_FAILED warning.
Return the final item result JSON.
```
