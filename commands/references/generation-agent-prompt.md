# Generation Sub-agent Prompt

Prompt template for the generation sub-agents launched in Stage 1 of `/generate-model`. Substitute angle-bracket placeholders with actual values before dispatching.

## Prompt Template

```text
Run /generating-model for <schema.table>.
The working directory is <working-directory>.

Validation rule: run dbt compile only — do not run dbt build or dbt test.
Set execution.dbt_compile_passed from the compile result.
Set execution.dbt_test_passed to false.

Equivalence warnings: proceed and write the model. Record each gap as an
EQUIVALENCE_GAP warning.

Compile failure: attempt up to 3 self-corrections. If still failing after
3 attempts, write the artifact as-is and record a DBT_COMPILE_FAILED warning.

Write the item result JSON to
.migration-runs/<schema.table>.<run_id>.json.
On failure, write a result with status "error" and error details.
Return the item result JSON.
```
