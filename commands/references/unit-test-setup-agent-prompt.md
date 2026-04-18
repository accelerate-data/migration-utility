# Unit-test Setup Sub-agent Prompt

Prompt template for the single unit-test setup sub-agent launched in Stage 3 of `/generate-model`. Substitute angle-bracket placeholders with actual values before dispatching.

## Prompt Template

```text
Materialise the direct source and ref() parents needed by the dbt unit tests
for these models: <model_names>.
The working directory is <working-directory>.

Resolve the direct parents of each model from the dbt manifest. Collect
the union of all unique direct parents across all models.

Run:
  cd <working-directory>/dbt && dbt run --select "<direct_parents_space_separated>" --empty

Build only the parent relations, not the models under test.

Write the setup result to
.migration-runs/unit-test-setup.<run_id>.json:
  { "parents_materialised": [...], "status": "ok" }

On failure, write status "error" with a "reason" field and return.
```
