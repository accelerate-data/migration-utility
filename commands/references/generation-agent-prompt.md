# Generation Sub-agent Prompt

Prompt template for the generation sub-agents launched in Stage 1 of `/generate-model`. Substitute angle-bracket placeholders with actual values before dispatching.

## Prompt Template

```text
Run /generating-model for <schema.table>.
The working directory is <working-directory>.

Materialization: follow the generating-model skill exactly. Ordinary mart tables use project defaults and must not add `config(materialized='table')`; add model-level `config()` only for exceptions such as aliases, schemas, incremental models, snapshots, or view materialization.

Equivalence warnings: proceed and write the model. Record each gap as an EQUIVALENCE_GAP warning.

Compile failure: attempt up to 3 total attempts. If still failing, write the artifact as-is and record a DBT_COMPILE_FAILED warning.

Write the item result JSON to .migration-runs/<schema.table>.<run_id>.json. On failure, write a result with status "error" and populate errors[].
Return the item result JSON.
```
