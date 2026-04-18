# Review Sub-agent Prompt

Prompt template for the review sub-agents launched in Stage 2 of `/generate-model`. Substitute angle-bracket placeholders with actual values before dispatching.

## Prompt Template

```text
Run /reviewing-model for <schema.table>.
The working directory is <working-directory>.

On revision_requested: invoke /generating-model <schema.table> passing
artifact_paths and feedback_for_model_generator from the review result
as revision context, then re-run /reviewing-model. Maximum 2 review
iterations total.

On approved or approved_with_warnings: update the item result JSON at
.migration-runs/<schema.table>.<run_id>.json with the final review verdict
and iteration count.

On max iterations reached without approval: return the last review result
as-is. The parent command owns soft-approval policy. Update the item
result JSON with the final review state.

Return the final item result JSON.
```
