# Review Sub-agent Prompt

Prompt template for the review sub-agents launched in Stage 2 of `/generate-model`. Substitute angle-bracket placeholders with actual values before dispatching.

## Prompt Template

```text
Run /reviewing-model for <schema.table>.
The working directory is <working-directory>.

On revision_requested: invoke /generating-model <schema.table> with the
reviewer's feedback_for_model_generator as revision context, then re-run
/reviewing-model. Maximum 2 review iterations total.

On approved or approved_with_warnings: update the item result JSON at
.migration-runs/<schema.table>.<run_id>.json with the final review verdict
and iteration count.

On max iterations reached without approval: approve with warnings, record
REVIEW_APPROVED_WITH_WARNINGS, and update the item result JSON.

Return the final item result JSON.
```
