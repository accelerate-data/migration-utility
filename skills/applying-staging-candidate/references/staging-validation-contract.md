# Staging Validation Contract

Use this contract when applying one approved staging candidate from a
refactor-mart plan.

## Scope

- One `stg` candidate is scoped to one source-facing staging model.
- A staging candidate may fan out to many declared downstream consumers.
- Validate the changed `stg_*` model.
- Validate each downstream consumer listed in `Validation:`.
- Do not validate or invalidate unrelated approved candidates.

## Status Vocabulary

Write back exactly one of these values in the candidate section:

- `Execution status: applied`
- `Execution status: failed`
- `Execution status: blocked`

Use `applied` only after the staging model, all declared rewires, and the
declared validation scope are complete.

Use `failed` when the candidate was attempted but validation did not pass. Add
one short `Validation result:` bullet in the same candidate section describing
the failing model or command.

Use `blocked` when required inputs are missing before edits begin, such as a
missing output path, ambiguous candidate fields, or a missing declared consumer
that prevents a scoped rewire. Add one short `Blocked reason:` bullet in the
same candidate section.

## Plan Updates

- Update only the selected candidate section.
- Preserve candidate IDs, approval state, candidate order, and unrelated
  candidate statuses.
- Never change non-staging candidate execution status from this workflow.
- Keep the plan as markdown; do not introduce a JSON or Python plan contract.
