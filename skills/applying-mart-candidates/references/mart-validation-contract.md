# Mart Candidate Validation Contract

Use this contract when applying one approved non-staging candidate from a
refactor-mart plan.

## Scope

- One candidate is scoped to one `int` or `mart` output model.
- The candidate may rewrite the declared output and declared consumers named by
  `Validation:`.
- Do not validate or invalidate unrelated candidates.

## Status Values

Write back exactly one of these values in the candidate section:

- `Execution status: applied`
- `Execution status: failed`
- `Execution status: blocked`

Use `applied` only when edits were attempted and candidate-scoped validation
passed. Add one short `Validation result:` bullet in the same candidate section
describing the command or scope used.

Use `failed` when edits were attempted but candidate-scoped validation did not
pass. Add one short `Validation result:` bullet in the same candidate section
describing the failure.

Use `blocked` when required inputs are missing before edits begin, such as a
missing output path, unsupported candidate type, ambiguous validation scope, or
a missing declared model. Add one short `Blocked reason:` bullet in the same
candidate section.

## Writeback Rules

- Update only the selected candidate section.
- Preserve candidate IDs, approval state, candidate order, dependency
  declarations, and unrelated candidate statuses.
- Do not add alternate status fields such as `Applied:`.
- Do not edit staging candidates from this skill.

## Validation Scope

Validation is candidate-scoped:

- validate the changed `int` or `mart` output model;
- validate every additional model named by `Validation:`;
- prefer the command listed in `Validation:` when it is executable;
- capture the command or scope in `Validation result:`; and
- do not broaden validation to unrelated dbt assets.
