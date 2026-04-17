# Status Writeback

Update only the selected candidate section.

Use exactly one status:

- `Execution status: applied`: edits were made and scoped validation passed.
- `Execution status: failed`: edits were attempted and scoped validation failed.
- `Execution status: blocked`: required candidate inputs were missing before
  edits began.

Detail bullets:

- Add exactly one `Validation result:` for `applied` or `failed`.
- Make `Validation result:` concrete: include the validation command or named
  models, plus whether declared models were validated or which model failed.
- Add exactly one `Blocked reason:` for `blocked`.
- Do not add alternate status fields such as `Applied:`.

Preserve candidate IDs, approval state, order, and unrelated candidate statuses.
Never change non-staging candidate status from this skill.
