# Mart Validation Contract

Use after dependency gating has passed for one `Type: int` or `Type: mart`
candidate.

## Output

Valid outputs:

- `Type: int`: an `int_*` model name or a path under
  `dbt/models/intermediate/**`.
- `Type: mart`: a model name with `fct_`, `dim_`, or `mart_`, or a path under
  `dbt/models/marts/**`.

Missing or invalid output blocks before edits.

## Rewrite Scope

`Validation:` is a validation command or scope, not a structured consumer list.

Rewire only:

- models explicitly named in the candidate text; or
- models required by an unambiguous local output rewrite.

Do not infer broad rewrites from dbt selector syntax such as
`+int_sales_orders`. Ambiguous rewrite scope blocks before edits.

## Validation

Validate only:

- the changed `int` or `mart` output model; and
- additional existing models required by the candidate section.

Prefer the command or scope listed in `Validation:` when executable. Do not
broaden validation to unrelated dbt assets.

## Status Writeback

Update only the selected candidate section.

Use exactly one status:

- `Execution status: applied`: edits were made and scoped validation passed.
- `Execution status: failed`: edits were attempted and scoped validation failed.
- `Execution status: blocked`: non-dependency inputs were missing or ambiguous
  before edits began.

Detail bullets:

- Add exactly one `Validation result:` for `applied` or `failed`.
- Add exactly one `Blocked reason:` for `blocked`.
- Do not add alternate status fields such as `Applied:`.

Preserve candidate IDs, approval state, order, dependency declarations, and
unrelated candidate statuses. Do not edit staging candidates or staging outputs.
