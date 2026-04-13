# Guard Rails Reference

Use this reference when the right action is clear at a high level but the edge case is easy to rationalize away.

## Merge mode

- Existing specs are hints, not authority.
- Preserve approved scenarios and `expect` blocks unless `feedback_for_generator` targets that scenario.
- Add new coverage incrementally. Do not regenerate the whole file just because SQL changed.
- If a previously stored branch disappears from current SQL, keep the history intact and add a `STALE_BRANCH` warning.

## Object type

- Do not collapse every `catalog/views/*.json` entry to `view`.
- If `is_materialized_view: true`, emit `object_type = mv`.

## Coverage ownership

- `generating-tests` must set valid `coverage` and `status` fields on the spec.
- `reviewing-tests` independently audits those fields; reviewer ownership does not make them optional here.

## Feedback handling

- Treat `feedback_for_generator` as a direct instruction, not a suggestion.
- Apply requested uncovered-branch work before broad generation.
- Apply `quality_fixes` narrowly to the named scenarios.

## Schema discipline

- Do not invent fields outside the runtime `TestSpec` model.
- Do not guess nullability, identity, or FK behavior when catalog metadata is available.

## Red Flags

- "The old branch manifest is close enough."
- "Materialized views are basically views."
- "Quality fixes mean regenerate everything."
- "Reviewer coverage is the real score."
- "This existing spec is probably still correct."

All of these mean: stop and use current SQL, current catalog data, and targeted merge behavior.
