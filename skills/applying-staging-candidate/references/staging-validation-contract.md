# Staging Validation Contract

Use for one approved `Type: stg` candidate.

## Scope

- One candidate owns one source-facing `stg_*` model.
- Staging may fan out to many downstream consumers.
- Do not edit non-staging candidate sections or unrelated dbt assets.

## Resolving Consumers

Prefer explicit consumer model names in the candidate text.

If candidate text has no separate consumer list, use model names explicitly listed in `Validation:` as the declared consumer scope. This staging-only rule exists because staging plans use `Validation:` to name the fan-out scope.

Do not infer consumers from selector operators alone. If the scope is ambiguous,
block before edits.

## Validation

Validate:

- the changed `stg_*` model; and
- every resolved downstream consumer.

Prefer the validation command or scope listed in `Validation:` when executable.
Do not validate or invalidate unrelated approved candidates.

Before marking `applied`, confirm each declared consumer file contains a `ref()` to the staging output. A consumer that still reads the old source is a failed candidate, not an applied one.
