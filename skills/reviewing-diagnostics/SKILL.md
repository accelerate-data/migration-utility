---
name: reviewing-diagnostics
description: >
  Use when the user asks to review, clear, suppress, or investigate /status catalog diagnostics for a single table or migration object.
user-invocable: true
argument-hint: "<schema.table>"
---

# Reviewing Diagnostics

Review all active catalog diagnostics for one table and either fix catalog state, ask for human input, write a reviewed-warning artifact, or leave the warning active.

## Arguments

`$ARGUMENTS` is one table FQN such as `gold.rpt_sales_by_category`.

Ask for a table FQN if it is missing. Do not run this skill for all tables at once.

## Required Inputs

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan
```

Find diagnostics where `fqn` matches the requested table.

Then load context:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" discover show --name <fqn>
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" discover refs --name <fqn>
```

Also inspect the relevant catalog JSON files directly when needed:

- `catalog/tables/<fqn>.json`
- selected writer from table scoping
- candidate or referenced writer procedures from `catalog/procedures/`
- `catalog/diagnostic-reviews.json`

## Decision Rules

Prefer fixes over suppression.

Fix catalog state when diagnostics show wrong or stale catalog facts, for example:

- wrong selected writer
- profile assigned to the wrong writer
- stale profile derived from incorrect scoping
- warning text says the actual writer appears to be another procedure

Ask the user when multiple plausible catalog fixes exist and the catalog does not identify one clear correction.

When one clear catalog correction exists, apply it. Do not ask permission to edit catalog files that the workflow owns.

Write a reviewed-warning artifact only when:

- the warning is real but acceptable,
- the table-specific evidence was inspected,
- the warning does not block a safe migration path,
- the rationale is specific enough for a future maintainer.

Leave the warning active when the skill cannot prove either a fix or an acceptable suppression.

Never suppress errors by default.

## Reviewed Warning Artifact

Use `catalog/diagnostic-reviews.json`.

Each review must match the active warning by:

- table FQN
- object type
- diagnostic code
- message hash

The support module `shared.diagnostic_reviews` defines the exact identity and artifact shape.

## Workflow

1. List all active diagnostics for the table from batch-plan output.
2. Load the table catalog and related writer context.
3. Group diagnostics by likely root cause.
4. For each root cause, choose one outcome:
   - catalog fix
   - human choice
   - reviewed-warning artifact
   - leave active
5. Apply catalog fixes using the existing write commands when available. If no write command exists for the section being changed, edit the catalog JSON carefully and preserve unrelated fields.
6. For accepted warnings, write the review artifact with concrete evidence paths.
7. Re-run batch-plan and report remaining visible diagnostics for the table.

## Output

Report:

- diagnostics reviewed
- catalog fixes applied
- reviewed warnings accepted
- warnings left active and why
- next recommended command

## Common Mistakes

- Do not review by diagnostic code alone; always review one table at a time.
- Do not suppress warnings that point to wrong scoping or wrong profile state.
- Do not hide a warning by deleting it from the source catalog entry unless a catalog fix naturally removes it.
- Do not write vague reasons such as "reviewed" or "acceptable"; include the concrete evidence.
