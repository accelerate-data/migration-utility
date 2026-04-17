---
name: applying-mart-candidates
description: Use internally when one approved intermediate or mart candidate from a refactor-mart plan must be applied and validated across its declared scope
user-invocable: false
argument-hint: "<plan-file> <candidate-id>"
---

# Applying Mart Candidates

Apply exactly one approved higher-layer candidate from a refactor-mart plan,
validate its declared scope, and update only that candidate section.

## Inputs

Parse exactly two positional arguments:

```text
<plan-file> <candidate-id>
```

Read `${CLAUDE_PLUGIN_ROOT}/skills/applying-mart-candidates/references/mart-validation-contract.md`
before editing files or plan status.

## Guards

- If the plan file is missing, stop with `PLAN_NOT_FOUND`.
- If the candidate section is missing, stop with `CANDIDATE_NOT_FOUND`.
- If the candidate is not approved with `- [x] Approve: yes`, stop with
  `CANDIDATE_NOT_APPROVED`.
- If `Type:` is `stg`, reject it with `STAGING_CANDIDATE_NOT_ALLOWED` and do
  not change its execution status.
- If `Type:` is anything other than `int` or `mart`, stop with
  `INVALID_CANDIDATE_TYPE`.
- If `Depends on:` is missing, empty, malformed, ambiguous, or does not
  explicitly include `none` or upstream candidate IDs, mark only this candidate
  blocked before editing files.
- If any listed dependency section is missing or its `Execution status:` is not
  `applied`, mark only this candidate blocked before editing files.
- If `Output:` is missing or does not identify a new or existing `int_*` model
  path or model name, or a mart output path under `dbt/models/marts/**` or a
  mart model name using the established `fct_`, `dim_`, or `mart_` prefixes,
  mark only this candidate blocked.
- If `Validation:` is missing, ambiguous, or does not identify the smallest
  executable validation command/scope and any required existing consumer models,
  mark only this candidate blocked.

## Workflow

1. Read the markdown plan and isolate exactly one `## Candidate: <candidate-id>`
   section.
2. Extract `Type:`, `Output:`, `Depends on:`, and `Validation:`.
3. Verify every declared dependency is already `Execution status: applied`
   before editing any dbt file.
4. Create or update the declared `int` or `mart` output model.
5. Rewire only consumers that are explicitly named in the candidate section's
   human-readable text or are an unambiguous direct consequence of the output
   change. Do not infer broad rewrites from selector syntax such as
   `+int_sales_orders`.
6. Use `Validation:` as the validation command or scope only. Run the smallest
   validation command that covers the changed output and any explicitly
   required existing consumer models. Prefer the command listed in
   `Validation:`.
7. Update only this candidate section:
   - `Execution status: applied` when validation passes.
   - `Execution status: failed` when attempted validation fails.
   - `Execution status: blocked` when required candidate inputs are missing
     before edits.
   - Add exactly one `Validation result:` bullet for `applied` or `failed`.
   - Add exactly one `Blocked reason:` bullet for `blocked`.
   - Do not add alternate status fields such as `Applied:`.

## Rewire Rules

- Rewire only models explicitly named in the candidate section's human-readable
  text or required by an unambiguous local output rewrite.
- Do not infer broad consumer rewrites from `Validation:` selector syntax.
- Do not edit staging candidates or staging outputs.
- Do not silently continue past failed or unapplied dependencies.

## Validation

Validation is candidate-scoped:

- validate the changed `int` or `mart` output model;
- validate each additional existing model required by the candidate section;
- capture the command or scope used in a `Validation result:` bullet; and
- do not alter unrelated candidate statuses after success, failure, or block.

If validation fails, keep attempted file edits in place only when they help the
user inspect the failure, and mark this candidate `failed`.

## Completion

Report:

```text
applying-mart-candidates complete -- <candidate-id>

status: applied|failed|blocked
validation: <command or reason>
plan: <plan-file>
```
