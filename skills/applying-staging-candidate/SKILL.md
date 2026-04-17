---
name: applying-staging-candidate
description: Use when one approved staging candidate from a refactor-mart plan must be applied and validated across all of its downstream consumers
user-invocable: false
argument-hint: "<plan-file> <candidate-id>"
---

# Applying Staging Candidate

Apply exactly one approved staging candidate from a refactor-mart plan, validate
its declared scope, and update only that candidate section.

## Inputs

Parse exactly two positional arguments:

```text
<plan-file> <candidate-id>
```

Read `${CLAUDE_PLUGIN_ROOT}/skills/applying-staging-candidate/references/staging-validation-contract.md`
before editing files or plan status.

## Guards

- If the plan file is missing, stop with `PLAN_NOT_FOUND`.
- If the candidate section is missing, stop with `CANDIDATE_NOT_FOUND`.
- If the candidate is not approved with `- [x] Approve: yes`, stop with
  `CANDIDATE_NOT_APPROVED`.
- If `Type:` is anything other than `stg`, reject it with
  `NON_STAGING_CANDIDATE` and do not change its execution status.
- If `Output:` is missing or does not identify a `stg_*` model path or model
  name, mark only this candidate `blocked`.
- If declared downstream consumers are missing or ambiguous, mark only this
  candidate `blocked`.

## Workflow

1. Read the markdown plan and isolate exactly one `## Candidate: <candidate-id>`
   section.
2. Extract `Output:`, `Validation:`, and any candidate notes that describe source
   changes or consumer rewires.
3. Create or update the declared `stg_*` model.
4. Rewire every downstream consumer declared by the candidate. Treat consumers
   named in `Validation:` as the authoritative validation scope.
5. Run the smallest validation command that covers the changed staging model and
   every declared consumer. Prefer the command listed in `Validation:`.
6. Update only this candidate section:
   - `Execution status: applied` when validation passes.
   - `Execution status: failed` when attempted validation fails.
   - `Execution status: blocked` when required candidate inputs are missing.

## Rewire Rules

- Rewire all declared consumers, not only the first consumer.
- Keep rewires mechanical and source-facing: update refs or source references
  needed by the staging candidate.
- Do not implement higher-layer `int` or `mart` refactors here.
- Do not infer extra consumers beyond the declared validation scope. If the plan
  appears incomplete, note the risk in the final response without expanding the
  candidate scope.

## Validation

Validation is candidate-scoped:

- validate the changed `stg_*` model;
- validate each downstream consumer listed in `Validation:`;
- capture the command or scope used in a `Validation result:` bullet; and
- do not alter unrelated candidate statuses after success, failure, or block.

If validation fails, keep any attempted file edits in place only when they help
the user inspect the failure, and mark this candidate `failed`.

## Completion

Report:

```text
applying-staging-candidate complete -- <candidate-id>

status: applied|failed|blocked
validation: <command or reason>
plan: <plan-file>
```
