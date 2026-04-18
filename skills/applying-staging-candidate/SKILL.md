---
name: applying-staging-candidate
description: Use internally when /refactor-mart dispatches one approved Type: stg candidate
user-invocable: false
argument-hint: "<plan-file> <candidate-id>"
---

# Applying Staging Candidate

Apply one approved staging candidate and update only that candidate section.

## Inputs

Parse exactly two positional arguments:

```text
<plan-file> <candidate-id>
```

Read references only when needed:

- `references/staging-validation-contract.md` before deciding scope, rewrites,
  validation, or status writeback.
- `references/status-writeback.md` before updating plan status.

## Checks

- Missing plan file: stop with `PLAN_NOT_FOUND`.
- Missing candidate section: stop with `CANDIDATE_NOT_FOUND`.
- Candidate not approved with `- [x] Approve: yes`: stop with
  `CANDIDATE_NOT_APPROVED`.
- `Type:` is not `stg`: stop with `NON_STAGING_CANDIDATE`; do not change
  execution status.
- Missing or invalid `Output:` for a `stg_*` model: mark this candidate
  `blocked`.
- Missing or ambiguous downstream scope: mark this candidate `blocked`.

## Workflow

1. Read the plan and isolate `## Candidate: <candidate-id>`.
2. Extract `Output:` and `Validation:`.
3. Resolve staging output and downstream consumers using
   `references/staging-validation-contract.md`.
4. Create or update the declared `stg_*` model.
5. Rewire every resolved downstream consumer file so it references the declared
   `stg_*` model with `ref()`.
6. Run the smallest validation command covering the staging model and resolved
   consumers. Prefer the command listed in `Validation:`.
7. Update only this candidate section using `references/status-writeback.md`.

## Completion

Report:

```text
applying-staging-candidate complete -- <candidate-id>

status: applied|failed|blocked
validation: <command or reason>
plan: <plan-file>
```
