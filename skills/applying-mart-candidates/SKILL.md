---
name: applying-mart-candidates
description: Use internally when /refactor-mart dispatches one approved Type: int or Type: mart candidate
user-invocable: false
argument-hint: "<plan-file> <candidate-id>"
---

# Applying Mart Candidates

Apply one approved higher-layer candidate and update only that candidate section.

## Inputs

Parse exactly two positional arguments:

```text
<plan-file> <candidate-id>
```

Read references only when needed:

- `references/dependency-gate.md` before deciding whether this skill may edit.
- `references/mart-validation-contract.md` before resolving output, rewrites,
  validation scope, or status writeback.

## Checks

- Missing plan file: stop with `PLAN_NOT_FOUND`.
- Missing candidate section: stop with `CANDIDATE_NOT_FOUND`.
- Candidate not approved with `- [x] Approve: yes`: stop with
  `CANDIDATE_NOT_APPROVED`.
- `Type: stg`: stop with `STAGING_CANDIDATE_NOT_ALLOWED`; do not change
  execution status.
- `Type:` is not `int` or `mart`: stop with `INVALID_CANDIDATE_TYPE`.
- Dependency gate fails: stop with `DEPENDENCY_GATE_NOT_SATISFIED`; do not
  change execution status.
- Post-gate candidate inputs are missing or ambiguous: mark this candidate
  `blocked`.

## Workflow

1. Read the plan and isolate `## Candidate: <candidate-id>`.
2. Extract `Type:`, `Output:`, `Depends on:`, and `Validation:`.
3. Apply `references/dependency-gate.md`. Stop unchanged if it fails.
4. Resolve output, rewrite scope, and validation scope with
   `references/mart-validation-contract.md`.
5. Create or update the declared `int` or `mart` output model.
6. Rewire only resolved consumers.
7. Run candidate-scoped validation.
8. Update only this candidate section using the writeback rules in
   `references/mart-validation-contract.md`.

## Completion

Report:

```text
applying-mart-candidates complete -- <candidate-id>

status: applied|failed|blocked
validation: <command or reason>
plan: <plan-file>
```
