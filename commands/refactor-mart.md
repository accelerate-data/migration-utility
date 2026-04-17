---
name: refactor-mart
description: >
  Mart refactor execution command. Consumes a markdown plan file and applies
  approved staging or higher-layer candidates with validation.
user-invocable: true
argument-hint: "<plan-file> stg|int"
---

# Refactor Mart

Consume a markdown candidate plan written by `/refactor-mart-plan` and apply one
approved wave. This command runs in two explicit modes:

- `stg`: apply only approved staging candidates.
- `int`: apply only approved non-staging candidates after dependency checks.

Validation is part of both modes. The command must not apply unapproved
candidates.

## Guards

- Parse exactly two positional arguments from `$ARGUMENTS`: `<plan-file>` and
  `stg|int`.
- If the plan file is missing, fail with `PLAN_NOT_FOUND`.
- If mode is not `stg` or `int`, fail with `INVALID_MODE`.
- `manifest.json` must exist. If missing, fail with `MANIFEST_NOT_FOUND`.
- `dbt/dbt_project.yml` must exist. If missing, fail with
  `DBT_PROJECT_MISSING` and tell the user to run `ad-migration setup-target`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. Create one task per
candidate selected for the current mode with status `pending`. Update each
candidate to `in_progress` before it is applied and to `completed` or
`cancelled` after validation and plan-status writeback.

## Pipeline

### Step 1 -- Setup

1. Parse `<plan-file>` and mode from `$ARGUMENTS`.
2. Generate a run slug from the plan filename and mode:
   `refactor-mart-<mode>-<plan-stem>`.
3. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns the default branch name, use the current repository root as
     the working directory.
   - Otherwise, use the returned worktree path for all reads, writes, commits,
     and plan-status updates.
4. Read the markdown plan in the selected working directory.

### Step 2 -- Select Candidates

Select only candidates with exact checked approval syntax:
`- [x] Approve: yes`.

Mode behavior:

- `stg` selects candidates with `Type: stg`.
- `int` selects candidates with `Type: int` or `Type: mart`.

For `stg` mode, reject non-staging candidates without changing their execution
status.

For `int` mode, check dependencies before applying each selected candidate and
before editing any dbt files:

- `Depends on: none` is satisfied.
- A dependency is satisfied only when the referenced candidate section has
  `Execution status: applied`.
- If any dependency is missing, unchecked, failed, blocked, planned, or
  otherwise not applied, mark the candidate as blocked in the plan, include
  the missing or unsatisfied dependency IDs, and skip application.

### Step 3 -- Apply Selected Wave

For each selected, unblocked candidate:

- `stg` mode: run the `applying-staging-candidate` skill for
  `<plan-file> <candidate-id>`.
- `int` mode: run the internal `applying-mart-candidates` skill for
  `<plan-file> <candidate-id>`.

The apply workflow owns dbt file changes, validation, and candidate status
writeback. It must update the candidate section to one of:

- `Execution status: applied`
- `Execution status: failed`
- `Execution status: blocked`

### Step 4 -- Summarize

After all selected candidates are processed, reread the plan and report:

```text
refactor-mart <mode> complete -- <plan-file>

applied: <n>
failed: <n>
blocked: <n>
skipped: <n>
```

For blocked candidates, list the candidate ID and missing dependencies. For
failed candidates, list the candidate ID and validation failure summary.

If every selected candidate is blocked or failed, report that no dbt changes
were completed and stop.
