# Command: /migrate-mart

## Purpose

Execute an approved whole-mart migration plan.

## Prerequisites

- `/migrate-mart-plan` produced a reviewed plan
- planning PR and catalog decisions are accepted
- target and sandbox remain reachable
- working repo has no uncommitted blocking changes

## Invocation

```text
/migrate-mart <plan-file>
```

## What it does

- attaches the coordinator worktree
- validates the plan metadata
- finds the first incomplete stage
- runs one stage at a time
- merges each stage PR into the coordinator branch
- updates and commits the plan after each stage
- opens or updates the final coordinator PR

## Stage flow

At a user level, the command coordinates these stages:

1. Profile tables
2. Validate or refresh target state
3. Validate or refresh sandbox state
4. Generate tests
5. Refactor source SQL
6. Replicate source tables
7. Generate dbt models
8. Refactor mart layers
9. Open the final coordinator PR

## Failure recovery

Rerun the same command with the same plan file. It resumes from the first incomplete stage and reuses recorded branches and worktrees.

## Final handoff

The final coordinator PR is for human review and merge.
