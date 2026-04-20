# Command: /migrate-mart-plan

## Purpose

Create or refresh the whole-mart migration plan.

## Prerequisites

- `/init-ad-migration` completed
- `ad-migration setup-source` completed
- `/scope-tables` completed for the mart scope
- source, seed, and excluded-object decisions are recorded
- `ad-migration setup-target` completed
- `ad-migration setup-sandbox` completed

## Invocation

```text
/migrate-mart-plan
/migrate-mart-plan <slug>
```

## Gates

The command blocks unless:

- `manifest.json` exists
- `runtime.source` exists and is reachable
- `runtime.target` exists and is reachable
- `runtime.sandbox` exists and is reachable
- `dbt/dbt_project.yml` exists
- source, seed, and excluded-object decisions are resolved

## Output

- Markdown plan under the migration plans directory
- planning branch and worktree
- planning PR for human review

## Rerun behavior

Rerun after catalog decisions change. The command refreshes the plan from current catalog state.

## Next step

Review and merge the planning PR, then run `/migrate-mart <plan-file>`.
