---
name: migrate-mart-plan
description: Whole-scope mart migration planner. Validates readiness, scopes when needed, enforces catalog ownership resolution, and writes a resumable Markdown operational plan.
user-invocable: true
argument-hint: "<slug>"
---

# Migrate Mart Plan

Plan the full mart migration workflow for one slug and write the coordinator plan under `docs/migration-plans/<slug>/README.md`.

This command does not open the final coordinator PR. `/migrate-mart` is the paired follow-on command that executes the approved plan and owns that PR lifecycle.

## Guards

- `$0` must be a lowercase hyphen-separated slug. If missing, fail with `SLUG_REQUIRED`.
- `manifest.json` must exist. If missing, fail with `MANIFEST_NOT_FOUND`.
- `runtime.source`, `runtime.target`, and `runtime.sandbox` must be present in `manifest.json`.
- Source, target, and sandbox must be reachable through existing CLI checks.
- `dbt/dbt_project.yml` must exist before writing an executable plan.
- If catalog ownership is unresolved after scoping, stop before writing an executable plan and tell the human which `ad-migration add-source-table`, `ad-migration add-seed-table`, or `ad-migration exclude-table` decisions are needed.

## Pipeline

1. Detect default branch.
2. Create or reuse the coordinator branch `feature/migrate-mart-<slug>` using `${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh`.
   - Run:

     ```bash
     "${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh" "feature/migrate-mart-<slug>" "<slug>" "<default-branch>"
     ```

   - Use the returned `worktree_path` for all reads, writes, commits, and plan updates.
3. Run fresh `migrate-util batch-plan`.
4. If `scope_phase` has objects, run `/scope-tables` in coordinator mode as Stage 020, merge the returned PR, clean up the stage worktree, refresh coordinator branch, and rerun `batch-plan`.
5. If source/seed/exclude decisions are unresolved, report required CLI decisions and stop.
6. Write `docs/migration-plans/<slug>/README.md`.
7. Commit the plan on the coordinator branch and hand off to `/migrate-mart` when the plan is ready for execution. Planning never opens or updates the final coordinator PR.

## Plan Template

The generated plan must include these top-level sections in order:

- `## Coordinator`
- `## Source Replication`
- `## Stage 010: Runtime Readiness`
- `## Stage 020: Scope`
- `## Stage 030: Catalog Ownership Check`
- `## Stage 040: Profile`
- `## Stage 050: Setup Target`
- `## Stage 060: Setup Sandbox`
- `## Stage 070: Generate Tests`
- `## Stage 080: Refactor Query`
- `## Stage 090: Replicate Source Tables`
- `## Stage 100: Generate Model`
- `## Stage 110: Refactor Mart Staging`
- `## Stage 120: Refactor Mart Higher`
- `## Stage 130: Final Status`

### Coordinator

| Field | Value |
|---|---|
| Agent | `migrate-mart-coordinator-<slug>` |
| Branch | `feature/migrate-mart-<slug>` |
| Worktree name | `migrate-mart-<slug>` |
| Worktree path | `../worktrees/feature/migrate-mart-<slug>` |
| Base branch | `<default-branch>` |
| Status | `pending` |

### Source Replication

| Field | Value |
|---|---|
| Row limit | `10000` |
| Command | `ad-migration replicate-source-tables --limit 10000 --yes` |
| Status | `planned` |

### Stage 010: Runtime Readiness

- Agent: `runtime-readiness`
- Slash command: `n/a`
- Invocation: `existing CLI reachability checks for source, target, and sandbox`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `none`
- Status: `planned`

### Stage 020: Scope

- Agent: `scope-tables`
- Slash command: `/scope-tables <plan-file> 020 020-scope-<slug> feature/migrate-mart-<slug> <scope-targets>`
- Invocation: `/scope-tables <plan-file> 020 020-scope-<slug> feature/migrate-mart-<slug> <scope-targets>`
- Branch: `feature/migrate-mart-<slug>/020-scope-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `020-scope-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/020-scope-<slug>`
- PR: `none`
- Status: `planned`

### Stage 030: Catalog Ownership Check

- Agent: `catalog-ownership-check`
- Slash command: `n/a`
- Invocation: `review unresolved source/seed/exclude decisions from scope`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `none`
- Status: `planned`

### Stage 040: Profile

- Agent: `profile-tables`
- Slash command: `/profile-tables <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Invocation: `/profile-tables <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Branch: `feature/migrate-mart-<slug>/040-profile-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `040-profile-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/040-profile-<slug>`
- PR: `<profile PR>`
- Status: `planned`

### Stage 050: Setup Target

- Agent: `setup-target`
- Slash command: `ad-migration setup-target`
- Invocation: `ad-migration setup-target`
- Branch: `feature/migrate-mart-<slug>/050-setup-target-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `050-setup-target-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/050-setup-target-<slug>`
- PR: `<setup-target PR>`
- Status: `planned`

### Stage 060: Setup Sandbox

- Agent: `setup-sandbox`
- Slash command: `ad-migration setup-sandbox --yes`
- Invocation: `ad-migration setup-sandbox --yes`
- Branch: `feature/migrate-mart-<slug>/060-setup-sandbox-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `060-setup-sandbox-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/060-setup-sandbox-<slug>`
- PR: `<setup-sandbox PR>`
- Status: `planned`

### Stage 070: Generate Tests

- Agent: `generate-tests`
- Slash command: `/generate-tests <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Invocation: `/generate-tests <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Branch: `feature/migrate-mart-<slug>/070-generate-tests-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `070-generate-tests-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/070-generate-tests-<slug>`
- PR: `<generate-tests PR>`
- Status: `planned`

### Stage 080: Refactor Query

- Agent: `refactor-query`
- Slash command: `/refactor-query <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Invocation: `/refactor-query <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Branch: `feature/migrate-mart-<slug>/080-refactor-query-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `080-refactor-query-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/080-refactor-query-<slug>`
- PR: `<refactor-query PR>`
- Status: `planned`

### Stage 090: Replicate Source Tables

- Agent: `replicate-source-tables`
- Slash command: `ad-migration replicate-source-tables --limit 10000 --yes`
- Invocation: `ad-migration replicate-source-tables --limit 10000 --yes`
- Branch: `feature/migrate-mart-<slug>/090-replicate-source-tables-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `090-replicate-source-tables-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/090-replicate-source-tables-<slug>`
- PR: `<replicate-source-tables PR>`
- Status: `planned`

### Stage 100: Generate Model

- Agent: `generate-model`
- Slash command: `/generate-model <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Invocation: `/generate-model <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <objects...>`
- Branch: `feature/migrate-mart-<slug>/100-generate-model-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `100-generate-model-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/100-generate-model-<slug>`
- PR: `<generate-model PR>`
- Status: `planned`

### Stage 110: Refactor Mart Staging

- Agent: `refactor-mart-staging`
- Slash command: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> stg`
- Invocation: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> stg`
- Branch: `feature/migrate-mart-<slug>/110-refactor-mart-staging-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `110-refactor-mart-staging-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/110-refactor-mart-staging-<slug>`
- PR: `<refactor-mart-stg PR>`
- Status: `planned`

### Stage 120: Refactor Mart Higher

- Agent: `refactor-mart-higher`
- Slash command: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> int`
- Invocation: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> int`
- Branch: `feature/migrate-mart-<slug>/120-refactor-mart-higher-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `120-refactor-mart-higher-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/120-refactor-mart-higher-<slug>`
- PR: `<refactor-mart-int PR>`
- Status: `planned`

### Stage 130: Final Status

- Agent: `final-status`
- Slash command: `n/a`
- Invocation: `report the completed coordinator plan and any remaining blockers`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<none until /migrate-mart>`
- Status: `planned`

The final coordinator PR is created or updated only by `/migrate-mart`.
`/migrate-mart-plan` writes the plan and stops; `/migrate-mart` is the paired execution command that consumes the approved plan and handles the final PR.

## Summary

When planning succeeds, report the plan path, note the `/migrate-mart` handoff, and stop without opening the final coordinator PR.
