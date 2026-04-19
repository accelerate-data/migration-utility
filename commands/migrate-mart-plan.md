---
name: migrate-mart-plan
description: Whole-scope mart migration planner. Validates readiness, scopes when needed, enforces catalog ownership resolution, and writes a resumable Markdown operational plan.
user-invocable: true
argument-hint: "<slug>"
---

# Migrate Mart Plan

Plan the full mart migration workflow for one slug and write the coordinator plan under `docs/migration-plans/<slug>/README.md`.

This command does not open the final coordinator PR. `/migrate-mart` owns that PR lifecycle.

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
7. Commit the plan on the coordinator branch and open or update the final coordinator PR only when explicitly instructed by `/migrate-mart`, not during planning.

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
| Branch | `feature/migrate-mart-<slug>` |
| Base branch | `<default-branch>` |
| Worktree name | `<slug>` |
| Worktree path | `<worktree-path>` |
| PR | `<none until /migrate-mart>` |
| Status | `planned` |

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
- Slash command: `/scope-tables <scope-targets>`
- Invocation: `/scope-tables <scope-targets> in coordinator mode`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
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
- Slash command: `/profile-tables <schema.table> [schema.table ...]`
- Invocation: `/profile-tables <schema.table> [schema.table ...]`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<profile PR>`
- Status: `planned`

### Stage 050: Setup Target

- Agent: `setup-target`
- Slash command: `ad-migration setup-target`
- Invocation: `ad-migration setup-target`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<setup-target PR>`
- Status: `planned`

### Stage 060: Setup Sandbox

- Agent: `setup-sandbox`
- Slash command: `ad-migration setup-sandbox --yes`
- Invocation: `ad-migration setup-sandbox --yes`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<setup-sandbox PR>`
- Status: `planned`

### Stage 070: Generate Tests

- Agent: `generate-tests`
- Slash command: `/generate-tests <schema.table> [schema.table ...]`
- Invocation: `/generate-tests <schema.table> [schema.table ...]`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<generate-tests PR>`
- Status: `planned`

### Stage 080: Refactor Query

- Agent: `refactor-query`
- Slash command: `/refactor-query <schema.table> [schema.table ...]`
- Invocation: `/refactor-query <schema.table> [schema.table ...]`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<refactor-query PR>`
- Status: `planned`

### Stage 090: Replicate Source Tables

- Agent: `replicate-source-tables`
- Slash command: `ad-migration replicate-source-tables --limit 10000 --yes`
- Invocation: `ad-migration replicate-source-tables --limit 10000 --yes`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<replicate-source-tables PR>`
- Status: `planned`

### Stage 100: Generate Model

- Agent: `generate-model`
- Slash command: `/generate-model <schema.table> [schema.table ...]`
- Invocation: `/generate-model <schema.table> [schema.table ...]`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<generate-model PR>`
- Status: `planned`

### Stage 110: Refactor Mart Staging

- Agent: `refactor-mart-staging`
- Slash command: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> stg`
- Invocation: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> stg`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<refactor-mart-stg PR>`
- Status: `planned`

### Stage 120: Refactor Mart Higher

- Agent: `refactor-mart-higher`
- Slash command: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> int`
- Invocation: `/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> int`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
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

## Summary

When planning succeeds, report the plan path and stop without opening the final coordinator PR.
