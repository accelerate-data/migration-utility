---
name: migrate-mart-plan
description: Whole-scope mart migration planner. Validates readiness, scopes when needed, enforces catalog ownership resolution, and writes a resumable Markdown operational plan.
user-invocable: true
argument-hint: "[slug]"
---

# Migrate Mart Plan

Plan the full mart migration workflow for one slug and write the coordinator plan under `docs/migration-plans/<slug>/README.md`.

This command opens or updates the planning PR for the generated plan branch, then stops. It does not execute migration stages and does not open the final coordinator PR. `/migrate-mart` is the paired follow-on command that executes the approved plan and owns the final coordinator PR lifecycle.

## Guards

- `$0` may be a lowercase hyphen-separated slug. If missing, generate a slug from the project directory name, lowercased and normalized to hyphen-separated words. If the generated slug is empty, use `mart-migration`.
- `manifest.json` must exist. If missing, fail with `MANIFEST_NOT_FOUND`.
- `runtime.source`, `runtime.target`, and `runtime.sandbox` must be present in `manifest.json`.
- Source, target, and sandbox must be reachable through existing CLI checks.
- `dbt/dbt_project.yml` must exist before writing an executable plan.
- If catalog ownership is unresolved after scoping, stop before writing an executable plan and tell the human which `ad-migration add-source-table`, `ad-migration add-seed-table`, or `ad-migration exclude-table` decisions are needed.

## Pipeline

1. Resolve the slug from `$0` or generate one from the project directory name.
2. Detect default branch.
3. Create or reuse the coordinator branch `feature/migrate-mart-<slug>` using `${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh`.
   - Run:

     ```bash
     "${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh" "feature/migrate-mart-<slug>" "migrate-mart-<slug>" "<default-branch>"
     ```

   - Use the returned `worktree_path` for all reads, writes, commits, and plan updates.
4. Run fresh `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan --project-root <worktree_path>`.
5. If `scope_phase` has objects, run `/scope-tables` in coordinator mode as Stage 020, merge the returned PR, clean up the stage worktree, refresh coordinator branch, and rerun `batch-plan`.
6. If source/seed/exclude decisions are unresolved, report required CLI decisions and stop.
7. Write `docs/migration-plans/<slug>/README.md`.
8. Commit the plan and any planning catalog changes on the coordinator branch.
9. Open or update the planning PR from `feature/migrate-mart-<slug>` to the default branch:

   ```bash
   "${CLAUDE_PLUGIN_ROOT}/scripts/stage-pr.sh" "feature/migrate-mart-<slug>" "<default-branch>" "Plan mart migration: <slug>" ".migration-runs/pr-body.<run_id>.md"
   ```

10. Report the planning PR URL and tell the human to review, merge, and clean up the planning worktree before running `/migrate-mart <plan-file>`. Planning never opens or updates the final coordinator PR.

## Plan Generation Requirements

The generated plan must substitute concrete values for every branch, base branch, worktree name, worktree path, object list, and plan file path. Do not write angle-bracket placeholders into executable stage metadata.

Use stable status tokens only: `planned`, `running`, `blocked`, `complete`, `skipped`, or `superseded`.

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

## Coordinator

| Field | Value |
|---|---|
| Agent | `migrate-mart-coordinator-<slug>` |
| Branch | `feature/migrate-mart-<slug>` |
| Worktree name | `migrate-mart-<slug>` |
| Worktree path | `../worktrees/feature/migrate-mart-<slug>` |
| Base branch | `<default-branch>` |
| Status | `pending` |

## Source Replication

| Field | Value |
|---|---|
| Row limit | `10000` |
| Command | `ad-migration replicate-source-tables --limit 10000 --yes` |
| Status | `planned` |

## Stage 010: Runtime Readiness

- Agent: `runtime-readiness`
- Slash command: `n/a`
- Invocation: `existing CLI reachability checks for source, target, and sandbox`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `none`
- Status: `complete`

## Stage 020: Scope

- Agent: `scope-tables`
- Slash command: `/scope-tables <plan-file> 020 020-scope-<slug> feature/migrate-mart-<slug> <scope-targets>`
- Invocation: `/scope-tables <plan-file> 020 020-scope-<slug> feature/migrate-mart-<slug> <scope-targets>`
- Branch: `feature/migrate-mart-<slug>/020-scope-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `020-scope-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/020-scope-<slug>`
- PR: `none`
- Status: `complete` or `skipped`

## Stage 030: Catalog Ownership Check

- Agent: `catalog-ownership-check`
- Slash command: `n/a`
- Invocation: `review unresolved source/seed/exclude decisions from scope`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `none`
- Status: `complete`

## Stage 040: Profile

- Agent: `profile-tables`
- Slash command: `/profile-tables`
- Invocation: `/profile-tables docs/migration-plans/<slug>/README.md 040 040-profile-<slug> feature/migrate-mart-<slug> <objects...>`
- Branch: `feature/migrate-mart-<slug>/040-profile-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `040-profile-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/040-profile-<slug>`
- PR: `<profile PR>`
- Status: `planned`

## Stage 050: Setup Target

- Agent: `setup-target`
- Slash command: `ad-migration setup-target`
- Invocation: `ad-migration setup-target`
- Branch: `feature/migrate-mart-<slug>/050-setup-target-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `050-setup-target-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/050-setup-target-<slug>`
- PR: `<setup-target PR>`
- Status: `planned`

## Stage 060: Setup Sandbox

- Agent: `setup-sandbox`
- Slash command: `ad-migration setup-sandbox --yes`
- Invocation: `ad-migration setup-sandbox --yes`
- Branch: `feature/migrate-mart-<slug>/060-setup-sandbox-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `060-setup-sandbox-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/060-setup-sandbox-<slug>`
- PR: `<setup-sandbox PR>`
- Status: `planned`

## Stage 070: Generate Tests

- Agent: `generate-tests`
- Slash command: `/generate-tests`
- Invocation: `/generate-tests docs/migration-plans/<slug>/README.md 070 070-generate-tests-<slug> feature/migrate-mart-<slug> <objects...>`
- Branch: `feature/migrate-mart-<slug>/070-generate-tests-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `070-generate-tests-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/070-generate-tests-<slug>`
- PR: `<generate-tests PR>`
- Status: `planned`

## Stage 080: Refactor Query

- Agent: `refactor-query`
- Slash command: `/refactor-query`
- Invocation: `/refactor-query docs/migration-plans/<slug>/README.md 080 080-refactor-query-<slug> feature/migrate-mart-<slug> <objects...>`
- Branch: `feature/migrate-mart-<slug>/080-refactor-query-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `080-refactor-query-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/080-refactor-query-<slug>`
- PR: `<refactor-query PR>`
- Status: `planned`

## Stage 090: Replicate Source Tables

- Agent: `replicate-source-tables`
- Slash command: `ad-migration replicate-source-tables --limit 10000 --yes`
- Invocation: `ad-migration replicate-source-tables --limit 10000 --yes`
- Branch: `feature/migrate-mart-<slug>/090-replicate-source-tables-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `090-replicate-source-tables-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/090-replicate-source-tables-<slug>`
- PR: `<replicate-source-tables PR>`
- Status: `planned`

## Stage 100: Generate Model

- Agent: `generate-model`
- Slash command: `/generate-model`
- Invocation: `/generate-model docs/migration-plans/<slug>/README.md 100 100-generate-model-<slug> feature/migrate-mart-<slug> <objects...>`
- Branch: `feature/migrate-mart-<slug>/100-generate-model-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `100-generate-model-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/100-generate-model-<slug>`
- PR: `<generate-model PR>`
- Status: `planned`

## Stage 110: Refactor Mart Staging

- Agent: `refactor-mart-staging`
- Slash command: `/refactor-mart`
- Invocation: `/refactor-mart docs/migration-plans/<slug>/README.md 110 110-refactor-mart-staging-<slug> feature/migrate-mart-<slug> <refactor-mart-plan-file> stg`
- Branch: `feature/migrate-mart-<slug>/110-refactor-mart-staging-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `110-refactor-mart-staging-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/110-refactor-mart-staging-<slug>`
- PR: `<refactor-mart-stg PR>`
- Status: `planned`

## Stage 120: Refactor Mart Higher

- Agent: `refactor-mart-higher`
- Slash command: `/refactor-mart`
- Invocation: `/refactor-mart docs/migration-plans/<slug>/README.md 120 120-refactor-mart-higher-<slug> feature/migrate-mart-<slug> <refactor-mart-plan-file> int`
- Branch: `feature/migrate-mart-<slug>/120-refactor-mart-higher-<slug>`
- Base branch: `feature/migrate-mart-<slug>`
- Worktree name: `120-refactor-mart-higher-<slug>`
- Worktree path: `../worktrees/feature/migrate-mart-<slug>/120-refactor-mart-higher-<slug>`
- PR: `<refactor-mart-int PR>`
- Status: `planned`

## Stage 130: Final Status

- Agent: `final-status`
- Slash command: `n/a`
- Invocation: `report the completed coordinator plan and any remaining blockers`
- Branch: `feature/migrate-mart-<slug>`
- Base branch: `<default-branch>`
- Worktree name: `<slug>`
- Worktree path: `<worktree-path>`
- PR: `<none until /migrate-mart>`
- Status: `planned`

The planning PR is created or updated only by `/migrate-mart-plan`. The final coordinator PR is created or updated only by `/migrate-mart`.
`/migrate-mart-plan` writes the plan, opens the planning PR, and stops; `/migrate-mart` is the paired execution command that consumes the approved plan and handles the final PR.

## Summary

When planning succeeds, report the plan path, planning PR URL, and `/migrate-mart` handoff, then stop without opening the final coordinator PR.
