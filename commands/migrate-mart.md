---
name: migrate-mart
description: Execute a migrate-mart Markdown plan by resuming the first incomplete task, launching one stage subagent at a time, merging stage PRs, and updating the coordinator plan.
user-invocable: true
argument-hint: "<plan-file>"
---

# Migrate Mart

Execute an approved migrate-mart plan by resuming the first incomplete executable stage and keeping the coordinator plan, stage PRs, and worktrees in sync.

## Guards

- `$0` must be an existing Markdown plan file. If missing, fail with `PLAN_REQUIRED`.
- The plan must include `## Coordinator`.
- The coordinator section must include Branch, Worktree name, Worktree path, Base branch, and Status.
- Every incomplete executable stage must include Invocation, Branch, Base branch, Worktree name, Worktree path, PR, and Status.
- If plan metadata is malformed or missing, mark the coordinator blocked with `PLAN_INVALID` and stop.

## Setup

1. Read the plan file and attach to the coordinator worktree using the coordinator Branch, Worktree name, and Base branch from the `## Coordinator` section.
2. Use `${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh` for that attachment instead of ad hoc git worktree setup. Use the returned `worktree_path` for all reads, writes, commits, prompts, and plan updates.
3. Verify pre-execution stages 010, 020, and 030 have stable `complete`, `skipped`, or `superseded` status. If any is incomplete, mark the coordinator blocked with `PLAN_INVALID` and stop.
4. Scan executable stage sections 040 through 120 in numeric order.
5. Pick the first executable stage with `Status` not in `complete`, `skipped`, or `superseded`.

## Resume Algorithm

1. Resume exactly one stage at a time.
2. For the first incomplete stage, reconcile git and PR state before launching anything else:
   - existing stage worktree with incomplete work: relaunch recorded invocation
   - stage branch commits without PR: call `stage-pr.sh`
   - open PR: call `stage-pr-merge.sh`
   - already merged PR: mark merge complete
   - merged stage with remaining worktree: call `stage-cleanup.sh`
3. After each merge, refresh coordinator worktree, rerun `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan --project-root <worktree_path>`, update the Markdown plan, and commit the plan update.
4. Launch exactly one subagent at a time.
5. Continue scanning numeric stage order after each completed merge so the next incomplete stage becomes the next resume target.

## Stage Execution

| Stage | Invocation source |
|---|---|
| 040 | recorded `/profile-tables ...` invocation |
| 050 | recorded target validation/refresh invocation |
| 060 | recorded sandbox validation/refresh invocation |
| 070 | recorded `/generate-tests ...` invocation |
| 080 | recorded `/refactor-query ...` invocation |
| 090 | recorded `ad-migration replicate-source-tables --limit <plan-limit> --yes` invocation |
| 100 | recorded `/generate-model ...` invocation |
| 110 | recorded `/refactor-mart ... stg` invocation |
| 120 | recorded `/refactor-mart ... int` invocation |

## Final Coordinator PR

When all stages are complete, open or update the final coordinator PR from the coordinator branch to the remote default branch. Do not merge the final coordinator PR. Report the URL for human review.

When stages 040 through 120 are complete, update Stage 130 and open or update the final coordinator PR.

Use `scripts/stage-pr.sh` for the PR handoff and keep the coordinator branch pointed at the remote default branch as the PR base.

## Summary

Report the first incomplete stage you resumed, any merge or cleanup action you had to reconcile, and the final coordinator PR URL once all stages are complete.
