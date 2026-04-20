# Migrate Mart Coordinator

## Decision

Add `/migrate-mart-plan` and `/migrate-mart` as the whole-scope mart migration workflow.
The planner prepares a resumable Markdown execution plan, and the coordinator executes the first
incomplete task in that plan until the mart is ready for a final human-reviewed PR.

The workflow is plan-led. The plan names the coordinator branch and worktree, plus every stage
agent, branch, worktree name, worktree path, PR base, and slash-command invocation before execution
starts.

## Customer Project Boundary

The plugin runs from the customer migration repository root. Slash commands may assume the current
working directory is the project root. Use `CLAUDE_PLUGIN_ROOT` only to locate plugin-owned scripts,
commands, skills, and internal packages.

`/init-ad-migration` remains a human-run prerequisite. It chooses source and target technologies,
checks local prerequisites, scaffolds project files, and prompts the human to fix missing
environment configuration before rerun.

## Script Helpers

Maintainer and plugin runtime scripts both live in the top-level `scripts/` folder.
The names separate their responsibilities:

```text
scripts/
  worktree.sh          # maintainer-only development worktree helper
  stage-worktree.sh    # plugin runtime stage worktree helper
  stage-pr.sh
  stage-pr-merge.sh
  stage-cleanup.sh
```

Stage commands call helpers through `CLAUDE_PLUGIN_ROOT`, for example:

```bash
"${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh" <branch> <worktree-name> <base-branch>
```

These helpers are deterministic scripts, not slash-command prose. They return structured JSON on
success and failure so command prompts can update the Markdown plan without inventing git behavior.

The old `git-checkpoints` skill is no longer the customer pipeline abstraction. It mixed worktree
creation with interactive default-branch policy. The new workflow needs explicit branch,
worktree-name, and base-branch inputs from the plan.

## Worktree Contract

`scripts/stage-worktree.sh` creates or reuses a worktree from explicit inputs:

```bash
"${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh" <branch> <worktree-name> <base-branch>
```

The script runs from the customer project root. It resolves the root with git, stores worktrees under
`../worktrees/<branch>`, and creates missing branches from `<base-branch>`, not from implicit `HEAD`.

If the expected branch already has a worktree, the script returns that path for reuse. If the branch
exists without a worktree, it attaches one. If the branch or worktree has unexpected dirty state, it
returns a deterministic blocker instead of letting a command continue.

## PR Contract

Stage commands open or update their own PRs. They do not ask the human whether to raise a PR.

Manual stage runs open PRs to the remote default branch. Coordinator stage runs open PRs to the
coordinator branch recorded in the plan.

The coordinator launches one subagent at a time. After a stage PR is open, the coordinator merges
that PR into the coordinator branch, cleans the stage worktree, refreshes its own worktree, updates
the plan, and continues to the next incomplete task.

The final coordinator PR targets the remote default branch and remains human-reviewed.

## Plan Location And Format

Customer execution plans live under `docs/`, for example:

```text
docs/migration-plans/<slug>/README.md
```

The plan is plain Markdown. There is no separate JSON or YAML state file. Each worktree receives the
same Markdown plan, updates only its assigned section, commits the update with its stage artifacts,
and contributes the plan change through its PR.

The plan must include a coordinator section:

```md
## Coordinator

- Agent: migrate-mart-coordinator-<slug>
- Branch: feature/migrate-mart-<slug>
- Worktree name: migrate-mart-<slug>
- Worktree path: ../worktrees/feature/migrate-mart-<slug>
- Base branch: <default-branch>
- Status: pending
```

Every executable stage must predeclare its execution context:

```md
## Stage 040: Profile

- Agent: migrate-mart-040-profile-<slug>
- Slash command: /profile-tables
- Invocation: `/profile-tables docs/migration-plans/<slug>/README.md 040 040-profile-<slug> feature/migrate-mart-<slug> <objects...>`
- Branch: feature/migrate-mart-<slug>/040-profile-<slug>
- Base branch: feature/migrate-mart-<slug>
- Worktree name: 040-profile-<slug>
- Worktree path: ../worktrees/feature/migrate-mart-<slug>/040-profile-<slug>
- PR: pending
- Status: pending
```

Missing stage metadata is a plan error. The coordinator must not invent branch names, worktree
names, or PR bases at execution time.

## Stage Arguments

Coordinator mode uses positional slash-command arguments only:

```text
/<stage-command> <plan-file> <stage-id> <worktree-name> <base-branch> <objects...>
```

Stage commands detect coordinator mode when the first argument is a Markdown plan path. Otherwise,
they treat all arguments as normal manual object arguments.

The numeric stage ID is deterministic and stable. Leave gaps for future insertions:

```text
010 runtime-readiness
020 scope
030 catalog-ownership-check
040 profile
050 setup-target
060 setup-sandbox
070 generate-tests
080 refactor-query
090 replicate-source-tables
100 generate-model
110 refactor-mart-staging
120 refactor-mart-higher
130 final-status
```

`/migrate-mart` takes only the plan path. It always resumes from the first incomplete task in
numeric order. It does not accept a start-stage argument.

## Planner Behavior

`/migrate-mart-plan <slug>` creates or reuses the coordinator branch and worktree, validates
runtime readiness, scopes objects if needed, and writes an executable plan only after catalog
ownership is resolved.

Source, seed, and exclude decisions happen before an executable plan is created. Scoping produces
the evidence a human needs to make those decisions. If decisions are missing, the planner reports
the exact catalog changes needed and stops. The human can run deterministic CLI commands such as
`ad-migration add-source-table`, `ad-migration add-seed-table`, or `ad-migration exclude-table`,
then rerun `/migrate-mart-plan`.

If catalog ownership changes after a plan is written, rerun `/migrate-mart-plan`. Replanning is the
supported way to refresh scope after catalog changes.

The planner writes source replication with a fixed default row cap:

```md
## Source Replication

- Row limit: 10000
- Command: `ad-migration replicate-source-tables --limit 10000 --yes`
```

Humans may edit this Markdown value before launching `/migrate-mart`.

## Coordinator Resume

`/migrate-mart <plan-file>` starts by attaching to the coordinator branch and worktree recorded in
the plan. If the worktree is missing but the branch exists, it recreates the worktree. If the branch
is checked out elsewhere, it attaches to that worktree and updates the plan.

The coordinator scans the plan in numeric order and resumes the first incomplete task. It reconciles
real git and PR state before acting:

- existing stage worktree with incomplete work: relaunch the recorded stage invocation
- branch commits without a PR: open or update the PR
- open stage PR: merge when allowed
- already merged stage PR: mark merge complete and continue cleanup
- remaining stage worktree after merge: clean it up
- uncommitted coordinator plan updates: commit them

Coordinator crashes are recovered by rerunning `/migrate-mart <plan-file>`. The plan and git state
determine where to continue.

## Guard Failures

Stage commands need deterministic whole-stage guards. A guard failure means no item work should
start, the stage is marked `blocked_human`, and the coordinator stops.

Examples include missing or unreachable runtimes, missing dbt project/profile, worktree conflicts,
PR creation failure, PR merge conflict, failed required checks, malformed plan metadata, or stale
plan state.

Item-level errors are not coordinator blockers. Stage commands already skip or record table errors
and continue with other eligible objects. The plan should record item summaries such as
`complete_with_item_errors`, but the coordinator can continue after the stage PR merges.

## Mart Refactor

The mart refactor phase is folded into `/migrate-mart`. There is no separate customer-facing
`/refactor-mart-plan` requirement for the whole-mart workflow.

Only mechanically safe mart refactor candidates are applied automatically. Unsafe or uncertain
candidates are left for later manual refactoring after the generated mart is complete.
