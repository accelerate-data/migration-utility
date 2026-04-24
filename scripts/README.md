# Scripts

This directory contains both maintainer-only repository helpers and plugin runtime helpers.

- `worktree.sh` is for maintainers working on this repository. It creates contributor
  development worktrees for `migration-utility`.
- `stage-worktree.sh`, `stage-pr.sh`, `stage-pr-merge.sh`, and `stage-cleanup.sh` are
  plugin runtime helpers. Slash commands call them from a customer project root through
  `${CLAUDE_PLUGIN_ROOT}/scripts/...`.

## Maintainer Worktree Helper

```bash
worktree.sh <branch-name>
```

Creates or attaches a maintainer development worktree for this repository.

`bootstrap_repo_local_env.py` is the shared repo-local dependency helper used by
maintainer bootstrap flows. It fingerprints `lib`, `mcp/ddl`, and `tests/evals`
manifests/lockfiles and only reruns `uv`/`npm` when a local environment is missing
or stale.

## Stage Worktree Helper

```bash
stage-worktree.sh <branch> <worktree-name> <base-branch>
```

Creates or reuses a git worktree for the requested branch, bootstraps the worktree environment,
and emits structured JSON on success or deterministic failure.

Success payload:

```json
{"status":"ready","branch":"<branch>","base_branch":"<base-branch>","worktree_name":"<worktree-name>","worktree_path":"<path>","reused":false}
```

## Stage PR Helper

```bash
stage-pr.sh <branch> <base-branch> <title> <body-file>
```

Creates or updates the stage PR for a worktree branch.

Success payload:

```json
{"status":"created|updated","branch":"<branch>","base_branch":"<base-branch>","pr_number":123,"pr_url":"<url>"}
```

## Stage PR Merge Helper

```bash
stage-pr-merge.sh <pr-number-or-url> <base-branch>
```

Merges a stage PR into the requested base branch.

Success and blocker payloads:

```json
{"status":"already_merged|checks_pending|checks_failed|merge_conflict|merged","pr_number":123,"pr_url":"<url>","base_branch":"<base-branch>"}
```

## Stage Cleanup Helper

```bash
stage-cleanup.sh <branch> <worktree-path>
```

Removes the stage worktree and clears any branch-local cleanup state.

Success payload:

```json
{"status":"cleaned|already_clean","branch":"<branch>","worktree_path":"<path>"}
```
