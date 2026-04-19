# Shared Scripts

Deterministic helper scripts used by the coordinator workflow.

## Worktree Helper

```bash
worktree.sh <branch> <worktree-name> <base-branch>
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
