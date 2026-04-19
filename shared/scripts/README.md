# Shared Scripts

Deterministic helper scripts used by the coordinator workflow.

## Worktree Helper

```bash
worktree.sh <branch> <worktree-name> <base-branch>
```

Creates or reuses a git worktree for the requested branch, bootstraps the worktree environment,
and emits structured JSON on success or deterministic failure.

## Stage PR Helper

```bash
stage-pr.sh <branch> <base-branch> <title> <body-file>
```

Creates or updates the stage PR for a worktree branch.

## Stage PR Merge Helper

```bash
stage-pr-merge.sh <pr-number-or-url> <base-branch>
```

Merges a stage PR into the requested base branch.

## Stage Cleanup Helper

```bash
stage-cleanup.sh <branch> <worktree-path>
```

Removes the stage worktree and clears any branch-local cleanup state.
