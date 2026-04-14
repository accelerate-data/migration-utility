#!/usr/bin/env bash
# cleanup-worktrees.sh — remove worktrees whose PRs have been merged.
# Usage: ./scripts/cleanup-worktrees.sh [branch-name]
set -euo pipefail

BRANCH_FILTER="${1:-}"

cleaned=0
skipped=0

while read -r worktree_path branch_ref; do
  branch="${branch_ref#refs/heads/}"
  [[ "$worktree_path" == "$(git rev-parse --show-toplevel)" ]] && continue
  [[ -n "$BRANCH_FILTER" && "$branch" != "$BRANCH_FILTER" ]] && continue

  merged=$(gh pr list --head "$branch" --state merged --json number --jq 'length' 2>/dev/null || echo 0)
  if [[ "$merged" -gt 0 ]]; then
    echo "  removing worktree: $worktree_path ($branch)"
    git worktree remove "$worktree_path" --force 2>/dev/null || true
    git branch -d "$branch" 2>/dev/null || true
    git push origin --delete "$branch" 2>/dev/null || true
    (( cleaned++ )) || true
  else
    echo "  skipping: $branch (no merged PR)"
    (( skipped++ )) || true
  fi
done < <(git worktree list --porcelain | awk '/^worktree /{wt=$2} /^branch /{print wt, $2}')

git fetch --prune --quiet

echo ""
echo "cleanup-worktrees complete (cleaned: $cleaned, skipped: $skipped)"
