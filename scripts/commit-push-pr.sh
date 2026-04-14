#!/usr/bin/env bash
# commit-push-pr.sh — commit, push, and open a PR.
# Usage: ./scripts/commit-push-pr.sh "commit message" "PR title" [file1 file2 ...]
# If no files given, commits all staged changes.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <commit-message> <pr-title> [file ...]" >&2
  exit 1
fi

COMMIT_MSG="$1"
PR_TITLE="$2"
shift 2

if [[ $# -gt 0 ]]; then
  git add -- "$@"
fi

if ! git diff --cached --quiet; then
  git commit -m "$COMMIT_MSG"
fi

BRANCH=$(git branch --show-current)
git push -u origin "$BRANCH"

gh pr create \
  --title "$PR_TITLE" \
  --body "$(cat <<'EOF'
## Summary

Auto-generated PR.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"

echo "PR created for branch: $BRANCH"
