#!/usr/bin/env bash
# commit-push-pr.sh — commit, push, and open a PR.
# Usage: ./scripts/commit-push-pr.sh "commit message" "PR title" ["PR body"] [file1 file2 ...]
# If no files given, commits all staged changes.
# PR body should include a "Fixes VU-XXX" line to auto-close the Linear issue.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <commit-message> <pr-title> [pr-body] [file ...]" >&2
  exit 1
fi

COMMIT_MSG="$1"
PR_TITLE="$2"
shift 2

PR_BODY=""
if [[ $# -gt 0 && ! -f "$1" ]]; then
  PR_BODY="$1"
  shift
fi

if [[ $# -gt 0 ]]; then
  git add -- "$@"
fi

if ! git diff --cached --quiet; then
  git commit -m "$COMMIT_MSG"
fi

BRANCH=$(git branch --show-current)
if [[ -z "$BRANCH" ]]; then
  echo "Error: not on a named branch (detached HEAD). Checkout a branch before running this script." >&2
  exit 1
fi
git push -u origin "$BRANCH"

gh pr create \
  --title "$PR_TITLE" \
  --body "${PR_BODY:-}"

echo "PR created for branch: $BRANCH"
