#!/usr/bin/env bash
# commit.sh — stage specific files and commit with a message.
# Usage: ./scripts/commit.sh "commit message" file1 [file2 ...]
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <message> <file> [file ...]" >&2
  exit 1
fi

MESSAGE="$1"
shift

git add -- "$@"

if git diff --cached --quiet; then
  echo "Nothing to commit."
  exit 0
fi

git commit -m "$MESSAGE"
echo "Committed: $MESSAGE"
