#!/usr/bin/env bash
set -euo pipefail

usage_failure() {
  local actual_argv0="$1"

  SCRIPT_PATH="$actual_argv0" python3 - <<'PY' >&2
import json
import os

payload = {
    "status": "failed",
    "code": "USAGE",
    "step": "argument_validation",
    "message": "Incorrect stage cleanup helper usage.",
    "contract": "stage-cleanup.sh <branch> <worktree-path>",
    "retry_command": os.environ["SCRIPT_PATH"],
    "suggested_fix": "Call the helper with exactly two arguments: <branch> <worktree-path>.",
    "can_retry": False,
}
print(json.dumps(payload))
PY
  exit 2
}

json_success() {
  local status_value="$1"
  local branch_value="$2"
  local worktree_path_value="$3"

  STATUS="$status_value" \
  BRANCH="$branch_value" \
  WORKTREE_PATH="$worktree_path_value" \
  python3 - <<'PY'
import json
import os

payload = {
    "status": os.environ["STATUS"],
    "branch": os.environ["BRANCH"],
    "worktree_path": os.environ["WORKTREE_PATH"],
}
print(json.dumps(payload))
PY
}

json_failure() {
  local code="$1"
  local step="$2"
  local message="$3"
  local branch_value="$4"
  local worktree_path_value="$5"

  CODE="$code" \
  STEP="$step" \
  MESSAGE="$message" \
  BRANCH="$branch_value" \
  WORKTREE_PATH="$worktree_path_value" \
  python3 - <<'PY' >&2
import json
import os

payload = {
    "status": "failed",
    "code": os.environ["CODE"],
    "step": os.environ["STEP"],
    "message": os.environ["MESSAGE"],
    "branch": os.environ["BRANCH"],
    "worktree_path": os.environ["WORKTREE_PATH"],
}
print(json.dumps(payload))
PY
  exit 1
}

if [[ $# -ne 2 ]]; then
  usage_failure "$0"
fi

branch="$1"
worktree_path="$2"

if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
  json_failure \
    "REPO_ROOT_NOT_FOUND" \
    "git_rev_parse" \
    "Could not resolve the repository root from git." \
    "$branch" \
    "$worktree_path"
fi

worktree_list="$(git worktree list --porcelain)"
if ! printf '%s\n' "$worktree_list" | grep -Fqx "worktree $worktree_path"; then
  worktree_present="0"
else
  worktree_present="1"
fi

if [[ "$worktree_present" == "1" && -d "$worktree_path" ]]; then
  if ! git worktree remove "$worktree_path"; then
    json_failure \
      "WORKTREE_REMOVE_FAILED" \
      "git_worktree_remove" \
      "Could not remove the worktree." \
      "$branch" \
      "$worktree_path"
    fi
fi

if git show-ref --verify --quiet "refs/heads/$branch"; then
  local_branch_present="1"
else
  local_branch_present="0"
fi

if git show-ref --verify --quiet "refs/remotes/origin/$branch"; then
  remote_branch_present="1"
else
  remote_branch_present="0"
fi

if [[ "$worktree_present" != "1" && "$local_branch_present" != "1" && "$remote_branch_present" != "1" ]]; then
  json_success "already_clean" "$branch" "$worktree_path"
  exit 0
fi

if [[ "$local_branch_present" == "1" ]]; then
  git branch -d "$branch" >/dev/null 2>&1 || true
fi

if [[ "$remote_branch_present" == "1" ]]; then
  git push origin --delete "$branch" >/dev/null 2>&1 || true
fi

json_success "cleaned" "$branch" "$worktree_path"
