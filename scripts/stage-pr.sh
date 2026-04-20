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
    "message": "Incorrect stage PR helper usage.",
    "contract": "stage-pr.sh <branch> <base-branch> <title> <body-file>",
    "retry_command": os.environ["SCRIPT_PATH"],
    "suggested_fix": "Call the helper with exactly four arguments: <branch> <base-branch> <title> <body-file>.",
    "can_retry": False,
}
print(json.dumps(payload))
PY
  exit 2
}

json_failure() {
  local code="$1"
  local step="$2"
  local message="$3"
  local branch_value="$4"
  local base_branch_value="$5"
  local title_value="$6"
  local body_file_value="$7"

  CODE="$code" \
  STEP="$step" \
  MESSAGE="$message" \
  BRANCH="$branch_value" \
  BASE_BRANCH="$base_branch_value" \
  TITLE="$title_value" \
  BODY_FILE="$body_file_value" \
  python3 - <<'PY' >&2
import json
import os

payload = {
    "status": "failed",
    "code": os.environ["CODE"],
    "step": os.environ["STEP"],
    "message": os.environ["MESSAGE"],
    "branch": os.environ["BRANCH"],
    "base_branch": os.environ["BASE_BRANCH"],
    "title": os.environ["TITLE"],
    "body_file": os.environ["BODY_FILE"],
    "pr_number": None,
    "pr_url": None,
}
print(json.dumps(payload))
PY
  exit 1
}

json_success() {
  local status_value="$1"
  local branch_value="$2"
  local base_branch_value="$3"
  local pr_number_value="$4"
  local pr_url_value="$5"

  STATUS="$status_value" \
  BRANCH="$branch_value" \
  BASE_BRANCH="$base_branch_value" \
  PR_NUMBER="$pr_number_value" \
  PR_URL="$pr_url_value" \
  python3 - <<'PY'
import json
import os

payload = {
    "status": os.environ["STATUS"],
    "branch": os.environ["BRANCH"],
    "base_branch": os.environ["BASE_BRANCH"],
    "pr_number": int(os.environ["PR_NUMBER"]),
    "pr_url": os.environ["PR_URL"],
}
print(json.dumps(payload))
PY
}

normalize_repo_root() {
  if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
    json_failure \
      "REPO_ROOT_NOT_FOUND" \
      "git_rev_parse" \
      "Could not resolve the repository root from git." \
      "$branch" \
      "$base_branch" \
      "$title" \
      "$body_file"
  fi
}

if [[ $# -ne 4 ]]; then
  usage_failure "$0"
fi

branch="$1"
base_branch="$2"
title="$3"
body_file="$4"

if [[ ! -f "$body_file" ]]; then
  json_failure \
    "BODY_FILE_MISSING" \
    "argument_validation" \
    "The PR body file is missing." \
    "$branch" \
    "$base_branch" \
    "$title" \
    "$body_file"
fi

normalize_repo_root

if ! git push --set-upstream origin "$branch"; then
  json_failure \
    "GIT_PUSH_FAILED" \
    "git_push" \
    "Could not push the stage branch." \
    "$branch" \
    "$base_branch" \
    "$title" \
    "$body_file"
fi

if ! existing_pr_json="$(gh pr list --head "$branch" --json number,url --limit 1)"; then
  json_failure \
    "GH_PR_LIST_FAILED" \
    "gh_pr_list" \
    "Could not read existing pull requests." \
    "$branch" \
    "$base_branch" \
    "$title" \
    "$body_file"
fi
existing_pr_number="$(STAGE_PR_JSON="$existing_pr_json" python3 - <<'PY'
import json
import os

items = json.loads(os.environ["STAGE_PR_JSON"] or "[]")
if items:
    print(items[0]["number"])
PY
)"
existing_pr_url="$(STAGE_PR_JSON="$existing_pr_json" python3 - <<'PY'
import json
import os

items = json.loads(os.environ["STAGE_PR_JSON"] or "[]")
if items:
    print(items[0]["url"])
PY
)"

if [[ -n "$existing_pr_number" ]]; then
  if ! gh pr edit "$existing_pr_number" --title "$title" --body-file "$body_file" --base "$base_branch" >/dev/null; then
    json_failure \
      "GH_PR_EDIT_FAILED" \
      "gh_pr_edit" \
      "Could not update the existing pull request." \
      "$branch" \
      "$base_branch" \
      "$title" \
      "$body_file"
  fi
  pr_number="$existing_pr_number"
  pr_url="$existing_pr_url"
  status_value="updated"
else
  if ! pr_create_output="$(gh pr create --title "$title" --body-file "$body_file" --base "$base_branch" --head "$branch")"; then
    json_failure \
      "GH_PR_CREATE_FAILED" \
      "gh_pr_create" \
      "Could not create the stage pull request." \
      "$branch" \
      "$base_branch" \
      "$title" \
      "$body_file"
  fi
  pr_url="$(PR_CREATE_OUTPUT="$pr_create_output" python3 - <<'PY'
import json
import os
import re

matches = re.findall(r"https?://\S+", os.environ["PR_CREATE_OUTPUT"])
if not matches:
    raise SystemExit(1)
print(matches[-1])
PY
)"
  pr_number="$(PR_CREATE_OUTPUT="$pr_create_output" python3 - <<'PY'
import os
import re

matches = re.findall(r"/pull/([0-9]+)(?:/|$)", os.environ["PR_CREATE_OUTPUT"])
if not matches:
    raise SystemExit(1)
print(matches[-1])
PY
)"
  status_value="created"
fi

json_success "$status_value" "$branch" "$base_branch" "$pr_number" "$pr_url"
