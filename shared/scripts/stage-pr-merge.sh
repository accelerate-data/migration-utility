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
    "message": "Incorrect stage PR merge helper usage.",
    "contract": "stage-pr-merge.sh <pr-number-or-url> <base-branch>",
    "retry_command": os.environ["SCRIPT_PATH"],
    "suggested_fix": "Call the helper with exactly two arguments: <pr-number-or-url> <base-branch>.",
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

  CODE="$code" \
  STEP="$step" \
  MESSAGE="$message" \
  PR_NUMBER="$pr_number" \
  PR_URL="$pr_url" \
  BASE_BRANCH="$base_branch" \
  python3 - <<'PY' >&2
import json
import os

payload = {
    "status": "failed",
    "code": os.environ["CODE"],
    "step": os.environ["STEP"],
    "message": os.environ["MESSAGE"],
    "pr_number": int(os.environ["PR_NUMBER"]),
    "pr_url": os.environ["PR_URL"],
    "base_branch": os.environ["BASE_BRANCH"],
}
print(json.dumps(payload))
PY
  exit 1
}

json_success() {
  local status_value="$1"

  STATUS="$status_value" \
  PR_NUMBER="$pr_number" \
  PR_URL="$pr_url" \
  BASE_BRANCH="$base_branch" \
  python3 - <<'PY'
import json
import os

payload = {
    "status": os.environ["STATUS"],
    "pr_number": int(os.environ["PR_NUMBER"]),
    "pr_url": os.environ["PR_URL"],
    "base_branch": os.environ["BASE_BRANCH"],
}
print(json.dumps(payload))
PY
}

normalize_pr_number() {
  local raw_value="$1"

  if [[ "$raw_value" =~ /pull/([0-9]+)(/|$) ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi

  if [[ "$raw_value" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "$raw_value"
    return 0
  fi

  return 1
}

if [[ $# -ne 2 ]]; then
  usage_failure "$0"
fi

raw_pr="$1"
base_branch="$2"
pr_url=""
pr_ref="$raw_pr"

if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
  pr_number="0"
  json_failure \
    "REPO_ROOT_NOT_FOUND" \
    "git_rev_parse" \
    "Could not resolve the repository root from git."
fi

if ! pr_number="$(normalize_pr_number "$raw_pr")"; then
  pr_number="0"
  pr_url=""
  json_failure \
    "INVALID_PR_REFERENCE" \
    "argument_validation" \
    "The PR reference must be a number or GitHub PR URL."
fi

if ! pr_view_json="$(gh pr view "$pr_ref" --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup)"; then
  json_failure \
    "GH_PR_VIEW_FAILED" \
    "gh_pr_view" \
    "Could not read the pull request."
fi
pr_state="$(PR_VIEW_JSON="$pr_view_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["PR_VIEW_JSON"])
print(payload.get("state", ""))
PY
)"
pr_url="$(PR_VIEW_JSON="$pr_view_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["PR_VIEW_JSON"])
print(payload.get("url", ""))
PY
)"
pr_base_branch="$(PR_VIEW_JSON="$pr_view_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["PR_VIEW_JSON"])
print(payload.get("baseRefName", ""))
PY
)"
pr_is_draft="$(PR_VIEW_JSON="$pr_view_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["PR_VIEW_JSON"])
print("true" if payload.get("isDraft") else "")
PY
)"
merge_state="$(PR_VIEW_JSON="$pr_view_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["PR_VIEW_JSON"])
print(payload.get("mergeStateStatus", ""))
PY
)"

if [[ "$pr_state" == "MERGED" ]]; then
  json_success "already_merged"
  exit 0
fi

if [[ "$pr_state" != "OPEN" || -n "$pr_is_draft" || "$pr_state" == "DRAFT" ]]; then
  json_success "merge_conflict"
  exit 0
fi

if [[ -n "$pr_base_branch" && "$pr_base_branch" != "$base_branch" ]]; then
  json_success "merge_conflict"
  exit 0
fi

blocker_status="$(PR_VIEW_JSON="$pr_view_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["PR_VIEW_JSON"])
checks = payload.get("statusCheckRollup") or []
states = {item.get("state") for item in checks}

if "PENDING" in states or "EXPECTED" in states or "QUEUED" in states or "IN_PROGRESS" in states:
    print("checks_pending")
elif "FAILURE" in states or "ERROR" in states or "CANCELLED" in states or "TIMED_OUT" in states or "ACTION_REQUIRED" in states:
    print("checks_failed")
else:
    print("")
PY
)"

if [[ -n "$blocker_status" ]]; then
  json_success "$blocker_status"
  exit 0
fi

if [[ -n "$merge_state" && "$merge_state" != "CLEAN" ]]; then
  json_success "merge_conflict"
  exit 0
fi

if ! gh pr merge "$pr_ref" --merge --delete-branch=false >/dev/null; then
  json_failure \
    "GH_PR_MERGE_FAILED" \
    "gh_pr_merge" \
    "Could not merge the pull request."
fi

json_success "merged"
