#!/usr/bin/env bash
set -euo pipefail

usage_failure() {
  local actual_argv0="$1"

  SCRIPT_PATH="$actual_argv0" python3 - <<'PY' >&2
import json
import os

payload = {
    "code": "USAGE",
    "step": "argument_validation",
    "message": "Incorrect worktree helper usage.",
    "contract": "worktree.sh <branch> <worktree-name> <base-branch>",
    "retry_command": os.environ["SCRIPT_PATH"],
    "suggested_fix": "Call the helper with exactly three arguments: <branch> <worktree-name> <base-branch>.",
    "can_retry": False,
}
print(json.dumps(payload))
PY
  exit 2
}

if [[ $# -ne 3 ]]; then
  usage_failure "$0"
fi

branch="$1"
worktree_name="$2"
base_branch="$3"
repo_root="$(pwd)"
plugin_root="${CLAUDE_PLUGIN_ROOT:-}"
script_path="$0"
worktree_base=""
worktree_path=""

retry_command() {
  printf '%s %s %s %s' "$script_path" "$branch" "$worktree_name" "$base_branch"
}

json_failure() {
  local code="$1"
  local step="$2"
  local message="$3"
  local can_retry="$4"
  local suggested_fix="$5"
  local existing_worktree_path="${6:-}"

  BRANCH="$branch" \
  BASE_BRANCH="$base_branch" \
  WORKTREE_NAME="$worktree_name" \
  REQUESTED_WORKTREE_PATH="$worktree_path" \
  CODE="$code" \
  STEP="$step" \
  MESSAGE="$message" \
  CAN_RETRY="$can_retry" \
  RETRY_COMMAND="$(retry_command)" \
  SUGGESTED_FIX="$suggested_fix" \
  EXISTING_WORKTREE_PATH="$existing_worktree_path" \
  python3 - <<'PY' >&2
import json
import os

payload = {
    "code": os.environ["CODE"],
    "step": os.environ["STEP"],
    "message": os.environ["MESSAGE"],
    "branch": os.environ["BRANCH"],
    "base_branch": os.environ["BASE_BRANCH"],
    "worktree_name": os.environ["WORKTREE_NAME"],
    "requested_worktree_path": os.environ["REQUESTED_WORKTREE_PATH"],
    "can_retry": os.environ["CAN_RETRY"].lower() == "true",
    "retry_command": os.environ["RETRY_COMMAND"],
    "suggested_fix": os.environ["SUGGESTED_FIX"],
}
existing = os.environ.get("EXISTING_WORKTREE_PATH")
if existing:
    payload["existing_worktree_path"] = existing
print(json.dumps(payload))
PY
  exit 1
}

json_success() {
  local reused="$1"
  local worktree_path_value="$2"
  local existing_worktree_path="${3:-}"

  BRANCH="$branch" \
  BASE_BRANCH="$base_branch" \
  WORKTREE_NAME="$worktree_name" \
  WORKTREE_PATH="$worktree_path_value" \
  REUSED="$reused" \
  EXISTING_WORKTREE_PATH="$existing_worktree_path" \
  python3 - <<'PY'
import json
import os

payload = {
    "status": "ready",
    "branch": os.environ["BRANCH"],
    "base_branch": os.environ["BASE_BRANCH"],
    "worktree_name": os.environ["WORKTREE_NAME"],
    "worktree_path": os.environ["WORKTREE_PATH"],
    "reused": os.environ["REUSED"].lower() == "true",
}
existing = os.environ.get("EXISTING_WORKTREE_PATH")
if existing:
    payload["existing_worktree_path"] = existing
print(json.dumps(payload))
PY
}

resolve_repo_root() {
  local resolved_root=""

  if ! resolved_root="$(git rev-parse --show-toplevel)"; then
    worktree_base="${WORKTREE_BASE_DIR:-$repo_root/../worktrees}"
    worktree_path="$worktree_base/$branch"
    json_failure \
      "WORKTREE_REPO_ROOT_NOT_FOUND" \
      "git_rev_parse" \
      "Could not resolve the repository root from git." \
      "false" \
      "Run the helper from inside the customer project repository."
  fi

  repo_root="$resolved_root"
  plugin_root="${CLAUDE_PLUGIN_ROOT:-$repo_root}"
  script_path="$plugin_root/shared/scripts/worktree.sh"
  worktree_base="${WORKTREE_BASE_DIR:-$repo_root/../worktrees}"
  worktree_path="$worktree_base/$branch"
}

run_in_dir() {
  local dir="$1"
  shift
  (
    cd "$dir" &&
      "$@"
  )
}

run_step() {
  local code="$1"
  local step="$2"
  local message="$3"
  local suggested_fix="$4"
  shift 4

  if ! "$@"; then
    json_failure "$code" "$step" "$message" "true" "$suggested_fix"
  fi
}

ensure_clean_worktree() {
  local target_path="$1"
  local status_output=""

  if [[ ! -d "$target_path" ]]; then
    return 0
  fi

  if ! status_output="$(git -C "$target_path" status --porcelain)"; then
    json_failure \
      "WORKTREE_STATUS_CHECK_FAILED" \
      "dirty_state" \
      "Could not inspect the worktree state." \
      "false" \
      "Resolve the git worktree state before rerunning the helper." \
      "$target_path"
  fi

  if [[ -n "$status_output" ]]; then
    json_failure \
      "WORKTREE_DIRTY_STATE_DETECTED" \
      "dirty_state" \
      "The worktree has uncommitted changes." \
      "false" \
      "Commit, stash, or discard the changes before rerunning the helper." \
      "$target_path"
  fi
}

existing_branch_worktree() {
  local target_branch="$1"
  local current_path=""
  local current_branch=""

  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ -z "$line" ]]; then
      if [[ "$current_branch" == "refs/heads/$target_branch" ]]; then
        printf '%s\n' "$current_path"
        return 0
      fi
      current_path=""
      current_branch=""
      continue
    fi

    case "$line" in
      worktree\ *) current_path="${line#worktree }" ;;
      branch\ *) current_branch="${line#branch }" ;;
    esac
  done < <(git worktree list --porcelain)

  if [[ "$current_branch" == "refs/heads/$target_branch" ]]; then
    printf '%s\n' "$current_path"
  fi
}

link_env_file() {
  local env_src="$repo_root/.env"
  local env_dst="$worktree_path/.env"

  if [[ ! -f "$env_src" ]]; then
    echo "ENV: skipped (no .env in $repo_root)"
    return
  fi

  rm -f "$env_dst"
  ln -s "$env_src" "$env_dst"
  echo "ENV: symlink $env_dst -> $env_src"
}

bootstrap_worktree() {
  local lib_dir="$worktree_path/lib"
  local evals_dir="$worktree_path/tests/evals"
  local repair_command="cd $lib_dir && rm -rf .venv && uv sync --extra dev"
  local npm_command=(install --no-audit --no-fund)
  local npm_fix="Run 'cd $evals_dir && npm install --no-audit --no-fund' to repair node dependencies, then rerun the worktree command."

  link_env_file

  if ! command -v direnv >/dev/null 2>&1; then
    echo "direnv: skipped (not installed)"
  elif [[ ! -f "$worktree_path/.envrc" ]]; then
    echo "direnv: skipped (no .envrc in worktree)"
  else
    run_step \
      "WORKTREE_DIRENV_ALLOW_FAILED" \
      "direnv_allow" \
      "direnv allow failed for the worktree." \
      "Fix direnv or remove the broken .envrc, then rerun the worktree command." \
      direnv allow "$worktree_path"
    echo "direnv: allowed $worktree_path"
  fi

  if [[ ! -f "$lib_dir/pyproject.toml" ]]; then
    echo "uv: skipped (no pyproject.toml in lib)"
  else
    echo "uv: syncing dev dependencies in $lib_dir"
    run_step \
      "WORKTREE_UV_SYNC_FAILED" \
      "uv_sync" \
      "uv sync failed while creating the worktree environment." \
      "Run '$repair_command' to repair the environment, then rerun the worktree command." \
      run_in_dir "$lib_dir" uv sync --extra dev
    run_step \
      "WORKTREE_DEPENDENCY_VERIFICATION_FAILED" \
      "uv_verify_dependencies" \
      "The worktree environment does not import pyodbc and oracledb." \
      "Run '$repair_command' to reinstall the integration dependencies, then rerun the worktree command." \
      run_in_dir "$lib_dir" uv run python -c 'import pyodbc, oracledb'
    echo "uv: verified worktree Python deps (pyodbc, oracledb)"
  fi

  if [[ ! -f "$evals_dir/package.json" ]]; then
    echo "npm: skipped (no package.json in tests/evals)"
    return
  fi

  if [[ -f "$evals_dir/package-lock.json" ]]; then
    npm_command=(ci --no-audit --no-fund)
    npm_fix="Run 'cd $evals_dir && npm ci --no-audit --no-fund' to repair node dependencies, then rerun the worktree command."
  fi

  echo "npm: bootstrapping eval dependencies in $evals_dir with npm ${npm_command[*]}"
  run_step \
    "WORKTREE_NPM_INSTALL_FAILED" \
    "npm_install" \
    "npm dependency bootstrap failed for worktree eval dependencies." \
    "$npm_fix" \
    run_in_dir "$evals_dir" npm "${npm_command[@]}"
}

main() {
  local checked_out_path=""
  local branch_exists=false

  resolve_repo_root
  mkdir -p "$(dirname "$worktree_path")"

  if git show-ref --verify --quiet "refs/heads/$branch"; then
    branch_exists=true
  fi

  checked_out_path="$(existing_branch_worktree "$branch")"
  if [[ -n "$checked_out_path" && "$checked_out_path" != "$worktree_path" ]]; then
    ensure_clean_worktree "$checked_out_path"
    echo "worktree: branch already attached at $checked_out_path; reusing existing worktree"
    json_success "true" "$checked_out_path" "$checked_out_path"
    return
  fi

  if [[ -n "$checked_out_path" ]]; then
    ensure_clean_worktree "$worktree_path"
    echo "worktree: branch already attached at $worktree_path; rerunning bootstrap"
    bootstrap_worktree
    json_success "true" "$worktree_path" "$worktree_path"
    return
  fi

  if $branch_exists; then
    run_step \
      "WORKTREE_ADD_FAILED" \
      "git_worktree_add" \
      "git worktree add failed for the requested branch." \
      "Ensure the branch exists and the requested worktree path is available, then rerun the helper." \
      git worktree add "$worktree_path" "$branch"
  else
    run_step \
      "WORKTREE_CREATE_FAILED" \
      "git_worktree_create" \
      "git worktree create failed for the requested branch." \
      "Ensure the base branch exists and the requested branch name is valid, then rerun the helper." \
      git worktree add -b "$branch" "$worktree_path" "$base_branch"
  fi
  echo "worktree: created worktree at $worktree_path"

  bootstrap_worktree
  json_success "false" "$worktree_path"
}

main
