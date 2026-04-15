#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <branch-name>" >&2
  exit 1
fi

branch="$1"

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
worktree_base="${WORKTREE_BASE_DIR:-$repo_root/../worktrees}"
worktree_path="$worktree_base/$branch"

retry_command() {
  printf '%s %s' "$0" "$branch"
}

fail_json() {
  local code="$1"
  local step="$2"
  local message="$3"
  local can_retry="$4"
  local suggested_fix="$5"
  local existing_worktree_path="${6:-}"
  local retry_cmd="${7:-$(retry_command)}"

  BRANCH="$branch" \
  REQUESTED_WORKTREE_PATH="$worktree_path" \
  CODE="$code" \
  STEP="$step" \
  MESSAGE="$message" \
  CAN_RETRY="$can_retry" \
  RETRY_COMMAND="$retry_cmd" \
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
    fail_json "$code" "$step" "$message" "true" "$suggested_fix"
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

  mkdir -p "$(dirname "$worktree_path")"

  if git show-ref --verify --quiet "refs/heads/$branch"; then
    branch_exists=true
  fi

  checked_out_path="$(existing_branch_worktree "$branch")"
  if [[ -n "$checked_out_path" && "$checked_out_path" != "$worktree_path" ]]; then
    fail_json \
      "WORKTREE_BRANCH_ALREADY_CHECKED_OUT" \
      "branch_conflict" \
      "Branch is already checked out in another worktree." \
      "false" \
      "Use the existing worktree or remove it before requesting a new worktree for this branch." \
      "$checked_out_path" \
      ""
  fi

  if [[ -n "$checked_out_path" ]]; then
    echo "worktree: branch already attached at $worktree_path; rerunning bootstrap"
    bootstrap_worktree
    echo "worktree: ready $worktree_path"
    return
  fi

  if $branch_exists; then
    git worktree add "$worktree_path" "$branch"
  else
    git worktree add -b "$branch" "$worktree_path" HEAD
  fi
  echo "worktree: created worktree at $worktree_path"

  bootstrap_worktree
  echo "worktree: ready $worktree_path"
}

main
