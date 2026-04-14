#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <branch-name>" >&2
  exit 1
fi

branch="$1"

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
worktree_base="${WORKTREE_BASE_DIR:-$repo_root/../worktrees}"
worktree_path="$worktree_base/$branch"

json_error() {
  local code="$1"
  local step="$2"
  local message="$3"
  local can_retry="$4"
  local retry_command="$5"
  local suggested_fix="$6"
  local existing_worktree_path="${7:-}"
  BRANCH="$branch" \
  REQUESTED_WORKTREE_PATH="$worktree_path" \
  CODE="$code" \
  STEP="$step" \
  MESSAGE="$message" \
  CAN_RETRY="$can_retry" \
  RETRY_COMMAND="$retry_command" \
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

bootstrap_worktree() {
  local env_src="$repo_root/.env"
  local env_dst="$worktree_path/.env"

  if [[ -f "$env_src" ]]; then
    if [[ -e "$env_dst" || -L "$env_dst" ]]; then
      rm -f "$env_dst"
    fi
    ln -s "$env_src" "$env_dst"
    echo "ENV: symlink $env_dst -> $env_src"
  else
    echo "ENV: skipped (no .env in $repo_root)"
  fi

  local promptfoo_src="$repo_root/tests/evals/.promptfoo"
  local promptfoo_dst="$worktree_path/tests/evals/.promptfoo"

  mkdir -p "$promptfoo_src"
  rm -rf "$promptfoo_dst"
  ln -s "$promptfoo_src" "$promptfoo_dst"
  echo "PROMPTFOO_DB: symlink $promptfoo_dst -> $promptfoo_src"

  if command -v direnv &>/dev/null && [[ -f "$worktree_path/.envrc" ]]; then
    direnv allow "$worktree_path" || json_error \
      "WORKTREE_DIRENV_ALLOW_FAILED" \
      "direnv_allow" \
      "direnv allow failed for the worktree." \
      "true" \
      "$0 $branch" \
      "Fix direnv or remove the broken .envrc, then rerun the worktree command."
    echo "direnv: allowed $worktree_path"
  else
    if ! command -v direnv &>/dev/null; then
      echo "direnv: skipped (not installed)"
    else
      echo "direnv: skipped (no .envrc in worktree)"
    fi
  fi

  local lib_dir="$worktree_path/lib"
  if [[ -f "$lib_dir/pyproject.toml" ]]; then
    echo "uv: syncing dev dependencies in $lib_dir"
    (
      cd "$lib_dir" &&
        uv sync --extra dev
    ) || json_error \
      "WORKTREE_UV_SYNC_FAILED" \
      "uv_sync" \
      "uv sync failed while creating the worktree environment." \
      "true" \
      "$0 $branch" \
      "Run 'cd $lib_dir && rm -rf .venv && uv sync --extra dev' to repair the environment, then rerun the worktree command."
    (
      cd "$lib_dir" &&
        uv run python -c 'import pyodbc, oracledb'
    ) || json_error \
      "WORKTREE_DEPENDENCY_VERIFICATION_FAILED" \
      "uv_verify_dependencies" \
      "The worktree environment does not import pyodbc and oracledb." \
      "true" \
      "$0 $branch" \
      "Run 'cd $lib_dir && rm -rf .venv && uv sync --extra dev' to reinstall the integration dependencies, then rerun the worktree command."
    echo "uv: verified worktree Python deps (pyodbc, oracledb)"
  else
    echo "uv: skipped (no pyproject.toml in lib)"
  fi

  local evals_dir="$worktree_path/tests/evals"
  if [[ -f "$evals_dir/package.json" ]]; then
    local npm_command=(
      install
      --no-audit
      --no-fund
    )
    local npm_command_str="npm install --no-audit --no-fund"

    if [[ -f "$evals_dir/package-lock.json" ]]; then
      npm_command=(
        ci
        --no-audit
        --no-fund
      )
      npm_command_str="npm ci --no-audit --no-fund"
    fi

    echo "npm: bootstrapping eval dependencies in $evals_dir with $npm_command_str"
    (
      cd "$evals_dir" &&
        npm "${npm_command[@]}"
    ) || json_error \
      "WORKTREE_NPM_INSTALL_FAILED" \
      "npm_install" \
      "npm dependency bootstrap failed for worktree eval dependencies." \
      "true" \
      "$0 $branch" \
      "Run 'cd $evals_dir && $npm_command_str' to repair node dependencies, then rerun the worktree command."
  else
    echo "npm: skipped (no package.json in tests/evals)"
  fi
}

mkdir -p "$(dirname "$worktree_path")"

branch_exists=false
if git show-ref --verify --quiet "refs/heads/$branch"; then
  branch_exists=true
fi

checked_out_path="$(existing_branch_worktree "$branch")"
if [[ -n "$checked_out_path" && "$checked_out_path" != "$worktree_path" ]]; then
  json_error \
    "WORKTREE_BRANCH_ALREADY_CHECKED_OUT" \
    "branch_conflict" \
    "Branch is already checked out in another worktree." \
    "false" \
    "" \
    "Use the existing worktree or remove it before requesting a new worktree for this branch." \
    "$checked_out_path"
fi

if [[ -n "$checked_out_path" && "$checked_out_path" == "$worktree_path" ]]; then
  echo "worktree: branch already attached at $worktree_path; rerunning bootstrap"
  bootstrap_worktree
  echo "worktree: ready $worktree_path"
  exit 0
fi

if $branch_exists; then
  git worktree add "$worktree_path" "$branch"
else
  git worktree add -b "$branch" "$worktree_path" HEAD
fi
echo "worktree: created worktree at $worktree_path"

bootstrap_worktree

echo "worktree: ready $worktree_path"
