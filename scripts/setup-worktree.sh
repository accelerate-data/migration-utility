#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <worktree-path>" >&2
  exit 1
fi

worktree_path="$1"

if [[ ! -d "$worktree_path" ]]; then
  echo "Worktree path does not exist: $worktree_path" >&2
  exit 1
fi

# Resolve the main repo root (the directory containing this script).
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

# ── 1. Symlink .env from main repo ─────────────────────────────────────────

env_src="$repo_root/.env"
env_dst="$worktree_path/.env"

if [[ -f "$env_src" ]]; then
  if [[ -e "$env_dst" || -L "$env_dst" ]]; then
    rm -f "$env_dst"
  fi
  ln -s "$env_src" "$env_dst"
  echo "ENV: symlink $env_dst -> $env_src"
else
  echo "ENV: skipped (no .env in $repo_root)"
fi

# ── 2. direnv allow ────────────────────────────────────────────────────────

if command -v direnv &>/dev/null && [[ -f "$worktree_path/.envrc" ]]; then
  direnv allow "$worktree_path"
  echo "direnv: allowed $worktree_path"
else
  if ! command -v direnv &>/dev/null; then
    echo "direnv: skipped (not installed)"
  else
    echo "direnv: skipped (no .envrc in worktree)"
  fi
fi

# ── 3. Sync Python dev dependencies ──────────────────────────────────────

lib_dir="$worktree_path/plugin/lib"
if [[ -f "$lib_dir/pyproject.toml" ]]; then
  echo "uv: syncing dev dependencies in $lib_dir"
  (cd "$lib_dir" && uv sync --extra dev 2>&1) || echo "uv: sync failed (non-fatal)"
else
  echo "uv: skipped (no pyproject.toml in plugin/lib)"
fi

# ── 4. Install Node.js eval dependencies ─────────────────────────────────

evals_dir="$worktree_path/tests/evals"
if [[ -f "$evals_dir/package.json" ]]; then
  echo "npm: installing eval dependencies in $evals_dir"
  (cd "$evals_dir" && npm install --no-audit --no-fund 2>&1) || echo "npm: install failed (non-fatal)"
else
  echo "npm: skipped (no package.json in tests/evals)"
fi
