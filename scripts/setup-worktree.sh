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
