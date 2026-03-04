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

# Override with MIGRATION_UTILITY_DB_PATH when needed.
default_db_path="$HOME/Library/Application Support/com.vibedata.migration-utility/migration-utility.db"
db_path="${MIGRATION_UTILITY_DB_PATH:-$default_db_path}"

if [[ ! -f "$db_path" ]]; then
  echo "SQLite DB not found: $db_path" >&2
  echo "Launch the app once to create it, or set MIGRATION_UTILITY_DB_PATH." >&2
  exit 1
fi

local_dir="$worktree_path/.local"
link_path="$local_dir/migration-utility.db"

mkdir -p "$local_dir"

if [[ -e "$link_path" || -L "$link_path" ]]; then
  rm -f "$link_path"
fi

# Prefer hard link so consumers that reject symlinks still see a regular file.
# Falls back to symlink if hard-linking is not possible.
if ln "$db_path" "$link_path" 2>/dev/null; then
  echo "Created hard link: $link_path -> $db_path"
  exit 0
fi

ln -s "$db_path" "$link_path"
echo "Created symlink fallback: $link_path -> $db_path"
