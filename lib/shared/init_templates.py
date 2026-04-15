"""init_templates.py — Technology-specific scaffold templates for migrate-util.

Contains template functions for SQL Server and Oracle project scaffolding.
Each function returns a string (or dict for repo_map) with the full file content.
"""

from __future__ import annotations

from typing import Any


# ── SQL Server templates ────────────────────────────────────────────────────


def _claude_md_sql_server() -> str:
    return """\
# Migration Project

## Domain

Migrating a data warehouse to **Vibedata Managed Warehouse Platform**. Source system: **Microsoft SQL Server** (T-SQL stored procedures).

Migration target: silver and gold dbt transformations on the managed warehouse platform. Bronze ingestion layers, ADF pipelines, and Power BI are out of scope.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source DDL access | DDL file MCP (`ddl_mcp`) | Pre-extracted `.sql` files; no live DB required |
| Transformation target | **dbt** | SQL models on the configured target runtime |
| Storage | Managed warehouse tables | Managed by the target platform |
| Orchestration | dbt build pipeline | |
| Platform | Vibedata managed warehouse platform | |

## Directory Layout

See `repo-map.json` for the full directory structure and agent notes.

## Skills

| Skill | Purpose |
|---|---|
| `/setup-ddl` | Extract DDL from live SQL Server and write local artifact files |
| `/listing-objects` | Browse the DDL catalog — list, show, refs |
| `/analyzing-table` | Writer discovery, procedure analysis, scope resolution, and catalog persistence |
| `/profiling-table` | Interactive single-table profiling with approval gates |
| `/generating-tests` | Generate ground truth test fixtures for a table |
| `/reviewing-tests` | Quality gate for test generation output |
| `/generate-model` | Generate dbt models from stored procedures |

## MCP Servers

| Server | Transport | Purpose |
|---|---|---|
| `ddl_mcp` | stdio | Structured DDL parsing from local `.sql` files |

## Git Workflow

Worktree conventions and PR format are in `.claude/rules/git-workflow.md`.

## Guardrails

- Never log or commit secrets (API keys, passwords, connection strings)
- All SQL parsing uses sqlglot — no regex fallbacks for DDL analysis
- Validate at system boundaries; trust internal library guarantees
- Pre-commit hook blocks common credential patterns (see `.githooks/pre-commit`)

## Maintenance

Update `repo-map.json` whenever files are added, removed, or renamed in structural directories (ddl/, catalog/, dbt/).

## Skill Reasoning

Before answering any LLM judgment step (classification, writer selection, statement tagging, profiling question), state your reasoning in 1–2 sentences. This trace must appear in the conversation log so reviewers and batch-run debuggers can see *why* a decision was made, not just *what* it was.

## Output Framing

Present results so the reader understands the output without mental overhead. Lead with the decision, then supporting evidence. At approval gates, the user should see the answer first and the reasoning second — not the other way around.

## Commit Discipline

Commit at logical checkpoints so work is never lost mid-session.

| Checkpoint | When to commit |
|---|---|
| After DDL extraction | `setup-ddl` completes writing DDL files and catalog |
| After discovery | `discover` produces new analysis or annotations |
| After scoping | Scoping agent finalises scope configuration |
| After model generation | A dbt model is written or updated |
| After config changes | Manifest, project config, or schema changes |

Commit messages: `type: short description` (e.g. `feat: extract DDL from AdventureWorks`).

If not a git repository, skip commit steps silently.
"""


def _readme_md_sql_server() -> str:
    return """\
# Migration Project

Data warehouse migration from Microsoft SQL Server to Vibedata Managed Warehouse Platform using dbt.

## Prerequisites

- **Python 3.11+**
- **uv** — Python package manager ([install](https://astral.sh/uv))
- **direnv** — credential management ([install](https://direnv.net)) — recommended

### Credential setup with direnv

1. Copy the `.envrc` template and fill in your values:

   ```bash
   # .envrc (gitignored) — source database
   export SOURCE_MSSQL_HOST=localhost
   export SOURCE_MSSQL_PORT=1433
   export SOURCE_MSSQL_DB=YourDatabase
   export SOURCE_MSSQL_USER=sa
   export SOURCE_MSSQL_PASSWORD=YourPassword
   ```

2. Run `direnv allow` to load the variables.

## Workflow

1. **`/init-ad-migration`** — verify prerequisites and scaffold project files
2. **`/setup-ddl`** — extract DDL from live SQL Server into local artifact files
3. **`/listing-objects`** — browse the DDL catalog (list objects by type)
4. **`/profiling-table`** — profile individual tables interactively
5. **`/generate-model`** — generate dbt models from stored procedures

## Directory Structure

```text
.
├── CLAUDE.md          # Agent instructions
├── README.md          # This file
├── repo-map.json      # Directory structure for agent discovery
├── .envrc             # Credentials (gitignored)
├── .gitignore         # Git ignore rules
├── .githooks/         # Git hooks (pre-commit secret blocking)
├── ddl/               # Extracted DDL files (from setup-ddl)
├── catalog/           # Catalog JSON files (from setup-ddl)
├── manifest.json      # Extraction manifest (from setup-ddl)
└── dbt/               # dbt project (from setup-target)
```

## Git Safety

A pre-commit hook in `.githooks/` blocks commits containing:

- Anthropic API keys (`sk-ant` prefix)
- SQL Server credentials (`SOURCE_MSSQL_*`, `SANDBOX_MSSQL_*`, `TARGET_MSSQL_*`) in `.env` or `.envrc` files

The hook is a safety net — `.env`, `.envrc`, and `.mcp.json` are also in `.gitignore`.

## Commit Conventions

Commit messages use the format: `type: short description`

Examples: `feat: extract DDL from AdventureWorks`, `fix: correct column type mapping`
"""


def _envrc_sql_server() -> str:
    return """\
# SQL Server credentials for the ad-migration CLI.
# Fill in your values and run `direnv allow`.
# This file is gitignored — do not commit it.

source_env_if_exists .env

# Source database (used by setup-source)
export SOURCE_MSSQL_HOST=localhost
export SOURCE_MSSQL_PORT=1433
export SOURCE_MSSQL_DB=
export SOURCE_MSSQL_USER=sa
export SOURCE_MSSQL_PASSWORD=

# Sandbox database (used by setup-sandbox)
export SANDBOX_MSSQL_HOST=localhost
export SANDBOX_MSSQL_PORT=1433
export SANDBOX_MSSQL_USER=sa
export SANDBOX_MSSQL_PASSWORD=

# Target database (used by setup-target)
export TARGET_MSSQL_HOST=localhost
export TARGET_MSSQL_PORT=1433
export TARGET_MSSQL_DB=
export TARGET_MSSQL_USER=sa
export TARGET_MSSQL_PASSWORD=
"""


def _repo_map_sql_server() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "project_type": "migration",
        "primary_languages": ["SQL"],
        "key_directories": {
            "ddl/": "Extracted DDL files from setup-ddl (tables.sql, procedures.sql, views.sql, functions.sql)",
            "catalog/": "Catalog JSON files — tables/, procedures/, views/, functions/ subdirectories",
            "dbt/": "dbt project (created by setup-target)",
        },
        "notes_for_agents": {
            "startup": "Read this file before exploring the project. It is the primary startup context for structure and conventions.",
            "catalog_mandatory": "discover and scoping tools require catalog/ files from setup-ddl. Errors if catalog is missing.",
            "ddl_filenames": "The loader auto-detects object types from CREATE statements — .sql filenames are not significant.",
            "mssql_env_vars": "CLI setup commands require SOURCE_MSSQL_HOST/PORT/DB/USER/PASSWORD (setup-source), SANDBOX_MSSQL_HOST/PORT/USER/PASSWORD (setup-sandbox), TARGET_MSSQL_HOST/PORT/DB/USER/PASSWORD (setup-target).",
        },
    }


def _pre_commit_hook_sql_server() -> str:
    return """\
#!/usr/bin/env bash
# Pre-commit hook: block secrets from being committed.
# Installed by: uv run init scaffold-hooks

set -euo pipefail

# 1. Anthropic API keys anywhere in staged files
ANT_KEY_PAT="sk-ant"; ANT_KEY_PAT="${ANT_KEY_PAT}-"
if git diff --cached --diff-filter=ACMR -z -- . | xargs -0 grep -lE "${ANT_KEY_PAT}\\S+" 2>/dev/null; then
    echo "ERROR: Staged file contains an Anthropic API key. Remove it before committing." >&2
    exit 1
fi

# 2. SQL Server credentials in .env / .envrc files
for f in $(git diff --cached --name-only | grep -E '\\.(env|envrc)$' || true); do
    if git show :"$f" 2>/dev/null | grep -qE '(SOURCE_MSSQL_|SANDBOX_MSSQL_|TARGET_MSSQL_)(HOST|PORT|DB|USER|PASSWORD)=.+'; then
        echo "ERROR: $f contains SQL Server credentials. This file should be in .gitignore." >&2
        exit 1
    fi
done
"""


# ── Oracle templates ────────────────────────────────────────────────────────


def _claude_md_oracle() -> str:
    return """\
# Migration Project

## Domain

Migrating a data warehouse to **Vibedata Managed Warehouse Platform**. Source system: **Oracle Database** (PL/SQL stored procedures).

Migration target: silver and gold dbt transformations on the managed warehouse platform. Bronze ingestion layers and ETL pipelines are out of scope.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source DDL access | DDL file MCP (`ddl_mcp`) | Pre-extracted `.sql` files; no live DB required |
| Transformation target | **dbt** | SQL models on the configured target runtime |
| Storage | Managed warehouse tables | Managed by the target platform |
| Orchestration | dbt build pipeline | |
| Platform | Vibedata managed warehouse platform | |

## Directory Layout

See `repo-map.json` for the full directory structure and agent notes.

## Skills

| Skill | Purpose |
|---|---|
| `/setup-ddl` | Extract DDL from live Oracle and write local artifact files |
| `/listing-objects` | Browse the DDL catalog — list, show, refs |
| `/analyzing-table` | Writer discovery, procedure analysis, scope resolution, and catalog persistence |
| `/profiling-table` | Interactive single-table profiling with approval gates |
| `/generating-tests` | Generate ground truth test fixtures for a table |
| `/reviewing-tests` | Quality gate for test generation output |
| `/generate-model` | Generate dbt models from stored procedures |

## MCP Servers

| Server | Transport | Purpose |
|---|---|---|
| `ddl_mcp` | stdio | Structured DDL parsing from local `.sql` files |

## Git Workflow

Worktree conventions and PR format are in `.claude/rules/git-workflow.md`.

## Guardrails

- Never log or commit secrets (API keys, passwords, connection strings)
- All SQL parsing uses sqlglot — no regex fallbacks for DDL analysis
- Validate at system boundaries; trust internal library guarantees
- Pre-commit hook blocks common credential patterns (see `.githooks/pre-commit`)

## Maintenance

Update `repo-map.json` whenever files are added, removed, or renamed in structural directories (ddl/, catalog/, dbt/).

## Skill Reasoning

Before answering any LLM judgment step (classification, writer selection, statement tagging, profiling question), state your reasoning in 1–2 sentences. This trace must appear in the conversation log so reviewers and batch-run debuggers can see *why* a decision was made, not just *what* it was.

## Output Framing

Present results so the reader understands the output without mental overhead. Lead with the decision, then supporting evidence. At approval gates, the user should see the answer first and the reasoning second — not the other way around.

## Commit Discipline

Commit at logical checkpoints so work is never lost mid-session.

| Checkpoint | When to commit |
|---|---|
| After DDL extraction | `setup-ddl` completes writing DDL files and catalog |
| After discovery | `discover` produces new analysis or annotations |
| After scoping | Scoping agent finalises scope configuration |
| After model generation | A dbt model is written or updated |
| After config changes | Manifest, project config, or schema changes |

Commit messages: `type: short description` (e.g. `feat: extract DDL from SH schema`).

If not a git repository, skip commit steps silently.
"""


def _readme_md_oracle() -> str:
    return """\
# Migration Project

Data warehouse migration from Oracle Database to Vibedata Managed Warehouse Platform using dbt.

## Prerequisites

- **Python 3.11+**
- **uv** — Python package manager ([install](https://astral.sh/uv))
- **direnv** — credential management ([install](https://direnv.net)) — recommended

### Credential setup with direnv

1. Copy the `.envrc` template and fill in your values:

   ```bash
   # .envrc (gitignored) — source database
   export SOURCE_ORACLE_HOST=localhost
   export SOURCE_ORACLE_PORT=1521
   export SOURCE_ORACLE_SERVICE=FREEPDB1
   export SOURCE_ORACLE_USER=YourUser
   export SOURCE_ORACLE_PASSWORD=YourPassword
   ```

2. Run `direnv allow` to load the variables.

## Workflow

1. **`/init-ad-migration`** — verify prerequisites and scaffold project files
2. **`/setup-ddl`** — extract DDL from live Oracle into local artifact files
3. **`/listing-objects`** — browse the DDL catalog (list objects by type)
4. **`/profiling-table`** — profile individual tables interactively
5. **`/generate-model`** — generate dbt models from stored procedures

## Directory Structure

```text
.
├── CLAUDE.md          # Agent instructions
├── README.md          # This file
├── repo-map.json      # Directory structure for agent discovery
├── .envrc             # Credentials (gitignored)
├── .gitignore         # Git ignore rules
├── .githooks/         # Git hooks (pre-commit secret blocking)
├── ddl/               # Extracted DDL files (from setup-ddl)
├── catalog/           # Catalog JSON files (from setup-ddl)
├── manifest.json      # Extraction manifest (from setup-ddl)
└── dbt/               # dbt project (from setup-target)
```

## Git Safety

A pre-commit hook in `.githooks/` blocks commits containing:

- Anthropic API keys (`sk-ant` prefix)
- Oracle credentials in `.env` or `.envrc` files

The hook is a safety net — `.env`, `.envrc`, and `.mcp.json` are also in `.gitignore`.

## Commit Conventions

Commit messages use the format: `type: short description`

Examples: `feat: extract DDL from SH schema`, `fix: correct column type mapping`
"""


def _envrc_oracle() -> str:
    return """\
# Oracle credentials for the ad-migration CLI.
# Fill in your values and run `direnv allow`.
# This file is gitignored — do not commit it.

source_env_if_exists .env

# Source database (used by setup-source)
export SOURCE_ORACLE_HOST=localhost
export SOURCE_ORACLE_PORT=1521
export SOURCE_ORACLE_SERVICE=FREEPDB1
export SOURCE_ORACLE_USER=
export SOURCE_ORACLE_PASSWORD=

# Sandbox database (used by setup-sandbox)
export SANDBOX_ORACLE_HOST=localhost
export SANDBOX_ORACLE_PORT=1521
export SANDBOX_ORACLE_SERVICE=FREEPDB1
export SANDBOX_ORACLE_USER=
export SANDBOX_ORACLE_PASSWORD=

# Target database (used by setup-target)
export TARGET_ORACLE_HOST=localhost
export TARGET_ORACLE_PORT=1521
export TARGET_ORACLE_SERVICE=FREEPDB1
export TARGET_ORACLE_USER=
export TARGET_ORACLE_PASSWORD=
"""


def _repo_map_oracle() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "project_type": "migration",
        "primary_languages": ["SQL"],
        "key_directories": {
            "ddl/": "Extracted DDL files from setup-ddl (tables.sql, procedures.sql, views.sql, functions.sql)",
            "catalog/": "Catalog JSON files — tables/, procedures/, views/, functions/ subdirectories",
            "dbt/": "dbt project (created by setup-target)",
        },
        "notes_for_agents": {
            "startup": "Read this file before exploring the project. It is the primary startup context for structure and conventions.",
            "catalog_mandatory": "discover and scoping tools require catalog/ files from setup-ddl. Errors if catalog is missing.",
            "ddl_filenames": "The loader auto-detects object types from CREATE statements — .sql filenames are not significant.",
            "oracle_env_vars": "CLI setup commands require SOURCE_ORACLE_HOST/PORT/SERVICE/USER/PASSWORD (setup-source), SANDBOX_ORACLE_HOST/PORT/SERVICE/USER/PASSWORD (setup-sandbox), TARGET_ORACLE_HOST/PORT/SERVICE/USER/PASSWORD (setup-target).",
        },
    }


def _pre_commit_hook_oracle() -> str:
    return """\
#!/usr/bin/env bash
# Pre-commit hook: block secrets from being committed.
# Installed by: uv run init scaffold-hooks

set -euo pipefail

# 1. Anthropic API keys anywhere in staged files
ANT_KEY_PAT="sk-ant"; ANT_KEY_PAT="${ANT_KEY_PAT}-"
if git diff --cached --diff-filter=ACMR -z -- . | xargs -0 grep -lE "${ANT_KEY_PAT}\\S+" 2>/dev/null; then
    echo "ERROR: Staged file contains an Anthropic API key. Remove it before committing." >&2
    exit 1
fi

# 2. Oracle credentials in .env / .envrc files
for f in $(git diff --cached --name-only | grep -E '\\.(env|envrc)$' || true); do
    if git show :"$f" 2>/dev/null | grep -qE '(SOURCE_ORACLE_|SANDBOX_ORACLE_|TARGET_ORACLE_)(HOST|PORT|SERVICE|USER|PASSWORD)=.+'; then
        echo "ERROR: $f contains Oracle credentials. This file should be in .gitignore." >&2
        exit 1
    fi
done
"""


def _worktree_sh() -> str:
    return """\
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
        printf '%s\\n' "$current_path"
        return 0
      fi
      current_path=""
      current_branch=""
      continue
    fi
    case "$line" in
      worktree\\ *) current_path="${line#worktree }" ;;
      branch\\ *) current_branch="${line#branch }" ;;
    esac
  done < <(git worktree list --porcelain)

  if [[ "$current_branch" == "refs/heads/$target_branch" ]]; then
    printf '%s\\n' "$current_path"
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

  if command -v direnv &>/dev/null && [[ -f "$worktree_path/.envrc" ]]; then
    direnv allow "$worktree_path" || json_error \\
      "WORKTREE_DIRENV_ALLOW_FAILED" \\
      "direnv_allow" \\
      "direnv allow failed for the worktree." \\
      "true" \\
      "$0 $branch" \\
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
    ) || json_error \\
      "WORKTREE_UV_SYNC_FAILED" \\
      "uv_sync" \\
      "uv sync failed while creating the worktree environment." \\
      "true" \\
      "$0 $branch" \\
      "Run 'cd $lib_dir && rm -rf .venv && uv sync --extra dev' to repair the environment, then rerun the worktree command."
    (
      cd "$lib_dir" &&
        uv run python -c 'import pyodbc, oracledb'
    ) || json_error \\
      "WORKTREE_DEPENDENCY_VERIFICATION_FAILED" \\
      "uv_verify_dependencies" \\
      "The worktree environment does not import pyodbc and oracledb." \\
      "true" \\
      "$0 $branch" \\
      "Run 'cd $lib_dir && rm -rf .venv && uv sync --extra dev' to reinstall the integration dependencies, then rerun the worktree command."
    echo "uv: verified worktree Python deps (pyodbc, oracledb)"
  else
    echo "uv: skipped (no pyproject.toml in lib)"
  fi

  local evals_dir="$worktree_path/tests/evals"
  if [[ -f "$evals_dir/package.json" ]]; then
    echo "npm: installing eval dependencies in $evals_dir"
    (
      cd "$evals_dir" &&
        npm install --no-audit --no-fund
    ) || json_error \\
      "WORKTREE_NPM_INSTALL_FAILED" \\
      "npm_install" \\
      "npm install failed for worktree eval dependencies." \\
      "true" \\
      "$0 $branch" \\
      "Run 'cd $evals_dir && npm install --no-audit --no-fund' to repair node dependencies, then rerun the worktree command."
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
  json_error \\
    "WORKTREE_BRANCH_ALREADY_CHECKED_OUT" \\
    "branch_conflict" \\
    "Branch is already checked out in another worktree." \\
    "false" \\
    "" \\
    "Use the existing worktree or remove it before requesting a new worktree for this branch." \\
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
"""
