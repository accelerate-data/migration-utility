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
| `/scope-tables` | Public scoping command for writer discovery, procedure analysis, scope resolution, and catalog persistence |
| `/profile-tables` | Public profiling command for table profiling with approval gates |
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

1. Commit the scaffolded `.envrc` with shared non-secret settings, then put secrets in `.env`:

   ```bash
   # .envrc (tracked) — shared non-secret settings
   export SOURCE_MSSQL_HOST=localhost
   export SOURCE_MSSQL_PORT=1433
   export SOURCE_MSSQL_DB=YourDatabase
   export SOURCE_MSSQL_USER=sa

   # .env (gitignored) — local secrets
   export SOURCE_MSSQL_PASSWORD=YourPassword
   ```

2. Run `direnv allow` to load the variables.

## Workflow

1. **`/init-ad-migration`** — verify prerequisites and scaffold project files
2. **`/setup-ddl`** — extract DDL from live SQL Server into local artifact files
3. **`/listing-objects`** — browse the DDL catalog (list objects by type)
4. **`/profile-tables`** — profile tables interactively
5. **`/generate-model`** — generate dbt models from stored procedures

## Directory Structure

```text
.
├── CLAUDE.md          # Agent instructions
├── README.md          # This file
├── repo-map.json      # Directory structure for agent discovery
├── .envrc             # Shared non-secret config (tracked)
├── .env               # Local secrets (gitignored)
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
- Password fields or API-key fields in tracked files

The hook is a safety net — `.env` stays gitignored, while tracked files such as `.envrc` and `.mcp.json` must stay secret-free.

## Commit Conventions

Commit messages use the format: `type: short description`

Examples: `feat: extract DDL from AdventureWorks`, `fix: correct column type mapping`
"""


def _envrc_sql_server() -> str:
    return """\
# SQL Server shared non-secret settings for the ad-migration CLI.
# Keep secrets in `.env`, then run `direnv allow`.

source_env_if_exists .env

# Source database (used by setup-source)
export SOURCE_MSSQL_HOST=localhost
export SOURCE_MSSQL_PORT=1433
export SOURCE_MSSQL_DB=
export SOURCE_MSSQL_USER=sa

# Sandbox database (used by setup-sandbox)
export SANDBOX_MSSQL_HOST=localhost
export SANDBOX_MSSQL_PORT=1433
export SANDBOX_MSSQL_USER=sa

# Target database (used by setup-target)
export TARGET_MSSQL_HOST=localhost
export TARGET_MSSQL_PORT=1433
export TARGET_MSSQL_DB=
export TARGET_MSSQL_USER=sa
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

# 2. Secret-bearing settings in tracked files
for f in $(git diff --cached --name-only --diff-filter=ACMR || true); do
    if git show :"$f" 2>/dev/null | grep -qiE '(^|[^A-Za-z0-9_])(PASSWORD|[A-Za-z0-9_]*_PASSWORD|API_KEY|[A-Za-z0-9_]*_API_KEY)"?[[:space:]]*[:=]'; then
        echo "ERROR: $f contains a tracked secret field. Keep secrets in .env and out of tracked files." >&2
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
| `/scope-tables` | Public scoping command for writer discovery, procedure analysis, scope resolution, and catalog persistence |
| `/profile-tables` | Public profiling command for table profiling with approval gates |
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

1. Commit the scaffolded `.envrc` with shared non-secret settings, then put secrets in `.env`:

   ```bash
   # .envrc (tracked) — shared non-secret settings
   export SOURCE_ORACLE_HOST=localhost
   export SOURCE_ORACLE_PORT=1521
   export SOURCE_ORACLE_SERVICE=FREEPDB1
   export SOURCE_ORACLE_USER=YourUser

   # .env (gitignored) — local secrets
   export SOURCE_ORACLE_PASSWORD=YourPassword
   ```

2. Run `direnv allow` to load the variables.

## Workflow

1. **`/init-ad-migration`** — verify prerequisites and scaffold project files
2. **`/setup-ddl`** — extract DDL from live Oracle into local artifact files
3. **`/listing-objects`** — browse the DDL catalog (list objects by type)
4. **`/profile-tables`** — profile tables interactively
5. **`/generate-model`** — generate dbt models from stored procedures

## Directory Structure

```text
.
├── CLAUDE.md          # Agent instructions
├── README.md          # This file
├── repo-map.json      # Directory structure for agent discovery
├── .envrc             # Shared non-secret config (tracked)
├── .env               # Local secrets (gitignored)
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
- Password fields or API-key fields in tracked files

The hook is a safety net — `.env` stays gitignored, while tracked files such as `.envrc` and `.mcp.json` must stay secret-free.

## Commit Conventions

Commit messages use the format: `type: short description`

Examples: `feat: extract DDL from SH schema`, `fix: correct column type mapping`
"""


def _envrc_oracle() -> str:
    return """\
# Oracle shared non-secret settings for the ad-migration CLI.
# Keep secrets in `.env`, then run `direnv allow`.

source_env_if_exists .env

# Source database (used by setup-source)
export SOURCE_ORACLE_HOST=localhost
export SOURCE_ORACLE_PORT=1521
export SOURCE_ORACLE_SERVICE=FREEPDB1
export SOURCE_ORACLE_USER=

# Sandbox database (used by setup-sandbox)
export SANDBOX_ORACLE_HOST=localhost
export SANDBOX_ORACLE_PORT=1521
export SANDBOX_ORACLE_SERVICE=FREEPDB1
export SANDBOX_ORACLE_USER=

# Target database (used by setup-target)
export TARGET_ORACLE_HOST=localhost
export TARGET_ORACLE_PORT=1521
export TARGET_ORACLE_SERVICE=FREEPDB1
export TARGET_ORACLE_USER=
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

# 2. Secret-bearing settings in tracked files
for f in $(git diff --cached --name-only --diff-filter=ACMR || true); do
    if git show :"$f" 2>/dev/null | grep -qiE '(^|[^A-Za-z0-9_])(PASSWORD|[A-Za-z0-9_]*_PASSWORD|API_KEY|[A-Za-z0-9_]*_API_KEY)"?[[:space:]]*[:=]'; then
        echo "ERROR: $f contains a tracked secret field. Keep secrets in .env and out of tracked files." >&2
        exit 1
    fi
done
"""
