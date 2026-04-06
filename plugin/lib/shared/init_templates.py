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

Migrating a data warehouse to **Vibedata Managed Fabric Lakehouse**. Source system: **Microsoft SQL Server** (T-SQL stored procedures).

Migration target: silver and gold dbt transformations on the Fabric Lakehouse endpoint. Bronze ingestion layers, ADF pipelines, and Power BI are out of scope.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source DDL access | DDL file MCP (`ddl_mcp`) | Pre-extracted `.sql` files; no live DB required |
| Live source DB access | `mssql` MCP via genai-toolbox | Requires `toolbox` binary on PATH |
| Transformation target | **dbt** (dbt-fabric adapter) | SQL models on Lakehouse endpoint |
| Storage | **Delta tables** on OneLake | Managed by Fabric Lakehouse |
| Orchestration | dbt build pipeline | |
| Platform | **Microsoft Fabric** on Azure | |

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
| `mssql` | HTTP (genai-toolbox) | Live SQL Server queries |

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

Data warehouse migration from Microsoft SQL Server to Vibedata Managed Fabric Lakehouse using dbt.

## Prerequisites

- **Python 3.11+**
- **uv** — Python package manager ([install](https://astral.sh/uv))
- **direnv** — credential management ([install](https://direnv.net)) — recommended
- **genai-toolbox** — for live SQL Server access ([releases](https://github.com/googleapis/genai-toolbox/releases)) — optional

### Credential setup with direnv

1. Copy the `.envrc` template and fill in your values:

   ```bash
   # .envrc (gitignored)
   export MSSQL_HOST=localhost
   export MSSQL_PORT=1433
   export MSSQL_DB=YourDatabase
   export SA_PASSWORD=YourPassword
   ```

2. Run `direnv allow` to load the variables.

These values are passed to the `mssql` MCP server at startup via environment inheritance — they must be set before launching `claude`.

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
└── dbt/               # dbt project (from init-dbt)
```

## Git Safety

A pre-commit hook in `.githooks/` blocks commits containing:

- Anthropic API keys (`sk-ant` prefix)
- `SA_PASSWORD` in `.mcp.json`
- MSSQL credentials in `.env` or `.envrc` files

The hook is a safety net — `.env`, `.envrc`, and `.mcp.json` are also in `.gitignore`.

## Commit Conventions

Commit messages use the format: `type: short description`

Examples: `feat: extract DDL from AdventureWorks`, `fix: correct column type mapping`
"""


def _envrc_sql_server() -> str:
    return """\
# SQL Server credentials for the mssql MCP server.
# Fill in your values and run `direnv allow`.
# This file is gitignored — do not commit it.

export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=
export SA_PASSWORD=
"""


def _repo_map_sql_server() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "project_type": "migration",
        "primary_languages": ["SQL"],
        "key_directories": {
            "ddl/": "Extracted DDL files from setup-ddl (tables.sql, procedures.sql, views.sql, functions.sql)",
            "catalog/": "Catalog JSON files — tables/, procedures/, views/, functions/ subdirectories",
            "dbt/": "dbt project (created by init-dbt)",
        },
        "notes_for_agents": {
            "startup": "Read this file before exploring the project. It is the primary startup context for structure and conventions.",
            "catalog_mandatory": "discover and scoping tools require catalog/ files from setup-ddl. Errors if catalog is missing.",
            "ddl_filenames": "The loader auto-detects object types from CREATE statements — .sql filenames are not significant.",
            "mssql_env_vars": "Live SQL Server access requires MSSQL_HOST, MSSQL_PORT, MSSQL_DB, SA_PASSWORD exported before launching claude.",
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

# 2. SA_PASSWORD in .mcp.json
if git diff --cached --name-only | grep -q '\\.mcp\\.json$'; then
    if git show :".mcp.json" 2>/dev/null | grep -q 'SA_PASSWORD'; then
        echo "ERROR: .mcp.json contains SA_PASSWORD. This file should be in .gitignore." >&2
        exit 1
    fi
fi

# 3. MSSQL credentials in .env / .envrc files
for f in $(git diff --cached --name-only | grep -E '\\.(env|envrc)$' || true); do
    if git show :"$f" 2>/dev/null | grep -qE '(MSSQL_HOST|MSSQL_PORT|MSSQL_DB|SA_PASSWORD)=.+'; then
        echo "ERROR: $f contains MSSQL credentials. This file should be in .gitignore." >&2
        exit 1
    fi
done
"""


# ── Oracle templates ────────────────────────────────────────────────────────


def _claude_md_oracle() -> str:
    return """\
# Migration Project

## Domain

Migrating a data warehouse to **Vibedata Managed Fabric Lakehouse**. Source system: **Oracle Database** (PL/SQL stored procedures).

Migration target: silver and gold dbt transformations on the Fabric Lakehouse endpoint. Bronze ingestion layers and ETL pipelines are out of scope.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source DDL access | DDL file MCP (`ddl_mcp`) | Pre-extracted `.sql` files; no live DB required |
| Live source DB access | Oracle MCP via SQLcl | Requires `sql` (SQLcl) binary and Java 11+ on PATH |
| Transformation target | **dbt** (dbt-fabric adapter) | SQL models on Lakehouse endpoint |
| Storage | **Delta tables** on OneLake | Managed by Fabric Lakehouse |
| Orchestration | dbt build pipeline | |
| Platform | **Microsoft Fabric** on Azure | |

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
| `oracle` | stdio (SQLcl) | Live Oracle queries |

**Important:** The Oracle MCP server does **not** auto-connect on startup. At the beginning of each session, run:

```text
mcp__oracle__run-sqlcl: connect $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_HOST:$ORACLE_PORT/$ORACLE_SERVICE
```

After connecting, use `mcp__oracle__run-sql` for queries and `mcp__oracle__schema-information` for metadata.

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

Data warehouse migration from Oracle Database to Vibedata Managed Fabric Lakehouse using dbt.

## Prerequisites

- **Python 3.11+**
- **uv** — Python package manager ([install](https://astral.sh/uv))
- **direnv** — credential management ([install](https://direnv.net)) — recommended
- **SQLcl** — Oracle SQL Developer Command Line ([install](https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/)) — required for live DB access
- **Java 11+** — required by SQLcl

### Credential setup with direnv

1. Copy the `.envrc` template and fill in your values:

   ```bash
   # .envrc (gitignored)
   export ORACLE_HOST=localhost
   export ORACLE_PORT=1521
   export ORACLE_SERVICE=FREEPDB1
   export ORACLE_USER=YourUser
   export ORACLE_PASSWORD=YourPassword
   ```

2. Run `direnv allow` to load the variables.

These values are used to connect the Oracle MCP server at session start — they must be set before launching `claude`.

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
└── dbt/               # dbt project (from init-dbt)
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
# Oracle credentials for the Oracle MCP server (SQLcl).
# Fill in your values and run `direnv allow`.
# This file is gitignored — do not commit it.

export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_SERVICE=FREEPDB1
export ORACLE_USER=
export ORACLE_PASSWORD=
"""


def _repo_map_oracle() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "project_type": "migration",
        "primary_languages": ["SQL"],
        "key_directories": {
            "ddl/": "Extracted DDL files from setup-ddl (tables.sql, procedures.sql, views.sql, functions.sql)",
            "catalog/": "Catalog JSON files — tables/, procedures/, views/, functions/ subdirectories",
            "dbt/": "dbt project (created by init-dbt)",
        },
        "notes_for_agents": {
            "startup": "Read this file before exploring the project. It is the primary startup context for structure and conventions.",
            "catalog_mandatory": "discover and scoping tools require catalog/ files from setup-ddl. Errors if catalog is missing.",
            "ddl_filenames": "The loader auto-detects object types from CREATE statements — .sql filenames are not significant.",
            "oracle_env_vars": "Live Oracle access requires ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD exported before launching claude.",
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
    if git show :"$f" 2>/dev/null | grep -qE '(ORACLE_HOST|ORACLE_PORT|ORACLE_SERVICE|ORACLE_USER|ORACLE_PASSWORD)=.+'; then
        echo "ERROR: $f contains Oracle credentials. This file should be in .gitignore." >&2
        exit 1
    fi
done
"""
