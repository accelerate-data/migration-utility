"""init.py — Scaffold a new migration project.

Two subcommands:

    scaffold-project   Write CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, .claude/rules/git-workflow.md
    scaffold-hooks     Write .githooks/pre-commit and configure git hooks path

Both are idempotent: existing files are merged or skipped, never overwritten.

Usage (via uv):
    uv run --project <shared> init scaffold-project --project-root <dir> --technology sql_server
    uv run --project <shared> init scaffold-hooks --project-root <dir> --technology sql_server

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure
    2  IO error
"""

from __future__ import annotations

import json
import logging
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Source registry ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SourceConfig:
    """Technology-specific configuration for project scaffolding."""

    slug: str
    display_name: str
    env_vars: list[str]
    dep_group: str
    claude_md_fn: Callable[[], str]
    readme_md_fn: Callable[[], str]
    envrc_fn: Callable[[], str]
    repo_map_fn: Callable[[], dict[str, Any]]
    pre_commit_hook_fn: Callable[[], str]


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


# ── Registry ────────────────────────────────────────────────────────────────

SOURCE_REGISTRY: dict[str, SourceConfig] = {
    "sql_server": SourceConfig(
        slug="sql_server",
        display_name="SQL Server",
        env_vars=["MSSQL_HOST", "MSSQL_PORT", "MSSQL_DB", "SA_PASSWORD"],
        dep_group="export",
        claude_md_fn=_claude_md_sql_server,
        readme_md_fn=_readme_md_sql_server,
        envrc_fn=_envrc_sql_server,
        repo_map_fn=_repo_map_sql_server,
        pre_commit_hook_fn=_pre_commit_hook_sql_server,
    ),
    "oracle": SourceConfig(
        slug="oracle",
        display_name="Oracle",
        env_vars=["ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "ORACLE_PASSWORD"],
        dep_group="oracle",
        claude_md_fn=_claude_md_oracle,
        readme_md_fn=_readme_md_oracle,
        envrc_fn=_envrc_oracle,
        repo_map_fn=_repo_map_oracle,
        pre_commit_hook_fn=_pre_commit_hook_oracle,
    ),
}


def get_source_config(technology: str) -> SourceConfig:
    """Look up a source config by slug. Raises ValueError for unknown slugs."""
    if technology not in SOURCE_REGISTRY:
        raise ValueError(
            f"Unknown technology: {technology!r}. "
            f"Must be one of {sorted(SOURCE_REGISTRY.keys())}."
        )
    return SOURCE_REGISTRY[technology]


# ── Shared templates (technology-independent) ───────────────────────────────

GITIGNORE_ENTRIES = [
    "# Staging files from setup-ddl (intermediate MCP query results)",
    ".staging/",
    "",
    "# Intermediate CLI-ready test spec JSON (committed artifact is .yml)",
    "test-specs/*.json",
    "",
    "# Batch command run metadata",
    ".migration-runs/",
    "",
    "# Python",
    "__pycache__/",
    "*.pyc",
    ".venv/",
    "",
    "# OS",
    ".DS_Store",
    "Thumbs.db",
    "",
    "# Environment",
    ".env",
    ".env.*",
    "!.env.example",
    ".envrc",
    "",
    "# MCP server config (may contain credentials)",
    ".mcp.json",
    "",
    "# dbt build artifacts",
    "dbt/logs/",
    "dbt/target/",
]

GIT_WORKFLOW_MD = """\
# Git Workflow

## Worktrees

Worktree base path: `{worktree_base}`

Commands create worktrees at `<base>/<run-slug>` where `<run-slug>` is generated from the command name and table names (e.g. `scope-dimcustomer-dimproduct`).

## Cleanup

Run `/cleanup-worktrees` after PRs are merged to remove worktrees and branches.
"""

# ── Required sections for CLAUDE.md idempotency check ────────────────────────

_CLAUDE_MD_REQUIRED_SECTIONS = [
    "Domain",
    "Stack",
    "Directory Layout",
    "Skills",
    "MCP Servers",
    "Guardrails",
    "Skill Reasoning",
    "Output Framing",
    "Maintenance",
    "Commit Discipline",
]


# ── Business logic (run_* functions) ─────────────────────────────────────────


def run_scaffold_project(project_root: Path, technology: str = "sql_server") -> dict[str, Any]:
    """Scaffold project files. Idempotent: skips existing, merges .gitignore."""
    config = get_source_config(technology)
    files_created: list[str] = []
    files_updated: list[str] = []
    files_skipped: list[str] = []

    # CLAUDE.md
    claude_md_path = project_root / "CLAUDE.md"
    if not claude_md_path.exists():
        claude_md_path.write_text(config.claude_md_fn(), encoding="utf-8")
        files_created.append("CLAUDE.md")
        logger.info("event=scaffold_file file=CLAUDE.md status=created technology=%s", technology)
    else:
        content = claude_md_path.read_text(encoding="utf-8")
        missing = [s for s in _CLAUDE_MD_REQUIRED_SECTIONS if f"## {s}" not in content]
        if missing:
            files_skipped.append(f"CLAUDE.md (missing sections: {', '.join(missing)})")
            logger.warning(
                "event=scaffold_file file=CLAUDE.md status=skipped missing_sections=%s",
                missing,
            )
        else:
            files_skipped.append("CLAUDE.md")

    # README.md
    readme_path = project_root / "README.md"
    if not readme_path.exists():
        readme_path.write_text(config.readme_md_fn(), encoding="utf-8")
        files_created.append("README.md")
        logger.info("event=scaffold_file file=README.md status=created technology=%s", technology)
    else:
        files_skipped.append("README.md")

    # repo-map.json
    repo_map_path = project_root / "repo-map.json"
    if not repo_map_path.exists():
        repo_map_path.write_text(
            json.dumps(config.repo_map_fn(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        files_created.append("repo-map.json")
        logger.info("event=scaffold_file file=repo-map.json status=created technology=%s", technology)
    else:
        files_skipped.append("repo-map.json")

    # .gitignore
    gitignore_path = project_root / ".gitignore"
    if not gitignore_path.exists():
        gitignore_path.write_text("\n".join(GITIGNORE_ENTRIES) + "\n", encoding="utf-8")
        files_created.append(".gitignore")
        logger.info("event=scaffold_file file=.gitignore status=created")
    else:
        existing = gitignore_path.read_text(encoding="utf-8")
        existing_lines = {line.strip() for line in existing.splitlines()}
        new_entries: list[str] = []
        for entry in GITIGNORE_ENTRIES:
            stripped = entry.strip()
            if stripped and not stripped.startswith("#") and stripped not in existing_lines:
                new_entries.append(entry)
        if new_entries:
            addition = "\n" + "\n".join(new_entries) + "\n"
            gitignore_path.write_text(existing.rstrip("\n") + addition, encoding="utf-8")
            files_updated.append(f".gitignore (+{len(new_entries)} entries)")
            logger.info(
                "event=scaffold_file file=.gitignore status=updated entries_added=%d",
                len(new_entries),
            )
        else:
            files_skipped.append(".gitignore")

    # .envrc
    envrc_path = project_root / ".envrc"
    if not envrc_path.exists():
        envrc_path.write_text(config.envrc_fn(), encoding="utf-8")
        files_created.append(".envrc")
        logger.info("event=scaffold_file file=.envrc status=created technology=%s", technology)
    else:
        files_skipped.append(".envrc")

    # .claude/rules/git-workflow.md
    workflow_path = project_root / ".claude" / "rules" / "git-workflow.md"
    if not workflow_path.exists():
        workflow_path.parent.mkdir(parents=True, exist_ok=True)
        content = GIT_WORKFLOW_MD.format(worktree_base="../worktrees")
        workflow_path.write_text(content, encoding="utf-8")
        files_created.append(".claude/rules/git-workflow.md")
        logger.info(
            "event=scaffold_file file=.claude/rules/git-workflow.md status=created"
        )
    else:
        files_skipped.append(".claude/rules/git-workflow.md")

    return {
        "files_created": files_created,
        "files_updated": files_updated,
        "files_skipped": files_skipped,
    }


def run_scaffold_hooks(project_root: Path, technology: str = "sql_server") -> dict[str, Any]:
    """Create .githooks/pre-commit and configure git hooks path. Idempotent."""
    config = get_source_config(technology)
    hook_dir = project_root / ".githooks"
    hook_path = hook_dir / "pre-commit"
    hook_created = False
    hooks_path_configured = False

    if not hook_path.exists():
        hook_dir.mkdir(parents=True, exist_ok=True)
        hook_path.write_text(config.pre_commit_hook_fn(), encoding="utf-8")
        hook_path.chmod(hook_path.stat().st_mode | stat.S_IEXEC)
        hook_created = True
        logger.info("event=scaffold_hook file=.githooks/pre-commit status=created technology=%s", technology)
    else:
        logger.info("event=scaffold_hook file=.githooks/pre-commit status=skipped")

    # Configure git hooks path
    try:
        subprocess.run(
            ["git", "config", "core.hooksPath", ".githooks"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
        hooks_path_configured = True
        logger.info("event=configure_hooks_path status=success")
    except subprocess.CalledProcessError as exc:
        logger.warning(
            "event=configure_hooks_path status=failed error=%s",
            exc.stderr.strip(),
        )

    return {
        "hook_created": hook_created,
        "hooks_path_configured": hooks_path_configured,
    }


# ── CLI wrappers ─────────────────────────────────────────────────────────────


@app.command("scaffold-project")
def scaffold_project(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root directory (defaults to CWD)",
    ),
    technology: str = typer.Option(
        "sql_server", "--technology",
        help=f"Source technology: {', '.join(sorted(SOURCE_REGISTRY.keys()))}",
    ),
) -> None:
    """Scaffold CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, and .claude/rules/git-workflow.md."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_scaffold_project(project_root, technology)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result))


@app.command("scaffold-hooks")
def scaffold_hooks(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root directory (defaults to CWD)",
    ),
    technology: str = typer.Option(
        "sql_server", "--technology",
        help=f"Source technology: {', '.join(sorted(SOURCE_REGISTRY.keys()))}",
    ),
) -> None:
    """Create .githooks/pre-commit and configure git hooks path."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_scaffold_hooks(project_root, technology)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result))


if __name__ == "__main__":
    app()
