"""Shared scaffold template renderers for migration project init."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SourceTemplateConfig:
    """Technology-specific scaffold values consumed by shared renderers."""

    source_system: str
    procedure_language: str
    out_of_scope: str
    setup_ddl_source: str
    example_source: str
    env_prefix: str
    default_port: str
    readme_user: str
    source_database_var: str
    source_database_default: str
    sandbox_database_default: str | None
    target_database_default: str
    source_user_default: str
    example_database: str | None = None
    example_service: str | None = None

    @property
    def env_note_key(self) -> str:
        return f"{self.env_prefix.lower()}_env_vars"

    @property
    def env_var_summary(self) -> str:
        sandbox_database = (
            f"/{self.source_database_var}" if self.sandbox_database_default is not None else ""
        )
        return (
            f"SOURCE_{self.env_prefix}_HOST/PORT/{self.source_database_var}/USER/PASSWORD "
            f"(setup-source), SANDBOX_{self.env_prefix}_HOST/PORT/"
            f"{sandbox_database.lstrip('/') + '/' if sandbox_database else ''}USER/PASSWORD (setup-sandbox), "
            f"TARGET_{self.env_prefix}_HOST/PORT/{self.source_database_var}/USER/PASSWORD "
            f"(setup-target)."
        )


SOURCE_TEMPLATE_CONFIGS: dict[str, SourceTemplateConfig] = {
    "sql_server": SourceTemplateConfig(
        source_system="Microsoft SQL Server",
        procedure_language="T-SQL",
        out_of_scope="Bronze ingestion layers, ADF pipelines, and Power BI are out of scope.",
        setup_ddl_source="SQL Server",
        example_source="AdventureWorks",
        env_prefix="MSSQL",
        default_port="1433",
        readme_user="sa",
        source_database_var="DB",
        source_database_default="",
        sandbox_database_default=None,
        target_database_default="",
        source_user_default="sa",
        example_database="YourDatabase",
    ),
    "oracle": SourceTemplateConfig(
        source_system="Oracle Database",
        procedure_language="PL/SQL",
        out_of_scope="Bronze ingestion layers and ETL pipelines are out of scope.",
        setup_ddl_source="Oracle",
        example_source="SH schema",
        env_prefix="ORACLE",
        default_port="1521",
        readme_user="YourUser",
        source_database_var="SERVICE",
        source_database_default="FREEPDB1",
        sandbox_database_default="FREEPDB1",
        target_database_default="FREEPDB1",
        source_user_default="",
        example_service="FREEPDB1",
    ),
}


def _render_claude_md(config: SourceTemplateConfig) -> str:
    return f"""\
# Migration Project

## Domain

Migrating a data warehouse to **Vibedata Managed Warehouse Platform**. Source system: **{config.source_system}** ({config.procedure_language} stored procedures).

Migration target: silver and gold dbt transformations on the managed warehouse platform. {config.out_of_scope}

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
| `/setup-ddl` | Extract DDL from live {config.setup_ddl_source} and write local artifact files |
| `/listing-objects` | Browse the DDL catalog — list, show, refs |
| `/scope-tables` | Public scoping command for migration targets; discover writers for tables and analyze views/MVs |
| `/profile-tables` | Public profiling command for migration targets; profile tables, views, and MVs with approval gates |
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

## Completion Claims

Before stating that work is complete, successful, passing, PR-ready, merged, or stage-complete, run the completion-claim verification skill.

Verify fresh evidence for the exact claim: command output, exit code, run artifact, catalog writeback, dbt result, comparison result, git state, PR state, or coordinator plan state.

Do not repeat a sub-agent's success claim without inspecting the evidence it produced. If evidence is partial, stale, missing, or contradictory, report the actual state instead of using completion language.

## Commit Discipline

Commit at logical checkpoints so work is never lost mid-session.

| Checkpoint | When to commit |
|---|---|
| After DDL extraction | `setup-ddl` completes writing DDL files and catalog |
| After discovery | `discover` produces new analysis or annotations |
| After scoping | Scoping agent finalises scope configuration |
| After model generation | A dbt model is written or updated |
| After config changes | Manifest, project config, or schema changes |

Commit messages: `type: short description` (e.g. `feat: extract DDL from {config.example_source}`).

If not a git repository, skip commit steps silently.
"""


def _render_readme_md(config: SourceTemplateConfig) -> str:
    env_example = _render_readme_env_example(config)
    return f"""\
# Migration Project

Data warehouse migration from {config.source_system} to Vibedata Managed Warehouse Platform using dbt.

## Prerequisites

- **Python 3.11+**
- **uv** — Python package manager ([install](https://astral.sh/uv))
- **direnv** — credential management ([install](https://direnv.net)) — recommended

### Credential setup with direnv

1. Commit the scaffolded `.envrc` with shared non-secret settings, then put secrets in `.env`:

   ```bash
{env_example}
   ```

2. Run `direnv allow` to load the variables.

## Workflow

1. **`/init-ad-migration`** — verify prerequisites and scaffold project files
2. **`/setup-ddl`** — extract DDL from live {config.setup_ddl_source} into local artifact files
3. **`/listing-objects`** — browse the DDL catalog (list objects by type)
4. **`/scope-tables`** — scope migration targets; discover table writers and analyze views/MVs
5. **`/profile-tables`** — profile migration targets interactively
6. **`/generate-model`** — generate dbt models from stored procedures

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

Examples: `feat: extract DDL from {config.example_source}`, `fix: correct column type mapping`
"""


def _render_readme_env_example(config: SourceTemplateConfig) -> str:
    database_value = config.example_database or config.example_service or ""
    return f"""\
   # .envrc (tracked) — shared non-secret settings
   export SOURCE_{config.env_prefix}_HOST=localhost
   export SOURCE_{config.env_prefix}_PORT={config.default_port}
   export SOURCE_{config.env_prefix}_{config.source_database_var}={database_value}
   export SOURCE_{config.env_prefix}_USER={config.readme_user}

   # .env (gitignored) — local secrets
   export SOURCE_{config.env_prefix}_PASSWORD=YourPassword"""


def _render_envrc(config: SourceTemplateConfig) -> str:
    source_database_assignment = (
        f"export SOURCE_{config.env_prefix}_{config.source_database_var}="
        f"{config.source_database_default}\n"
    )
    sandbox_database_assignment = (
        f"export SANDBOX_{config.env_prefix}_{config.source_database_var}="
        f"{config.sandbox_database_default}\n"
        if config.sandbox_database_default is not None
        else ""
    )
    target_database_assignment = (
        f"export TARGET_{config.env_prefix}_{config.source_database_var}="
        f"{config.target_database_default}\n"
    )
    return f"""\
# {config.setup_ddl_source} shared non-secret settings for the ad-migration CLI.
# Keep secrets in `.env`, then run `direnv allow`.

source_env_if_exists .env

# Source database (used by setup-source)
export SOURCE_{config.env_prefix}_HOST=localhost
export SOURCE_{config.env_prefix}_PORT={config.default_port}
{source_database_assignment}export SOURCE_{config.env_prefix}_USER={config.source_user_default}

# Sandbox database (used by setup-sandbox)
export SANDBOX_{config.env_prefix}_HOST=localhost
export SANDBOX_{config.env_prefix}_PORT={config.default_port}
{sandbox_database_assignment}export SANDBOX_{config.env_prefix}_USER={config.source_user_default}

# Target database (used by setup-target)
export TARGET_{config.env_prefix}_HOST=localhost
export TARGET_{config.env_prefix}_PORT={config.default_port}
{target_database_assignment}export TARGET_{config.env_prefix}_USER={config.source_user_default}
"""


def _render_repo_map(config: SourceTemplateConfig) -> dict[str, Any]:
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
            config.env_note_key: f"CLI setup commands require {config.env_var_summary}",
        },
    }


def _render_pre_commit_hook() -> str:
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


def _claude_md_sql_server() -> str:
    return _render_claude_md(SOURCE_TEMPLATE_CONFIGS["sql_server"])


def _readme_md_sql_server() -> str:
    return _render_readme_md(SOURCE_TEMPLATE_CONFIGS["sql_server"])


def _envrc_sql_server() -> str:
    return _render_envrc(SOURCE_TEMPLATE_CONFIGS["sql_server"])


def _repo_map_sql_server() -> dict[str, Any]:
    return _render_repo_map(SOURCE_TEMPLATE_CONFIGS["sql_server"])


def _pre_commit_hook_sql_server() -> str:
    return _render_pre_commit_hook()


def _claude_md_oracle() -> str:
    return _render_claude_md(SOURCE_TEMPLATE_CONFIGS["oracle"])


def _readme_md_oracle() -> str:
    return _render_readme_md(SOURCE_TEMPLATE_CONFIGS["oracle"])


def _envrc_oracle() -> str:
    return _render_envrc(SOURCE_TEMPLATE_CONFIGS["oracle"])


def _repo_map_oracle() -> dict[str, Any]:
    return _render_repo_map(SOURCE_TEMPLATE_CONFIGS["oracle"])


def _pre_commit_hook_oracle() -> str:
    return _render_pre_commit_hook()
