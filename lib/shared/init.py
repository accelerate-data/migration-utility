"""init.py — Scaffold a new migration project.

Two subcommands:

    scaffold-project   Write CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, scripts/worktree.sh, .claude/rules/git-workflow.md
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
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import typer

from shared.freetds import run_check_freetds
from shared.init_templates import (
    _claude_md_oracle,
    _claude_md_sql_server,
    _envrc_oracle,
    _envrc_sql_server,
    _pre_commit_hook_oracle,
    _pre_commit_hook_sql_server,
    _readme_md_oracle,
    _readme_md_sql_server,
    _repo_map_oracle,
    _repo_map_sql_server,
    _worktree_sh,
)
from shared.output_models.init import (
    LocalOverrideDiscoveryOutput,
    LocalEnvOverrideWriteOutput,
    ScaffoldHooksOutput,
    ScaffoldProjectOutput,
)

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


# ── Registry ────────────────────────────────────────────────────────────────

SOURCE_REGISTRY: dict[str, SourceConfig] = {
    "sql_server": SourceConfig(
        slug="sql_server",
        display_name="SQL Server",
        env_vars=["SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PORT", "SOURCE_MSSQL_DB", "SOURCE_MSSQL_USER", "SOURCE_MSSQL_PASSWORD"],
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
        env_vars=["SOURCE_ORACLE_HOST", "SOURCE_ORACLE_PORT", "SOURCE_ORACLE_SERVICE", "SOURCE_ORACLE_USER", "SOURCE_ORACLE_PASSWORD"],
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
]

GIT_WORKFLOW_MD = """\
# Git Workflow

## Worktrees

Worktree base path: `{worktree_base}`

Use `./scripts/worktree.sh <branch-name>` to create or attach a worktree and bootstrap it in one step.

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

_ENV_ASSIGNMENT_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=")
_ENVRC_DOTENV_LINE = "source_env_if_exists .env"
DEFAULT_MSSQL_DRIVER = "FreeTDS"
SQL_SERVER_DRIVER_CANDIDATES = (
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
)
MSSQL_DRIVER_MANUAL_MESSAGE = (
    'Set MSSQL_DRIVER="ODBC Driver 18 for SQL Server" after installing a SQL Server ODBC driver.'
)


# ── Business logic (run_* functions) ─────────────────────────────────────────


def run_scaffold_project(project_root: Path, technology: str = "sql_server") -> ScaffoldProjectOutput:
    """Scaffold project files. Idempotent: skips existing, merges .gitignore."""
    config = get_source_config(technology)
    project_root.mkdir(parents=True, exist_ok=True)
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
        envrc_text = envrc_path.read_text(encoding="utf-8")
        if _ENVRC_DOTENV_LINE not in envrc_text:
            updated_envrc = envrc_text.rstrip("\n")
            if updated_envrc:
                updated_envrc += "\n\n"
            updated_envrc += f"{_ENVRC_DOTENV_LINE}\n"
            envrc_path.write_text(updated_envrc, encoding="utf-8")
            files_updated.append(".envrc (+local .env loader)")
            logger.info(
                "event=scaffold_file file=.envrc status=updated technology=%s",
                technology,
            )
        else:
            files_skipped.append(".envrc")

    # scripts/worktree.sh
    worktree_script_path = project_root / "scripts" / "worktree.sh"
    if not worktree_script_path.exists():
        worktree_script_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_script_path.write_text(_worktree_sh(), encoding="utf-8")
        worktree_script_path.chmod(worktree_script_path.stat().st_mode | stat.S_IEXEC)
        files_created.append("scripts/worktree.sh")
        logger.info("event=scaffold_file file=scripts/worktree.sh status=created")
    else:
        files_skipped.append("scripts/worktree.sh")

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

    return ScaffoldProjectOutput(
        files_created=files_created,
        files_updated=files_updated,
        files_skipped=files_skipped,
    )


def _quote_env_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _query_odbc_drivers() -> list[str]:
    try:
        output = subprocess.run(
            ["odbcinst", "-q", "-d"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        return []
    return [line.strip().strip("[]") for line in output.splitlines() if line.strip()]


def _is_executable_file(path_str: str) -> bool:
    path = Path(path_str)
    return path.is_file() and os.access(path, os.X_OK)


def run_discover_mssql_driver_override() -> LocalOverrideDiscoveryOutput:
    """Resolve the effective local SQL Server ODBC driver override."""
    installed_drivers = _query_odbc_drivers()
    configured_driver = (os.environ.get("MSSQL_DRIVER", "") or "").strip()
    if configured_driver:
        if configured_driver not in installed_drivers:
            logger.warning(
                "event=discover_local_override key=MSSQL_DRIVER status=manual source=env_invalid",
            )
            return LocalOverrideDiscoveryOutput(
                key="MSSQL_DRIVER",
                status="manual",
                message=MSSQL_DRIVER_MANUAL_MESSAGE,
            )
        if configured_driver == DEFAULT_MSSQL_DRIVER:
            logger.info(
                "event=discover_local_override key=MSSQL_DRIVER status=default source=env",
            )
            return LocalOverrideDiscoveryOutput(
                key="MSSQL_DRIVER",
                status="default",
            )
        logger.info(
            "event=discover_local_override key=MSSQL_DRIVER status=resolved source=env",
        )
        return LocalOverrideDiscoveryOutput(
            key="MSSQL_DRIVER",
            status="resolved",
            value=configured_driver,
        )

    if DEFAULT_MSSQL_DRIVER in installed_drivers:
        logger.info(
            "event=discover_local_override key=MSSQL_DRIVER status=default",
        )
        return LocalOverrideDiscoveryOutput(
            key="MSSQL_DRIVER",
            status="default",
        )

    for candidate in SQL_SERVER_DRIVER_CANDIDATES:
        if candidate in installed_drivers:
            logger.info(
                "event=discover_local_override key=MSSQL_DRIVER status=resolved source=odbcinst",
            )
            return LocalOverrideDiscoveryOutput(
                key="MSSQL_DRIVER",
                status="resolved",
                value=candidate,
            )

    logger.warning(
        "event=discover_local_override key=MSSQL_DRIVER status=manual",
    )
    return LocalOverrideDiscoveryOutput(
        key="MSSQL_DRIVER",
        status="manual",
        message=MSSQL_DRIVER_MANUAL_MESSAGE,
    )


def run_write_local_env_overrides(
    project_root: Path,
    overrides: dict[str, str],
) -> LocalEnvOverrideWriteOutput:
    """Write machine-local, non-secret overrides into project_root/.env."""
    project_root.mkdir(parents=True, exist_ok=True)
    env_path = project_root / ".env"
    existing_lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    updated_lines = list(existing_lines)
    changed = False

    for key, value in overrides.items():
        rendered = f"{key}={_quote_env_value(value)}"
        replacement_index: int | None = None
        for idx, line in enumerate(updated_lines):
            match = _ENV_ASSIGNMENT_RE.match(line)
            if match and match.group(1) == key:
                replacement_index = idx
                break
        if replacement_index is None:
            updated_lines.append(rendered)
            changed = True
        elif updated_lines[replacement_index] != rendered:
            updated_lines[replacement_index] = rendered
            changed = True

    if not changed:
        logger.info(
            "event=local_env_override_write status=skipped file=%s",
            env_path,
        )
        return LocalEnvOverrideWriteOutput(file=str(env_path), changed=False)

    env_path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
    logger.info(
        "event=local_env_override_write status=updated file=%s override_count=%d",
        env_path,
        len(overrides),
    )
    return LocalEnvOverrideWriteOutput(file=str(env_path), changed=True)


def write_local_env_overrides(project_root: Path, overrides: dict[str, str]) -> bool:
    """Backwards-compatible bool wrapper for tests and callers."""
    return run_write_local_env_overrides(project_root, overrides).changed


def run_scaffold_hooks(project_root: Path, technology: str = "sql_server") -> ScaffoldHooksOutput:
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

    return ScaffoldHooksOutput(
        hook_created=hook_created,
        hooks_path_configured=hooks_path_configured,
    )


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
    typer.echo(json.dumps(result.model_dump(mode="json", exclude_none=True)))


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
    typer.echo(json.dumps(result.model_dump(mode="json", exclude_none=True)))


@app.command("check-freetds")
def check_freetds(
    register_missing: bool = typer.Option(
        False,
        "--register-missing",
        help="Register FreeTDS in unixODBC when the brew package is installed but not registered.",
    ),
) -> None:
    """Check Homebrew FreeTDS installation and unixODBC registration."""
    try:
        result = run_check_freetds(register_missing=register_missing)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result.model_dump(mode="json", exclude_none=True)))


@app.command("write-local-env-overrides")
def write_local_env_overrides_cmd(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root directory (defaults to CWD)",
    ),
    overrides_json: str = typer.Option(
        ...,
        "--overrides-json",
        help="JSON object of non-secret local env overrides to write into .env",
    ),
) -> None:
    """Write machine-local non-secret overrides to .env."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        overrides = json.loads(overrides_json)
    except json.JSONDecodeError as exc:
        typer.echo(f"Invalid --overrides-json: {exc}", err=True)
        raise typer.Exit(1) from exc
    if not isinstance(overrides, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in overrides.items()
    ):
        typer.echo("--overrides-json must be a JSON object of string keys and string values", err=True)
        raise typer.Exit(1)

    result = run_write_local_env_overrides(project_root, overrides)
    typer.echo(json.dumps(result.model_dump(mode="json", exclude_none=True)))


@app.command("discover-mssql-driver-override")
def discover_mssql_driver_override_cmd() -> None:
    """Resolve the effective local SQL Server driver override."""
    result = run_discover_mssql_driver_override()
    typer.echo(json.dumps(result.model_dump(mode="json", exclude_none=True)))


if __name__ == "__main__":
    app()
