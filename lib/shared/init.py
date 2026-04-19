"""init.py — public CLI and facade for migration project initialization."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from shared.freetds import run_check_freetds
from shared.init_support.local_env import (
    quote_env_value as _quote_env_value,
    run_write_local_env_overrides,
    write_local_env_overrides,
)
from shared.init_support.scaffold import (
    GITIGNORE_ENTRIES,
    GIT_WORKFLOW_MD,
    is_executable_file as _is_executable_file,
    run_scaffold_hooks,
    run_scaffold_project,
)
from shared.init_support.source_config import (
    SOURCE_REGISTRY,
    SourceConfig,
    get_source_config,
)
from shared.platform import HostPlatform  # noqa: F401
from shared.platform import build_init_platform_gate_message  # noqa: F401
from shared.platform import classify_host_platform  # noqa: F401
from shared.platform import supports_homebrew_install  # noqa: F401
from shared.platform import supports_native_windows  # noqa: F401

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


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
        help="Register FreeTDS in unixODBC when the driver library is installed but not registered.",
    ),
) -> None:
    """Check FreeTDS installation and unixODBC registration."""
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


if __name__ == "__main__":
    app()
