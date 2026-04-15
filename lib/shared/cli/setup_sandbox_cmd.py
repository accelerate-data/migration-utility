"""setup-sandbox command — provision sandbox database from manifest config."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.cli.error_handler import cli_error_handler
from shared.cli.git_ops import git_push, is_git_repo, stage_and_commit
from shared.cli.output import console, error, print_table, success, warn
from shared.loader_io import write_manifest_sandbox
from shared.runtime_config import get_extracted_schemas
from shared.sandbox.base import SandboxBackend
from shared.test_harness_support.manifest import _create_backend as _th_create_backend
from shared.test_harness_support.manifest import _load_manifest as _th_load_manifest

logger = logging.getLogger(__name__)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    """Thin wrapper around test_harness_support._load_manifest for patching."""
    return _th_load_manifest(project_root)


def _create_backend(manifest: dict[str, Any]) -> SandboxBackend:
    """Thin wrapper around test_harness_support._create_backend for patching."""
    return _th_create_backend(manifest)


def _get_schemas(manifest: dict[str, Any]) -> list[str]:
    """Return extracted schemas from manifest."""
    return get_extracted_schemas(manifest)


def _write_sandbox_to_manifest(project_root: Path, sandbox_database: str) -> None:
    """Persist sandbox database name into manifest.json."""
    write_manifest_sandbox(project_root, sandbox_database)


def setup_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after setup"),
    push: bool = typer.Option(False, "--push", help="Push to remote after commit"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Provision sandbox schema from manifest runtime.sandbox configuration."""
    root = project_root if project_root is not None else Path.cwd()

    manifest = _load_manifest(root)
    schemas = _get_schemas(manifest)

    if not yes:
        confirmed = typer.confirm(
            f"Create sandbox database cloning schemas: {', '.join(schemas) or '(none)'}?"
        )
        if not confirmed:
            console.print("Aborted.")
            raise typer.Exit(code=0)

    backend = _create_backend(manifest)

    console.print(f"Provisioning sandbox for schemas: [bold]{', '.join(schemas)}[/bold]...")
    with console.status("Running sandbox_up..."):
        with cli_error_handler("provisioning sandbox database"):
            result = backend.sandbox_up(schemas=schemas)

    logger.info(
        "event=sandbox_up status=%s sandbox_database=%s tables=%d views=%d procedures=%d errors=%d",
        result.status,
        result.sandbox_database,
        len(result.tables_cloned),
        len(result.views_cloned),
        len(result.procedures_cloned),
        len(result.errors),
    )

    if result.status == "error":
        if result.errors:
            for entry in result.errors:
                error(f"[{entry.code}] {entry.message}")
        raise typer.Exit(code=1)

    _write_sandbox_to_manifest(root, result.sandbox_database)

    print_table(
        "Sandbox Setup",
        [
            ("Database", result.sandbox_database),
            ("Tables cloned", str(len(result.tables_cloned))),
            ("Views cloned", str(len(result.views_cloned))),
            ("Procedures cloned", str(len(result.procedures_cloned))),
            ("Status", result.status),
        ],
        columns=("", ""),
    )

    if no_commit:
        return
    if not is_git_repo(root):
        warn("Not a git repository — skipping commit.")
        return
    try:
        stage_and_commit(
            [root / "manifest.json"],
            f"sandbox: provision {result.sandbox_database}",
            root,
        )
    except RuntimeError as exc:
        error(f"Git commit failed: {exc}")
        raise typer.Exit(code=1) from exc
    success("Sandbox setup committed.")
    if push and not git_push(root):
        warn("git push failed — changes committed locally.")
