"""teardown-sandbox command — drop sandbox database from manifest config."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.cli.error_handler import cli_error_handler
from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, error, print_table, success, warn
from shared.loader_io import clear_manifest_sandbox
from shared.runtime_config import get_sandbox_name
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


def _get_sandbox_name(manifest: dict[str, Any]) -> str | None:
    """Return active sandbox database name from manifest."""
    return get_sandbox_name(manifest)


def teardown_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after teardown"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Tear down sandbox schema from manifest runtime.sandbox configuration."""
    root = project_root if project_root is not None else Path.cwd()

    manifest = _load_manifest(root)

    sandbox_db = _get_sandbox_name(manifest)
    if not sandbox_db:
        error("No sandbox database name found in manifest.json. Run setup-sandbox first.")
        raise typer.Exit(code=1)

    if not yes:
        confirmed = typer.confirm("Tear down sandbox database? This action cannot be undone.")
        if not confirmed:
            console.print("Aborted.")
            raise typer.Exit(code=0)

    backend = _create_backend(manifest)

    console.print(f"Tearing down sandbox database: [bold]{sandbox_db}[/bold]...")
    with console.status("Running sandbox_down..."):
        with cli_error_handler("tearing down sandbox database"):
            result = backend.sandbox_down(sandbox_db)

    logger.info(
        "event=sandbox_down status=%s sandbox_database=%s",
        result.status,
        result.sandbox_database,
    )

    if result.status == "ok":
        clear_manifest_sandbox(root)
        print_table(
            "Teardown Summary",
            [("Database", result.sandbox_database), ("Status", result.status)],
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
                f"sandbox: teardown {result.sandbox_database}",
                root,
            )
        except RuntimeError as exc:
            error(f"Git commit failed: {exc}")
            raise typer.Exit(code=1) from exc
        success("Sandbox teardown committed.")
    else:
        error(f"Sandbox teardown failed: {result.status}")
        for entry in (result.errors or []):
            error(f"[{entry.code}] {entry.message}")
        raise typer.Exit(code=1)
