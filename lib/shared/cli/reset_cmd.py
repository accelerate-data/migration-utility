"""reset command — reset pipeline state for a given stage and objects."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.dry_run_core import RESET_GLOBAL_MANIFEST_SECTIONS, RESET_GLOBAL_PATHS, RESETTABLE_STAGES, run_reset_migration
from shared.cli.output import console, error, print_table, success
from shared.loader_io import clear_manifest_sandbox
from shared.runtime_config import get_sandbox_name
from shared.test_harness_support.manifest import _create_backend as _th_create_backend
from shared.test_harness_support.manifest import _load_manifest as _th_load_manifest

logger = logging.getLogger(__name__)

_GLOBAL_BLAST_RADIUS = (
    "Directories:  " + ", ".join(RESET_GLOBAL_PATHS) + "\n"
    "Manifest:     " + ", ".join(RESET_GLOBAL_MANIFEST_SECTIONS)
)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    return _th_load_manifest(project_root)


def _create_backend(manifest: dict[str, Any]) -> Any:
    return _th_create_backend(manifest)


def _get_sandbox_name(manifest: dict[str, Any]) -> str | None:
    return get_sandbox_name(manifest)


def _teardown_sandbox_if_configured(root: Path) -> bool:
    """Tear down sandbox if configured. Returns False if teardown fails."""
    try:
        manifest = _load_manifest(root)
    except Exception:
        return True

    sandbox_db = _get_sandbox_name(manifest)
    if not sandbox_db:
        return True

    console.print(f"Sandbox configured: [bold]{sandbox_db}[/bold] — tearing down first...")
    logger.info("event=global_reset_sandbox_teardown component=reset_cmd sandbox=%s", sandbox_db)

    try:
        backend = _create_backend(manifest)
        result = backend.sandbox_down(sandbox_db)
    except (OSError, ConnectionError) as exc:
        error(f"Sandbox teardown connection error: {exc}")
        logger.error("event=global_reset_sandbox_teardown_failed component=reset_cmd sandbox=%s error=%s", sandbox_db, exc)
        return False

    if result.status != "ok":
        error(f"Sandbox teardown failed (status={result.status}) — aborting global reset.")
        for entry in (result.errors or []):
            error(f"[{entry.code}] {entry.message}")
        logger.error("event=global_reset_sandbox_teardown_failed component=reset_cmd sandbox=%s status=%s", sandbox_db, result.status)
        return False

    clear_manifest_sandbox(root)
    logger.info("event=global_reset_sandbox_teardown_ok component=reset_cmd sandbox=%s", sandbox_db)
    return True


def reset(
    stage: str = typer.Argument(..., help="Pipeline stage to reset (scope|profile|generate-tests|refactor|all)"),
    fqns: list[str] = typer.Argument(default=None, help="Fully-qualified table names (not used for 'all')"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Reset pipeline state for the given stage and objects.

    Use 'all' to wipe the full migration state and return the project to a
    clean post-init state ready for setup-source. Any configured sandbox is
    torn down first.
    """
    root = project_root if project_root is not None else Path.cwd()

    if stage == "all":
        if fqns:
            error("Global reset ('all') does not accept table arguments.")
            raise typer.Exit(code=1)

        if not yes:
            console.print(f"[bold red]Global reset[/bold red] will permanently delete:\n{_GLOBAL_BLAST_RADIUS}")
            confirmed = typer.confirm("This cannot be undone. Continue?", default=False)
            if not confirmed:
                console.print("Aborted.")
                return

        logger.info("event=global_reset_start component=reset_cmd stage=all")

        if not _teardown_sandbox_if_configured(root):
            raise typer.Exit(code=1)

        result = run_reset_migration(root, "all", [])

        logger.info(
            "event=global_reset_complete component=reset_cmd deleted_paths=%s missing_paths=%s cleared=%s",
            result.deleted_paths, result.missing_paths, result.cleared_manifest_sections,
        )

        print_table(
            "Global Reset Summary",
            [
                ("Deleted", ", ".join(result.deleted_paths) if result.deleted_paths else "—"),
                ("Missing", ", ".join(result.missing_paths) if result.missing_paths else "—"),
                ("Manifest cleared", ", ".join(result.cleared_manifest_sections) if result.cleared_manifest_sections else "—"),
            ],
            columns=("", ""),
        )
        success("Project reset to post-init state. Run setup-source to restart the pipeline.")
        return

    if stage not in RESETTABLE_STAGES:
        error(f"Invalid stage {stage!r}. Must be one of: {', '.join(sorted(RESETTABLE_STAGES))} or 'all'")
        raise typer.Exit(code=1)

    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    if not yes:
        fqn_list = ", ".join(fqns)
        confirmed = typer.confirm(f"Reset stage '{stage}' for: {fqn_list}?", default=False)
        if not confirmed:
            console.print("Aborted.")
            return

    logger.info("event=reset_start component=reset_cmd stage=%s fqns=%s", stage, fqns)

    result = run_reset_migration(root, stage, list(fqns))

    logger.info(
        "event=reset_complete component=reset_cmd stage=%s reset=%s noop=%s blocked=%s not_found=%s",
        stage, result.reset, result.noop, result.blocked, result.not_found,
    )

    print_table(
        "Reset Summary",
        [
            ("Reset", str(len(result.reset))),
            ("No-op", str(len(result.noop))),
            ("Blocked", str(len(result.blocked))),
            ("Not found", str(len(result.not_found))),
        ],
        columns=("Category", "Count"),
    )
    if result.blocked:
        error(f"Blocked: {', '.join(result.blocked)}")
    if result.not_found:
        error(f"Not found: {', '.join(result.not_found)}")
    if result.blocked or result.not_found:
        raise typer.Exit(code=1)
