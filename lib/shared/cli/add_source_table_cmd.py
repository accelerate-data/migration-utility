"""add-source-table command — add source tables to the migration catalog."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.catalog_writer import run_write_source
from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, error, success, warn
from shared.dry_run_core import run_ready
from shared.loader_data import CatalogFileMissingError
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)


def add_source_table(
    fqns: list[str] = typer.Argument(default=None, help="One or more fully-qualified table names to add"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after update"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Add one or more source tables to the migration catalog."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    root = project_root if project_root is not None else Path.cwd()

    logger.info(
        "event=add_source_table_start component=add_source_table_cmd operation=add_source_table fqns=%s",
        fqns,
    )

    written_files: list[Path] = []

    for fqn in fqns:
        ready_result: Any = run_ready(root, "scope", fqn)

        # Support both dict (mocked in tests) and DryRunOutput (real usage)
        if isinstance(ready_result, dict):
            ready = ready_result.get("ready", False)
            reason = ready_result.get("reason", "unknown")
        else:
            ready = ready_result.ready
            reason = ready_result.project.reason if ready_result.project else "unknown"

        if not ready:
            warn(f"Skipping {fqn!r}: not ready for scope ({reason})")
            logger.info(
                "event=add_source_table_skip component=add_source_table_cmd "
                "operation=add_source_table fqn=%s reason=%s",
                fqn,
                reason,
            )
            continue

        try:
            write_result = run_write_source(root, fqn, value=True)
        except CatalogFileMissingError as exc:
            error(f"Catalog file missing for {fqn!r}: {exc}")
            logger.error(
                "event=add_source_table_error component=add_source_table_cmd "
                "operation=add_source_table fqn=%s error=%s",
                fqn,
                exc,
            )
            continue

        success(f"Marked as source: {normalize(fqn)}")
        logger.info(
            "event=add_source_table_written component=add_source_table_cmd "
            "operation=add_source_table fqn=%s written=%s status=success",
            fqn,
            write_result.written,
        )
        written_files.append(root / write_result.written)

    if no_commit or not written_files:
        return

    if not is_git_repo(root):
        warn("Not a git repository — skipping commit.")
        return

    stage_and_commit(
        [f for f in written_files if f.exists()],
        f"add source tables: {', '.join(normalize(fqn) for fqn in fqns)}",
        root,
    )
    console.print("Changes committed.")
