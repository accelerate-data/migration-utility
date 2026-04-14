"""add-source-table command — add source tables to the migration catalog."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.catalog_writer import run_write_source
from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, success, warn
from shared.dry_run_core import run_ready
from shared.loader_data import CatalogFileMissingError
from shared.name_resolver import normalize
from shared.output_models.dry_run import DryRunOutput

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

    written_pairs: list[tuple[str, Path]] = []

    for fqn in fqns:
        ready_result: DryRunOutput = run_ready(root, "scope", fqn)

        if ready_result.object is not None:
            is_ready = ready_result.object.ready
            reason = ready_result.object.reason
        elif ready_result.project is not None:
            is_ready = ready_result.project.ready
            reason = ready_result.project.reason
        else:
            is_ready = False
            reason = "unknown"

        if not is_ready:
            warn(f"skipped  {fqn} — {reason}")
            logger.info(
                "event=add_source_table_skip component=add_source_table_cmd "
                "operation=add_source_table fqn=%s reason=%s",
                fqn,
                reason,
            )
            continue

        try:
            write_result = run_write_source(root, fqn, value=True)
            success(f"source   {fqn} → is_source: true")
            logger.info(
                "event=add_source_table_written component=add_source_table_cmd "
                "operation=add_source_table fqn=%s written=%s status=success",
                fqn,
                write_result.written,
            )
            written_pairs.append((fqn, root / write_result.written))
        except CatalogFileMissingError:
            warn(f"missing  {fqn} (no catalog file — run setup-source first)")
        except ValueError as exc:
            warn(f"skipped  {fqn} — {exc}")

    if not written_pairs or no_commit:
        return

    if not is_git_repo(root):
        warn("Not a git repository — skipping commit.")
        return

    written_files = [p for _, p in written_pairs]
    written_fqns = [fqn for fqn, _ in written_pairs]

    stage_and_commit(
        [f for f in written_files if f.exists()],
        f"add source tables: {', '.join(normalize(fqn) for fqn in written_fqns)}",
        root,
    )
    console.print("Changes committed.")
