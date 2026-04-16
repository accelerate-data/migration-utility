"""add-seed-table command -- add seed tables to the migration catalog."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.catalog_writer import run_write_seed
from shared.cli.output import remind_review_and_commit, success, warn
from shared.dry_run_core import run_ready
from shared.loader_data import CatalogFileMissingError
from shared.output_models.dry_run import DryRunOutput

logger = logging.getLogger(__name__)


def add_seed_table(
    fqns: list[str] = typer.Argument(default=None, help="One or more fully-qualified table names to add"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Add one or more seed tables to the migration catalog."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    root = project_root if project_root is not None else Path.cwd()
    logger.info(
        "event=add_seed_table_start component=add_seed_table_cmd operation=add_seed_table fqns=%s",
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
            raise AssertionError(f"run_ready returned neither object nor project payload for {fqn}")

        if not is_ready:
            warn(f"skipped  {fqn} -- {reason}")
            logger.info(
                "event=add_seed_table_skip component=add_seed_table_cmd "
                "operation=add_seed_table fqn=%s reason=%s",
                fqn,
                reason,
            )
            continue

        try:
            write_result = run_write_seed(root, fqn, value=True)
            success(f"seed     {fqn} -> is_seed: true")
            logger.info(
                "event=add_seed_table_written component=add_seed_table_cmd "
                "operation=add_seed_table fqn=%s written=%s status=success",
                fqn,
                write_result.written,
            )
            written_pairs.append((fqn, root / write_result.written))
        except CatalogFileMissingError:
            warn(f"missing  {fqn} (no catalog file -- run setup-source first)")
        except ValueError as exc:
            warn(f"skipped  {fqn} -- {exc}")

    if written_pairs:
        remind_review_and_commit()
