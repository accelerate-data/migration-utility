"""exclude-table command — mark tables as excluded from migration."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.cli.error_handler import cli_error_handler
from shared.cli.output import error, remind_review_and_commit, success
from shared.catalog import detect_catalog_bucket
from shared.dry_run_core import run_exclude
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)


def exclude_table(
    fqns: list[str] = typer.Argument(default=None, help="One or more fully-qualified table names to exclude"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Mark one or more source tables as excluded from migration."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    root = project_root if project_root is not None else Path.cwd()

    logger.info(
        "event=exclude_table_start component=exclude_table_cmd operation=exclude_table fqns=%s",
        fqns,
    )

    with cli_error_handler("excluding tables from catalog"):
        result = run_exclude(root, list(fqns))

    logger.info(
        "event=exclude_table_complete component=exclude_table_cmd operation=exclude_table "
        "marked=%s not_found=%s",
        result.marked,
        result.not_found,
    )

    if result.marked:
        success(f"Excluded ({len(result.marked)}): {', '.join(result.marked)}")
        remind_review_and_commit(_catalog_paths_for_marked(root, result.marked))
    if result.not_found:
        error(f"Not found ({len(result.not_found)}): {', '.join(result.not_found)}")


def _catalog_paths_for_marked(root: Path, fqns: list[str]) -> list[str]:
    paths: list[str] = []
    for fqn in fqns:
        norm = normalize(fqn)
        bucket = detect_catalog_bucket(root, norm) or "tables"
        paths.append(f"catalog/{bucket}/{norm}.json")
    return paths
