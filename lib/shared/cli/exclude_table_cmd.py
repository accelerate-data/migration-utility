"""exclude-table command — mark tables as excluded from migration."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.cli.error_handler import cli_error_handler
from shared.cli.git_ops import git_push, is_git_repo, stage_and_commit
from shared.cli.output import console, error, success, warn
from shared.dry_run_core import run_exclude
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)


def exclude_table(
    fqns: list[str] = typer.Argument(default=None, help="One or more fully-qualified table names to exclude"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after update"),
    push: bool = typer.Option(False, "--push", help="Push to remote after commit"),
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
    if result.not_found:
        error(f"Not found ({len(result.not_found)}): {', '.join(result.not_found)}")

    if no_commit:
        return

    if not result.marked:
        return

    if not is_git_repo(root):
        warn("Not a git repository — skipping commit.")
        return

    catalog_files = [
        root / "catalog" / "tables" / f"{normalize(fqn)}.json"
        for fqn in result.marked
    ]
    try:
        stage_and_commit(
            [f for f in catalog_files if f.exists()],
            f"exclude tables: {', '.join(result.marked)}",
            root,
        )
    except RuntimeError as exc:
        error(f"Git commit failed: {exc}")
        raise typer.Exit(code=1) from exc
    console.print("Changes committed.")
    if push and not git_push(root):
        warn("git push failed — changes committed locally.")
