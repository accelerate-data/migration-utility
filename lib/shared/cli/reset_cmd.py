"""reset command — reset pipeline state for a given stage and objects."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.dry_run_core import RESETTABLE_STAGES, run_reset_migration
from shared.cli.output import console, error, print_table, success

logger = logging.getLogger(__name__)


def reset(
    stage: str = typer.Argument(..., help="Pipeline stage to reset (scope|profile|generate-tests|refactor)"),
    fqns: list[str] = typer.Argument(default=None, help="Fully-qualified table names"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Reset pipeline state for the given stage and objects."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    if stage not in RESETTABLE_STAGES:
        error(f"Invalid stage {stage!r}. Must be one of: {', '.join(sorted(RESETTABLE_STAGES))}")
        raise typer.Exit(code=1)

    root = project_root if project_root is not None else Path.cwd()

    if not yes:
        fqn_list = ", ".join(fqns)
        confirmed = typer.confirm(
            f"Reset stage '{stage}' for: {fqn_list}?",
            default=False,
        )
        if not confirmed:
            console.print("Aborted.")
            return

    logger.info(
        "event=reset_start component=reset_cmd operation=reset stage=%s fqns=%s",
        stage,
        fqns,
    )

    result = run_reset_migration(root, stage, list(fqns))

    logger.info(
        "event=reset_complete component=reset_cmd operation=reset stage=%s "
        "reset=%s noop=%s blocked=%s not_found=%s",
        stage,
        result.reset,
        result.noop,
        result.blocked,
        result.not_found,
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
