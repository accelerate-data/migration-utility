"""replicate-source-tables command."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.cli.output import console, error, print_table, success
from shared.replicate_source_tables import run_replicate_source_tables

logger = logging.getLogger(__name__)


def replicate_source_tables(
    limit: int | None = typer.Option(None, "--limit", help="Required per-table source row cap"),
    select: list[str] | None = typer.Option(None, "--select", help="Confirmed source table FQN to include"),
    exclude: list[str] | None = typer.Option(None, "--exclude", help="Confirmed source table FQN to omit"),
    filters: list[str] | None = typer.Option(None, "--filter", help="Raw source-side filter as <fqn>=<predicate>"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the replication plan without copying rows"),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON output"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip destructive confirmation prompt"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Copy capped source rows into configured target-side source tables."""
    root = project_root if project_root is not None else Path.cwd()

    if not dry_run and not yes:
        confirmed = typer.confirm("Truncate and reload selected target source tables?")
        if not confirmed:
            console.print("Aborted.")
            return

    try:
        result = run_replicate_source_tables(
            root,
            limit=limit,
            select=select,
            exclude=exclude,
            filters=filters,
            dry_run=dry_run,
        )
    except ValueError as exc:
        message = str(exc)
        error(message)
        logger.error(
            "event=replicate_source_tables_validation status=failure error=%s",
            message.split(":", 1)[0],
        )
        raise typer.Exit(code=1) from exc

    if json_output:
        console.print(result.model_dump_json(indent=2))
    else:
        rows = [
            (
                table.fqn,
                table.status,
                str(table.rows_copied),
                table.error or table.predicate or "",
            )
            for table in result.tables
        ]
        print_table(
            "Source Table Replication",
            rows,
            columns=("Table", "Status", "Rows", "Detail"),
        )
        if result.status == "ok":
            success("source table replication complete" if not result.dry_run else "source table replication plan ready")
        else:
            error("source table replication failed for one or more tables")

    if result.status != "ok":
        raise typer.Exit(code=1)
