"""dry_run.py — Migration stage readiness checker and status collator.

Standalone CLI with subcommands:

    ready                   Check whether the prior stage's CLI-written status
                            allows proceeding to a given stage. Returns
                            {"ready": true/false, "reason": "..."} JSON.

    status                  Collate CLI-written statuses from catalog files.
                            Single-object or full-matrix mode.

    batch-plan              Build a dependency-aware parallel batch plan for all
                            catalog objects. Output includes pipeline status per
                            object, transitive dep graph, and catalog diagnostics.

    exclude                 Set excluded: true on one or more table or view catalog
                            files, removing them from the batch pipeline.

    sync-excluded-warnings  Write or clear EXCLUDED_DEP warnings on active catalog
                            objects whose transitive deps include excluded objects.

Designed for consumption by the /status plugin command which adds LLM
reasoning on top of the deterministic output.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure (invalid stage, bad FQN)
    2  IO or parse error
"""

import json
import logging
from pathlib import Path
from typing import List, Optional

import typer

from shared.batch_plan import build_batch_plan
from shared.cli_utils import emit
from shared.loader_data import CatalogLoadError
from shared.dry_run_core import (
    run_exclude,
    run_ready,
    run_reset_migration,
    run_status,
    run_sync_excluded_warnings,
)
from shared.env_config import resolve_project_root
from shared.name_resolver import fqn_parts, normalize  # re-exported for test compat

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── CLI ──────────────────────────────────────────────────────────────────────


@app.command("ready")
def ready_cmd(
    stage: str = typer.Argument(..., help="Pipeline stage to check readiness for"),
    object_fqn: str | None = typer.Option(
        None, "--object", help="Optional fully-qualified object name for object-scoped readiness",
    ),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Check if prior stage status allows proceeding to this stage."""
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_ready(root, stage, object_fqn=object_fqn)
    emit(result)


@app.command("status")
def status_cmd(
    fqn: str = typer.Argument(None, help="Optional FQN for single-object detail"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Collate pipeline statuses from catalog files."""
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_status(root, fqn)
    emit(result)


@app.command("batch-plan")
def batch_plan_cmd(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Build a dependency-aware parallel batch plan for all catalog objects.

    Reads all table and view catalog files, builds the transitive dependency
    graph (proc → tables/views, view → views, proc → procs transitively),
    and outputs a JSON batch plan with pipeline status and catalog diagnostics
    per object.  Output contract: output_models.BatchPlanOutput.
    """
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    try:
        result = build_batch_plan(root)
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("event=batch_plan_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc
    emit(result)

@app.command("exclude")
def exclude_cmd(
    fqns: List[str] = typer.Argument(..., help="Fully-qualified table or view names to exclude"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Mark tables or views as excluded from the migration pipeline.

    Sets ``excluded: true`` in each named catalog file.  Excluded objects are
    hidden from batch-plan output and skipped by pipeline scheduling.
    Output contract: output_models.ExcludeOutput.
    """
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_exclude(root, list(fqns))
    emit(result)


@app.command("reset-migration")
def reset_migration_cmd(
    stage: str = typer.Argument(..., help="Pre-model stage to reset"),
    # Optional so `all` can run without positional FQNs; core handles mode rules.
    fqns: List[str] = typer.Option(
        [],
        "--fqn",
        help="Fully-qualified table names to reset (can be specified multiple times)",
    ),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Reset pre-model migration state for one or more selected tables."""
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    try:
        result = run_reset_migration(root, stage, list(fqns or []))
    except ValueError as exc:
        logger.error("event=reset_migration_failed stage=%s error=%s", stage, exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=1) from exc
    except (OSError, json.JSONDecodeError, CatalogLoadError) as exc:
        logger.error("event=reset_migration_failed stage=%s error=%s", stage, exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command("sync-excluded-warnings")
def sync_excluded_warnings_cmd(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Write or clear EXCLUDED_DEP warnings on active catalog objects.

    Idempotent — safe to run on every /status invocation.
    Output contract: output_models.SyncExcludedWarningsOutput.
    """
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_sync_excluded_warnings(root)
    emit(result)


if __name__ == "__main__":
    app()
