"""dry_run.py — Migration stage prerequisite checker and content reader.

Standalone CLI with three subcommands:

    dry-run     Check guards for a (table, stage) pair and return
                eligibility + catalog/dbt content as JSON.

    guard       Check guards only (no content collection). Returns
                pass/fail JSON for use by skills and plugin commands.

    batch-plan  Build a dependency-aware parallel batch plan for all
                catalog objects. Output includes pipeline status per
                object, transitive dep graph, and catalog diagnostics.

Designed for consumption by the /status plugin command which adds LLM
reasoning on top of the deterministic output.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success (guards_passed field indicates pass/fail)
    1  domain failure (invalid stage, bad table FQN)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import typer

from shared.batch_plan import build_batch_plan
from shared.catalog import load_table_catalog, load_view_catalog
from shared.loader_data import CatalogLoadError
from shared.cli_utils import emit
from shared.dry_run_content import _CONTENT_COLLECTORS
from shared.env_config import resolve_project_root
from shared.guards import run_guards  # re-exported for callers using dry_run.run_guards
from shared.name_resolver import fqn_parts, normalize  # re-exported for test compat

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Orchestrator ─────────────────────────────────────────────────────────────

_WRITER_DEPENDENT_STAGES = frozenset({"profile", "test-gen", "refactor", "migrate"})


def _detect_object_type(project_root: Path, norm_fqn: str) -> str:
    """Detect whether a normalised FQN refers to a table, view, or MV.

    Checks catalog/tables/ first, then catalog/views/.  Falls back to 'table'
    so that a missing file surfaces through the normal guard failure path.
    """
    table_path = project_root / "catalog" / "tables" / f"{norm_fqn}.json"
    if table_path.exists():
        return "table"
    view_path = project_root / "catalog" / "views" / f"{norm_fqn}.json"
    if view_path.exists():
        try:
            cat = load_view_catalog(project_root, norm_fqn)
            if cat and cat.get("is_materialized_view"):
                return "mv"
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            pass
        return "view"
    return "table"  # fallback — guard will surface CATALOG_FILE_MISSING


def _is_writerless(project_root: Path, norm_fqn: str) -> bool:
    """Return True if the table catalog has scoping.status == 'no_writer_found'."""
    try:
        cat = load_table_catalog(project_root, norm_fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return False
    if cat is None:
        return False
    scoping = cat.get("scoping") or {}
    return scoping.get("status") == "no_writer_found"


def run_dry_run(
    project_root: Path,
    table_fqn: str,
    stage: str,
    detail: bool = False,
) -> dict[str, Any]:
    """Run dry-run checks for a (table/view, stage) pair.

    Returns a dict matching schemas/dry_run_output.json.
    """
    norm = normalize(table_fqn)
    object_type = _detect_object_type(project_root, norm)

    result: dict[str, Any] = {
        "table": norm,
        "stage": stage,
        "object_type": object_type,
        "guards_passed": False,
        "guard_results": [],
    }

    # ── View / MV routing ────────────────────────────────────────────────────
    if object_type in ("view", "mv"):
        if stage == "scope":
            guards_passed, guard_results = run_guards(project_root, norm, "scope-view")
            result["guards_passed"] = guards_passed
            result["guard_results"] = guard_results
            if guards_passed:
                mode = "detail" if detail else "summary"
                result["content"] = _CONTENT_COLLECTORS["scope-view"][mode](project_root, norm)
        else:
            # Writer-dependent stages do not apply to views; show as blocked.
            result["guards_passed"] = False
            result["guard_results"] = [
                {
                    "check": "object_type",
                    "passed": False,
                    "code": "VIEW_STAGE_NOT_SUPPORTED",
                    "message": f"Stage '{stage}' is not supported for {object_type} objects.",
                },
            ]
        return result

    # ── Table routing ────────────────────────────────────────────────────────

    # Writerless tables: writer-dependent stages are N/A, not blocked.
    if stage in _WRITER_DEPENDENT_STAGES and _is_writerless(project_root, norm):
        result["guards_passed"] = False
        result["not_applicable"] = True
        result["guard_results"] = [
            {
                "check": "writerless_table",
                "passed": False,
                "code": "WRITERLESS_TABLE",
                "message": f"Stage '{stage}' is not applicable: no writer found for {norm}.",
            },
        ]
        return result

    guards_passed, guard_results = run_guards(project_root, norm, stage)
    result["guards_passed"] = guards_passed
    result["guard_results"] = guard_results

    if guards_passed:
        mode = "detail" if detail else "summary"
        collector = _CONTENT_COLLECTORS[stage][mode]
        result["content"] = collector(project_root, norm)

    return result


# ── CLI ──────────────────────────────────────────────────────────────────────


class Stage(str, Enum):
    scope = "scope"
    profile = "profile"
    test_gen = "test-gen"
    refactor = "refactor"
    migrate = "migrate"


class GuardStage(str, Enum):
    """Stages accepted by the guard subcommand.

    Includes pipeline stages plus skill-specific guard sets.
    """
    scope = "scope"
    scope_view = "scope-view"
    profile = "profile"
    test_gen = "test-gen"
    refactor = "refactor"
    migrate = "migrate"
    generating_model = "generating-model"
    reviewing_model = "reviewing-model"
    reviewing_tests = "reviewing-tests"
    refactoring_sql = "refactoring-sql"
    setup_ddl = "setup-ddl"


@app.command("dry-run")
def dry_run_cmd(
    table: str = typer.Argument(..., help="Fully-qualified table name (schema.Name)"),
    stage: Stage = typer.Argument(..., help="Migration stage to check"),
    detail: bool = typer.Option(False, "--detail", help="Include full content blobs"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Check prerequisites for a migration stage and return eligibility + content."""
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_dry_run(root, table, stage.value, detail=detail)
    emit(result)


@app.command("guard")
def guard_cmd(
    table: str = typer.Argument(..., help="Fully-qualified table name (schema.Name)"),
    stage: GuardStage = typer.Argument(..., help="Stage or skill name to check guards for"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Check guards only (no content collection). Returns pass/fail JSON."""
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    norm = normalize(table)
    guards_passed, guard_results = run_guards(root, norm, stage.value)
    emit({
        "table": norm,
        "stage": stage.value,
        "passed": guards_passed,
        "guard_results": guard_results,
    })


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
    per object.  Output matches schemas/batch_plan_output.json.
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
