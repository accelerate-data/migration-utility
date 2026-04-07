"""dry_run.py — Migration stage prerequisite checker and content reader.

Standalone CLI with five subcommands:

    dry-run                 Check guards for a (table, stage) pair and return
                            eligibility + catalog/dbt content as JSON.

    guard                   Check guards only (no content collection). Returns
                            pass/fail JSON for use by skills and plugin commands.

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
    0  success (guards_passed field indicates pass/fail)
    1  domain failure (invalid stage, bad table FQN)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional

import typer

from shared.batch_plan import build_batch_plan, collect_deps
from shared.catalog import load_table_catalog, load_view_catalog, write_json
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
        elif stage == "profile":
            guards_passed, guard_results = run_guards(project_root, norm, "profile-view")
            result["guards_passed"] = guards_passed
            result["guard_results"] = guard_results
            if guards_passed:
                mode = "detail" if detail else "summary"
                result["content"] = _CONTENT_COLLECTORS["profile-view"][mode](project_root, norm)
        elif stage == "refactor":
            guards_passed, guard_results = run_guards(project_root, norm, "refactor-view")
            result["guards_passed"] = guards_passed
            result["guard_results"] = guard_results
            if guards_passed:
                mode = "detail" if detail else "summary"
                result["content"] = _CONTENT_COLLECTORS["refactor-view"][mode](project_root, norm)
        else:
            # test-gen and migrate are not applicable for views.
            result["guards_passed"] = False
            result["not_applicable"] = True
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
    profile_view = "profile-view"
    test_gen = "test-gen"
    refactor = "refactor"
    refactor_view = "refactor-view"
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


# ── Exclude helpers ───────────────────────────────────────────────────────────


def _detect_catalog_bucket(project_root: Path, norm_fqn: str) -> str | None:
    """Return 'tables' or 'views' if a catalog file exists, else None."""
    if (project_root / "catalog" / "tables" / f"{norm_fqn}.json").exists():
        return "tables"
    if (project_root / "catalog" / "views" / f"{norm_fqn}.json").exists():
        return "views"
    return None


def run_exclude(project_root: Path, fqns: list[str]) -> dict[str, Any]:
    """Set ``excluded: true`` on each named table or view catalog file.

    Returns a dict matching schemas/exclude_output.json.
    """
    marked: list[str] = []
    not_found: list[str] = []

    for raw_fqn in fqns:
        norm = normalize(raw_fqn)
        bucket = _detect_catalog_bucket(project_root, norm)
        if bucket is None:
            logger.warning(
                "event=exclude_not_found component=exclude operation=run_exclude fqn=%s",
                norm,
            )
            not_found.append(norm)
            continue

        catalog_path = project_root / "catalog" / bucket / f"{norm}.json"
        try:
            data = json.loads(catalog_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "event=exclude_read_error component=exclude operation=run_exclude "
                "fqn=%s error=%s",
                norm, exc,
            )
            not_found.append(norm)
            continue

        data["excluded"] = True
        write_json(catalog_path, data)
        marked.append(norm)
        logger.info(
            "event=exclude_marked component=exclude operation=run_exclude "
            "fqn=%s bucket=%s status=success",
            norm, bucket,
        )

    return {"marked": marked, "not_found": not_found}


def run_sync_excluded_warnings(project_root: Path) -> dict[str, Any]:
    """Write or clear EXCLUDED_DEP warnings on active catalog objects.

    For every active (non-excluded) table and view, checks whether any of its
    transitive dependencies are currently excluded.  If so, writes an
    EXCLUDED_DEP warning to the object's catalog ``warnings[]``.  If a
    previously written EXCLUDED_DEP warning no longer applies (the referenced
    object is no longer excluded), it is removed.

    Full replacement of ``warnings[]`` is idempotent — matches the pattern
    used by the diagnostics runner.

    Returns a dict matching schemas/sync_excluded_warnings_output.json.
    """
    catalog_dir = project_root / "catalog"
    table_dir = catalog_dir / "tables"
    view_dir = catalog_dir / "views"

    # ── Collect all FQNs and identify excluded ones ────────────────────────
    excluded_fqns: set[str] = set()
    all_entries: list[tuple[str, str]] = []  # (fqn, bucket)

    for bucket, bucket_dir in (("tables", table_dir), ("views", view_dir)):
        if not bucket_dir.is_dir():
            continue
        for p in sorted(bucket_dir.glob("*.json")):
            fqn = p.stem
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            all_entries.append((fqn, bucket))
            if data.get("excluded"):
                excluded_fqns.add(fqn)

    warnings_written = 0
    warnings_cleared = 0

    if not excluded_fqns:
        # No excluded objects — clear any stale EXCLUDED_DEP warnings everywhere.
        for fqn, bucket in all_entries:
            catalog_path = catalog_dir / bucket / f"{fqn}.json"
            try:
                data = json.loads(catalog_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            existing_warnings: list[dict[str, Any]] = data.get("warnings") or []
            cleaned = [w for w in existing_warnings if w.get("code") != "EXCLUDED_DEP"]
            if len(cleaned) != len(existing_warnings):
                data["warnings"] = cleaned
                write_json(catalog_path, data)
                warnings_cleared += len(existing_warnings) - len(cleaned)
        return {"warnings_written": warnings_written, "warnings_cleared": warnings_cleared}

    # ── Determine all_fqns (active only, for dep graph) ────────────────────
    active_entries = [(fqn, bucket) for fqn, bucket in all_entries if fqn not in excluded_fqns]
    active_fqns: set[str] = {fqn for fqn, _ in active_entries}

    # ── For each active object, find which excluded fqns are in its dep set ─
    for fqn, bucket in active_entries:
        obj_type = "table" if bucket == "tables" else "view"
        try:
            full_deps = collect_deps(project_root, fqn, obj_type)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            full_deps = set()

        excluded_deps = sorted(full_deps & excluded_fqns)

        catalog_path = catalog_dir / bucket / f"{fqn}.json"
        try:
            data = json.loads(catalog_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        existing_warnings: list[dict[str, Any]] = data.get("warnings") or []
        non_excluded_warnings = [w for w in existing_warnings if w.get("code") != "EXCLUDED_DEP"]
        old_excluded_warning_count = len(existing_warnings) - len(non_excluded_warnings)

        if excluded_deps:
            dep_list = ", ".join(excluded_deps)
            new_warning: dict[str, Any] = {
                "code": "EXCLUDED_DEP",
                "message": (
                    f"Depends on excluded object(s): {dep_list}. "
                    "Consider adding as a dbt source instead."
                ),
                "severity": "warning",
            }
            data["warnings"] = non_excluded_warnings + [new_warning]
            write_json(catalog_path, data)
            warnings_written += 1
            if old_excluded_warning_count > 0:
                warnings_cleared += old_excluded_warning_count
            logger.info(
                "event=excluded_dep_warning_written component=sync_excluded_warnings "
                "fqn=%s excluded_deps=%s",
                fqn, dep_list,
            )
        elif old_excluded_warning_count > 0:
            # Previously had EXCLUDED_DEP warning — clear it.
            data["warnings"] = non_excluded_warnings
            write_json(catalog_path, data)
            warnings_cleared += old_excluded_warning_count
            logger.info(
                "event=excluded_dep_warning_cleared component=sync_excluded_warnings fqn=%s",
                fqn,
            )

    return {"warnings_written": warnings_written, "warnings_cleared": warnings_cleared}


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
    Output matches schemas/exclude_output.json.
    """
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_exclude(root, list(fqns))
    emit(result)


@app.command("sync-excluded-warnings")
def sync_excluded_warnings_cmd(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Write or clear EXCLUDED_DEP warnings on active catalog objects.

    Idempotent — safe to run on every /status invocation.
    Output matches schemas/sync_excluded_warnings_output.json.
    """
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_sync_excluded_warnings(root)
    emit(result)
