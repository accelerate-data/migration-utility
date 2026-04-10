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

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, List, Optional

from shared.output_models import (
    DryRunOutput,
    ExcludeOutput,
    ObjectStatus,
    StageStatuses,
    StatusOutput,
    StatusSummary,
    SyncExcludedWarningsOutput,
)

import typer

from shared.batch_plan import build_batch_plan
from shared.deps import collect_deps
from shared.catalog import (
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    read_selected_writer,
    write_json,
)
from shared.loader_data import CatalogLoadError
from shared.cli_utils import emit
from shared.env_config import resolve_project_root
from shared.name_resolver import fqn_parts, normalize  # re-exported for test compat

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _detect_object_type(project_root: Path, norm_fqn: str) -> str:
    """Detect whether a normalised FQN refers to a table, view, or MV.

    Checks catalog/tables/ first, then catalog/views/.  Falls back to 'table'
    so that a missing file surfaces through the normal failure path.
    """
    table_path = project_root / "catalog" / "tables" / f"{norm_fqn}.json"
    if table_path.exists():
        return "table"
    view_path = project_root / "catalog" / "views" / f"{norm_fqn}.json"
    if view_path.exists():
        try:
            cat = load_view_catalog(project_root, norm_fqn)
            if cat and cat.is_materialized_view:
                return "mv"
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            pass
        return "view"
    return "table"  # fallback


# ── Ready subcommand ─────────────────────────────────────────────────────────

_VALID_STAGES = frozenset({"scope", "profile", "test-gen", "refactor", "migrate", "generate"})


def run_ready(project_root: Path, fqn: str, stage: str) -> DryRunOutput:
    """Check whether the prior stage's CLI-written status allows proceeding.

    Returns a DryRunOutput with ready/reason fields.
    """
    norm = normalize(fqn)
    obj_type = _detect_object_type(project_root, norm)

    def _out(ready: bool, reason: str, code: str | None = None) -> DryRunOutput:
        return DryRunOutput(ready=ready, reason=reason, code=code)

    if stage not in _VALID_STAGES:
        return _out(False, "invalid_stage")

    # ── Special cases (check BEFORE stage logic) ─────────────────────────
    if obj_type == "table":
        try:
            cat = load_table_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None:
            if cat.is_source:
                return _out(False, "not_applicable", "SOURCE_TABLE")
            if cat.excluded:
                return _out(False, "not_applicable", "EXCLUDED")
    else:
        try:
            cat = load_view_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None:
            if cat.excluded:
                return _out(False, "not_applicable", "EXCLUDED")

    # ── Stage logic ──────────────────────────────────────────────────────
    if stage == "scope":
        manifest_path = project_root / "manifest.json"
        if not manifest_path.exists():
            return _out(False, "manifest_missing")
        if obj_type == "table":
            catalog_exists = (project_root / "catalog" / "tables" / f"{norm}.json").exists()
        else:
            catalog_exists = (project_root / "catalog" / "views" / f"{norm}.json").exists()
        if not catalog_exists:
            return _out(False, "catalog_missing")
        return _out(True, "ok")

    if stage == "profile":
        if obj_type in ("view", "mv"):
            if cat is None:
                return _out(False, "catalog_missing")
            scoping_status = cat.scoping.status if cat.scoping else None
            if scoping_status != "analyzed":
                return _out(False, "scoping_not_analyzed")
            return _out(True, "ok")
        if cat is None:
            return _out(False, "catalog_missing")
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status == "no_writer_found":
            return _out(False, "not_applicable", "WRITERLESS_TABLE")
        if scoping_status != "resolved":
            return _out(False, "scoping_not_resolved")
        return _out(True, "ok")

    if stage == "test-gen":
        if cat is None:
            return _out(False, "catalog_missing")
        profile_status = cat.profile.status if cat.profile else None
        if profile_status not in ("ok", "partial"):
            return _out(False, "profile_not_complete")
        return _out(True, "ok")

    if stage == "refactor":
        if cat is None:
            return _out(False, "catalog_missing")
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return _out(False, "test_gen_not_complete")
        return _out(True, "ok")

    # "generate"/"migrate" checks whether refactor is complete (can I start?),
    # NOT whether generate already succeeded. Use run_status() to check if
    # generate.status == "ok" (is it done?).
    if stage in ("migrate", "generate"):
        if obj_type in ("view", "mv"):
            if cat is None:
                return _out(False, "catalog_missing")
            refactor_status = cat.refactor.status if cat.refactor else None
            if refactor_status != "ok":
                return _out(False, "refactor_not_complete")
            return _out(True, "ok")
        if cat is None:
            return _out(False, "catalog_missing")
        writer = cat.scoping.selected_writer if cat.scoping else None
        if not writer:
            return _out(False, "no_writer")
        writer_norm = normalize(writer)
        try:
            proc_cat = load_proc_catalog(project_root, writer_norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            proc_cat = None
        refactor_status = proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
        if refactor_status != "ok":
            return _out(False, "refactor_not_complete")
        return _out(True, "ok")

    return _out(False, "invalid_stage")


# ── Status subcommand ────────────────────────────────────────────────────────


def _single_object_status(project_root: Path, norm_fqn: str) -> ObjectStatus:
    """Collate all stage statuses for a single object."""
    obj_type = _detect_object_type(project_root, norm_fqn)

    scope = profile = test_gen = refactor = generate = None

    if obj_type in ("view", "mv"):
        try:
            cat = load_view_catalog(project_root, norm_fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        scope = cat.scoping.status if cat and cat.scoping else None
        profile = cat.profile.status if cat and cat.profile else None
        test_gen = cat.test_gen.status if cat and cat.test_gen else None
        refactor = cat.refactor.status if cat and cat.refactor else None
        generate = cat.generate.status if cat and cat.generate else None
    else:
        try:
            cat = load_table_catalog(project_root, norm_fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        scope = cat.scoping.status if cat and cat.scoping else None
        profile = cat.profile.status if cat and cat.profile else None
        test_gen = cat.test_gen.status if cat and cat.test_gen else None
        refactor_status: str | None = None
        if cat and cat.refactor:
            refactor_status = cat.refactor.status
        else:
            writer = cat.scoping.selected_writer if cat and cat.scoping else None
            if writer:
                writer_norm = normalize(writer)
                try:
                    proc_cat = load_proc_catalog(project_root, writer_norm)
                except (json.JSONDecodeError, OSError, CatalogLoadError):
                    proc_cat = None
                refactor_status = proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
        refactor = refactor_status
        generate = cat.generate.status if cat and cat.generate else None

    return ObjectStatus(
        fqn=norm_fqn,
        type=obj_type,
        stages=StageStatuses(
            scope=scope, profile=profile, test_gen=test_gen,
            refactor=refactor, generate=generate,
        ),
    )


def run_status(project_root: Path, fqn: str | None = None) -> StatusOutput | ObjectStatus:
    """Collate CLI-written statuses from catalog files.

    With fqn: returns an ObjectStatus for one object.
    Without fqn: returns a StatusOutput with full matrix for all objects.
    """
    if fqn is not None:
        norm = normalize(fqn)
        return _single_object_status(project_root, norm)

    # ── All-objects mode ─────────────────────────────────────────────────
    catalog_dir = project_root / "catalog"
    table_dir = catalog_dir / "tables"
    view_dir = catalog_dir / "views"

    objects: list[ObjectStatus] = []
    stage_counts: dict[str, dict[str, int]] = {
        stage: {} for stage in ("scope", "profile", "test_gen", "refactor", "generate")
    }

    for bucket, bucket_dir in (("tables", table_dir), ("views", view_dir)):
        if not bucket_dir.is_dir():
            continue
        for p in sorted(bucket_dir.glob("*.json")):
            norm_fqn = p.stem
            if bucket == "tables":
                try:
                    cat_data = json.loads(p.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    cat_data = {}
                if cat_data.get("is_source"):
                    continue

            obj_status = _single_object_status(project_root, norm_fqn)
            objects.append(obj_status)

            stages_dict = obj_status.stages.model_dump()
            for stage_name, status_val in stages_dict.items():
                label = status_val if status_val else "pending"
                stage_counts[stage_name][label] = stage_counts[stage_name].get(label, 0) + 1

    return StatusOutput(
        objects=objects,
        summary=StatusSummary(total=len(objects), by_stage=stage_counts),
    )


# ── CLI ──────────────────────────────────────────────────────────────────────


@app.command("ready")
def ready_cmd(
    fqn: str = typer.Argument(..., help="Fully-qualified object name"),
    stage: str = typer.Argument(..., help="Pipeline stage to check readiness for"),
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

    result = run_ready(root, fqn, stage)
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


# ── Exclude helpers ───────────────────────────────────────────────────────────


def _detect_catalog_bucket(project_root: Path, norm_fqn: str) -> str | None:
    """Return 'tables' or 'views' if a catalog file exists, else None."""
    if (project_root / "catalog" / "tables" / f"{norm_fqn}.json").exists():
        return "tables"
    if (project_root / "catalog" / "views" / f"{norm_fqn}.json").exists():
        return "views"
    return None


def run_exclude(project_root: Path, fqns: list[str]) -> ExcludeOutput:
    """Set ``excluded: true`` on each named table or view catalog file.

    Returns an ExcludeOutput with marked/not_found lists.
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

    return ExcludeOutput(marked=marked, not_found=not_found)


def run_sync_excluded_warnings(project_root: Path) -> SyncExcludedWarningsOutput:
    """Write or clear EXCLUDED_DEP warnings on active catalog objects.

    For every active (non-excluded) table and view, checks whether any of its
    transitive dependencies are currently excluded.  If so, writes an
    EXCLUDED_DEP warning to the object's catalog ``warnings[]``.  If a
    previously written EXCLUDED_DEP warning no longer applies (the referenced
    object is no longer excluded), it is removed.

    Full replacement of ``warnings[]`` is idempotent — matches the pattern
    used by the diagnostics runner.

    Returns a SyncExcludedWarningsOutput.
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
        return SyncExcludedWarningsOutput(warnings_written=warnings_written, warnings_cleared=warnings_cleared)

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

    return SyncExcludedWarningsOutput(warnings_written=warnings_written, warnings_cleared=warnings_cleared)


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
