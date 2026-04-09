"""pipeline_status.py — Pipeline stage status and diagnostics for catalog objects.

Determines the current pipeline position (scope → profile → test-gen →
refactor → migrate → complete) for each catalog object and collects
warnings/errors from catalog sub-sections.

Split from batch_plan.py for module focus.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import load_proc_catalog, load_table_catalog, load_view_catalog
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)

# Maps diagnostic codes to the pipeline stage most impacted by that diagnostic.
# Used to pre-compute diagnostic_stage_flags per object node so the display layer
# does not need to reason about code->stage mappings.
_DIAG_STAGE_MAP: dict[str, str] = {
    "PARSE_ERROR": "refactor",
    "DDL_PARSE_ERROR": "refactor",
    "MULTI_TABLE_WRITE": "scope",
    "REMOTE_EXEC_UNSUPPORTED": "scope",
}

# Severity rank for worst-severity promotion. Higher rank wins.
_SEV_RANK: dict[str, int] = {"warning": 0, "error": 1}


def _compute_diagnostic_stage_flags(diagnostics: list[dict[str, Any]]) -> dict[str, str]:
    """Map a node's diagnostics to their most-impacted pipeline stage.

    Returns a dict of {stage: worst_severity} for stages with at least one
    relevant diagnostic, e.g. {"refactor": "error"} or {"scope": "warning"}.
    The highest-ranked severity for each stage wins (error > warning).
    Unknown severity values are treated as lower than warning and do not
    overwrite a known severity.
    """
    flags: dict[str, str] = {}
    for d in diagnostics:
        stage = _DIAG_STAGE_MAP.get(d.get("code", ""))
        if not stage:
            continue
        sev = d.get("severity", "warning")
        if _SEV_RANK.get(sev, -1) > _SEV_RANK.get(flags.get(stage, ""), -1):
            flags[stage] = sev
    return flags


# ── Pipeline status ──────────────────────────────────────────────────────────


def object_pipeline_status(
    project_root: Path,
    fqn: str,
    obj_type: str,
    dbt_root: Path,
) -> str:
    """Determine the first incomplete pipeline stage for an object.

    Uses direct status field reads from catalog files rather than inference.

    Returns one of:
        scope_needed    — not yet scoped / view not yet analyzed
        profile_needed  — scoped but not profiled
        test_gen_needed — profiled but test-gen not complete
        refactor_needed — test-gen complete but writer not refactored
        migrate_needed  — refactored but no dbt model / generate not ok
        complete        — dbt model exists
        n_a             — writerless table (writer-dependent stages N/A)
    """
    if obj_type in ("view", "mv"):
        try:
            cat = load_view_catalog(project_root, fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            return "scope_needed"
        if cat is None:
            return "scope_needed"
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status != "analyzed":
            return "scope_needed"
        profile_status = cat.profile.status if cat.profile else None
        if profile_status not in ("ok", "partial"):
            return "profile_needed"
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return "test_gen_needed"
        refactor_status = cat.refactor.status if cat.refactor else None
        if refactor_status != "ok":
            return "refactor_needed"
        generate_status = cat.generate.status if cat.generate else None
        if generate_status != "ok":
            return "migrate_needed"
        return "complete"

    # TABLE
    try:
        cat = load_table_catalog(project_root, fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return "scope_needed"
    if cat is None:
        return "scope_needed"

    scoping_status = cat.scoping.status if cat.scoping else None
    if scoping_status == "no_writer_found":
        return "n_a"
    if scoping_status != "resolved":
        return "scope_needed"

    profile_status = cat.profile.status if cat.profile else None
    if profile_status not in ("ok", "partial"):
        return "profile_needed"

    test_gen_status = cat.test_gen.status if cat.test_gen else None
    if test_gen_status != "ok":
        return "test_gen_needed"

    writer = cat.scoping.selected_writer if cat.scoping else None
    if writer:
        writer_norm = normalize(writer)
        try:
            proc_cat = load_proc_catalog(project_root, writer_norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            proc_cat = None
        refactor_status = proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
        if refactor_status != "ok":
            return "refactor_needed"

    generate_status = cat.generate.status if cat.generate else None
    if generate_status != "ok":
        return "migrate_needed"

    return "complete"


# ── Diagnostics ──────────────────────────────────────────────────────────────


def collect_object_diagnostics(
    project_root: Path,
    fqn: str,
    obj_type: str,
) -> list[dict[str, Any]]:
    """Collect all warnings and errors from a catalog object and its sub-sections."""
    diagnostics: list[dict[str, Any]] = []

    def _gather(source: Any) -> None:
        if not source:
            return
        for entry in getattr(source, "warnings", None) or []:
            if isinstance(entry, dict) and "severity" not in entry:
                entry = {**entry, "severity": "warning"}
            diagnostics.append(entry)
        for entry in getattr(source, "errors", None) or []:
            if isinstance(entry, dict) and "severity" not in entry:
                entry = {**entry, "severity": "error"}
            diagnostics.append(entry)

    try:
        if obj_type in ("view", "mv"):
            cat = load_view_catalog(project_root, fqn)
            if cat is None:
                return diagnostics
            _gather(cat)
            _gather(cat.scoping)
        else:
            cat = load_table_catalog(project_root, fqn)
            if cat is None:
                return diagnostics
            _gather(cat)
            _gather(cat.scoping)
            _gather(cat.profile)
            _gather(cat.refactor)
            writer = cat.scoping.selected_writer if cat.scoping else None
            if writer:
                try:
                    proc_cat = load_proc_catalog(project_root, normalize(writer))
                    if proc_cat is not None:
                        _gather(proc_cat)
                        _gather(proc_cat.refactor)
                except (json.JSONDecodeError, OSError, CatalogLoadError):
                    pass
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        pass

    return diagnostics


def _compute_status_and_diagnostics(
    project_root: Path,
    fqn: str,
    obj_type: str,
    dbt_root: Path,
) -> tuple[str, list[dict[str, Any]]]:
    """Compute pipeline status and diagnostics in a single catalog load pass.

    Combines ``object_pipeline_status`` and ``collect_object_diagnostics``
    to avoid loading the same catalog files twice per object.
    """
    diagnostics: list[dict[str, Any]] = []

    def _gather(source: Any) -> None:
        if not source:
            return
        for entry in getattr(source, "warnings", None) or []:
            if isinstance(entry, dict) and "severity" not in entry:
                entry = {**entry, "severity": "warning"}
            diagnostics.append(entry)
        for entry in getattr(source, "errors", None) or []:
            if isinstance(entry, dict) and "severity" not in entry:
                entry = {**entry, "severity": "error"}
            diagnostics.append(entry)

    if obj_type in ("view", "mv"):
        try:
            cat = load_view_catalog(project_root, fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            return "scope_needed", diagnostics
        if cat is None:
            return "scope_needed", diagnostics
        _gather(cat)
        _gather(cat.scoping)
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status != "analyzed":
            return "scope_needed", diagnostics
        profile_status = cat.profile.status if cat.profile else None
        if profile_status not in ("ok", "partial"):
            return "profile_needed", diagnostics
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return "test_gen_needed", diagnostics
        refactor_status = cat.refactor.status if cat.refactor else None
        if refactor_status != "ok":
            return "refactor_needed", diagnostics
        generate_status = cat.generate.status if cat.generate else None
        if generate_status != "ok":
            return "migrate_needed", diagnostics
        return "complete", diagnostics

    # TABLE
    try:
        cat = load_table_catalog(project_root, fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return "scope_needed", diagnostics
    if cat is None:
        return "scope_needed", diagnostics
    _gather(cat)
    _gather(cat.scoping)
    _gather(cat.profile)
    _gather(cat.refactor)

    scoping_status = cat.scoping.status if cat.scoping else None
    if scoping_status == "no_writer_found":
        return "n_a", diagnostics
    if scoping_status != "resolved":
        return "scope_needed", diagnostics

    profile_status = cat.profile.status if cat.profile else None
    if profile_status not in ("ok", "partial"):
        return "profile_needed", diagnostics

    test_gen_status = cat.test_gen.status if cat.test_gen else None
    if test_gen_status != "ok":
        return "test_gen_needed", diagnostics

    writer = cat.scoping.selected_writer if cat.scoping else None
    proc_cat = None
    if writer:
        writer_norm = normalize(writer)
        try:
            proc_cat = load_proc_catalog(project_root, writer_norm)
            if proc_cat is not None:
                _gather(proc_cat)
                _gather(proc_cat.refactor)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            pass
        refactor_status = proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
        if refactor_status != "ok":
            return "refactor_needed", diagnostics

    generate_status = cat.generate.status if cat.generate else None
    if generate_status != "ok":
        return "migrate_needed", diagnostics

    return "complete", diagnostics
