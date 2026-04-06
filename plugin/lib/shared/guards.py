"""guards.py — Stage guard functions and registry for migrate-util.

Each check_* function validates one prerequisite and returns a guard result dict.
run_guards() runs all guards for a given stage, short-circuiting on first failure.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import (
    load_proc_catalog,
    load_table_catalog,
    read_selected_writer,
)
from shared.env_config import resolve_dbt_project_path
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────────

STAGES = ("scope", "profile", "test-gen", "refactor", "migrate")

KNOWN_TECHNOLOGIES = frozenset({
    "sql_server", "fabric_warehouse", "fabric_lakehouse", "snowflake", "oracle",
})


# ── Guard helpers ────────────────────────────────────────────────────────────


def _guard_ok(check: str) -> dict[str, Any]:
    return {"check": check, "passed": True}


def _guard_fail(check: str, code: str, message: str) -> dict[str, Any]:
    return {"check": check, "passed": False, "code": code, "message": message}


# ── Individual guard functions ───────────────────────────────────────────────


def check_manifest(project_root: Path) -> dict[str, Any]:
    """Check manifest.json exists and is readable."""
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return _guard_fail(
            "manifest_exists",
            "MANIFEST_NOT_FOUND",
            "manifest.json not found in project root.",
        )
    try:
        json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return _guard_fail(
            "manifest_exists",
            "MANIFEST_CORRUPT",
            f"manifest.json is not valid JSON: {exc}",
        )
    return _guard_ok("manifest_exists")


def check_table_catalog(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Check catalog/tables/<item_id>.json exists and is valid JSON."""
    try:
        cat = load_table_catalog(project_root, table_fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError) as exc:
        norm = normalize(table_fqn)
        return _guard_fail(
            "table_catalog_exists",
            "CATALOG_FILE_CORRUPT",
            f"catalog/tables/{norm}.json is not valid JSON: {exc}",
        )
    if cat is None:
        norm = normalize(table_fqn)
        return _guard_fail(
            "table_catalog_exists",
            "CATALOG_FILE_MISSING",
            f"catalog/tables/{norm}.json not found.",
        )
    return _guard_ok("table_catalog_exists")


def check_selected_writer(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Check scoping.selected_writer is set in the table catalog."""
    writer = read_selected_writer(project_root, table_fqn)
    if writer is None:
        norm = normalize(table_fqn)
        return _guard_fail(
            "selected_writer_set",
            "SCOPING_NOT_COMPLETED",
            f"scoping.selected_writer missing in catalog for {norm}.",
        )
    return _guard_ok("selected_writer_set")


def check_statements_resolved(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Check all writer statements are resolved to migrate|skip."""
    writer = read_selected_writer(project_root, table_fqn)
    if writer is None:
        return _guard_fail(
            "statements_resolved",
            "SCOPING_NOT_COMPLETED",
            "Cannot check statements — no selected_writer.",
        )
    try:
        proc_cat = load_proc_catalog(project_root, writer)
    except (json.JSONDecodeError, OSError, CatalogLoadError) as exc:
        return _guard_fail(
            "statements_resolved",
            "STATEMENTS_NOT_RESOLVED",
            f"Procedure catalog for {writer} is not valid JSON: {exc}",
        )
    if proc_cat is None:
        return _guard_fail(
            "statements_resolved",
            "STATEMENTS_NOT_RESOLVED",
            f"Procedure catalog for {writer} not found.",
        )
    statements = proc_cat.get("statements", [])
    if not statements:
        return _guard_fail(
            "statements_resolved",
            "STATEMENTS_NOT_RESOLVED",
            f"No statements found in procedure catalog for {writer}.",
        )
    unresolved = [
        s for s in statements if s.get("action") not in ("migrate", "skip")
    ]
    if unresolved:
        return _guard_fail(
            "statements_resolved",
            "STATEMENTS_NOT_RESOLVED",
            f"{len(unresolved)} statement(s) not resolved to migrate|skip.",
        )
    return _guard_ok("statements_resolved")


def check_profile_ok(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Check profile section exists with status ok|partial."""
    cat = load_table_catalog(project_root, table_fqn)
    if cat is None:
        return _guard_fail(
            "profile_completed",
            "PROFILE_NOT_COMPLETED",
            "Table catalog not found.",
        )
    profile = cat.get("profile")
    if profile is None:
        return _guard_fail(
            "profile_completed",
            "PROFILE_NOT_COMPLETED",
            "Profile section missing from catalog.",
        )
    status = profile.get("status")
    if status not in ("ok", "partial"):
        return _guard_fail(
            "profile_completed",
            "PROFILE_NOT_COMPLETED",
            f"Profile status is '{status}', expected ok or partial.",
        )
    return _guard_ok("profile_completed")


def check_sandbox_metadata(project_root: Path) -> dict[str, Any]:
    """Check manifest.json has sandbox metadata (database name)."""
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return _guard_fail(
            "sandbox_configured",
            "SANDBOX_NOT_CONFIGURED",
            "manifest.json not found.",
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return _guard_fail(
            "sandbox_configured",
            "SANDBOX_NOT_CONFIGURED",
            f"manifest.json is not valid JSON: {exc}",
        )
    sandbox = manifest.get("sandbox")
    if not sandbox or not sandbox.get("database"):
        return _guard_fail(
            "sandbox_configured",
            "SANDBOX_NOT_CONFIGURED",
            "Sandbox metadata (database) missing from manifest.",
        )
    return _guard_ok("sandbox_configured")


def check_test_spec(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Check test-specs/<item_id>.json exists."""
    norm = normalize(table_fqn)
    spec_path = project_root / "test-specs" / f"{norm}.json"
    if not spec_path.exists():
        return _guard_fail(
            "test_spec_exists",
            "TEST_SPEC_NOT_FOUND",
            f"test-specs/{norm}.json not found.",
        )
    return _guard_ok("test_spec_exists")


def check_refactor_complete(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Check refactor section on the writer procedure's catalog.

    Resolves the writer via ``scoping.selected_writer`` on the table catalog,
    then validates the ``refactor`` block on the procedure catalog: status must
    be ``ok`` and both ``extracted_sql`` and ``refactored_sql`` must be present.
    """
    writer_fqn = read_selected_writer(project_root, table_fqn)
    if not writer_fqn:
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            "No scoping.selected_writer in table catalog.",
        )
    writer_norm = normalize(writer_fqn)
    cat = load_proc_catalog(project_root, writer_norm)
    if cat is None:
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            f"Procedure catalog not found for writer {writer_norm}.",
        )
    refactor = cat.get("refactor")
    if refactor is None:
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            "Refactor section missing from procedure catalog.",
        )
    status = refactor.get("status")
    if status != "ok":
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            f"Refactor status is '{status}', expected ok.",
        )
    extracted = refactor.get("extracted_sql")
    if not extracted or not extracted.strip():
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            "Refactor section has no extracted_sql.",
        )
    refactored = refactor.get("refactored_sql")
    if not refactored or not refactored.strip():
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            "Refactor section has no refactored_sql.",
        )
    return _guard_ok("refactor_completed")


def check_dbt_project(project_root: Path) -> dict[str, Any]:
    """Check dbt_project.yml exists in the dbt project directory."""
    dbt_root = resolve_dbt_project_path(project_root)
    if not (dbt_root / "dbt_project.yml").exists():
        return _guard_fail(
            "dbt_project_exists",
            "DBT_PROJECT_MISSING",
            "dbt_project.yml not found. Run /init-dbt first.",
        )
    return _guard_ok("dbt_project_exists")


def check_view_dependencies_migrated(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Check that all view dependencies of the selected writer proc are migrated.

    Reads the proc catalog's ``references.views.in_scope`` entries and verifies
    that a ``stg_<view_name>.sql`` file exists in ``dbt/models/staging/`` for
    each view.  Views not yet migrated are reported so the user can run
    ``/refactor-view`` first.
    """
    writer = read_selected_writer(project_root, table_fqn)
    if writer is None:
        return _guard_fail(
            "view_dependencies_migrated",
            "SCOPING_NOT_COMPLETED",
            "Cannot check view dependencies — no selected_writer.",
        )
    try:
        proc_cat = load_proc_catalog(project_root, writer)
    except (json.JSONDecodeError, OSError, CatalogLoadError) as exc:
        return _guard_fail(
            "view_dependencies_migrated",
            "VIEW_DEP_CHECK_ERROR",
            f"Could not load procedure catalog for {writer}: {exc}",
        )
    if proc_cat is None:
        return _guard_fail(
            "view_dependencies_migrated",
            "VIEW_DEP_CHECK_ERROR",
            f"Procedure catalog for {writer} not found.",
        )

    refs = proc_cat.get("references", {})
    # Older catalog format may have references as a list — treat as no view deps.
    if not isinstance(refs, dict):
        return _guard_ok("view_dependencies_migrated")
    view_entries = refs.get("views", {}).get("in_scope", [])
    if not view_entries:
        return _guard_ok("view_dependencies_migrated")

    dbt_root = resolve_dbt_project_path(project_root)
    staging_dir = dbt_root / "models" / "staging"
    missing: list[str] = []
    for entry in view_entries:
        view_name = entry.get("name", "")
        stg_file = staging_dir / f"stg_{view_name}.sql"
        if not stg_file.exists():
            schema = entry.get("schema", "")
            missing.append(f"{schema}.{view_name}" if schema else view_name)

    if missing:
        missing_str = ", ".join(sorted(missing))
        return _guard_fail(
            "view_dependencies_migrated",
            "VIEW_DEPENDENCIES_NOT_MIGRATED",
            f"View dependencies not yet migrated. Run /refactor-view on: {missing_str}",
        )
    return _guard_ok("view_dependencies_migrated")


def check_technology(project_root: Path) -> dict[str, Any]:
    """Check manifest.json exists and contains a known technology value."""
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return _guard_fail(
            "technology_configured",
            "MANIFEST_NOT_FOUND",
            "manifest.json not found. Run /init-ad-migration to initialise the project.",
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return _guard_fail(
            "technology_configured",
            "MANIFEST_CORRUPT",
            f"manifest.json is not valid JSON: {exc}",
        )
    technology = manifest.get("technology")
    if technology is None:
        return _guard_fail(
            "technology_configured",
            "TECHNOLOGY_NOT_SET",
            "manifest.json has no 'technology' field. Run /init-ad-migration.",
        )
    if technology not in KNOWN_TECHNOLOGIES:
        return _guard_fail(
            "technology_configured",
            "TECHNOLOGY_UNKNOWN",
            f"technology '{technology}' is not recognised. Known: {sorted(KNOWN_TECHNOLOGIES)}.",
        )
    return _guard_ok("technology_configured")


# ── Guard runner ─────────────────────────────────────────────────────────────

# Stage → ordered list of guard callables.
# Each callable takes (project_root, table_fqn) or (project_root,) and returns
# a guard result dict.
_STAGE_GUARDS: dict[str, list[tuple[str, ...]]] = {
    "scope": [
        ("check_manifest",),
        ("check_table_catalog",),
    ],
    "profile": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
    ],
    "test-gen": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
        ("check_profile_ok",),
        ("check_sandbox_metadata",),
    ],
    "refactor": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
        ("check_profile_ok",),
        ("check_sandbox_metadata",),
        ("check_test_spec",),
    ],
    "migrate": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
        ("check_profile_ok",),
        ("check_sandbox_metadata",),
        ("check_test_spec",),
        ("check_refactor_complete",),
    ],
    # Skill-specific guard sets (not pipeline stages, but callable via guard CLI)
    "generating-model": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
        ("check_profile_ok",),
        ("check_sandbox_metadata",),
        ("check_test_spec",),
        ("check_refactor_complete",),
        ("check_dbt_project",),
        ("check_view_dependencies_migrated",),
    ],
    "reviewing-model": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
        ("check_profile_ok",),
        ("check_sandbox_metadata",),
        ("check_test_spec",),
        ("check_refactor_complete",),
        ("check_dbt_project",),
        ("check_view_dependencies_migrated",),
    ],
    "reviewing-tests": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
        ("check_profile_ok",),
        ("check_sandbox_metadata",),
        ("check_test_spec",),
    ],
    "refactoring-sql": [
        ("check_manifest",),
        ("check_table_catalog",),
        ("check_selected_writer",),
        ("check_statements_resolved",),
        ("check_profile_ok",),
        ("check_sandbox_metadata",),
        ("check_test_spec",),
    ],
    "setup-ddl": [
        ("check_technology",),
    ],
}

_GUARD_FNS = {
    "check_manifest": check_manifest,
    "check_table_catalog": check_table_catalog,
    "check_selected_writer": check_selected_writer,
    "check_statements_resolved": check_statements_resolved,
    "check_profile_ok": check_profile_ok,
    "check_sandbox_metadata": check_sandbox_metadata,
    "check_test_spec": check_test_spec,
    "check_refactor_complete": check_refactor_complete,
    "check_dbt_project": check_dbt_project,
    "check_technology": check_technology,
    "check_view_dependencies_migrated": check_view_dependencies_migrated,
}

# Guards that only need project_root (no table_fqn).
_PROJECT_ONLY_GUARDS = frozenset({
    "check_manifest", "check_sandbox_metadata", "check_dbt_project", "check_technology",
})


def run_guards(
    project_root: Path, table_fqn: str, stage: str,
) -> tuple[bool, list[dict[str, Any]]]:
    """Run all guards for a stage, short-circuiting on first failure.

    Returns (all_passed, guard_results).
    """
    results: list[dict[str, Any]] = []
    for (guard_name,) in _STAGE_GUARDS[stage]:
        fn = _GUARD_FNS[guard_name]
        if guard_name in _PROJECT_ONLY_GUARDS:
            result = fn(project_root)
        else:
            result = fn(project_root, table_fqn)
        results.append(result)
        if not result["passed"]:
            return False, results
    return True, results
