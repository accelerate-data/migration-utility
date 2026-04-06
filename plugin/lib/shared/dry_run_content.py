"""dry_run_content.py — Stage content collectors for migrate-util dry-run.

Each stage exposes a detail function (full catalog/dbt blobs) and a summary
function (compact status fields).  _CONTENT_COLLECTORS maps stage → {detail, summary}.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import yaml

from shared.catalog import load_proc_catalog, load_table_catalog, read_selected_writer
from shared.context_helpers import sandbox_metadata
from shared.env_config import resolve_dbt_project_path
from shared.name_resolver import fqn_parts, model_name_from_table, normalize

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────────

PROFILE_QUESTIONS = (
    "classification",
    "primary_key",
    "foreign_keys",
    "natural_key",
    "watermark",
    "pii_actions",
)


# ── Content helpers ───────────────────────────────────────────────────────────


def _statement_counts(
    project_root: Path, table_fqn: str,
) -> dict[str, int]:
    """Count statements by action in the selected writer's proc catalog."""
    writer = read_selected_writer(project_root, table_fqn)
    if not writer:
        return {"migrate": 0, "skip": 0, "unresolved": 0, "total": 0}
    proc_cat = load_proc_catalog(project_root, writer)
    if not proc_cat:
        return {"migrate": 0, "skip": 0, "unresolved": 0, "total": 0}
    statements = proc_cat.get("statements", [])
    migrate = sum(1 for s in statements if s.get("action") == "migrate")
    skip = sum(1 for s in statements if s.get("action") == "skip")
    unresolved = len(statements) - migrate - skip
    return {
        "migrate": migrate,
        "skip": skip,
        "unresolved": unresolved,
        "total": len(statements),
    }


def _profile_question_status(profile: dict[str, Any]) -> dict[str, str]:
    """Check which of the 6 profiling questions are answered."""
    result: dict[str, str] = {}
    for q in PROFILE_QUESTIONS:
        val = profile.get(q)
        if val is None:
            result[q] = "missing"
        elif isinstance(val, dict) and not val:
            result[q] = "missing"
        elif isinstance(val, list) and not val:
            result[q] = "empty"
        else:
            result[q] = "answered"
    return result


def _find_dbt_model(dbt_root: Path, model_name: str) -> Path | None:
    """Find a dbt model SQL file by name under dbt/models/."""
    models_dir = dbt_root / "models"
    if not models_dir.is_dir():
        return None
    matches = list(models_dir.rglob(f"{model_name}.sql"))
    return matches[0] if matches else None


def _find_schema_yaml(model_path: Path) -> tuple[Path | None, bool]:
    """Find schema YAML alongside a model and check for unit_tests key.

    Returns (yaml_path, has_unit_tests).
    """
    if model_path is None:
        return None, False
    parent = model_path.parent
    model_stem = model_path.stem
    # Convention: _<model_name>.yml or schema.yml in same dir
    for candidate in [
        parent / f"_{model_stem}.yml",
        parent / f"{model_stem}.yml",
        parent / "schema.yml",
        parent / f"_{model_stem}.yaml",
        parent / f"{model_stem}.yaml",
        parent / "schema.yaml",
    ]:
        if candidate.exists():
            try:
                content = yaml.safe_load(candidate.read_text(encoding="utf-8"))
                has_tests = _yaml_has_unit_tests(content, model_stem)
                return candidate, has_tests
            except (yaml.YAMLError, OSError) as exc:
                logger.warning(
                    "event=schema_yaml_parse_error component=dry_run_content operation=load_schema path=%s status=failure error=%s",
                    candidate, exc,
                )
                return candidate, False
    return None, False


def _yaml_has_unit_tests(content: Any, model_name: str) -> bool:
    """Check if a schema YAML has unit_tests for the given model."""
    if not isinstance(content, dict):
        return False
    models = content.get("models", [])
    if not isinstance(models, list):
        return False
    for model in models:
        if not isinstance(model, dict):
            continue
        if model.get("name") == model_name and model.get("unit_tests"):
            return True
    return False


def _dbt_evidence(
    project_root: Path, table_fqn: str,
) -> dict[str, Any]:
    """Collect dbt artifact evidence for a table."""
    dbt_root = resolve_dbt_project_path(project_root)
    model_name = model_name_from_table(table_fqn)

    model_path = _find_dbt_model(dbt_root, model_name)
    schema_yaml_path, has_unit_tests = _find_schema_yaml(model_path)

    # Compiled artifacts
    target_dir = dbt_root / "target"
    compiled_matches = (
        list(target_dir.rglob(f"compiled/**/{model_name}.sql"))
        if target_dir.is_dir()
        else []
    )
    compiled_path = compiled_matches[0] if compiled_matches else None

    # Run results
    run_results_path = target_dir / "run_results.json"
    test_results_exist = False
    if run_results_path.exists():
        try:
            rr = json.loads(run_results_path.read_text(encoding="utf-8"))
            results = rr.get("results", [])
            test_results_exist = any(
                model_name in r.get("unique_id", "") for r in results
            )
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(
                "event=run_results_parse_error component=dry_run_content operation=load_run_results path=%s status=failure error=%s",
                run_results_path, exc,
            )

    return {
        "model_name": model_name,
        "model_path": str(model_path) if model_path else None,
        "model_exists": model_path is not None,
        "schema_yaml_path": str(schema_yaml_path) if schema_yaml_path else None,
        "schema_yaml_exists": schema_yaml_path is not None,
        "has_unit_tests": has_unit_tests,
        "compiled_path": str(compiled_path) if compiled_path else None,
        "compiled_exists": compiled_path is not None,
        "test_results_exist": test_results_exist,
    }


# ── Stage content: scope ─────────────────────────────────────────────────────


def scope_detail(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Full catalog file + scoping section + statement resolution breakdown."""
    cat = load_table_catalog(project_root, table_fqn) or {}
    scoping = cat.get("scoping")
    return {
        "catalog": cat,
        "scoping": scoping,
        "statements": _statement_counts(project_root, table_fqn),
    }


def scope_summary(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Compact scope status."""
    cat = load_table_catalog(project_root, table_fqn) or {}
    scoping = cat.get("scoping") or {}
    candidates = scoping.get("candidates", [])
    return {
        "scoping_status": scoping.get("status"),
        "selected_writer": scoping.get("selected_writer"),
        "candidate_count": len(candidates),
        "statements": _statement_counts(project_root, table_fqn),
    }


# ── Stage content: profile ───────────────────────────────────────────────────


def profile_detail(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Scoping summary + full profile section."""
    cat = load_table_catalog(project_root, table_fqn) or {}
    profile = cat.get("profile") or {}
    return {
        "scoping": scope_summary(project_root, table_fqn),
        "profile": profile,
        "questions": _profile_question_status(profile),
    }


def profile_summary(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Compact profile status."""
    cat = load_table_catalog(project_root, table_fqn) or {}
    profile = cat.get("profile") or {}
    classification = profile.get("classification") or {}
    pk = profile.get("primary_key") or {}
    fks = profile.get("foreign_keys") or []
    pii = profile.get("pii_actions") or []
    watermark = profile.get("watermark") or {}
    return {
        "scoping_status": (cat.get("scoping") or {}).get("status"),
        "profile_status": profile.get("status"),
        "resolved_kind": classification.get("resolved_kind"),
        "pk_type": pk.get("primary_key_type"),
        "has_watermark": bool(watermark.get("column")),
        "fk_count": len(fks),
        "pii_action_count": len(pii),
        "questions": _profile_question_status(profile),
    }


# ── Stage content: test-gen ──────────────────────────────────────────────────


def _load_test_spec(project_root: Path, table_fqn: str) -> dict[str, Any] | None:
    """Load a test spec file if it exists."""
    norm = normalize(table_fqn)
    spec_path = project_root / "test-specs" / f"{norm}.json"
    if not spec_path.exists():
        return None
    return json.loads(spec_path.read_text(encoding="utf-8"))



def test_gen_detail(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Profile summary + full test-spec + sandbox metadata."""
    spec = _load_test_spec(project_root, table_fqn)
    return {
        "profile": profile_summary(project_root, table_fqn),
        "test_spec": spec,
        "sandbox": sandbox_metadata(project_root),
    }


def test_gen_summary(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Compact test-gen status."""
    spec = _load_test_spec(project_root, table_fqn)
    sandbox = sandbox_metadata(project_root)
    if spec:
        branches = spec.get("branch_manifest", [])
        tests = spec.get("unit_tests", [])
        return {
            "profile_status": (
                load_table_catalog(project_root, table_fqn) or {}
            ).get("profile", {}).get("status"),
            "test_spec_status": spec.get("status"),
            "coverage": spec.get("coverage"),
            "branch_count": len(branches),
            "test_count": len(tests),
            "sandbox_database": sandbox.get("database") if sandbox else None,
        }
    return {
        "profile_status": (
            load_table_catalog(project_root, table_fqn) or {}
        ).get("profile", {}).get("status"),
        "test_spec_status": None,
        "coverage": None,
        "branch_count": 0,
        "test_count": 0,
        "sandbox_database": sandbox.get("database") if sandbox else None,
    }


# ── Stage content: refactor ──────────────────────────────────────────────────


def _load_proc_refactor(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Load the refactor section from the writer procedure's catalog."""
    writer_fqn = read_selected_writer(project_root, table_fqn)
    if not writer_fqn:
        return {}
    cat = load_proc_catalog(project_root, normalize(writer_fqn)) or {}
    return cat.get("refactor") or {}


def refactor_detail(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Test-gen summary + full refactor section from procedure catalog."""
    return {
        "test_gen": test_gen_summary(project_root, table_fqn),
        "refactor": _load_proc_refactor(project_root, table_fqn),
    }


def refactor_summary(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Compact refactor status."""
    refactor = _load_proc_refactor(project_root, table_fqn)
    return {
        "refactor_status": refactor.get("status"),
        "has_extracted_sql": bool(refactor.get("extracted_sql")),
        "has_refactored_sql": bool(refactor.get("refactored_sql")),
    }


# ── Stage content: migrate ───────────────────────────────────────────────────


def migrate_detail(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Test-spec summary + dbt evidence."""
    return {
        "test_spec": test_gen_summary(project_root, table_fqn),
        "dbt": _dbt_evidence(project_root, table_fqn),
    }


def migrate_summary(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Compact migrate status."""
    evidence = _dbt_evidence(project_root, table_fqn)
    spec = _load_test_spec(project_root, table_fqn)
    return {
        "test_spec_status": spec.get("status") if spec else None,
        "dbt_model_exists": evidence["model_exists"],
        "schema_yaml_exists": evidence["schema_yaml_exists"],
        "has_unit_tests": evidence["has_unit_tests"],
        "compiled_exists": evidence["compiled_exists"],
        "test_results_exist": evidence["test_results_exist"],
    }


# ── Stage content dispatch ───────────────────────────────────────────────────

_CONTENT_COLLECTORS: dict[str, dict[str, Any]] = {
    "scope": {"detail": scope_detail, "summary": scope_summary},
    "profile": {"detail": profile_detail, "summary": profile_summary},
    "test-gen": {"detail": test_gen_detail, "summary": test_gen_summary},
    "refactor": {"detail": refactor_detail, "summary": refactor_summary},
    "migrate": {"detail": migrate_detail, "summary": migrate_summary},
}
