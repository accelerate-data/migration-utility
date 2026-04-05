"""dry_run.py — Migration stage prerequisite checker and content reader.

Standalone CLI with two subcommands:

    dry-run   Check guards for a (table, stage) pair and return
              eligibility + catalog/dbt content as JSON.

    guard     Check guards only (no content collection). Returns
              pass/fail JSON for use by skills and plugin commands.

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
import yaml

from shared.catalog import (
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
    read_selected_writer,
)
from shared.env_config import resolve_dbt_project_path, resolve_project_root
from shared.loader_data import CatalogLoadError
from shared.name_resolver import fqn_parts, normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Constants ────────────────────────────────────────────────────────────────

STAGES = ("scope", "profile", "test-gen", "refactor", "migrate")

PROFILE_QUESTIONS = (
    "classification",
    "primary_key",
    "foreign_keys",
    "natural_key",
    "watermark",
    "pii_actions",
)


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
    """Check refactor section exists with status ok."""
    cat = load_table_catalog(project_root, table_fqn)
    if cat is None:
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            "Table catalog not found.",
        )
    refactor = cat.get("refactor")
    if refactor is None:
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            "Refactor section missing from catalog.",
        )
    status = refactor.get("status")
    if status != "ok":
        return _guard_fail(
            "refactor_completed",
            "REFACTOR_NOT_COMPLETED",
            f"Refactor status is '{status}', expected ok.",
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
}

# Guards that only need project_root (no table_fqn).
_PROJECT_ONLY_GUARDS = frozenset({
    "check_manifest", "check_sandbox_metadata", "check_dbt_project",
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


# ── Content collectors ───────────────────────────────────────────────────────


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


def _model_name_from_table(table_fqn: str) -> str:
    """Derive a dbt model name from a table FQN.

    ``silver.dim_customer`` → ``stg_dim_customer``
    """
    _, name = fqn_parts(normalize(table_fqn))
    return f"stg_{name}"


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
            except Exception:
                logger.debug("event=schema_yaml_parse_error path=%s", candidate)
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
    model_name = _model_name_from_table(table_fqn)

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
        except Exception:
            logger.debug("event=run_results_parse_error path=%s", run_results_path)

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


def _sandbox_metadata(project_root: Path) -> dict[str, Any] | None:
    """Read sandbox metadata from manifest."""
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return manifest.get("sandbox")


def test_gen_detail(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Profile summary + full test-spec + sandbox metadata."""
    spec = _load_test_spec(project_root, table_fqn)
    return {
        "profile": profile_summary(project_root, table_fqn),
        "test_spec": spec,
        "sandbox": _sandbox_metadata(project_root),
    }


def test_gen_summary(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Compact test-gen status."""
    spec = _load_test_spec(project_root, table_fqn)
    sandbox = _sandbox_metadata(project_root)
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


def refactor_detail(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Test-gen summary + full refactor section from catalog."""
    cat = load_table_catalog(project_root, table_fqn) or {}
    refactor = cat.get("refactor") or {}
    return {
        "test_gen": test_gen_summary(project_root, table_fqn),
        "refactor": refactor,
    }


def refactor_summary(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Compact refactor status."""
    cat = load_table_catalog(project_root, table_fqn) or {}
    refactor = cat.get("refactor") or {}
    return {
        "refactor_status": refactor.get("status"),
        "has_refactored_sql": bool(refactor.get("refactored_sql_hash")),
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


# ── Orchestrator ─────────────────────────────────────────────────────────────


def run_dry_run(
    project_root: Path,
    table_fqn: str,
    stage: str,
    detail: bool = False,
) -> dict[str, Any]:
    """Run dry-run checks for a (table, stage) pair.

    Returns a dict matching schemas/dry_run_output.json.
    """
    norm = normalize(table_fqn)
    guards_passed, guard_results = run_guards(project_root, norm, stage)

    result: dict[str, Any] = {
        "table": norm,
        "stage": stage,
        "guards_passed": guards_passed,
        "guard_results": guard_results,
    }

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
    profile = "profile"
    test_gen = "test-gen"
    refactor = "refactor"
    migrate = "migrate"
    generating_model = "generating-model"
    reviewing_model = "reviewing-model"
    reviewing_tests = "reviewing-tests"


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


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
        _emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_dry_run(root, table, stage.value, detail=detail)
    _emit(result)


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
        _emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    norm = normalize(table)
    guards_passed, guard_results = run_guards(root, norm, stage.value)
    _emit({
        "table": norm,
        "stage": stage.value,
        "passed": guards_passed,
        "guard_results": guard_results,
    })
