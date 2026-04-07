"""Tests for dry_run.py — migration stage prerequisite checker.

Tests import shared.dry_run core functions directly (not via subprocess) to keep
execution fast and test coverage clear.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from shared import dry_run
from shared import generate_sources as gen_src

_cli_runner = CliRunner()

_TESTS_DIR = Path(__file__).parent
_FIXTURES = _TESTS_DIR / "fixtures" / "dry_run"


def _make_project(*, include_sandbox: bool = True) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy dry_run fixtures to a temp dir and git-init it."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    shutil.copytree(_FIXTURES, dst)
    if not include_sandbox:
        manifest_path = dst / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        del manifest["sandbox"]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    subprocess.run(["git", "init"], cwd=dst, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=dst, capture_output=True, check=True,
        env={"GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "t@t", "HOME": str(Path.home())},
    )
    return tmp, dst


def _make_bare_project() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Create a project with only manifest.json (no catalog, no scoping)."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    dst.mkdir(parents=True)
    manifest = {
        "schema_version": "1.0",
        "technology": "sql_server",
        "dialect": "tsql",
        "source_database": "TestDB",
        "extracted_schemas": ["silver"],
        "extracted_at": "2026-04-01T00:00:00Z",
    }
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "catalog" / "tables").mkdir(parents=True)
    # Table catalog without scoping or profile
    table_cat = {
        "schema": "silver",
        "name": "DimDate",
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "referenced_by": [],
    }
    (dst / "catalog" / "tables" / "silver.dimdate.json").write_text(
        json.dumps(table_cat), encoding="utf-8",
    )
    subprocess.run(["git", "init"], cwd=dst, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=dst, capture_output=True, check=True,
        env={"GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "t@t", "HOME": str(Path.home())},
    )
    return tmp, dst


# ── Guard tests: scope ───────────────────────────────────────────────────────


def test_scope_guards_pass(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        assert all(g["passed"] for g in result["guard_results"])
        assert "content" in result


def test_scope_guards_fail_no_manifest(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").unlink()
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "MANIFEST_NOT_FOUND"
        assert "content" not in result


def test_scope_guards_fail_no_catalog_file(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.NonExistent", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "CATALOG_FILE_MISSING"


# ── Guard tests: profile ─────────────────────────────────────────────────────


def test_profile_guards_pass(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True


def test_profile_guards_fail_no_selected_writer(assert_valid_schema) -> None:
    tmp, root = _make_bare_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimDate", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "SCOPING_NOT_COMPLETED"


def test_profile_guards_fail_unresolved_statements(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        # Mutate proc catalog to have an unresolved statement
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        proc["statements"].append({"index": 3, "action": "needs_llm", "source": "llm", "sql": "EXEC dbo.other_proc"})
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "STATEMENTS_NOT_RESOLVED"


# ── Guard tests: test-gen ────────────────────────────────────────────────────


def test_test_gen_guards_pass(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True


def test_test_gen_guards_fail_no_sandbox(assert_valid_schema) -> None:
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "SANDBOX_NOT_CONFIGURED"


# ── Guard tests: migrate ─────────────────────────────────────────────────────


def test_migrate_guards_pass(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True


def test_migrate_guards_fail_no_test_spec(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        (root / "test-specs" / "silver.dimcustomer.json").unlink()
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "TEST_SPEC_NOT_FOUND"


# ── Guard tests: setup-ddl ───────────────────────────────────────────────────


def test_setup_ddl_guard_passes() -> None:
    tmp, root = _make_project()
    with tmp:
        passed, results = dry_run.run_guards(root, "silver.Foo", "setup-ddl")
        assert passed is True
        assert all(g["passed"] for g in results)


def test_setup_ddl_guard_fails_no_manifest() -> None:
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").unlink()
        passed, results = dry_run.run_guards(root, "silver.Foo", "setup-ddl")
        assert passed is False
        assert results[-1]["code"] == "MANIFEST_NOT_FOUND"
        assert "/init-ad-migration" in results[-1]["message"]


def test_setup_ddl_guard_fails_no_technology() -> None:
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        del manifest["technology"]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        passed, results = dry_run.run_guards(root, "silver.Foo", "setup-ddl")
        assert passed is False
        assert results[-1]["code"] == "TECHNOLOGY_NOT_SET"


def test_setup_ddl_guard_fails_unknown_technology() -> None:
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["technology"] = "unknown_db"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        passed, results = dry_run.run_guards(root, "silver.Foo", "setup-ddl")
        assert passed is False
        assert results[-1]["code"] == "TECHNOLOGY_UNKNOWN"


# ── Content tests: scope ─────────────────────────────────────────────────────


def test_scope_summary_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["scoping_status"] == "resolved"
        assert content["selected_writer"] == "dbo.usp_load_dimcustomer"
        assert content["candidate_count"] == 1
        assert content["statements"]["migrate"] == 2
        assert content["statements"]["skip"] == 1
        assert content["statements"]["unresolved"] == 0


def test_scope_detail_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope", detail=True)
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert "catalog" in content
        assert content["catalog"]["schema"] == "silver"
        assert content["scoping"]["status"] == "resolved"
        assert content["statements"]["total"] == 3


# ── Content tests: profile ───────────────────────────────────────────────────


def test_profile_summary_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["profile_status"] == "ok"
        assert content["resolved_kind"] == "dim_scd1"
        assert content["pk_type"] == "surrogate"
        assert content["has_watermark"] is True
        assert content["fk_count"] == 1
        assert content["pii_action_count"] == 1
        assert all(v == "answered" for v in content["questions"].values())


def test_profile_detail_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile", detail=True)
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert "profile" in content
        assert content["profile"]["status"] == "ok"
        assert content["profile"]["classification"]["resolved_kind"] == "dim_scd1"
        assert "scoping" in content


# ── Content tests: test-gen ──────────────────────────────────────────────────


def test_test_gen_summary_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["test_spec_status"] == "ok"
        assert content["coverage"] == "complete"
        assert content["branch_count"] == 2
        assert content["test_count"] == 2
        assert content["sandbox_database"] == "__test_abc123"


def test_test_gen_detail_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen", detail=True)
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["test_spec"]["item_id"] == "silver.dimcustomer"
        assert content["sandbox"]["database"] == "__test_abc123"


# ── Content tests: migrate ───────────────────────────────────────────────────


def test_migrate_summary_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["test_spec_status"] == "ok"
        assert content["dbt_model_exists"] is True
        assert content["schema_yaml_exists"] is True
        assert content["has_unit_tests"] is True
        assert content["compiled_exists"] is True


def test_migrate_detail_dbt_evidence(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate", detail=True)
        assert_valid_schema(result, "dry_run_output.json")
        dbt = result["content"]["dbt"]
        assert dbt["model_exists"] is True
        assert dbt["schema_yaml_exists"] is True
        assert dbt["has_unit_tests"] is True
        assert dbt["compiled_exists"] is True
        assert "stg_dimcustomer" in dbt["model_path"]


# ── Corrupted / malformed JSON ───────────────────────────────────────────────


def test_corrupt_manifest_json(assert_valid_schema) -> None:
    """Corrupted manifest.json should fail with MANIFEST_CORRUPT."""
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").write_text("NOT JSON{{{", encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "MANIFEST_CORRUPT"


def test_corrupt_table_catalog_json(assert_valid_schema) -> None:
    """Corrupted table catalog should fail with CATALOG_FILE_CORRUPT."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat_path.write_text("{broken json", encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "CATALOG_FILE_CORRUPT"


def test_corrupt_proc_catalog_json(assert_valid_schema) -> None:
    """Corrupted procedure catalog should fail with STATEMENTS_NOT_RESOLVED."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_path.write_text("<<<not json>>>", encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "STATEMENTS_NOT_RESOLVED"


def test_corrupt_manifest_sandbox_check(assert_valid_schema) -> None:
    """Corrupted manifest during sandbox check should fail gracefully."""
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").write_text("!corrupt!", encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False


# ── Incomplete / partial state scenarios ─────────────────────────────────────


def test_scoped_but_not_profiled(assert_valid_schema) -> None:
    """Table with scoping done but no profile section — profile guard fails."""
    tmp, root = _make_project()
    with tmp:
        # Remove profile section from catalog
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["profile"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        # Scope should still pass
        scope_result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert scope_result["guards_passed"] is True
        # Profile should also pass (guards check scoping + statements, not profile itself)
        profile_result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert profile_result["guards_passed"] is True
        # But test-gen should fail — needs profile
        test_gen_result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen")
        assert_valid_schema(test_gen_result, "dry_run_output.json")
        assert test_gen_result["guards_passed"] is False
        assert test_gen_result["guard_results"][-1]["code"] == "PROFILE_NOT_COMPLETED"


def test_profile_status_error_blocks_test_gen(assert_valid_schema) -> None:
    """Profile with status 'error' should block test-gen."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["profile"]["status"] = "error"
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "PROFILE_NOT_COMPLETED"


def test_profile_partial_allows_test_gen(assert_valid_schema) -> None:
    """Profile with status 'partial' should still allow test-gen."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["profile"]["status"] = "partial"
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True


def test_ambiguous_multi_writer_blocks_profile(assert_valid_schema) -> None:
    """Scoping with ambiguous_multi_writer (no selected_writer) blocks profile."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["scoping"]["status"] = "ambiguous_multi_writer"
        del cat["scoping"]["selected_writer"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "SCOPING_NOT_COMPLETED"


def test_empty_statements_array_blocks_profile(assert_valid_schema) -> None:
    """Proc catalog with empty statements array blocks profile."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        proc["statements"] = []
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "STATEMENTS_NOT_RESOLVED"


def test_sandbox_with_missing_database(assert_valid_schema) -> None:
    """Sandbox section present but missing database should fail."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["sandbox"] = {}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "test-gen")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "SANDBOX_NOT_CONFIGURED"


# ── dbt evidence edge cases ──────────────────────────────────────────────────


def test_migrate_no_dbt_model(assert_valid_schema) -> None:
    """Model missing from dbt/ — evidence shows model and YAML both missing.

    Schema YAML is located relative to the model file, so when the model
    is absent the YAML lookup also returns None.
    """
    tmp, root = _make_project()
    with tmp:
        (root / "dbt" / "models" / "staging" / "stg_dimcustomer.sql").unlink()
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        content = result["content"]
        assert content["dbt_model_exists"] is False
        assert content["schema_yaml_exists"] is False


def test_migrate_no_schema_yaml(assert_valid_schema) -> None:
    """dbt model exists but no schema YAML alongside it."""
    tmp, root = _make_project()
    with tmp:
        (root / "dbt" / "models" / "staging" / "_stg_dimcustomer.yml").unlink()
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        content = result["content"]
        assert content["dbt_model_exists"] is True
        assert content["schema_yaml_exists"] is False
        assert content["has_unit_tests"] is False


def test_migrate_schema_yaml_without_unit_tests(assert_valid_schema) -> None:
    """Schema YAML exists but has no unit_tests key."""
    tmp, root = _make_project()
    with tmp:
        yaml_path = root / "dbt" / "models" / "staging" / "_stg_dimcustomer.yml"
        yaml_path.write_text(
            "models:\n  - name: stg_dimcustomer\n    description: No tests\n",
            encoding="utf-8",
        )
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["schema_yaml_exists"] is True
        assert content["has_unit_tests"] is False


def test_migrate_no_compiled_artifacts(assert_valid_schema) -> None:
    """No compiled artifacts in target/ — compiled_exists is False."""
    tmp, root = _make_project()
    with tmp:
        import shutil as _shutil
        _shutil.rmtree(root / "dbt" / "target")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["compiled_exists"] is False
        assert content["test_results_exist"] is False


# ── Profile content: partial / missing questions ─────────────────────────────


def test_profile_summary_partial_questions(assert_valid_schema) -> None:
    """Profile with some questions missing shows them correctly."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["profile"]["status"] = "partial"
        del cat["profile"]["watermark"]
        del cat["profile"]["natural_key"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["profile_status"] == "partial"
        assert content["has_watermark"] is False
        assert content["questions"]["watermark"] == "missing"
        assert content["questions"]["natural_key"] == "missing"
        assert content["questions"]["classification"] == "answered"


# ── Multi-table mixed state ──────────────────────────────────────────────────


def _add_table_to_project(
    root: Path,
    table_fqn: str,
    *,
    include_scoping: bool = False,
    include_profile: bool = False,
) -> None:
    """Add a table catalog file to an existing project fixture."""
    norm = dry_run.normalize(table_fqn)
    schema, name = dry_run.fqn_parts(norm)
    cat: dict[str, Any] = {
        "schema": schema,
        "name": name,
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "referenced_by": [],
    }
    if include_scoping:
        cat["scoping"] = {
            "status": "resolved",
            "selected_writer": f"dbo.usp_load_{name}",
            "candidates": [
                {"procedure_name": f"dbo.usp_load_{name}", "dependencies": {"tables": [], "views": [], "functions": []}, "rationale": "test"}
            ],
            "warnings": [],
            "validation": {"passed": True, "issues": []},
            "errors": [],
        }
        # Also create proc catalog with resolved statements
        proc_dir = root / "catalog" / "procedures"
        proc_dir.mkdir(parents=True, exist_ok=True)
        proc_cat = {
            "schema": "dbo",
            "name": f"usp_load_{name}",
            "statements": [{"index": 0, "action": "migrate", "source": "ast", "sql": "INSERT INTO ..."}],
            "references": [],
        }
        (proc_dir / f"dbo.usp_load_{name}.json").write_text(
            json.dumps(proc_cat), encoding="utf-8",
        )
    if include_profile:
        cat["profile"] = {
            "status": "ok",
            "writer": f"dbo.usp_load_{name}",
            "classification": {"resolved_kind": "fact_transaction", "rationale": "test", "source": "llm"},
            "primary_key": {"columns": ["id"], "primary_key_type": "surrogate", "source": "llm"},
            "natural_key": {},
            "watermark": {"column": "load_date", "rationale": "test", "source": "llm"},
            "foreign_keys": [],
            "pii_actions": [],
            "warnings": [],
            "errors": [],
        }
    (root / "catalog" / "tables" / f"{norm}.json").write_text(
        json.dumps(cat), encoding="utf-8",
    )


def test_multi_table_at_different_stages(assert_valid_schema) -> None:
    """Multiple tables at different pipeline stages produce correct results."""
    tmp, root = _make_project()
    with tmp:
        # silver.DimCustomer is fully complete (from fixtures)
        # Add silver.FactSales — scoped + profiled but no test-spec
        _add_table_to_project(root, "silver.FactSales", include_scoping=True, include_profile=True)
        # Add silver.DimDate — only catalog exists (no scoping)
        _add_table_to_project(root, "silver.DimDate")
        # Add silver.DimProduct — scoped but not profiled
        _add_table_to_project(root, "silver.DimProduct", include_scoping=True)

        # DimCustomer: all stages pass
        r1 = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(r1, "dry_run_output.json")
        assert r1["guards_passed"] is True

        # FactSales: scoped + profiled, but test-gen fails (no test-spec... but
        # test-gen guard checks profile + sandbox, not test-spec)
        r2 = dry_run.run_dry_run(root, "silver.FactSales", "test-gen")
        assert_valid_schema(r2, "dry_run_output.json")
        assert r2["guards_passed"] is True  # has profile + sandbox

        # FactSales: migrate fails — no test-spec
        r2m = dry_run.run_dry_run(root, "silver.FactSales", "migrate")
        assert_valid_schema(r2m, "dry_run_output.json")
        assert r2m["guards_passed"] is False
        assert r2m["guard_results"][-1]["code"] == "TEST_SPEC_NOT_FOUND"

        # DimDate: scope passes, profile fails (no scoping)
        r3s = dry_run.run_dry_run(root, "silver.DimDate", "scope")
        assert r3s["guards_passed"] is True
        r3p = dry_run.run_dry_run(root, "silver.DimDate", "profile")
        assert r3p["guards_passed"] is False
        assert r3p["guard_results"][-1]["code"] == "SCOPING_NOT_COMPLETED"

        # DimProduct: scope passes, profile passes (scoped with statements),
        # but test-gen fails (no profile section)
        r4p = dry_run.run_dry_run(root, "silver.DimProduct", "profile")
        assert r4p["guards_passed"] is True
        r4t = dry_run.run_dry_run(root, "silver.DimProduct", "test-gen")
        assert r4t["guards_passed"] is False
        assert r4t["guard_results"][-1]["code"] == "PROFILE_NOT_COMPLETED"


# ── FQN normalization edge cases ─────────────────────────────────────────────


def test_bracketed_table_name(assert_valid_schema) -> None:
    """Bracketed T-SQL identifiers are normalized correctly."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "[silver].[DimCustomer]", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        assert result["table"] == "silver.dimcustomer"


def test_case_insensitive_table_name(assert_valid_schema) -> None:
    """Mixed-case table names are normalized to lowercase."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "SILVER.DIMCUSTOMER", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        assert result["table"] == "silver.dimcustomer"


# ── CliRunner integration tests ──────────────────────────────────────────────


def test_cli_dry_run_scope_summary() -> None:
    """CLI dry-run scope outputs valid JSON to stdout."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["dry-run", "silver.DimCustomer", "scope", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["guards_passed"] is True
        assert output["stage"] == "scope"
        assert "content" in output


def test_cli_dry_run_detail_flag() -> None:
    """CLI --detail flag produces full content."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["dry-run", "silver.DimCustomer", "scope", "--detail", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["guards_passed"] is True
        assert "catalog" in output["content"]


def test_cli_dry_run_guard_failure_exit_0() -> None:
    """CLI returns exit 0 even on guard failure — guards_passed=false in JSON."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["dry-run", "silver.NonExistent", "scope", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["guards_passed"] is False


def test_cli_dry_run_invalid_stage() -> None:
    """CLI rejects invalid stage name."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["dry-run", "silver.DimCustomer", "bogus", "--project-root", str(root)],
        )
        assert result.exit_code != 0


def test_cli_dry_run_not_git_repo() -> None:
    """CLI exits 2 when project root is not a git repo."""
    tmp = tempfile.TemporaryDirectory()
    with tmp:
        root = Path(tmp.name) / "not-a-repo"
        root.mkdir()
        result = _cli_runner.invoke(
            dry_run.app,
            ["dry-run", "silver.DimCustomer", "scope", "--project-root", str(root)],
        )
        assert result.exit_code == 2


def test_cli_dry_run_all_stages() -> None:
    """CLI works for all five stages on a fully-populated fixture."""
    tmp, root = _make_project()
    with tmp:
        for stage in ("scope", "profile", "test-gen", "refactor", "migrate"):
            result = _cli_runner.invoke(
                dry_run.app,
                ["dry-run", "silver.DimCustomer", stage, "--project-root", str(root)],
            )
            assert result.exit_code == 0, f"stage {stage} failed"
            output = json.loads(result.stdout)
            assert output["guards_passed"] is True, f"stage {stage} guards failed"
            assert output["stage"] == stage


# ── Guard tests: refactor ────────────────────────────────────────────────────


def test_refactor_guards_pass(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "refactor")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True


def test_refactor_guards_fail_no_test_spec(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        (root / "test-specs" / "silver.dimcustomer.json").unlink()
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "refactor")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "TEST_SPEC_NOT_FOUND"


def test_migrate_guards_fail_no_refactor(assert_valid_schema) -> None:
    """Migrate guard fails when refactor section is missing from procedure catalog."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        cat = json.loads(proc_path.read_text(encoding="utf-8"))
        del cat["refactor"]
        proc_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "REFACTOR_NOT_COMPLETED"


def test_migrate_guards_fail_refactor_partial(assert_valid_schema) -> None:
    """Migrate guard fails when refactor status is partial (audit failed)."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        cat = json.loads(proc_path.read_text(encoding="utf-8"))
        cat["refactor"]["status"] = "partial"
        proc_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "REFACTOR_NOT_COMPLETED"


def test_migrate_guards_fail_refactor_error(assert_valid_schema) -> None:
    """Migrate guard fails when refactor status is error (could not proceed)."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        cat = json.loads(proc_path.read_text(encoding="utf-8"))
        cat["refactor"]["status"] = "error"
        proc_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["guard_results"][-1]["code"] == "REFACTOR_NOT_COMPLETED"


# ── Content tests: refactor ─────────────────────────────────────────────────


def test_refactor_summary_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "refactor")
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["refactor_status"] == "ok"
        assert content["has_refactored_sql"] is True


def test_refactor_detail_content(assert_valid_schema) -> None:
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "refactor", detail=True)
        assert_valid_schema(result, "dry_run_output.json")
        content = result["content"]
        assert content["refactor"]["status"] == "ok"
        assert "test_gen" in content


# ── Guard CLI tests ─────────────────────────────────────────────────────────


def test_guard_cli_subcommand(assert_valid_schema) -> None:
    """Guard CLI returns pass/fail JSON."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["guard", "silver.DimCustomer", "scope", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert_valid_schema(output, "guard_output.json")
        assert output["passed"] is True
        assert output["table"] == "silver.dimcustomer"
        assert output["stage"] == "scope"
        assert len(output["guard_results"]) > 0


def test_guard_cli_failure() -> None:
    """Guard CLI returns passed=false for missing table."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["guard", "silver.NonExistent", "scope", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["passed"] is False


def test_guard_cli_skill_stage() -> None:
    """Guard CLI accepts skill-specific stage names like generating-model."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["guard", "silver.DimCustomer", "generating-model", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        # generating-model requires dbt_project — fixture has it
        assert output["passed"] is True
        assert output["stage"] == "generating-model"


def test_guard_cli_invalid_stage() -> None:
    """Guard CLI rejects invalid stage name."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["guard", "silver.DimCustomer", "bogus", "--project-root", str(root)],
        )
        assert result.exit_code != 0


def test_guard_cli_dbt_project_missing() -> None:
    """Guard CLI fails for generating-model when dbt_project.yml is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "dbt" / "dbt_project.yml").unlink()
        result = _cli_runner.invoke(
            dry_run.app,
            ["guard", "silver.DimCustomer", "generating-model", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["passed"] is False
        assert output["guard_results"][-1]["code"] == "DBT_PROJECT_MISSING"


# ── Guard tests: check_view_dependencies_migrated ───────────────────────────


def _make_project_with_view_deps(
    *,
    view_entries: list[dict],
    stg_files: list[str] | None = None,
) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Create a minimal project for view dependency guard tests.

    *view_entries* is the list for ``references.views.in_scope`` in the proc catalog.
    *stg_files* lists which ``stg_*.sql`` files to create in ``dbt/models/staging/``.
    """
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)

    # manifest
    manifest = {
        "schema_version": "1.0",
        "technology": "sql_server",
        "dialect": "tsql",
        "source_database": "TestDB",
        "extracted_schemas": ["silver"],
        "extracted_at": "2026-04-01T00:00:00Z",
        "sandbox": {"database": "TestDB_sandbox"},
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    # table catalog: silver.DimCustomer with selected_writer
    (root / "catalog" / "tables").mkdir(parents=True)
    table_cat = {
        "schema": "silver",
        "name": "dimcustomer",
        "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load"},
        "primary_keys": [],
        "columns": [],
        "referenced_by": {},
    }
    (root / "catalog" / "tables" / "silver.dimcustomer.json").write_text(
        json.dumps(table_cat), encoding="utf-8",
    )

    # proc catalog: dbo.usp_load with given view entries
    (root / "catalog" / "procedures").mkdir(parents=True)
    proc_cat = {
        "schema": "dbo",
        "name": "usp_load",
        "references": {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": view_entries, "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
    }
    (root / "catalog" / "procedures" / "dbo.usp_load.json").write_text(
        json.dumps(proc_cat), encoding="utf-8",
    )

    # dbt project
    staging_dir = root / "dbt" / "models" / "staging"
    staging_dir.mkdir(parents=True)
    (root / "dbt" / "dbt_project.yml").write_text("name: test", encoding="utf-8")

    for stg in (stg_files or []):
        (staging_dir / stg).write_text("-- generated", encoding="utf-8")

    return tmp, root


def test_view_deps_guard_passes_no_views() -> None:
    """Guard passes when proc has no view dependencies."""
    tmp, root = _make_project_with_view_deps(view_entries=[])
    with tmp:
        from shared.guards import check_view_dependencies_migrated
        result = check_view_dependencies_migrated(root, "silver.DimCustomer")
        assert result["passed"] is True
        assert result["check"] == "view_dependencies_migrated"


def test_view_deps_guard_passes_all_migrated() -> None:
    """Guard passes when all view stg_ files exist."""
    view_entries = [{"schema": "silver", "name": "vw_customer_base", "is_selected": True, "is_updated": False}]
    tmp, root = _make_project_with_view_deps(
        view_entries=view_entries,
        stg_files=["stg_vw_customer_base.sql"],
    )
    with tmp:
        from shared.guards import check_view_dependencies_migrated
        result = check_view_dependencies_migrated(root, "silver.DimCustomer")
        assert result["passed"] is True


def test_view_deps_guard_fails_missing_stg_file() -> None:
    """Guard fails when a dependent view has no stg_ file yet."""
    view_entries = [{"schema": "silver", "name": "vw_customer_base", "is_selected": True, "is_updated": False}]
    tmp, root = _make_project_with_view_deps(view_entries=view_entries, stg_files=[])
    with tmp:
        from shared.guards import check_view_dependencies_migrated
        result = check_view_dependencies_migrated(root, "silver.DimCustomer")
        assert result["passed"] is False
        assert result["code"] == "VIEW_DEPENDENCIES_NOT_MIGRATED"
        assert "vw_customer_base" in result["message"]
        assert "/refactor-view" in result["message"]


def test_view_deps_guard_reports_all_missing() -> None:
    """Guard message lists all missing view stg_ files."""
    view_entries = [
        {"schema": "silver", "name": "vw_a", "is_selected": True, "is_updated": False},
        {"schema": "silver", "name": "vw_b", "is_selected": True, "is_updated": False},
    ]
    tmp, root = _make_project_with_view_deps(view_entries=view_entries, stg_files=["stg_vw_a.sql"])
    with tmp:
        from shared.guards import check_view_dependencies_migrated
        result = check_view_dependencies_migrated(root, "silver.DimCustomer")
        assert result["passed"] is False
        assert "vw_b" in result["message"]
        # vw_a was migrated, should not appear in the message
        assert "vw_a" not in result["message"]


def test_view_deps_guard_handles_legacy_list_references() -> None:
    """Guard passes gracefully for old-format proc catalogs with references as a list."""
    tmp, root = _make_project_with_view_deps(view_entries=[])
    with tmp:
        # Overwrite proc catalog with old list format
        proc_cat = {"schema": "dbo", "name": "usp_load", "references": []}
        (root / "catalog" / "procedures" / "dbo.usp_load.json").write_text(
            json.dumps(proc_cat), encoding="utf-8",
        )
        from shared.guards import check_view_dependencies_migrated
        result = check_view_dependencies_migrated(root, "silver.DimCustomer")
        assert result["passed"] is True


def test_generating_model_guard_includes_view_dep_check() -> None:
    """generating-model stage guard list includes check_view_dependencies_migrated."""
    from shared.guards import _STAGE_GUARDS, check_view_dependencies_migrated
    guard_fns = [fn for (fn,) in _STAGE_GUARDS["generating-model"]]
    assert check_view_dependencies_migrated in guard_fns


def test_reviewing_model_guard_includes_view_dep_check() -> None:
    """reviewing-model stage guard list includes check_view_dependencies_migrated."""
    from shared.guards import _STAGE_GUARDS, check_view_dependencies_migrated
    guard_fns = [fn for (fn,) in _STAGE_GUARDS["reviewing-model"]]
    assert check_view_dependencies_migrated in guard_fns


# ── generate-sources tests ──────────────────────────────────────────────────


def _add_source_table(root: Path, schema: str, name: str) -> None:
    """Add a table with no_writer_found scoping (a true source)."""
    norm = f"{schema.lower()}.{name.lower()}"
    cat = {
        "schema": schema,
        "name": name,
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "referenced_by": [],
        "scoping": {
            "status": "no_writer_found",
            "selected_writer": None,
            "selected_writer_rationale": "No procedures found that write to this table.",
        },
    }
    (root / "catalog" / "tables" / f"{norm}.json").write_text(
        json.dumps(cat), encoding="utf-8",
    )


def test_generate_sources_only_includes_no_writer_found(assert_valid_schema) -> None:
    """Only tables with no_writer_found are included in sources."""
    tmp, root = _make_project()
    with tmp:
        # silver.DimCustomer has scoping.status == "resolved" (from fixture)
        # silver.RefCurrency has scoping.status == "no_writer_found" (from fixture)
        # Add a bronze source table
        _add_source_table(root, "bronze", "CustomerRaw")

        result = gen_src.generate_sources(root)
        assert_valid_schema(result, "generate_sources_output.json")
        assert "bronze.customerraw" in result["included"]
        assert "silver.refcurrency" in result["included"]
        assert "silver.dimcustomer" in result["excluded"]
        assert result["incomplete"] == []
        assert result["sources"] is not None
        # Both bronze and silver schemas appear in sources
        schema_names = [s["name"] for s in result["sources"]["sources"]]
        assert "bronze" in schema_names
        assert "silver" in schema_names


def test_generate_sources_excludes_resolved_tables() -> None:
    """Tables with resolved status are excluded; writerless tables are included."""
    tmp, root = _make_project()
    with tmp:
        result = gen_src.generate_sources(root)
        # silver.DimCustomer is resolved → excluded
        assert "silver.dimcustomer" in result["excluded"]
        # silver.RefCurrency is no_writer_found → included as source
        assert "silver.refcurrency" in result["included"]
        assert result["sources"] is not None


def test_generate_sources_detects_incomplete_scoping() -> None:
    """Tables without scoping section are flagged as incomplete."""
    tmp, root = _make_project()
    with tmp:
        _add_table_to_project(root, "silver.DimDate")  # no scoping
        result = gen_src.generate_sources(root)
        assert "silver.dimdate" in result["incomplete"]


def test_generate_sources_mixed_statuses() -> None:
    """Mixed resolved, no_writer_found, and incomplete tables."""
    tmp, root = _make_project()
    with tmp:
        # silver.DimCustomer: resolved (from fixture)
        # silver.RefCurrency: no_writer_found (from fixture)
        _add_source_table(root, "bronze", "CustomerRaw")  # no_writer_found
        _add_source_table(root, "bronze", "OrderRaw")  # no_writer_found
        _add_table_to_project(root, "silver.DimDate")  # no scoping

        result = gen_src.generate_sources(root)
        assert sorted(result["included"]) == ["bronze.customerraw", "bronze.orderraw", "silver.refcurrency"]
        assert result["excluded"] == ["silver.dimcustomer"]
        assert result["incomplete"] == ["silver.dimdate"]
        # Sources should have bronze and silver schemas
        schema_names = {s["name"] for s in result["sources"]["sources"]}
        assert "bronze" in schema_names
        assert "silver" in schema_names


def test_generate_sources_empty_catalog() -> None:
    """Empty catalog/tables/ produces empty result."""
    tmp, root = _make_bare_project()
    with tmp:
        # bare project has silver.DimDate without scoping
        # Remove it to test truly empty
        for f in (root / "catalog" / "tables").glob("*.json"):
            f.unlink()
        result = gen_src.generate_sources(root)
        assert result["sources"] is None
        assert result["included"] == []
        assert result["excluded"] == []
        assert result["incomplete"] == []


def test_generate_sources_multiple_schemas() -> None:
    """Sources from multiple schemas are grouped correctly."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        _add_source_table(root, "staging", "LookupRegion")

        result = gen_src.generate_sources(root)
        schema_names = sorted(s["name"] for s in result["sources"]["sources"])
        # silver.RefCurrency (no_writer_found fixture) also appears as a source
        assert "bronze" in schema_names
        assert "staging" in schema_names


def test_write_sources_yml() -> None:
    """write_sources_yml creates the YAML file on disk."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = gen_src.write_sources_yml(root)
        assert result["path"] is not None
        sources_path = Path(result["path"])
        assert sources_path.exists()
        content = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
        assert content["version"] == 2
        schema_names = {s["name"] for s in content["sources"]}
        # bronze from added table, silver from RefCurrency fixture (no_writer_found)
        assert "bronze" in schema_names
        assert "silver" in schema_names


def test_write_sources_yml_no_sources() -> None:
    """write_sources_yml writes a file when writerless tables exist."""
    tmp, root = _make_project()
    with tmp:
        # silver.RefCurrency (no_writer_found) is in the fixture — sources.yml is written
        result = gen_src.write_sources_yml(root)
        assert result["path"] is not None
        assert result["sources"] is not None


def test_cli_generate_sources() -> None:
    """CLI generate-sources outputs valid JSON."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = _cli_runner.invoke(
            gen_src.app,
            ["--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert "bronze.customerraw" in output["included"]


def test_cli_generate_sources_strict_blocks_on_incomplete() -> None:
    """CLI --strict exits 1 when incomplete scoping exists."""
    tmp, root = _make_project()
    with tmp:
        _add_table_to_project(root, "silver.DimDate")  # no scoping
        result = _cli_runner.invoke(
            gen_src.app,
            ["--strict", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["error"] == "INCOMPLETE_SCOPING"
        assert "silver.dimdate" in output["incomplete"]


def test_cli_generate_sources_strict_passes_when_complete() -> None:
    """CLI --strict exits 0 when all tables have complete scoping."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = _cli_runner.invoke(
            gen_src.app,
            ["--strict", "--project-root", str(root)],
        )
        assert result.exit_code == 0


# ── View and MV object type detection ───────────────────────────────────────


def test_scope_view_guards_pass(assert_valid_schema) -> None:
    """View FQN in scope stage returns object_type=view, guards_passed=True."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.vDimSalesTerritory", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        assert result["object_type"] == "view"
        assert "content" in result
        assert result["content"]["scoping_status"] is None  # no scoping section in fixture
        assert result["content"]["is_materialized_view"] is False


def test_scope_mv_returns_object_type_mv(assert_valid_schema) -> None:
    """MV FQN in scope stage returns object_type=mv."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.mv_FactSales", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        assert result["object_type"] == "mv"
        assert result["content"]["is_materialized_view"] is True


def test_scope_view_guards_fail_no_view_catalog(assert_valid_schema) -> None:
    """View FQN for a non-existent view returns VIEW_CATALOG_FILE_MISSING."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.NonExistentView", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        # Falls back to 'table' detection, then fails on CATALOG_FILE_MISSING
        assert result["guards_passed"] is False


def test_view_profile_blocked_not_not_applicable(assert_valid_schema) -> None:
    """View on profile stage returns blocked (VIEW_STAGE_NOT_SUPPORTED), not N/A."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.vDimSalesTerritory", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result.get("not_applicable") is not True
        assert result["guard_results"][-1]["code"] == "VIEW_STAGE_NOT_SUPPORTED"
        assert result["object_type"] == "view"


def test_view_all_non_scope_stages_blocked(assert_valid_schema) -> None:
    """All writer-dependent stages for a view return VIEW_STAGE_NOT_SUPPORTED."""
    tmp, root = _make_project()
    with tmp:
        for stage in ("profile", "test-gen", "refactor", "migrate"):
            result = dry_run.run_dry_run(root, "silver.vDimSalesTerritory", stage)
            assert_valid_schema(result, "dry_run_output.json")
            assert result["guards_passed"] is False, f"stage {stage} should be blocked"
            assert result["guard_results"][-1]["code"] == "VIEW_STAGE_NOT_SUPPORTED", stage


def test_table_object_type_in_result(assert_valid_schema) -> None:
    """Normal table FQN produces object_type=table in result."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.DimCustomer", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["object_type"] == "table"


# ── N/A logic for writerless tables ─────────────────────────────────────────


def test_writerless_table_scope_guards_pass(assert_valid_schema) -> None:
    """Writerless table scope stage still passes — scoping is complete."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.RefCurrency", "scope")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is True
        assert result["object_type"] == "table"
        assert result["content"]["scoping_status"] == "no_writer_found"


def test_profile_not_applicable_for_writerless_table(assert_valid_schema) -> None:
    """Writerless table profile stage returns not_applicable=True with a synthetic guard entry."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.RefCurrency", "profile")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["not_applicable"] is True
        assert len(result["guard_results"]) == 1
        assert result["guard_results"][0]["code"] == "WRITERLESS_TABLE"


def test_test_gen_not_applicable_for_writerless_table(assert_valid_schema) -> None:
    """Writerless table test-gen stage returns not_applicable=True."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.RefCurrency", "test-gen")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["not_applicable"] is True


def test_refactor_not_applicable_for_writerless_table(assert_valid_schema) -> None:
    """Writerless table refactor stage returns not_applicable=True."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.RefCurrency", "refactor")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["not_applicable"] is True


def test_migrate_not_applicable_for_writerless_table(assert_valid_schema) -> None:
    """Writerless table migrate stage returns not_applicable=True."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_dry_run(root, "silver.RefCurrency", "migrate")
        assert_valid_schema(result, "dry_run_output.json")
        assert result["guards_passed"] is False
        assert result["not_applicable"] is True


def test_cli_generate_sources_write() -> None:
    """CLI --write creates sources.yml on disk."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = _cli_runner.invoke(
            gen_src.app,
            ["--write", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["path"] is not None
        assert Path(output["path"]).exists()
