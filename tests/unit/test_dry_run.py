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

import pytest

from shared import dry_run

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
        proc["statements"].append({"index": 3, "action": "claude", "source": "llm", "sql": "EXEC dbo.other_proc"})
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
        assert content["sandbox"]["run_id"] == "abc123"


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
