"""Tests for dry_run.py — readiness checker, status collator, exclude, and sources.

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
from typer.testing import CliRunner

from shared import dry_run
from shared import dry_run_core
from shared.dry_run_support import status as dry_run_status
from shared import generate_sources as gen_src
from shared.output_models.dry_run import DryRunOutput

_cli_runner = CliRunner()

_TESTS_DIR = Path(__file__).parent
_FIXTURES = _TESTS_DIR / "fixtures"


def test_dry_run_core_is_split_into_support_modules() -> None:
    """dry_run_core stays as a compatibility barrel over focused support modules."""
    from shared.dry_run_support import excluded_warnings, exclusions, readiness, reset, status

    assert dry_run_core.run_ready is readiness.run_ready
    assert dry_run_core.run_exclude is exclusions.run_exclude
    assert dry_run_core.run_reset_migration is reset.run_reset_migration
    assert dry_run_core.run_sync_excluded_warnings is excluded_warnings.run_sync_excluded_warnings
    assert status.run_status is not None


def _make_project(
    *,
    include_sandbox: bool = True,
    include_target: bool = True,
) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy dry_run fixtures to a temp dir and git-init it."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    shutil.copytree(_FIXTURES, dst)
    if not include_sandbox or not include_target:
        manifest_path = dst / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        runtime = manifest.setdefault("runtime", {})
        if not include_sandbox:
            runtime.pop("sandbox", None)
        if not include_target:
            runtime.pop("target", None)
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
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "TestDB"},
            }
        },
        "extraction": {
            "schemas": ["silver"],
            "extracted_at": "2026-04-01T00:00:00Z",
        },
        "init_handoff": {
            "timestamp": "2026-04-01T00:00:00+00:00",
            "env_vars": {"MSSQL_HOST": True, "MSSQL_PORT": True, "MSSQL_DB": True, "SA_PASSWORD": True},
            "tools": {"uv": True, "python": True, "shared_deps": True, "ddl_mcp": True, "freetds": True},
        },
    }
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "catalog" / "tables").mkdir(parents=True)
    (dst / "dbt").mkdir(parents=True)
    (dst / "dbt" / "dbt_project.yml").write_text("name: bare\n", encoding="utf-8")
    (dst / "dbt" / "profiles.yml").write_text("bare:\n  target: dev\n", encoding="utf-8")
    # Table catalog without scoping or profile
    table_cat = {
        "schema": "silver",
        "name": "DimDate",
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
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


# ── run_ready tests: scope stage ─────────────────────────────────────────────


def test_ready_scope_passes() -> None:
    """Object scope ready when manifest and catalog file exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "scope", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is not None
        assert result.object.ready is True
        assert result.project.reason == "ok"
        assert result.object.reason == "ok"


def test_ready_scope_no_manifest() -> None:
    """Scope not ready when manifest is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").unlink()
        result = dry_run.run_ready(root, "scope", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.ready is False
        assert result.object is None
        assert result.project.reason == "manifest_missing"


def test_ready_scope_no_catalog_file() -> None:
    """Scope not ready when catalog file does not exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "scope", object_fqn="silver.NonExistent")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is not None
        assert result.object.ready is False
        assert result.object.reason == "object_not_found"
        assert result.object.object_type is None
        assert result.object.code == "OBJECT_NOT_FOUND"


def test_ready_setup_ddl_passes_with_manifest() -> None:
    """setup-ddl project readiness uses the manifest gate with no object input."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "setup-ddl")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is None
        assert result.project.reason == "ok"


# ── run_ready tests: profile stage ───────────────────────────────────────────


def test_ready_profile_passes() -> None:
    """Profile ready when scoping.status == resolved."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"


def test_ready_profile_not_scoped() -> None:
    """Profile not ready when table has no scoping section."""
    tmp, root = _make_bare_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimDate")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "scoping_not_resolved"


def test_ready_profile_writerless_table() -> None:
    """Profile not applicable for writerless table."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.RefCurrency")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "WRITERLESS_TABLE"


def test_ready_profile_ignores_corrupt_manifest_contents() -> None:
    """Profile readiness requires manifest presence, not valid runtime JSON."""
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").write_text("{not json", encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.ready is True
        assert result.project.reason == "ok"
        assert result.object is not None
        assert result.object.reason == "ok"


# ── run_ready tests: test-gen stage ──────────────────────────────────────────


def test_ready_test_gen_passes() -> None:
    """test-gen ready when profile.status is ok or partial."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"


def test_ready_test_gen_no_profile() -> None:
    """test-gen not ready when no profile section."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["profile"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "profile_not_complete"


def test_ready_test_gen_no_target_runtime() -> None:
    """test-gen is blocked when runtime.target is missing from manifest."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"


def test_ready_test_gen_no_target_runtime_without_object() -> None:
    """test-gen fails with a target code even without an object overlay."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"
        assert result.object is None


def test_ready_test_gen_missing_target_and_sandbox_reports_target_first() -> None:
    """test-gen setup guidance reports target before sandbox when both are absent."""
    tmp, root = _make_project(include_target=False, include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"
        assert result.object is None


def test_ready_test_gen_requires_configured_sandbox_runtime() -> None:
    """test-gen is blocked when init only seeded an empty sandbox role."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"


def test_ready_test_gen_no_sandbox_runtime() -> None:
    """test-gen is blocked when runtime.sandbox is missing from manifest."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"


def test_ready_test_gen_no_sandbox_runtime_without_object() -> None:
    """test-gen fails with a sandbox code even without an object overlay."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"
        assert result.object is None


def test_ready_test_gen_accepts_oracle_sandbox_with_dsn() -> None:
    """test-gen treats a DSN-backed Oracle sandbox as configured."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "oracle",
            "dialect": "oracle",
            "connection": {"dsn": "localhost:1521/FREEPDB1"},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.reason == "ok"


def test_ready_test_gen_accepts_sql_server_sandbox_without_named_env() -> None:
    """test-gen accepts a runnable SQL Server sandbox even without database/schema names."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {
                "host": "localhost",
                "port": "1433",
                "user": "sa",
                "driver": "FreeTDS",
                "password_env": "SA_PASSWORD",
            },
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.reason == "ok"


# ── run_ready tests: refactor stage ──────────────────────────────────────────


def test_ready_refactor_needs_test_gen() -> None:
    """Refactor not ready when test_gen.status is absent."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["test_gen"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "refactor", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "test_gen_not_complete"


def test_ready_refactor_passes_with_test_gen() -> None:
    """Refactor ready when test_gen.status == ok."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["test_gen"] = {"status": "ok"}
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "refactor", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"


# ── run_ready tests: generate stage ──────────────────────────────────────────


def test_ready_generate_with_refactor() -> None:
    """Generate ready when refactor.status == ok on proc catalog."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.reason == "ok"
        assert result.object is not None
        assert result.object.reason == "ok"


def test_ready_generate_no_refactor() -> None:
    """Generate not ready when refactor missing from proc catalog."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        del proc["refactor"]
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "refactor_not_complete"


def test_ready_generate_no_sandbox() -> None:
    """Generate not ready when runtime.target is missing from manifest."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"


def test_ready_generate_no_sandbox_runtime() -> None:
    """Generate not ready when runtime.sandbox is missing from manifest."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"


def test_ready_generate_requires_configured_target_runtime() -> None:
    """Generate is blocked when init only seeded an empty target role."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["target"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"


def test_ready_generate_requires_configured_sandbox_runtime() -> None:
    """Generate is blocked when init only seeded an empty sandbox role."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"


def test_ready_generate_missing_dbt_project() -> None:
    """Generate not ready when dbt_project.yml is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "dbt" / "dbt_project.yml").unlink()
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "dbt_project_missing"
        assert result.project.code == "DBT_PROJECT_MISSING"


def test_ready_generate_missing_dbt_profile() -> None:
    """Generate not ready when profiles.yml is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "dbt" / "profiles.yml").unlink(missing_ok=True)
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "dbt_profile_missing"
        assert result.project.code == "DBT_PROFILE_MISSING"


def test_ready_generate_requires_test_gen() -> None:
    """Generate not ready when test_gen.status is absent."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["test_gen"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "test_gen_not_complete"
        assert result.object.code == "TEST_SPEC_MISSING"


def test_ready_generate_passes_with_both_gates() -> None:
    """Generate ready when both test_gen and refactor are ok."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"


# ── run_ready tests: special cases ───────────────────────────────────────────


def test_ready_source_table() -> None:
    """Source table (is_source=True) returns not_applicable."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "scope", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "SOURCE_TABLE"


def test_ready_seed_table() -> None:
    """Seed table returns not_applicable with SEED_TABLE code."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = False
        cat["is_seed"] = True
        cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "seed", "source": "catalog"},
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "SEED_TABLE"


def test_ready_excluded_table() -> None:
    """Excluded table returns not_applicable."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["excluded"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "EXCLUDED"


def test_ready_invalid_stage() -> None:
    """Invalid stage returns ready=False with reason=invalid_stage."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "bogus", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "invalid_stage"


# ── run_ready tests: view stages ─────────────────────────────────────────────


def test_ready_view_scope_passes() -> None:
    """View scope ready when manifest and view catalog exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "scope", object_fqn="silver.vDimSalesTerritory")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"


def test_ready_view_profile_not_scoped() -> None:
    """View profile not ready when scoping.status != analyzed."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.vDimSalesTerritory")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "scoping_not_analyzed"


def test_ready_view_profile_when_analyzed() -> None:
    """View profile ready when scoping.status == analyzed."""
    tmp, root = _make_project()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.vdimsalesterritory.json"
        cat = json.loads(view_path.read_text(encoding="utf-8"))
        cat["scoping"] = {"status": "analyzed", "sql_elements": [], "logic_summary": "test"}
        view_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.vDimSalesTerritory")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"


def test_ready_setup_ddl_without_object_reports_project_only() -> None:
    """Project-only readiness should not attach an object section."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "setup-ddl")
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is None


def test_ready_generate_object_failure_preserves_project_success() -> None:
    """Object overlay should fail independently after project readiness passes."""
    tmp, root = _make_project(include_sandbox=False, include_target=False)
    with tmp:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        manifest.setdefault("runtime", {})["target"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {"database": "TargetDB"},
        }
        manifest.setdefault("runtime", {})["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {"database": "__test_abc123"},
        }
        (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        (root / "dbt").mkdir(exist_ok=True)
        (root / "dbt" / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")
        (root / "dbt" / "profiles.yml").write_text("test:\n  target: dev\n", encoding="utf-8")
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        del proc["refactor"]
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is not None
        assert result.object.ready is False
        assert result.object.reason == "refactor_not_complete"


# ── run_status tests ─────────────────────────────────────────────────────────


def test_status_single_object() -> None:
    """Status for a single object returns all stage statuses."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root, "silver.DimCustomer")
        assert result.fqn == "silver.dimcustomer"
        assert result.type == "table"
        assert result.stages.scope == "ok"
        assert result.stages.profile == "ok"
        assert result.stages.test_gen == "ok"


def test_status_all_objects() -> None:
    """Status with no FQN returns all objects with summary."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root)
        assert result.summary.total > 0
        # Check that silver.dimcustomer is in the list
        fqns = [obj.fqn for obj in result.objects]
        assert "silver.dimcustomer" in fqns


def test_status_all_objects_excludes_seed_tables() -> None:
    """Bulk status excludes seed tables from active migration stage counts."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = False
        cat["is_seed"] = True
        cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "seed", "source": "catalog"},
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        result = dry_run.run_status(root)

    fqns = [obj.fqn for obj in result.objects]
    assert "silver.dimcustomer" not in fqns


def test_status_source_table_detail_is_workflow_exempt() -> None:
    """Single-object status reports source tables as workflow-exempt."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        result = dry_run.run_status(root, "silver.DimCustomer")

    assert result.fqn == "silver.dimcustomer"
    assert result.type == "table"
    assert result.stages.scope == "N/A"
    assert result.stages.profile == "N/A"
    assert result.stages.test_gen == "N/A"
    assert result.stages.refactor == "N/A"
    assert result.stages.generate == "N/A"


def test_status_seed_table_detail_is_workflow_exempt() -> None:
    """Single-object status reports seed tables as workflow-exempt."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = False
        cat["is_seed"] = True
        cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "seed", "source": "catalog"},
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        result = dry_run.run_status(root, "silver.DimCustomer")

    assert result.fqn == "silver.dimcustomer"
    assert result.type == "table"
    assert result.stages.scope == "N/A"
    assert result.stages.profile == "N/A"
    assert result.stages.test_gen == "N/A"
    assert result.stages.refactor == "N/A"
    assert result.stages.generate == "N/A"


def test_status_all_objects_skips_summary_count_for_missing_status_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bulk status should not crash if a single-object probe returns StatusOutput."""
    tmp, root = _make_project()
    original = dry_run_status._single_object_status

    def _fake_single_object_status(*args, **kwargs):
        norm_fqn = args[1]
        if norm_fqn == "silver.dimcustomer":
            return dry_run_core.StatusOutput(fqn=norm_fqn, type=None, stages=None)
        return original(*args, **kwargs)

    with tmp:
        monkeypatch.setattr(dry_run_status, "_single_object_status", _fake_single_object_status)
        result = dry_run.run_status(root)

    assert result.summary is not None
    assert result.summary.total > 0


def test_status_view_object() -> None:
    """Status for a view returns correct type and stages."""
    tmp, root = _make_project()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.vdimsalesterritory.json"
        cat = json.loads(view_path.read_text(encoding="utf-8"))
        cat["scoping"] = {"status": "analyzed", "sql_elements": [], "logic_summary": "test"}
        view_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_status(root, "silver.vDimSalesTerritory")
        assert result.type == "view"
        assert result.stages.scope == "ok"


def test_status_view_logs_warning_on_corrupt_catalog(caplog: pytest.LogCaptureFixture) -> None:
    """A corrupt view catalog JSON logs a warning and degrades gracefully."""
    tmp, root = _make_project()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.vdimsalesterritory.json"
        view_path.write_text("{bad json", encoding="utf-8")
        with caplog.at_level("WARNING"):
            result = dry_run.run_status(root, "silver.vDimSalesTerritory")
        assert result.type == "view"
        assert result.stages.scope is None
        assert any("view_catalog_load_failed" in r.message for r in caplog.records)


def test_status_pending_scope_preserves_specific_status() -> None:
    """Status output preserves incomplete scope states verbatim."""
    tmp, root = _make_project()
    with tmp:
        table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(table_path.read_text(encoding="utf-8"))
        cat["scoping"]["status"] = "ambiguous_multi_writer"
        table_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_status(root, "silver.DimCustomer")
        assert result.stages.scope == "ambiguous_multi_writer"


def test_status_mv_object() -> None:
    """Status for a materialized view returns type=mv."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root, "silver.mv_FactSales")
        assert result.type == "mv"


def test_status_single_missing_object_reports_not_found() -> None:
    """Single-object status should not fabricate a table for missing objects."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root, "silver.Missing")
        assert result.fqn == "silver.missing"
        assert result.type is None
        assert result.stages is None


# ── CLI: ready subcommand ────────────────────────────────────────────────────


def test_cli_ready_scope() -> None:
    """CLI ready returns JSON for object-scoped readiness."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "scope", "--object", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is True
        assert output["project"]["ready"] is True
        assert output["object"]["ready"] is True
        assert output["project"]["reason"] == "ok"
        assert output["object"]["reason"] == "ok"


def test_cli_ready_invalid_stage() -> None:
    """CLI ready with invalid stage still returns JSON (ready=False)."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "bogus", "--object", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["reason"] == "invalid_stage"


def test_cli_ready_project_only() -> None:
    """CLI ready supports project-only readiness without object input."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "setup-ddl", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is True
        assert output["project"]["ready"] is True
        assert output.get("object") is None
        assert output["project"]["reason"] == "ok"


def test_cli_ready_test_gen_missing_target_exits_with_code() -> None:
    """CLI ready test-gen exits non-zero with a clear target setup code."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "test-gen", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "TARGET_NOT_CONFIGURED"


def test_cli_ready_test_gen_missing_sandbox_exits_with_code() -> None:
    """CLI ready test-gen exits non-zero with a clear sandbox setup code."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "test-gen", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "SANDBOX_NOT_CONFIGURED"


def test_cli_ready_generate_missing_target_preserves_zero_exit() -> None:
    """CLI ready generate keeps JSON-only readiness behavior for setup failures."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "generate", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "TARGET_NOT_CONFIGURED"


def test_cli_ready_refactor_missing_sandbox_preserves_zero_exit() -> None:
    """CLI ready refactor keeps JSON-only readiness behavior for setup failures."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "refactor", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "SANDBOX_NOT_CONFIGURED"


# ── CLI: status subcommand ───────────────────────────────────────────────────


def test_cli_status_single() -> None:
    """CLI status with FQN returns single-object status."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["status", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["fqn"] == "silver.dimcustomer"


def test_cli_status_all() -> None:
    """CLI status without FQN returns all objects."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["status", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert "objects" in output
        assert "summary" in output


# ── generate-sources tests ──────────────────────────────────────────────────


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
        "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
    }
    if include_scoping:
        cat["scoping"] = {
            "status": "resolved",
            "selected_writer": f"dbo.usp_load_{name}",
            "candidates": [
                {"procedure_name": f"dbo.usp_load_{name}", "dependencies": {"tables": [], "views": [], "functions": []}, "rationale": "test"}
            ],
            "warnings": [],
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


def _add_source_table(root: Path, schema: str, name: str) -> None:
    """Add a table confirmed as a dbt source (no_writer_found + is_source: true)."""
    norm = f"{schema.lower()}.{name.lower()}"
    cat = {
        "schema": schema,
        "name": name,
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
        "is_source": True,
        "scoping": {
            "status": "no_writer_found",
            "selected_writer": None,
            "selected_writer_rationale": "No procedures found that write to this table.",
        },
    }
    (root / "catalog" / "tables" / f"{norm}.json").write_text(
        json.dumps(cat), encoding="utf-8",
    )


def test_generate_sources_only_includes_is_source_true() -> None:
    """Only tables with is_source: true are included in sources."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")

        result = gen_src.generate_sources(root)
        assert "bronze.customerraw" in result.included
        assert "silver.refcurrency" not in result.included
        assert "silver.refcurrency" in result.unconfirmed
        assert "silver.dimcustomer" in result.excluded
        assert result.incomplete == []
        assert result.sources is not None
        schema_names = [s["name"] for s in result.sources["sources"]]
        assert "bronze" in schema_names
        assert "silver" not in schema_names


def test_generate_sources_excludes_resolved_tables() -> None:
    """Tables with resolved status are excluded; unconfirmed writerless go to unconfirmed."""
    tmp, root = _make_project()
    with tmp:
        result = gen_src.generate_sources(root)
        assert "silver.dimcustomer" in result.excluded
        assert "silver.refcurrency" in result.unconfirmed
        assert "silver.refcurrency" not in result.included
        assert result.sources is None


def test_generate_sources_detects_incomplete_scoping() -> None:
    """Tables without scoping section are flagged as incomplete."""
    tmp, root = _make_project()
    with tmp:
        _add_table_to_project(root, "silver.DimDate")
        result = gen_src.generate_sources(root)
        assert "silver.dimdate" in result.incomplete


def test_generate_sources_mixed_statuses() -> None:
    """Mixed resolved, no_writer_found (confirmed and unconfirmed), and incomplete tables."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        _add_source_table(root, "bronze", "OrderRaw")
        _add_table_to_project(root, "silver.DimDate")

        result = gen_src.generate_sources(root)
        assert sorted(result.included) == ["bronze.customerraw", "bronze.orderraw"]
        assert result.excluded == ["silver.dimcustomer"]
        assert result.unconfirmed == ["silver.refcurrency"]
        assert result.incomplete == ["silver.dimdate"]
        schema_names = {s["name"] for s in result.sources["sources"]}
        assert "bronze" in schema_names
        assert "silver" not in schema_names


def test_generate_sources_empty_catalog() -> None:
    """Empty catalog/tables/ produces empty result."""
    tmp, root = _make_bare_project()
    with tmp:
        for f in (root / "catalog" / "tables").glob("*.json"):
            f.unlink()
        result = gen_src.generate_sources(root)
        assert result.sources is None
        assert result.included == []
        assert result.excluded == []
        assert result.incomplete == []


def test_generate_sources_multiple_schemas() -> None:
    """Sources from multiple schemas are emitted under the bronze namespace."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        _add_source_table(root, "staging", "LookupRegion")

        result = gen_src.generate_sources(root)
        schema_names = sorted(s["name"] for s in result.sources["sources"])
        assert schema_names == ["bronze"]
        assert "silver" not in schema_names
        table_names = {table["name"] for table in result.sources["sources"][0]["tables"]}
        assert table_names == {"CustomerRaw", "LookupRegion"}


def test_write_sources_yml() -> None:
    """write_sources_yml creates the YAML file on disk with only is_source: true tables."""
    import yaml

    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = gen_src.write_sources_yml(root)
        assert result.path is not None
        sources_path = Path(result.path)
        assert sources_path.exists()
        content = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
        assert content["version"] == 2
        schema_names = {s["name"] for s in content["sources"]}
        assert "bronze" in schema_names
        assert "silver" not in schema_names


def test_write_sources_yml_no_confirmed_sources() -> None:
    """write_sources_yml returns path=None when no tables have is_source: true."""
    tmp, root = _make_project()
    with tmp:
        result = gen_src.write_sources_yml(root)
        assert result.path is None
        assert result.sources is None


def test_cli_generate_sources() -> None:
    """CLI generate-sources outputs valid JSON with is_source: true table included."""
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
        assert "silver.refcurrency" in output["unconfirmed"]


def test_cli_generate_sources_strict_blocks_on_incomplete() -> None:
    """CLI --strict exits 1 when incomplete scoping exists."""
    tmp, root = _make_project()
    with tmp:
        _add_table_to_project(root, "silver.DimDate")
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


# ── run_exclude tests ────────────────────────────────────────────────────────


_GIT_ENV = {
    "GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "t@t",
    "HOME": str(Path.home()),
}


def _make_exclude_project(tmp_path: Path) -> Path:
    """Create a minimal project with one table and one view for exclude tests."""
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    (tmp_path / "catalog" / "views").mkdir(parents=True)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
    )
    (tmp_path / "catalog" / "tables" / "silver.auditlog.json").write_text(
        json.dumps({"schema": "silver", "name": "AuditLog", "primary_keys": []}),
        encoding="utf-8",
    )
    (tmp_path / "catalog" / "views" / "silver.vw_legacy.json").write_text(
        json.dumps({"schema": "silver", "name": "vw_legacy", "references": {}}),
        encoding="utf-8",
    )
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=tmp_path, capture_output=True, check=True, env=_GIT_ENV,
    )
    return tmp_path


def _make_reset_project(tmp_path: Path) -> Path:
    """Create a minimal project with resettable migration state."""
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    (tmp_path / "catalog" / "procedures").mkdir(parents=True)
    (tmp_path / "test-specs").mkdir(parents=True)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
        encoding="utf-8",
    )

    table_cat = {
        "schema": "silver",
        "name": "DimCustomer",
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "referenced_by": {
            "procedures": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
        },
        "scoping": {
            "status": "resolved",
            "selected_writer": "dbo.usp_load_dimcustomer",
            "warnings": [],
            "errors": [],
        },
        "profile": {
            "status": "ok",
            "classification": {"resolved_kind": "dim_scd1", "source": "llm"},
            "primary_key": {"columns": ["CustomerKey"], "primary_key_type": "surrogate", "source": "catalog"},
            "natural_key": {"columns": ["CustomerID"], "source": "llm"},
            "watermark": {"column": "ModifiedDate", "source": "llm"},
            "foreign_keys": [],
            "pii_actions": [],
            "warnings": [],
            "errors": [],
        },
        "test_gen": {
            "status": "ok",
            "test_spec_path": "test-specs/silver.dimcustomer.json",
            "branches": 2,
            "unit_tests": 2,
            "coverage": "complete",
            "warnings": [],
            "errors": [],
        },
    }
    (tmp_path / "catalog" / "tables" / "silver.dimcustomer.json").write_text(
        json.dumps(table_cat), encoding="utf-8",
    )

    proc_cat = {
        "schema": "dbo",
        "name": "usp_load_dimcustomer",
        "statements": [{"index": 0, "action": "migrate", "source": "ast", "sql": "INSERT INTO silver.DimCustomer SELECT ..."}],
        "references": {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
        "refactor": {
            "status": "ok",
            "extracted_sql": "SELECT 1",
            "refactored_sql": "WITH src AS (SELECT 1) SELECT * FROM src",
        },
    }
    (tmp_path / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json").write_text(
        json.dumps(proc_cat), encoding="utf-8",
    )

    second_table = json.loads(json.dumps(table_cat))
    second_table["name"] = "DimProduct"
    second_table["scoping"]["selected_writer"] = "dbo.usp_load_dimproduct"
    second_table["test_gen"]["test_spec_path"] = "test-specs/silver.dimproduct.json"
    (tmp_path / "catalog" / "tables" / "silver.dimproduct.json").write_text(
        json.dumps(second_table), encoding="utf-8",
    )

    second_proc = json.loads(json.dumps(proc_cat))
    second_proc["name"] = "usp_load_dimproduct"
    (tmp_path / "catalog" / "procedures" / "dbo.usp_load_dimproduct.json").write_text(
        json.dumps(second_proc), encoding="utf-8",
    )

    for norm in ("silver.dimcustomer", "silver.dimproduct"):
        (tmp_path / "test-specs" / f"{norm}.json").write_text(
            json.dumps({"item_id": norm, "status": "ok", "scenarios": [{"name": "basic"}]}),
            encoding="utf-8",
        )

    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=tmp_path, capture_output=True, check=True, env=_GIT_ENV,
    )
    return tmp_path


def test_run_exclude_table_sets_flag(tmp_path: Path) -> None:
    """run_exclude sets excluded: true on a table catalog file."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.AuditLog"])
    assert result.marked == ["silver.auditlog"]
    assert result.not_found == []
    assert result.written_paths == ["catalog/tables/silver.auditlog.json"]
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat.get("excluded") is True


def test_run_exclude_view_sets_flag(tmp_path: Path) -> None:
    """run_exclude sets excluded: true on a view catalog file."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.vw_legacy"])
    assert result.marked == ["silver.vw_legacy"]
    assert result.not_found == []
    assert result.written_paths == ["catalog/views/silver.vw_legacy.json"]
    cat = json.loads((dst / "catalog" / "views" / "silver.vw_legacy.json").read_text())
    assert cat.get("excluded") is True


def test_run_exclude_multiple_fqns(tmp_path: Path) -> None:
    """run_exclude marks multiple objects in one call."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.AuditLog", "silver.vw_legacy"])
    assert sorted(result.marked) == ["silver.auditlog", "silver.vw_legacy"]
    assert result.not_found == []
    assert sorted(result.written_paths) == [
        "catalog/tables/silver.auditlog.json",
        "catalog/views/silver.vw_legacy.json",
    ]


def test_run_exclude_not_found_reported(tmp_path: Path) -> None:
    """FQN with no catalog file appears in not_found, does not raise."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.Nonexistent"])
    assert result.marked == []
    assert result.not_found == ["silver.nonexistent"]


def test_run_exclude_mixed_found_and_not_found(tmp_path: Path) -> None:
    """Partial success: found items are marked, missing items are in not_found."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.AuditLog", "silver.Missing"])
    assert result.marked == ["silver.auditlog"]
    assert result.not_found == ["silver.missing"]


def test_run_exclude_idempotent(tmp_path: Path) -> None:
    """Calling run_exclude twice on the same FQN does not corrupt the catalog."""
    dst = _make_exclude_project(tmp_path)
    dry_run.run_exclude(dst, ["silver.AuditLog"])
    result = dry_run.run_exclude(dst, ["silver.AuditLog"])
    assert result.marked == ["silver.auditlog"]
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat.get("excluded") is True
    assert cat.get("primary_keys") is not None


def test_run_exclude_preserves_existing_catalog_fields(tmp_path: Path) -> None:
    """run_exclude only adds excluded: true — it does not strip other catalog fields."""
    dst = _make_exclude_project(tmp_path)
    dry_run.run_exclude(dst, ["silver.AuditLog"])
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat["schema"] == "silver"
    assert cat["name"] == "AuditLog"


def test_exclude_cli_subcommand(tmp_path: Path) -> None:
    """CLI exclude subcommand emits valid JSON and sets excluded: true."""
    dst = _make_exclude_project(tmp_path)
    result = _cli_runner.invoke(
        dry_run.app,
        ["exclude", "silver.AuditLog", "--project-root", str(dst)],
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["marked"] == ["silver.auditlog"]
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat.get("excluded") is True


# ── run_reset_migration tests ───────────────────────────────────────────────


def test_run_reset_migration_profile_clears_downstream_and_preserves_scoping(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(dst, "profile", ["silver.DimCustomer"])

    assert result.reset == ["silver.dimcustomer"]
    assert result.noop == []
    target = result.targets[0]
    assert target.status == "reset"
    assert "table.profile" in target.cleared_sections
    assert "table.test_gen" in target.cleared_sections
    assert "procedure:dbo.usp_load_dimcustomer.refactor" in target.cleared_sections
    assert target.deleted_files == ["test-specs/silver.dimcustomer.json"]
    assert "catalog/procedures/dbo.usp_load_dimcustomer.json" in target.mutated_files
    assert "catalog/tables/silver.dimcustomer.json" in target.mutated_files

    table_cat = json.loads((dst / "catalog" / "tables" / "silver.dimcustomer.json").read_text())
    assert "scoping" in table_cat
    assert "profile" not in table_cat
    assert "test_gen" not in table_cat
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()

    proc_cat = json.loads((dst / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json").read_text())
    assert "refactor" not in proc_cat


def test_run_reset_migration_refactor_only_clears_writer_refactor(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(dst, "refactor", ["silver.DimCustomer"])

    assert result.reset == ["silver.dimcustomer"]
    assert "catalog/procedures/dbo.usp_load_dimcustomer.json" in result.targets[0].mutated_files
    table_cat = json.loads((dst / "catalog" / "tables" / "silver.dimcustomer.json").read_text())
    assert "profile" in table_cat
    assert "test_gen" in table_cat
    assert (dst / "test-specs" / "silver.dimcustomer.json").exists()

    proc_cat = json.loads((dst / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json").read_text())
    assert "refactor" not in proc_cat


def test_run_reset_migration_is_idempotent_noop(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    dry_run.run_reset_migration(dst, "refactor", ["silver.DimCustomer"])
    result = dry_run.run_reset_migration(dst, "refactor", ["silver.DimCustomer"])

    assert result.reset == []
    assert result.noop == ["silver.dimcustomer"]
    assert result.targets[0].status == "noop"


def test_run_reset_migration_multiple_tables(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(
        dst,
        "generate-tests",
        ["silver.DimCustomer", "silver.DimProduct"],
    )

    assert sorted(result.reset) == ["silver.dimcustomer", "silver.dimproduct"]
    assert result.blocked == []
    assert result.not_found == []
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()
    assert not (dst / "test-specs" / "silver.dimproduct.json").exists()


def test_run_reset_migration_blocks_model_complete_before_mutation(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    table_path = dst / "catalog" / "tables" / "silver.dimcustomer.json"
    table_cat = json.loads(table_path.read_text(encoding="utf-8"))
    table_cat["generate"] = {"status": "ok"}
    table_path.write_text(json.dumps(table_cat), encoding="utf-8")

    result = dry_run.run_reset_migration(
        dst,
        "profile",
        ["silver.DimCustomer", "silver.DimProduct"],
    )

    assert result.reset == []
    assert result.blocked == ["silver.dimcustomer"]
    assert result.targets[0].status == "blocked"

    untouched = json.loads(table_path.read_text(encoding="utf-8"))
    assert "profile" in untouched
    assert (dst / "test-specs" / "silver.dimproduct.json").exists()


def test_run_reset_migration_not_found_returns_without_mutation(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(dst, "profile", ["silver.Missing"])

    assert result.not_found == ["silver.missing"]
    assert result.targets[0].status == "not_found"
    assert (dst / "test-specs" / "silver.dimcustomer.json").exists()


def test_run_reset_migration_mixed_valid_and_missing_resets_valid_targets(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    result = dry_run.run_reset_migration(
        dst,
        "profile",
        ["silver.DimCustomer", "silver.Missing"],
    )

    assert result.not_found == ["silver.missing"]
    assert result.reset == ["silver.dimcustomer"]
    assert {target.fqn: target.status for target in result.targets} == {
        "silver.missing": "not_found",
        "silver.dimcustomer": "reset",
    }
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()


def test_reset_migration_global_output_contract_serializes_deleted_paths(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    (dst / "CLAUDE.md").write_text("# local scaffold\n", encoding="utf-8")
    (dst / ".envrc").write_text("export TEST=1\n", encoding="utf-8")
    (dst / "repo-map.json").write_text("{\"name\": \"fixture\"}\n", encoding="utf-8")
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "ddl").mkdir()
    (dst / "ddl" / "legacy.sql").write_text("select 1;", encoding="utf-8")
    (dst / ".staging").mkdir()
    (dst / ".staging" / "state.json").write_text("{}", encoding="utf-8")
    (dst / "dbt" / "models" / "marts").mkdir(parents=True)
    (dst / "dbt" / "models" / "marts" / "dim_customer.sql").write_text(
        "select 1;", encoding="utf-8"
    )
    (dst / "dbt" / "target").mkdir(parents=True)
    (dst / "dbt" / "target" / "compiled.json").write_text("{}", encoding="utf-8")

    result = dry_run.run_reset_migration(dst, "all", [])
    payload = result.model_dump(mode="json", exclude_none=True)

    assert result.stage == "all"
    assert result.targets == []
    assert result.reset == []
    assert result.noop == []
    assert result.blocked == []
    assert result.not_found == []
    assert result.deleted_paths == ["catalog", "ddl", ".staging", "test-specs", "dbt"]
    assert result.missing_paths == []
    assert result.cleared_manifest_sections == [
        "runtime.source",
        "runtime.target",
        "runtime.sandbox",
        "extraction",
        "init_handoff",
    ]
    assert payload["stage"] == "all"
    assert payload["deleted_paths"] == ["catalog", "ddl", ".staging", "test-specs", "dbt"]
    assert payload["missing_paths"] == []
    assert payload["cleared_manifest_sections"] == [
        "runtime.source",
        "runtime.target",
        "runtime.sandbox",
        "extraction",
        "init_handoff",
    ]
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    assert "runtime" not in manifest
    assert "extraction" not in manifest
    assert "init_handoff" not in manifest
    assert (dst / "manifest.json").exists()
    assert not (dst / "catalog").exists()
    assert not (dst / "ddl").exists()
    assert not (dst / ".staging").exists()
    assert not (dst / "test-specs").exists()
    assert not (dst / "dbt").exists()
    assert (dst / "CLAUDE.md").exists()
    assert (dst / ".envrc").exists()
    assert (dst / "repo-map.json").exists()


def test_run_reset_migration_all_reports_missing_paths_as_noop(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "ddl").mkdir()
    (dst / ".staging").mkdir()

    result = dry_run.run_reset_migration(dst, "all", [])

    assert result.deleted_paths == ["catalog", "ddl", ".staging", "test-specs"]
    assert result.missing_paths == ["dbt"]
    assert result.cleared_manifest_sections == [
        "runtime.source",
        "runtime.target",
        "runtime.sandbox",
        "extraction",
        "init_handoff",
    ]
    assert (dst / "ddl").exists() is False
    assert (dst / ".staging").exists() is False
    assert (dst / "dbt").exists() is False


def test_run_reset_migration_all_invalid_manifest_preserves_directories(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    (dst / "ddl").mkdir()
    (dst / "ddl" / "legacy.sql").write_text("select 1;", encoding="utf-8")
    (dst / "dbt" / "models").mkdir(parents=True)
    (dst / "dbt" / "models" / "model.sql").write_text("select 1;", encoding="utf-8")
    (dst / ".staging").mkdir()
    (dst / "test-specs").mkdir(exist_ok=True)
    (dst / "manifest.json").write_text("{not valid json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        dry_run.run_reset_migration(dst, "all", [])

    assert (dst / "catalog").exists()
    assert (dst / "ddl").exists()
    assert (dst / ".staging").exists()
    assert (dst / "test-specs").exists()
    assert (dst / "dbt").exists()


def test_run_reset_migration_all_rejects_extra_table_arguments(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    with pytest.raises(ValueError, match="global reset stage 'all' does not accept table arguments"):
        dry_run.run_reset_migration(dst, "all", ["silver.DimCustomer"])


def test_reset_migration_requires_at_least_one_fqn(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    with pytest.raises(ValueError, match="reset-migration requires at least one FQN for staged resets"):
        dry_run.run_reset_migration(dst, "profile", [])


def test_reset_migration_cli_all_succeeds_without_fqns(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "ddl").mkdir()
    (dst / ".staging").mkdir()
    (dst / "dbt" / "models").mkdir(parents=True)

    result = _cli_runner.invoke(dry_run.app, ["reset-migration", "all", "--project-root", str(dst)])

    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["stage"] == "all"
    assert output["deleted_paths"] == ["catalog", "ddl", ".staging", "test-specs", "dbt"]
    assert not (dst / "catalog").exists()
    assert not (dst / "ddl").exists()
    assert not (dst / ".staging").exists()
    assert not (dst / "test-specs").exists()
    assert not (dst / "dbt").exists()


def test_reset_migration_cli_all_rejects_extra_fqns(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    result = _cli_runner.invoke(
        dry_run.app,
        [
            "reset-migration",
            "all",
            "--fqn",
            "silver.DimCustomer",
            "--project-root",
            str(dst),
        ],
    )

    assert result.exit_code == 1, result.output
    output = json.loads(result.stdout)
    assert "global reset stage 'all' does not accept table arguments" in output["error"]


def test_reset_migration_cli_subcommand(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = _cli_runner.invoke(
        dry_run.app,
        [
            "reset-migration",
            "generate-tests",
            "--fqn",
            "silver.DimCustomer",
            "--project-root",
            str(dst),
        ],
    )

    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["stage"] == "generate-tests"
    assert output["reset"] == ["silver.dimcustomer"]
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()


def test_reset_migration_cli_corrupt_catalog_exits_2(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    table_path = dst / "catalog" / "tables" / "silver.dimcustomer.json"
    table_path.write_text("{not valid json", encoding="utf-8")

    result = _cli_runner.invoke(
        dry_run.app,
        [
            "reset-migration",
            "profile",
            "--fqn",
            "silver.DimCustomer",
            "--project-root",
            str(dst),
        ],
    )

    assert result.exit_code == 2
    output = json.loads(result.stdout)
    assert "error" in output
