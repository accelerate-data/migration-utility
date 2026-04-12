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
from shared import generate_sources as gen_src
from shared.output_models.dry_run import DryRunOutput

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
        "init_handoff": {
            "timestamp": "2026-04-01T00:00:00+00:00",
            "env_vars": {"MSSQL_HOST": True, "MSSQL_PORT": True, "MSSQL_DB": True, "SA_PASSWORD": True},
            "tools": {"uv": True, "python": True, "shared_deps": True, "ddl_mcp": True, "freetds": True},
        },
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
    """Scope ready when manifest and catalog file exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.DimCustomer", "scope")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.reason == "ok"


def test_ready_scope_no_manifest() -> None:
    """Scope not ready when manifest is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").unlink()
        result = dry_run.run_ready(root, "silver.DimCustomer", "scope")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "manifest_missing"


def test_ready_scope_no_catalog_file() -> None:
    """Scope not ready when catalog file does not exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.NonExistent", "scope")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "catalog_missing"


def test_ready_setup_ddl_passes_with_manifest() -> None:
    """setup-ddl readiness uses the manifest gate and accepts the stage name."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "_", "setup-ddl")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.reason == "ok"


# ── run_ready tests: profile stage ───────────────────────────────────────────


def test_ready_profile_passes() -> None:
    """Profile ready when scoping.status == resolved."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.DimCustomer", "profile")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.reason == "ok"


def test_ready_profile_not_scoped() -> None:
    """Profile not ready when table has no scoping section."""
    tmp, root = _make_bare_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.DimDate", "profile")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "scoping_not_resolved"


def test_ready_profile_writerless_table() -> None:
    """Profile not applicable for writerless table."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.RefCurrency", "profile")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "not_applicable"
        assert result.code == "WRITERLESS_TABLE"


# ── run_ready tests: test-gen stage ──────────────────────────────────────────


def test_ready_test_gen_passes() -> None:
    """test-gen ready when profile.status is ok or partial."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.DimCustomer", "test-gen")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.reason == "ok"


def test_ready_test_gen_no_profile() -> None:
    """test-gen not ready when no profile section."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["profile"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.DimCustomer", "test-gen")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "profile_not_complete"


# ── run_ready tests: refactor stage ──────────────────────────────────────────


def test_ready_refactor_needs_test_gen() -> None:
    """Refactor not ready when test_gen.status is absent."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["test_gen"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.DimCustomer", "refactor")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "test_gen_not_complete"


def test_ready_refactor_passes_with_test_gen() -> None:
    """Refactor ready when test_gen.status == ok."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["test_gen"] = {"status": "ok"}
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.DimCustomer", "refactor")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.reason == "ok"


# ── run_ready tests: migrate stage ───────────────────────────────────────────


def test_ready_migrate_passes() -> None:
    """Migrate ready when refactor.status == ok on proc catalog."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.DimCustomer", "migrate")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.reason == "ok"


def test_ready_migrate_no_refactor() -> None:
    """Migrate not ready when refactor missing from proc catalog."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        del proc["refactor"]
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.DimCustomer", "migrate")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "refactor_not_complete"


# ── run_ready tests: generate stage ──────────────────────────────────────────


def test_ready_generate_requires_test_gen() -> None:
    """Generate not ready when test_gen.status is absent."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["test_gen"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.DimCustomer", "generate")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "test_gen_not_complete"
        assert result.code == "TEST_SPEC_MISSING"


def test_ready_generate_passes_with_both_gates() -> None:
    """Generate ready when both test_gen and refactor are ok."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.DimCustomer", "generate")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.reason == "ok"


# ── run_ready tests: special cases ───────────────────────────────────────────


def test_ready_source_table() -> None:
    """Source table (is_source=True) returns not_applicable."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.DimCustomer", "scope")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "not_applicable"
        assert result.code == "SOURCE_TABLE"


def test_ready_excluded_table() -> None:
    """Excluded table returns not_applicable."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["excluded"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.DimCustomer", "profile")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "not_applicable"
        assert result.code == "EXCLUDED"


def test_ready_invalid_stage() -> None:
    """Invalid stage returns ready=False with reason=invalid_stage."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.DimCustomer", "bogus")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "invalid_stage"


# ── run_ready tests: view stages ─────────────────────────────────────────────


def test_ready_view_scope_passes() -> None:
    """View scope ready when manifest and view catalog exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.vDimSalesTerritory", "scope")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True


def test_ready_view_profile_not_scoped() -> None:
    """View profile not ready when scoping.status != analyzed."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "silver.vDimSalesTerritory", "profile")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.reason == "scoping_not_analyzed"


def test_ready_view_profile_when_analyzed() -> None:
    """View profile ready when scoping.status == analyzed."""
    tmp, root = _make_project()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.vdimsalesterritory.json"
        cat = json.loads(view_path.read_text(encoding="utf-8"))
        cat["scoping"] = {"status": "analyzed", "sql_elements": [], "logic_summary": "test"}
        view_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "silver.vDimSalesTerritory", "profile")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True


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


# ── CLI: ready subcommand ────────────────────────────────────────────────────


def test_cli_ready_scope() -> None:
    """CLI ready returns JSON with ready/reason."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "silver.DimCustomer", "scope", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is True


def test_cli_ready_invalid_stage() -> None:
    """CLI ready with invalid stage still returns JSON (ready=False)."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "silver.DimCustomer", "bogus", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False


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
    """Sources from multiple schemas are grouped correctly."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        _add_source_table(root, "staging", "LookupRegion")

        result = gen_src.generate_sources(root)
        schema_names = sorted(s["name"] for s in result.sources["sources"])
        assert "bronze" in schema_names
        assert "staging" in schema_names
        assert "silver" not in schema_names


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
            "writer": "dbo.usp_load_dimcustomer",
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
    second_table["profile"]["writer"] = "dbo.usp_load_dimproduct"
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
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat.get("excluded") is True


def test_run_exclude_view_sets_flag(tmp_path: Path) -> None:
    """run_exclude sets excluded: true on a view catalog file."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.vw_legacy"])
    assert result.marked == ["silver.vw_legacy"]
    assert result.not_found == []
    cat = json.loads((dst / "catalog" / "views" / "silver.vw_legacy.json").read_text())
    assert cat.get("excluded") is True


def test_run_exclude_multiple_fqns(tmp_path: Path) -> None:
    """run_exclude marks multiple objects in one call."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.AuditLog", "silver.vw_legacy"])
    assert sorted(result.marked) == ["silver.auditlog", "silver.vw_legacy"]
    assert result.not_found == []


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
        "test-gen",
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


def test_reset_migration_cli_subcommand(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = _cli_runner.invoke(
        dry_run.app,
        ["reset-migration", "test-gen", "silver.DimCustomer", "--project-root", str(dst)],
    )

    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["stage"] == "test-gen"
    assert output["reset"] == ["silver.dimcustomer"]
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()
