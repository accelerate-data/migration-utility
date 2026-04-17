"""Shared fixtures for dry-run command tests."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from shared import dry_run

_cli_runner = CliRunner()

_TESTS_DIR = Path(__file__).parent
_FIXTURES = _TESTS_DIR / "fixtures"

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
