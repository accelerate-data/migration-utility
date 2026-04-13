"""Tests for catalog_enrich.py — offline AST enrichment for catalog JSON files."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")

from shared.catalog_enrich import enrich_catalog
from shared import catalog_enrich
from tests.helpers import (
    git_init as _git_init,
    run_catalog_enrich_cli as _run_enrich_cli,
    run_setup_ddl_cli as _run_setup_ddl_cli,
)


def _write_sql(path: Path, filename: str, content: str) -> None:
    """Write a .sql file into the DDL directory."""
    (path / filename).write_text(content, encoding="utf-8")


def _write_catalog_json(path: Path, subdir: str, fqn: str, data: dict[str, Any]) -> None:
    """Write a catalog JSON file."""
    catalog_dir = path / "catalog" / subdir
    catalog_dir.mkdir(parents=True, exist_ok=True)
    (catalog_dir / f"{fqn}.json").write_text(
        json.dumps(data, indent=2) + "\n", encoding="utf-8",
    )


def _read_catalog_json(path: Path, subdir: str, fqn: str) -> dict[str, Any] | None:
    """Read a catalog JSON file."""
    p = path / "catalog" / subdir / f"{fqn}.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _empty_scoped() -> dict[str, list[dict[str, Any]]]:
    return {"in_scope": [], "out_of_scope": []}


def _empty_references() -> dict[str, dict[str, list[dict[str, Any]]]]:
    return {
        "tables": _empty_scoped(),
        "views": _empty_scoped(),
        "functions": _empty_scoped(),
        "procedures": _empty_scoped(),
    }


def _empty_referenced_by() -> dict[str, dict[str, list[dict[str, Any]]]]:
    return {
        "procedures": _empty_scoped(),
        "views": _empty_scoped(),
        "functions": _empty_scoped(),
    }


# ── Fixtures ────────────────────────────────────────────────────────────────


def _setup_select_into(tmp_path: Path) -> Path:
    """Set up a DDL directory with a proc that does SELECT INTO.

    Returns the root path (tmp_path) — load_directory resolves ddl/ internally.
    """
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()

    _write_sql(ddl_dir, "procedures.sql", """\
CREATE PROCEDURE [dbo].[usp_create_snapshot]
AS
BEGIN
    SELECT * INTO [silver].[Snapshot] FROM [bronze].[Source];
END
GO
""")

    _write_sql(ddl_dir, "tables.sql", """\
CREATE TABLE [bronze].[Source] (
    id INT NOT NULL,
    val NVARCHAR(100) NULL
)
GO
CREATE TABLE [silver].[Snapshot] (
    id INT NOT NULL,
    val NVARCHAR(100) NULL
)
GO
""")

    # Pre-populate catalog: proc has no reference to silver.Snapshot (DMF misses SELECT INTO)
    proc_refs = _empty_references()
    proc_refs["tables"]["in_scope"].append({
        "schema": "bronze",
        "name": "Source",
        "is_selected": True,
        "is_updated": False,
    })
    _write_catalog_json(tmp_path, "procedures", "dbo.usp_create_snapshot", {
        "references": proc_refs,
        "mode": "deterministic",
        "routing_reasons": [],
        "needs_enrich": True,
    })

    # Table catalog: silver.Snapshot exists but has no referenced_by for the proc
    _write_catalog_json(tmp_path, "tables", "silver.snapshot", {
        "primary_keys": [],
        "referenced_by": _empty_referenced_by(),
    })

    return tmp_path


def _setup_exec_chain(tmp_path: Path) -> Path:
    """Set up DDL with proc A calling proc B, proc B writes to table T."""
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()

    _write_sql(ddl_dir, "procedures.sql", """\
CREATE PROCEDURE [dbo].[usp_orchestrator]
AS
BEGIN
    EXEC [dbo].[usp_load_data];
END
GO
CREATE PROCEDURE [dbo].[usp_load_data]
AS
BEGIN
    INSERT INTO [silver].[Target] (id, val)
    SELECT id, val FROM [bronze].[Source];
END
GO
""")

    _write_sql(ddl_dir, "tables.sql", """\
CREATE TABLE [bronze].[Source] (
    id INT NOT NULL,
    val NVARCHAR(100) NULL
)
GO
CREATE TABLE [silver].[Target] (
    id INT NOT NULL,
    val NVARCHAR(100) NULL
)
GO
""")

    # Proc B: DMF correctly shows it writes to silver.Target
    proc_b_refs = _empty_references()
    proc_b_refs["tables"]["in_scope"].append({
        "schema": "silver",
        "name": "Target",
        "is_selected": False,
        "is_updated": True,
    })
    proc_b_refs["tables"]["in_scope"].append({
        "schema": "bronze",
        "name": "Source",
        "is_selected": True,
        "is_updated": False,
    })
    _write_catalog_json(tmp_path, "procedures", "dbo.usp_load_data", {
        "references": proc_b_refs,
        "mode": "deterministic",
        "routing_reasons": [],
    })

    # Proc A: DMF only shows the call to proc B, NOT the indirect write to silver.Target
    proc_a_refs = _empty_references()
    proc_a_refs["procedures"]["in_scope"].append({
        "schema": "dbo",
        "name": "usp_load_data",
        "is_selected": False,
        "is_updated": False,
    })
    _write_catalog_json(tmp_path, "procedures", "dbo.usp_orchestrator", {
        "references": proc_a_refs,
        "mode": "call_graph_enrich",
        "routing_reasons": ["static_exec"],
        "needs_enrich": False,
    })

    # Table: silver.Target has referenced_by for proc B only
    table_ref_by = _empty_referenced_by()
    table_ref_by["procedures"]["in_scope"].append({
        "schema": "dbo",
        "name": "usp_load_data",
        "is_selected": False,
        "is_updated": True,
    })
    _write_catalog_json(tmp_path, "tables", "silver.target", {
        "primary_keys": [],
        "referenced_by": table_ref_by,
    })

    return tmp_path


def _setup_catalog_query_only(tmp_path: Path) -> Path:
    """Set up DDL where all writes are already catalog-query-detectable."""
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()

    _write_sql(ddl_dir, "procedures.sql", """\
CREATE PROCEDURE [dbo].[usp_simple_insert]
AS
BEGIN
    INSERT INTO [silver].[Target] (id, val)
    SELECT id, val FROM [bronze].[Source];
END
GO
""")

    _write_sql(ddl_dir, "tables.sql", """\
CREATE TABLE [bronze].[Source] (
    id INT NOT NULL,
    val NVARCHAR(100) NULL
)
GO
CREATE TABLE [silver].[Target] (
    id INT NOT NULL,
    val NVARCHAR(100) NULL
)
GO
""")

    # DMF already captured the write
    proc_refs = _empty_references()
    proc_refs["tables"]["in_scope"].append({
        "schema": "silver",
        "name": "Target",
        "is_selected": False,
        "is_updated": True,
    })
    proc_refs["tables"]["in_scope"].append({
        "schema": "bronze",
        "name": "Source",
        "is_selected": True,
        "is_updated": False,
    })
    _write_catalog_json(tmp_path, "procedures", "dbo.usp_simple_insert", {
        "references": proc_refs,
        "mode": "deterministic",
        "routing_reasons": [],
    })

    return tmp_path


def _setup_dynamic_sql(tmp_path: Path) -> Path:
    """Set up DDL where the proc uses dynamic SQL (EXEC(@sql))."""
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()

    _write_sql(ddl_dir, "procedures.sql", """\
CREATE PROCEDURE [dbo].[usp_dynamic]
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'INSERT INTO [silver].[Target] SELECT * FROM [bronze].[Source]';
    EXEC(@sql);
END
GO
""")

    _write_sql(ddl_dir, "tables.sql", """\
CREATE TABLE [bronze].[Source] (
    id INT NOT NULL
)
GO
CREATE TABLE [silver].[Target] (
    id INT NOT NULL
)
GO
""")

    # DMF has nothing for this proc (dynamic SQL is invisible)
    _write_catalog_json(tmp_path, "procedures", "dbo.usp_dynamic", {
        "references": _empty_references(),
        "mode": "llm_required",
        "routing_reasons": ["dynamic_sql_variable"],
        "needs_llm": True,
    })

    return tmp_path


# ── Tests ───────────────────────────────────────────────────────────────────


def test_enrich_detects_select_into(tmp_path: Path) -> None:
    """Proc with SELECT INTO — AST should detect target table DMF missed."""
    ddl = _setup_select_into(tmp_path)
    result = enrich_catalog(ddl)

    assert result.procedures_augmented >= 1
    assert result.entries_added >= 1

    # Proc catalog should now have silver.Snapshot as a write target
    proc_data = _read_catalog_json(ddl, "procedures", "dbo.usp_create_snapshot")
    assert proc_data is not None
    tables_in_scope = proc_data["references"]["tables"]["in_scope"]
    snapshot_entries = [
        e for e in tables_in_scope
        if e["name"].lower() == "snapshot" and e["schema"].lower() == "silver"
    ]
    assert len(snapshot_entries) == 1
    assert snapshot_entries[0]["detection"] == "ast_scan"
    assert snapshot_entries[0]["is_updated"] is True

    # Table catalog should have proc in referenced_by
    table_data = _read_catalog_json(ddl, "tables", "silver.snapshot")
    assert table_data is not None
    procs_in_scope = table_data["referenced_by"]["procedures"]["in_scope"]
    proc_entries = [
        e for e in procs_in_scope
        if e["name"].lower() == "usp_create_snapshot"
    ]
    assert len(proc_entries) == 1
    assert proc_entries[0]["detection"] == "ast_scan"


def test_enrich_detects_exec_chain(tmp_path: Path) -> None:
    """Proc A calls proc B which writes to table T — A should be indirect writer."""
    ddl = _setup_exec_chain(tmp_path)
    result = enrich_catalog(ddl)

    assert result.procedures_augmented >= 1
    assert result.entries_added >= 1


def test_augment_proc_catalogs_skips_missing_catalog_on_second_load(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                catalog_enrich,
                "load_proc_catalog",
                lambda _project_root, _proc_fqn: None,
            )
            procedures_augmented, entries_added = catalog_enrich._augment_proc_catalogs(
                tmp_path,
                {"dbo.usp_missing": {"silver.target"}},
                {},
            )

    assert procedures_augmented == set()
    assert entries_added == 0
    assert "reason=missing_catalog_on_write" in caplog.text


def test_enrich_preserves_catalog_query_entries(tmp_path: Path) -> None:
    """Proc with catalog-query-sourced refs — those should be unchanged after enrich."""
    ddl = _setup_catalog_query_only(tmp_path)

    # Capture original proc catalog
    original_proc = _read_catalog_json(ddl, "procedures", "dbo.usp_simple_insert")
    assert original_proc is not None
    original_tables = original_proc["references"]["tables"]["in_scope"]

    result = enrich_catalog(ddl)

    # Nothing should be added — DMF already captured everything
    assert result.entries_added == 0

    # Proc catalog should be unchanged (no detection field on DMF entries)
    proc_data = _read_catalog_json(ddl, "procedures", "dbo.usp_simple_insert")
    assert proc_data is not None
    tables_in_scope = proc_data["references"]["tables"]["in_scope"]
    for entry in tables_in_scope:
        assert "detection" not in entry


def test_enrich_idempotent(tmp_path: Path) -> None:
    """Running enrich twice produces identical output."""
    ddl = _setup_select_into(tmp_path)

    result1 = enrich_catalog(ddl)
    assert result1.entries_added >= 1

    # Snapshot after first run
    proc_after_1 = _read_catalog_json(ddl, "procedures", "dbo.usp_create_snapshot")
    table_after_1 = _read_catalog_json(ddl, "tables", "silver.snapshot")

    result2 = enrich_catalog(ddl)
    assert result2.entries_added == 0

    # Data should be identical
    proc_after_2 = _read_catalog_json(ddl, "procedures", "dbo.usp_create_snapshot")
    table_after_2 = _read_catalog_json(ddl, "tables", "silver.snapshot")
    assert proc_after_1 == proc_after_2
    assert table_after_1 == table_after_2


def test_enrich_dynamic_sql_not_augmented(tmp_path: Path) -> None:
    """Proc with EXEC(@sql) should NOT be augmented with false writes."""
    ddl = _setup_dynamic_sql(tmp_path)
    result = enrich_catalog(ddl)

    # The proc body has no parseable write statements (it is all dynamic SQL),
    # so AST enrichment should not add any table references.
    assert result.entries_added == 0

    proc_data = _read_catalog_json(ddl, "procedures", "dbo.usp_dynamic")
    assert proc_data is not None
    tables_in_scope = proc_data["references"]["tables"]["in_scope"]
    ast_entries = [e for e in tables_in_scope if e.get("detection") == "ast_scan"]
    assert len(ast_entries) == 0


# ── Corrupt catalog JSON tests ──────────────────────────────────────────


def test_enrich_skips_corrupt_proc_catalog(tmp_path: Path) -> None:
    """Enrichment skips procs with corrupt catalog JSON and continues."""
    ddl = tmp_path / "project"
    (ddl / "ddl").mkdir(parents=True)
    _write_sql(ddl / "ddl", "procedures.sql",
        "CREATE PROCEDURE dbo.usp_valid AS INSERT INTO silver.T SELECT 1\nGO\n"
        "CREATE PROCEDURE dbo.usp_corrupt AS INSERT INTO silver.T2 SELECT 1\nGO\n"
    )
    _write_sql(ddl / "ddl", "tables.sql",
        "CREATE TABLE silver.T (Id INT)\nGO\nCREATE TABLE silver.T2 (Id INT)\nGO\n"
    )
    _write_catalog_json(ddl, "procedures", "dbo.usp_valid", {
        "references": {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
    })
    cat_dir = ddl / "catalog" / "procedures"
    cat_dir.mkdir(parents=True, exist_ok=True)
    (cat_dir / "dbo.usp_corrupt.json").write_text("{truncated", encoding="utf-8")
    _write_catalog_json(ddl, "tables", "silver.t", {
        "referenced_by": {
            "procedures": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
        },
    })
    _write_catalog_json(ddl, "tables", "silver.t2", {
        "referenced_by": {
            "procedures": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
        },
    })
    (ddl / "manifest.json").write_text('{"dialect":"tsql"}', encoding="utf-8")

    result = enrich_catalog(ddl)
    assert result.procedures_augmented == 1  # only usp_valid was enriched; usp_corrupt was skipped


def test_enrich_skips_corrupt_table_catalog(tmp_path: Path) -> None:
    """Enrichment skips corrupt table catalogs in flip-to-table phase."""
    ddl = tmp_path / "project"
    (ddl / "ddl").mkdir(parents=True)
    # Proc that writes to a table with corrupt catalog
    _write_sql(ddl / "ddl", "procedures.sql",
        "CREATE PROCEDURE dbo.usp_writer AS INSERT INTO silver.Corrupt SELECT 1\nGO\n"
    )
    _write_sql(ddl / "ddl", "tables.sql",
        "CREATE TABLE silver.Corrupt (Id INT)\nGO\n"
    )
    _write_catalog_json(ddl, "procedures", "dbo.usp_writer", {
        "references": {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
    })
    # Corrupt table catalog
    cat_dir = ddl / "catalog" / "tables"
    cat_dir.mkdir(parents=True, exist_ok=True)
    (cat_dir / "silver.corrupt.json").write_text("{truncated", encoding="utf-8")
    (ddl / "manifest.json").write_text('{"dialect":"tsql"}', encoding="utf-8")

    # Should complete without error — corrupt table catalog is skipped in flip phase
    result = enrich_catalog(ddl)
    assert result.procedures_augmented >= 0


def test_enrich_partial_corruption_enriches_valid(tmp_path: Path) -> None:
    """One corrupt catalog among several: valid procs still get enriched."""
    ddl = tmp_path / "project"
    (ddl / "ddl").mkdir(parents=True)
    _write_sql(ddl / "ddl", "procedures.sql",
        "CREATE PROCEDURE dbo.usp_good AS INSERT INTO silver.T1 SELECT 1 FROM silver.T2\nGO\n"
        "CREATE PROCEDURE dbo.usp_bad AS SELECT 1\nGO\n"
    )
    _write_sql(ddl / "ddl", "tables.sql",
        "CREATE TABLE silver.T1 (Id INT)\nGO\n"
        "CREATE TABLE silver.T2 (Id INT)\nGO\n"
    )
    # Valid proc catalog
    _write_catalog_json(ddl, "procedures", "dbo.usp_good", {
        "references": {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
    })
    # Corrupt proc catalog
    cat_dir = ddl / "catalog" / "procedures"
    (cat_dir / "dbo.usp_bad.json").write_text("{truncated", encoding="utf-8")
    # Table catalogs
    for t in ("silver.t1", "silver.t2"):
        _write_catalog_json(ddl, "tables", t, {
            "referenced_by": {
                "procedures": {"in_scope": [], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
            },
        })
    (ddl / "manifest.json").write_text('{"dialect":"tsql"}', encoding="utf-8")

    result = enrich_catalog(ddl)
    # The valid proc should still be processed despite the corrupt one
    assert result.procedures_augmented >= 1


# ── View classification via object_types ─────────────────────────────────────


def _make_view_classification_project(tmp_path: Path) -> Path:
    """Set up a project where a proc references a view and a table.

    The proc catalog is pre-populated as if setup-ddl ran: the view is in
    ``references.views.in_scope`` (correctly classified via object_types).
    enrich_catalog must not move the view entry into ``references.tables``.
    """
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()

    _write_sql(ddl_dir, "procedures.sql", """\
CREATE PROCEDURE [dbo].[usp_load]
AS
BEGIN
    INSERT INTO [silver].[Target] (id, name)
    SELECT v.id, v.name FROM [silver].[vw_customer] v;
END
GO
""")
    _write_sql(ddl_dir, "views.sql", """\
CREATE VIEW [silver].[vw_customer] AS
SELECT id, name FROM [bronze].[CustomerRaw]
GO
""")
    _write_sql(ddl_dir, "tables.sql", """\
CREATE TABLE [silver].[Target] (id INT NOT NULL, name NVARCHAR(100) NULL)
GO
CREATE TABLE [bronze].[CustomerRaw] (id INT NOT NULL, name NVARCHAR(100) NULL)
GO
""")

    # Proc catalog as written by setup-ddl: view correctly in references.views
    _write_catalog_json(tmp_path, "procedures", "dbo.usp_load", {
        "references": {
            "tables": {
                "in_scope": [
                    {"schema": "silver", "name": "Target", "is_selected": False, "is_updated": True},
                ],
                "out_of_scope": [],
            },
            "views": {
                "in_scope": [
                    {"schema": "silver", "name": "vw_customer", "is_selected": True, "is_updated": False},
                ],
                "out_of_scope": [],
            },
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
        "needs_enrich": True,
        "mode": "deterministic",
    })

    # Table catalog for Target (the write target)
    _write_catalog_json(tmp_path, "tables", "silver.target", {
        "referenced_by": _empty_referenced_by(),
    })

    (tmp_path / "manifest.json").write_text('{"dialect":"tsql"}', encoding="utf-8")
    return tmp_path


def test_enrich_preserves_view_classification(tmp_path: Path) -> None:
    """enrich_catalog does not move view references from references.views to references.tables."""
    root = _make_view_classification_project(tmp_path)
    enrich_catalog(root)

    proc_cat = _read_catalog_json(root, "procedures", "dbo.usp_load")
    assert proc_cat is not None

    views_in_scope = proc_cat["references"]["views"]["in_scope"]
    tables_in_scope = proc_cat["references"]["tables"]["in_scope"]

    # View must stay in views bucket
    view_names = [e["name"] for e in views_in_scope]
    assert "vw_customer" in view_names

    # View must not leak into tables bucket
    table_names = [e["name"].lower() for e in tables_in_scope]
    assert "vw_customer" not in table_names


def test_dmf_processing_classifies_view_via_object_types() -> None:
    """process_dmf_results puts a view reference in the views bucket using object_types."""
    from shared.dmf_processing import process_dmf_results

    rows = [
        {
            "referencing_schema": "dbo",
            "referencing_name": "usp_load",
            "referenced_schema": "silver",
            "referenced_entity": "vw_customer",
            "referenced_minor_name": "",
            "referenced_class_desc": "OBJECT",  # SQL Server returns OBJECT for views
            "is_selected": True,
            "is_updated": False,
            "is_select_all": False,
            "is_insert_all": False,
            "is_all_columns_found": True,
            "is_caller_dependent": False,
            "is_ambiguous": False,
        },
    ]
    object_types = {"silver.vw_customer": "views"}

    result, _dmf_errors = process_dmf_results(rows, object_types, database="TestDB")
    assert "dbo.usp_load" in result
    refs = result["dbo.usp_load"]

    # View must be in views bucket
    view_names = [e["name"] for e in refs["views"]["in_scope"]]
    assert "vw_customer" in view_names

    # View must not appear in tables bucket
    table_names = [e["name"] for e in refs["tables"]["in_scope"]]
    assert "vw_customer" not in table_names


def test_dmf_processing_falls_back_to_tables_without_object_types() -> None:
    """Without object_types, an OBJECT class_desc reference falls back to tables bucket."""
    from shared.dmf_processing import process_dmf_results

    rows = [
        {
            "referencing_schema": "dbo",
            "referencing_name": "usp_load",
            "referenced_schema": "silver",
            "referenced_entity": "vw_customer",
            "referenced_minor_name": "",
            "referenced_class_desc": "OBJECT",
            "is_selected": True,
            "is_updated": False,
            "is_select_all": False,
            "is_insert_all": False,
            "is_all_columns_found": True,
            "is_caller_dependent": False,
            "is_ambiguous": False,
        },
    ]

    result, _dmf_errors = process_dmf_results(rows, object_types=None, database="TestDB")
    refs = result["dbo.usp_load"]
    # Without type info, falls back to tables bucket (existing behavior)
    table_names = [e["name"] for e in refs["tables"]["in_scope"]]
    assert "vw_customer" in table_names


# ── CLI: dialect from manifest.json ──────────────────────────────────────────


def test_cli_reads_dialect_from_manifest(tmp_path: Path) -> None:
    """CLI reads dialect from manifest.json when --dialect is not passed."""
    _git_init(tmp_path)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"dialect": "tsql"}), encoding="utf-8"
    )
    result = _run_enrich_cli(tmp_path)
    assert result.returncode == 0, result.stderr


def test_cli_dialect_flag_overrides_manifest(tmp_path: Path) -> None:
    """--dialect flag takes precedence over the manifest value."""
    _git_init(tmp_path)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"dialect": "oracle"}), encoding="utf-8"
    )
    result = _run_enrich_cli(tmp_path, ["--dialect", "tsql"])
    assert result.returncode == 0, result.stderr


def test_cli_missing_manifest_without_dialect_errors(tmp_path: Path) -> None:
    """Missing manifest.json and no --dialect flag produces a clear error."""
    _git_init(tmp_path)
    result = _run_enrich_cli(tmp_path)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "manifest.json" in combined


def test_cli_unsupported_manifest_without_dialect_errors(tmp_path: Path) -> None:
    """Unsupported manifest technology must fail instead of defaulting to its dialect."""
    _git_init(tmp_path)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"technology": "duckdb", "dialect": "duckdb"}),
        encoding="utf-8",
    )
    result = _run_enrich_cli(tmp_path)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "unsupported: ['duckdb']" in combined.lower()
