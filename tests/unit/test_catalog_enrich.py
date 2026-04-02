"""Tests for catalog_enrich.py — offline AST enrichment for catalog JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from shared.catalog_enrich import enrich_catalog


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

    assert result["procedures_augmented"] >= 1
    assert result["entries_added"] >= 1

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

    assert result["procedures_augmented"] >= 1
    assert result["entries_added"] >= 1

    # Proc A should now have silver.Target as a write target (indirect)
    proc_data = _read_catalog_json(ddl, "procedures", "dbo.usp_orchestrator")
    assert proc_data is not None
    tables_in_scope = proc_data["references"]["tables"]["in_scope"]
    target_entries = [
        e for e in tables_in_scope
        if e["name"].lower() == "target" and e["schema"].lower() == "silver"
    ]
    assert len(target_entries) == 1
    assert target_entries[0]["detection"] == "ast_scan"
    assert target_entries[0]["is_updated"] is True
    assert proc_data["mode"] == "call_graph_enrich"
    assert proc_data["routing_reasons"] == ["static_exec"]

    # Table catalog should have proc A in referenced_by
    table_data = _read_catalog_json(ddl, "tables", "silver.target")
    assert table_data is not None
    procs_in_scope = table_data["referenced_by"]["procedures"]["in_scope"]
    orch_entries = [
        e for e in procs_in_scope
        if e["name"].lower() == "usp_orchestrator"
    ]
    assert len(orch_entries) == 1
    assert orch_entries[0]["detection"] == "ast_scan"


def test_enrich_preserves_catalog_query_entries(tmp_path: Path) -> None:
    """Proc with catalog-query-sourced refs — those should be unchanged after enrich."""
    ddl = _setup_catalog_query_only(tmp_path)

    # Capture original proc catalog
    original_proc = _read_catalog_json(ddl, "procedures", "dbo.usp_simple_insert")
    assert original_proc is not None
    original_tables = original_proc["references"]["tables"]["in_scope"]

    result = enrich_catalog(ddl)

    # Nothing should be added — DMF already captured everything
    assert result["entries_added"] == 0

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
    assert result1["entries_added"] >= 1

    # Snapshot after first run
    proc_after_1 = _read_catalog_json(ddl, "procedures", "dbo.usp_create_snapshot")
    table_after_1 = _read_catalog_json(ddl, "tables", "silver.snapshot")

    result2 = enrich_catalog(ddl)
    assert result2["entries_added"] == 0

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
    assert result["entries_added"] == 0

    proc_data = _read_catalog_json(ddl, "procedures", "dbo.usp_dynamic")
    assert proc_data is not None
    tables_in_scope = proc_data["references"]["tables"]["in_scope"]
    ast_entries = [e for e in tables_in_scope if e.get("detection") == "ast_scan"]
    assert len(ast_entries) == 0
