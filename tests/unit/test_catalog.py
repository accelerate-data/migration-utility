"""Tests for catalog.py — catalog JSON file I/O and DMF result processing."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from shared.catalog import (
    has_catalog,
    load_function_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    read_selected_writer,
    restore_enriched_fields,
    scan_routing_flags,
    snapshot_enriched_fields,
    write_object_catalog,
    write_proc_statements,
    write_table_catalog,
)
from shared.loader_data import CatalogLoadError
from shared.dmf_processing import (
    flip_references,
    process_dmf_results,
)
from shared.catalog_dmf import write_catalog_files

FIXTURES = Path(__file__).parent / "fixtures" / "catalog"


# ── Loading fixtures ────────────────────────────────────────────────────────


def test_load_table_catalog_from_fixture() -> None:
    data = load_table_catalog(FIXTURES.parent, "silver.FactSales")
    assert data is not None
    assert data["change_capture"] is None
    assert len(data["primary_keys"]) == 1
    assert data["primary_keys"][0]["columns"] == ["sale_id"]
    writers = [
        e for e in data["referenced_by"]["procedures"]["in_scope"] if e["is_updated"]
    ]
    assert len(writers) == 1
    assert writers[0]["name"] == "usp_load_fact_sales"


def test_load_proc_catalog_from_fixture() -> None:
    data = load_proc_catalog(FIXTURES.parent, "dbo.usp_load_fact_sales")
    assert data is not None
    tables = data["references"]["tables"]["in_scope"]
    assert len(tables) == 2
    written = [t for t in tables if t["is_updated"]]
    assert len(written) == 1
    assert written[0]["name"] == "FactSales"


def test_load_missing_returns_none() -> None:
    assert load_table_catalog(FIXTURES.parent, "dbo.nonexistent") is None
    assert load_proc_catalog(FIXTURES.parent, "dbo.nonexistent") is None
    assert load_view_catalog(FIXTURES.parent, "dbo.nonexistent") is None
    assert load_function_catalog(FIXTURES.parent, "dbo.nonexistent") is None


# ── has_catalog ─────────────────────────────────────────────────────────────


def test_has_catalog_true() -> None:
    assert has_catalog(FIXTURES.parent) is True


def test_has_catalog_false() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        assert has_catalog(Path(tmp)) is False


# ── Write round-trip ────────────────────────────────────────────────────────


def test_write_table_catalog_round_trip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        signals = {
            "primary_keys": [{"constraint_name": "PK_T1", "columns": ["id"]}],
            "unique_indexes": [],
            "foreign_keys": [],
            "auto_increment_columns": [{"column": "id", "mechanism": "identity"}],
            "change_capture": {"enabled": True, "mechanism": "cdc"},
            "sensitivity_classifications": [],
        }
        ref_by = {
            "procedures": {
                "in_scope": [
                    {"schema": "dbo", "name": "usp_load", "is_selected": False, "is_updated": True}
                ],
                "out_of_scope": [],
            },
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
        }
        write_table_catalog(ddl_path, "dbo.T1", signals, ref_by)
        loaded = load_table_catalog(ddl_path, "dbo.T1")
        assert loaded is not None
        assert loaded["change_capture"] == {"enabled": True, "mechanism": "cdc"}
        assert loaded["auto_increment_columns"] == [{"column": "id", "mechanism": "identity"}]
        assert len(loaded["referenced_by"]["procedures"]["in_scope"]) == 1


def test_write_object_catalog_round_trip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        refs = {
            "tables": {
                "in_scope": [
                    {"schema": "dbo", "name": "T1", "is_selected": True, "is_updated": False}
                ],
                "out_of_scope": [],
            },
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        }
        write_object_catalog(ddl_path, "procedures", "dbo.usp_test", refs)
        loaded = load_proc_catalog(ddl_path, "dbo.usp_test")
        assert loaded is not None
        assert loaded["references"]["tables"]["in_scope"][0]["name"] == "T1"


# ── DMF result processing ──────────────────────────────────────────────────


def _make_dmf_row(
    ref_schema: str = "dbo",
    ref_name: str = "usp_load",
    tgt_schema: str = "silver",
    tgt_entity: str = "FactSales",
    minor_name: str = "",
    class_desc: str = "USER_TABLE",
    is_selected: bool = False,
    is_updated: bool = False,
    is_insert_all: bool = False,
) -> dict:
    return {
        "referencing_schema": ref_schema,
        "referencing_name": ref_name,
        "referenced_schema": tgt_schema,
        "referenced_entity": tgt_entity,
        "referenced_minor_name": minor_name,
        "referenced_class_desc": class_desc,
        "is_selected": is_selected,
        "is_updated": is_updated,
        "is_select_all": False,
        "is_insert_all": is_insert_all,
        "is_all_columns_found": True,
        "is_caller_dependent": False,
        "is_ambiguous": False,
    }


def test_process_dmf_results_groups_by_referencing_object() -> None:
    rows = [
        _make_dmf_row(ref_name="usp_load", tgt_entity="FactSales", is_updated=True),
        _make_dmf_row(ref_name="usp_load", tgt_entity="SalesRaw", tgt_schema="bronze", is_selected=True),
        _make_dmf_row(ref_name="usp_other", tgt_entity="FactSales", is_selected=True),
    ]
    result, _dmf_errors = process_dmf_results(rows)
    assert "dbo.usp_load" in result
    assert "dbo.usp_other" in result

    load_refs = result["dbo.usp_load"]
    assert len(load_refs["tables"]["in_scope"]) == 2
    fact_entry = next(t for t in load_refs["tables"]["in_scope"] if t["name"] == "FactSales")
    assert fact_entry["is_updated"] is True


def test_process_dmf_results_column_detail() -> None:
    rows = [
        _make_dmf_row(tgt_entity="FactSales", minor_name="sale_id", is_selected=True),
        _make_dmf_row(tgt_entity="FactSales", minor_name="amount", is_updated=True),
        _make_dmf_row(tgt_entity="FactSales", is_updated=True),  # entity-level
    ]
    result, _dmf_errors = process_dmf_results(rows)
    refs = result["dbo.usp_load"]
    fact = next(t for t in refs["tables"]["in_scope"] if t["name"] == "FactSales")
    assert fact["is_selected"] is True  # from column-level row
    assert fact["is_updated"] is True  # from entity-level row
    assert len(fact["columns"]) == 2
    col_names = {c["name"] for c in fact["columns"]}
    assert col_names == {"sale_id", "amount"}


def test_process_dmf_results_classifies_types() -> None:
    rows = [
        _make_dmf_row(tgt_entity="vw_summary", class_desc="VIEW", is_selected=True),
        _make_dmf_row(tgt_entity="fn_calc", class_desc="SQL_SCALAR_FUNCTION", is_selected=True),
        _make_dmf_row(tgt_entity="usp_helper", class_desc="SQL_STORED_PROCEDURE"),
    ]
    result, _dmf_errors = process_dmf_results(rows)
    refs = result["dbo.usp_load"]
    assert len(refs["views"]["in_scope"]) == 1
    assert len(refs["functions"]["in_scope"]) == 1
    assert len(refs["procedures"]["in_scope"]) == 1
    assert refs["tables"]["in_scope"] == []


# ── flip_references ─────────────────────────────────────────────────────────


def test_flip_references_builds_table_referenced_by() -> None:
    proc_refs = {
        "dbo.usp_load": {
            "tables": {
                "in_scope": [
                    {"schema": "silver", "name": "FactSales", "is_selected": False, "is_updated": True},
                    {"schema": "bronze", "name": "SalesRaw", "is_selected": True, "is_updated": False},
                ],
                "out_of_scope": [],
            },
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
        "dbo.usp_read": {
            "tables": {
                "in_scope": [
                    {"schema": "silver", "name": "FactSales", "is_selected": True, "is_updated": False},
                ],
                "out_of_scope": [],
            },
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        },
    }
    flipped = flip_references(proc_refs, "procedures")
    assert "silver.factsales" in flipped
    assert "bronze.salesraw" in flipped
    fact_refs = flipped["silver.factsales"]["procedures"]["in_scope"]
    assert len(fact_refs) == 2
    writer = next(e for e in fact_refs if e["is_updated"])
    assert writer["name"] == "usp_load"
    reader = next(e for e in fact_refs if e["is_selected"] and not e["is_updated"])
    assert reader["name"] == "usp_read"


# ── write_catalog_files (integration) ──────────────────────────────────────


def test_write_catalog_files_end_to_end() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        table_signals = {
            "silver.factsales": {
                "primary_keys": [],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
            }
        }
        proc_rows = [
            _make_dmf_row(
                ref_name="usp_load_fact_sales",
                tgt_entity="FactSales",
                tgt_schema="silver",
                is_updated=True,
            ),
            _make_dmf_row(
                ref_name="usp_load_fact_sales",
                tgt_entity="SalesRaw",
                tgt_schema="bronze",
                is_selected=True,
            ),
        ]
        view_rows = [
            _make_dmf_row(
                ref_schema="dbo",
                ref_name="vw_sales_summary",
                tgt_entity="FactSales",
                tgt_schema="silver",
                class_desc="USER_TABLE",
                is_selected=True,
            ),
        ]
        counts = write_catalog_files(
            ddl_path,
            table_signals=table_signals,
            proc_dmf_rows=proc_rows,
            view_dmf_rows=view_rows,
            func_dmf_rows=[],
        )
        assert counts["procedures"] == 1
        assert counts["views"] == 1
        assert counts["tables"] >= 1

        # Verify table file has referenced_by from both procs and views
        table_data = load_table_catalog(ddl_path, "silver.FactSales")
        assert table_data is not None
        proc_writers = [
            e for e in table_data["referenced_by"]["procedures"]["in_scope"] if e["is_updated"]
        ]
        assert len(proc_writers) == 1
        view_readers = table_data["referenced_by"]["views"]["in_scope"]
        assert len(view_readers) == 1

        # Verify proc file has outbound references
        proc_data = load_proc_catalog(ddl_path, "dbo.usp_load_fact_sales")
        assert proc_data is not None
        assert len(proc_data["references"]["tables"]["in_scope"]) == 2


# ── scan_routing_flags ──────────────────────────────────────────────────────


def test_scan_routing_flags_exec_dynamic_needs_llm() -> None:
    flags = scan_routing_flags("EXEC(@sql)")
    assert flags["needs_llm"] is True
    assert flags["needs_enrich"] is False
    assert flags["mode"] == "llm_required"
    assert flags["routing_reasons"] == ["dynamic_sql_variable"]

    flags = scan_routing_flags("EXECUTE(@sql)")
    assert flags["needs_llm"] is True

    flags = scan_routing_flags("EXEC (@sql)")
    assert flags["needs_llm"] is True


def test_scan_routing_flags_try_catch_needs_llm() -> None:
    flags = scan_routing_flags("BEGIN TRY\n  INSERT INTO dbo.T1 VALUES(1);\nEND TRY BEGIN CATCH END CATCH")
    assert flags["needs_llm"] is False
    assert flags["needs_enrich"] is False
    assert flags["mode"] == "control_flow_fallback"
    assert flags["routing_reasons"] == ["try_catch"]


def test_scan_routing_flags_while_needs_llm() -> None:
    flags = scan_routing_flags("WHILE @i < 10 BEGIN SET @i = @i + 1; END")
    assert flags["needs_llm"] is False
    assert flags["mode"] == "control_flow_fallback"
    assert flags["routing_reasons"] == ["while_loop"]


def test_scan_routing_flags_if_needs_llm() -> None:
    flags = scan_routing_flags("IF EXISTS (SELECT 1 FROM dbo.T) INSERT INTO dbo.T2 VALUES(1)")
    assert flags["needs_llm"] is False
    assert flags["mode"] == "control_flow_fallback"
    assert flags["routing_reasons"] == ["if_else"]


def test_scan_routing_flags_sp_executesql_no_flags() -> None:
    flags = scan_routing_flags("EXEC sp_executesql @sql")
    assert flags["needs_llm"] is True
    assert flags["needs_enrich"] is False
    assert flags["mode"] == "llm_required"
    assert flags["routing_reasons"] == ["dynamic_sql_variable"]


def test_scan_routing_flags_sp_executesql_literal() -> None:
    flags = scan_routing_flags("EXEC sp_executesql N'INSERT INTO dbo.T VALUES (1)'")
    assert flags["needs_llm"] is False
    assert flags["needs_enrich"] is False
    assert flags["mode"] == "dynamic_sql_literal"
    assert flags["routing_reasons"] == ["dynamic_sql_literal"]


def test_scan_routing_flags_select_into_needs_enrich() -> None:
    # SELECT INTO: INTO on a line without INSERT
    flags = scan_routing_flags("SELECT id, val\nINTO dbo.target\nFROM dbo.source")
    assert flags["needs_enrich"] is True
    assert flags["needs_llm"] is False
    assert flags["mode"] == "deterministic"

    # INSERT INTO must NOT match
    flags2 = scan_routing_flags("INSERT INTO dbo.target SELECT id FROM dbo.source")
    assert flags2["needs_enrich"] is False


def test_scan_routing_flags_truncate_needs_enrich() -> None:
    flags = scan_routing_flags("TRUNCATE TABLE dbo.target; INSERT INTO dbo.target SELECT * FROM dbo.src")
    assert flags["needs_enrich"] is True
    assert flags["needs_llm"] is False
    assert flags["mode"] == "deterministic"


def test_scan_routing_flags_static_exec_needs_enrich() -> None:
    flags = scan_routing_flags("EXEC dbo.usp_helper")
    assert flags["needs_enrich"] is True
    assert flags["needs_llm"] is False
    assert flags["mode"] == "call_graph_enrich"
    assert flags["routing_reasons"] == ["static_exec"]

    flags = scan_routing_flags("EXECUTE schema.usp_other @param = 1")
    assert flags["needs_enrich"] is True


def test_scan_routing_flags_linked_server_exec_needs_enrich() -> None:
    flags = scan_routing_flags("EXEC [LinkedSrv].[SalesDb].[dbo].[usp_remote_load]")
    assert flags["needs_enrich"] is True
    assert flags["needs_llm"] is False
    assert flags["mode"] == "call_graph_enrich"
    assert flags["routing_reasons"] == ["linked_server_exec", "static_exec"]


def test_scan_routing_flags_exec_concat_needs_llm() -> None:
    flags = scan_routing_flags("EXEC ('SELECT * FROM dbo.T WHERE id = ' + CAST(@id AS NVARCHAR(12)))")
    assert flags["needs_llm"] is True
    assert flags["needs_enrich"] is False
    assert flags["mode"] == "llm_required"
    assert flags["routing_reasons"] == ["dynamic_sql_variable"]


def test_scan_routing_flags_pure_dml_no_flags() -> None:
    flags = scan_routing_flags("INSERT INTO dbo.T1 SELECT * FROM dbo.T2")
    assert flags["needs_llm"] is False
    assert flags["needs_enrich"] is False
    assert flags["mode"] == "deterministic"
    assert flags["routing_reasons"] == []


def test_scan_routing_flags_ignores_keywords_in_unterminated_block_comment() -> None:
    flags = scan_routing_flags("/* IF WHILE BEGIN TRY dynamic tail")
    assert flags["needs_llm"] is False
    assert flags["needs_enrich"] is False
    assert flags["mode"] == "deterministic"
    assert flags["routing_reasons"] == []


def test_write_object_catalog_with_needs_llm_flag() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        refs = {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        }
        write_object_catalog(ddl_path, "procedures", "dbo.usp_dynamic", refs, needs_llm=True)
        loaded = load_proc_catalog(ddl_path, "dbo.usp_dynamic")
        assert loaded is not None
        assert loaded["needs_llm"] is True
        assert "needs_enrich" not in loaded


def test_write_object_catalog_with_needs_enrich_flag() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        refs = {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        }
        write_object_catalog(ddl_path, "procedures", "dbo.usp_static", refs, needs_enrich=True)
        loaded = load_proc_catalog(ddl_path, "dbo.usp_static")
        assert loaded is not None
        assert loaded["needs_enrich"] is True
        assert "needs_llm" not in loaded


def test_write_object_catalog_no_flags() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        refs = {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        }
        write_object_catalog(ddl_path, "procedures", "dbo.usp_plain", refs)
        loaded = load_proc_catalog(ddl_path, "dbo.usp_plain")
        assert loaded is not None
        assert "needs_llm" not in loaded
        assert "needs_enrich" not in loaded


def test_write_object_catalog_with_routing_summary() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        refs = {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        }
        write_object_catalog(
            ddl_path,
            "procedures",
            "dbo.usp_route",
            refs,
            mode="control_flow_fallback",
            routing_reasons=["if_else", "static_exec"],
        )
        loaded = load_proc_catalog(ddl_path, "dbo.usp_route")
        assert loaded is not None
        assert loaded["mode"] == "control_flow_fallback"
        assert loaded["routing_reasons"] == ["if_else", "static_exec"]


# ── Cross-database / cross-server scoping ─────────────────────────────────


def test_process_dmf_results_cross_database() -> None:
    """Rows with referenced_database_name != current database land in out_of_scope."""
    rows = [
        {
            **_make_dmf_row(tgt_entity="FactSales", is_updated=True),
            "referenced_database_name": "",
            "referenced_server_name": "",
        },
        {
            **_make_dmf_row(tgt_entity="ExternalTable", tgt_schema="dbo"),
            "referenced_database_name": "OtherDB",
            "referenced_server_name": "",
        },
        {
            **_make_dmf_row(tgt_entity="RemoteTable", tgt_schema="dbo"),
            "referenced_database_name": "RemoteDB",
            "referenced_server_name": "LinkedServer1",
        },
    ]
    result, _dmf_errors = process_dmf_results(rows, database="MyDB")
    refs = result["dbo.usp_load"]

    # In-scope: only FactSales
    assert len(refs["tables"]["in_scope"]) == 1
    assert refs["tables"]["in_scope"][0]["name"] == "FactSales"

    # Out-of-scope: ExternalTable (cross_database) and RemoteTable (cross_server)
    out = refs["tables"]["out_of_scope"]
    assert len(out) == 2
    by_name = {e["name"]: e for e in out}

    ext = by_name["ExternalTable"]
    assert ext["reason"] == "cross_database_reference"
    assert ext["database"] == "OtherDB"
    assert ext["server"] is None

    remote = by_name["RemoteTable"]
    assert remote["reason"] == "cross_server_reference"
    assert remote["server"] == "LinkedServer1"


def test_process_dmf_results_same_database_is_in_scope() -> None:
    """Rows where referenced_database_name matches the current database stay in_scope."""
    rows = [
        {
            **_make_dmf_row(tgt_entity="LocalTable", is_selected=True),
            "referenced_database_name": "MyDB",
            "referenced_server_name": "",
        },
    ]
    result, _dmf_errors = process_dmf_results(rows, database="MyDB")
    refs = result["dbo.usp_load"]
    assert len(refs["tables"]["in_scope"]) == 1
    assert refs["tables"]["out_of_scope"] == []


# ── Corrupt JSON / CatalogLoadError ──────────────────────────────────────


def _write_corrupt(tmp: Path, object_type: str, fqn: str, content: str | bytes) -> None:
    """Write corrupt content to the appropriate catalog path."""
    d = tmp / "catalog" / object_type
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{fqn}.json"
    if isinstance(content, bytes):
        p.write_bytes(content)
    else:
        p.write_text(content, encoding="utf-8")


def test_load_table_catalog_truncated_json_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "tables", "dbo.broken", '{"primary_keys": [')
        with pytest.raises(CatalogLoadError) as exc_info:
            load_table_catalog(root, "dbo.broken")
        assert "dbo.broken" in exc_info.value.path


def test_load_proc_catalog_empty_file_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "procedures", "dbo.broken", "")
        with pytest.raises(CatalogLoadError):
            load_proc_catalog(root, "dbo.broken")


def test_load_view_catalog_binary_garbage_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "views", "dbo.broken", b"\x80\x81\x82\x83")
        with pytest.raises(CatalogLoadError):
            load_view_catalog(root, "dbo.broken")


def test_load_function_catalog_corrupt_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "functions", "dbo.broken", "{not valid")
        with pytest.raises(CatalogLoadError):
            load_function_catalog(root, "dbo.broken")


def test_write_proc_statements_corrupt_existing_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "procedures", "dbo.broken", "{truncated")
        with pytest.raises(CatalogLoadError):
            write_proc_statements(root, "dbo.broken", [{"type": "insert"}])


def test_write_proc_statements_missing_catalog_raises_file_not_found() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with pytest.raises(FileNotFoundError):
            write_proc_statements(root, "dbo.nonexistent", [{"type": "insert"}])


def test_read_selected_writer_corrupt_table_catalog_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "tables", "dbo.broken", "{not json")
        with pytest.raises(CatalogLoadError):
            read_selected_writer(root, "dbo.broken")


# ── Cross-cutting edge cases ─────────────────────────────────────────────


def test_load_catalog_whitespace_only_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "tables", "dbo.ws", "   \n  \t  ")
        with pytest.raises(CatalogLoadError):
            load_table_catalog(root, "dbo.ws")


def test_load_catalog_null_literal_loads() -> None:
    """A file containing just 'null' is valid JSON."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "tables", "dbo.nullval", "null")
        result = load_table_catalog(root, "dbo.nullval")
        assert result is None


def test_load_catalog_bom_prefix_raises_with_path() -> None:
    """UTF-8 BOM prefix produces a clear CatalogLoadError with file path."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        bom_content = '\ufeff{"key": "value"}'
        _write_corrupt(root, "tables", "dbo.bom", bom_content)
        with pytest.raises(CatalogLoadError) as exc_info:
            load_table_catalog(root, "dbo.bom")
        assert "dbo.bom" in exc_info.value.path


def test_load_catalog_trailing_comma_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "tables", "dbo.comma", '{"a": 1,}')
        with pytest.raises(CatalogLoadError) as exc_info:
            load_table_catalog(root, "dbo.comma")
        assert "dbo.comma" in exc_info.value.path


def test_load_catalog_oserror_propagates() -> None:
    """An OSError reading a catalog file should propagate (not masked by JSONDecodeError catch)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_corrupt(root, "tables", "dbo.locked", '{"ok": true}')
        with patch("pathlib.Path.read_text", side_effect=OSError("locked")):
            with pytest.raises(OSError, match="locked"):
                load_table_catalog(root, "dbo.locked")


# ── MULTI_TABLE_WRITE warning ────────────────────────────────────────────


def _multi_table_refs(updated_count: int) -> dict:
    """Build references with *updated_count* is_updated tables."""
    tables = [
        {"schema": "silver", "name": f"Table{i}", "is_selected": True, "is_updated": True}
        for i in range(updated_count)
    ]
    return {
        "tables": {"in_scope": tables, "out_of_scope": []},
        "views": {"in_scope": [], "out_of_scope": []},
        "functions": {"in_scope": [], "out_of_scope": []},
        "procedures": {"in_scope": [], "out_of_scope": []},
    }


def test_multi_table_write_no_inline_warning() -> None:
    """write_object_catalog no longer emits MULTI_TABLE_WRITE inline (moved to diagnostics runner)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_object_catalog(root, "procedures", "dbo.usp_multi", _multi_table_refs(2))
        loaded = load_proc_catalog(root, "dbo.usp_multi")
        assert loaded is not None
        assert "warnings" not in loaded


# ── scan_routing_flags — cross_db_exec ──────────────────────────────────────


def test_scan_routing_flags_cross_db_exec() -> None:
    """Three-part EXEC (db.schema.proc) sets cross_db_exec without linked_server_exec."""
    flags = scan_routing_flags("EXEC OtherDB.dbo.usp_helper")
    assert "cross_db_exec" in flags["routing_reasons"]
    assert "linked_server_exec" not in flags["routing_reasons"]
    assert flags["needs_llm"] is False
    assert flags["needs_enrich"] is True
    assert flags["mode"] == "call_graph_enrich"


def test_scan_routing_flags_linked_server_is_not_cross_db_exec() -> None:
    """Four-part EXEC (server.db.schema.proc) sets linked_server_exec but not cross_db_exec."""
    flags = scan_routing_flags("EXEC [LinkedSrv].[SalesDb].[dbo].[usp_remote]")
    assert "linked_server_exec" in flags["routing_reasons"]
    assert "cross_db_exec" not in flags["routing_reasons"]


# ── snapshot_enriched_fields / restore_enriched_fields ──────────────────────


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_snapshot_returns_only_non_none_enriched_keys() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_json(
            root / "catalog" / "procedures" / "dbo.usp_a.json",
            {"scoping": {"tier": "gold"}, "profile": None, "refactor": {"note": "x"}, "other": "ignored"},
        )
        snapshot = snapshot_enriched_fields(root)
        assert "dbo.usp_a" in snapshot
        assert snapshot["dbo.usp_a"] == {"scoping": {"tier": "gold"}, "refactor": {"note": "x"}}
        assert "profile" not in snapshot["dbo.usp_a"]
        assert "other" not in snapshot["dbo.usp_a"]


def test_snapshot_refactor_not_captured_from_tables() -> None:
    """refactor key must not be snapshotted from table catalogs."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_json(
            root / "catalog" / "tables" / "silver.t1.json",
            {"scoping": {"tier": "silver"}, "refactor": {"note": "should be ignored"}},
        )
        snapshot = snapshot_enriched_fields(root)
        assert "silver.t1" in snapshot
        assert "refactor" not in snapshot["silver.t1"]
        assert snapshot["silver.t1"] == {"scoping": {"tier": "silver"}}


def test_snapshot_skips_corrupt_catalog_file() -> None:
    """Corrupt JSON files are silently skipped during snapshot."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "procedures").mkdir(parents=True)
        (root / "catalog" / "procedures" / "dbo.corrupt.json").write_text("{truncated", encoding="utf-8")
        _write_json(
            root / "catalog" / "procedures" / "dbo.good.json",
            {"scoping": {"tier": "gold"}},
        )
        snapshot = snapshot_enriched_fields(root)
        assert "dbo.corrupt" not in snapshot
        assert "dbo.good" in snapshot


def test_restore_merges_enriched_fields_back() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_json(
            root / "catalog" / "procedures" / "dbo.usp_a.json",
            {"references": {}, "ddl_hash": "abc"},
        )
        snapshot = {"dbo.usp_a": {"scoping": {"tier": "gold"}, "refactor": {"note": "r1"}}}
        restore_enriched_fields(root, snapshot)
        data = json.loads((root / "catalog" / "procedures" / "dbo.usp_a.json").read_text())
        assert data["scoping"] == {"tier": "gold"}
        assert data["refactor"] == {"note": "r1"}
        assert data["ddl_hash"] == "abc"


def test_restore_skips_fqn_not_found_after_reextract() -> None:
    """FQN in snapshot but absent from all buckets after re-extraction is a no-op."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "procedures").mkdir(parents=True)
        snapshot = {"dbo.usp_gone": {"scoping": {"tier": "gold"}}}
        restore_enriched_fields(root, snapshot)  # must not raise


def test_restore_no_write_when_values_unchanged() -> None:
    """restore_enriched_fields does not rewrite files when values are already equal."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        catalog_file = root / "catalog" / "procedures" / "dbo.usp_a.json"
        _write_json(catalog_file, {"scoping": {"tier": "gold"}})
        mtime_before = catalog_file.stat().st_mtime
        snapshot = {"dbo.usp_a": {"scoping": {"tier": "gold"}}}
        restore_enriched_fields(root, snapshot)
        assert catalog_file.stat().st_mtime == mtime_before
