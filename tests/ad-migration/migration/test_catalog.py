"""Tests for catalog.py — catalog JSON file I/O and DMF result processing."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from shared.catalog import (
    flip_references,
    has_catalog,
    has_dynamic_sql,
    load_function_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    process_dmf_results,
    write_catalog_files,
    write_object_catalog,
    write_table_catalog,
)

FIXTURES = Path(__file__).parent / "fixtures" / "catalog"


# ── Loading fixtures ────────────────────────────────────────────────────────


def test_load_table_catalog_from_fixture() -> None:
    data = load_table_catalog(FIXTURES.parent, "silver.FactSales")
    assert data is not None
    assert data["cdc_enabled"] is False
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
            "cdc_enabled": True,
            "change_tracking_enabled": None,
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
        assert loaded["cdc_enabled"] is True
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
    result = process_dmf_results(rows)
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
    result = process_dmf_results(rows)
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
    result = process_dmf_results(rows)
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
                "cdc_enabled": False,
                "change_tracking_enabled": None,
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


# ── has_dynamic_sql detection ───────────────────────────────────────────


def test_has_dynamic_sql_exec_variable() -> None:
    assert has_dynamic_sql("EXEC(@sql)") is True
    assert has_dynamic_sql("EXECUTE(@sql)") is True
    assert has_dynamic_sql("EXEC (@sql)") is True


def test_has_dynamic_sql_sp_executesql() -> None:
    assert has_dynamic_sql("EXEC sp_executesql @sql, N'@id INT', @id = 1") is True


def test_has_dynamic_sql_static_exec_not_flagged() -> None:
    # Static EXEC dbo.usp_foo should NOT match — no @var or parenthesized expression
    assert has_dynamic_sql("EXEC dbo.usp_helper") is False
    assert has_dynamic_sql("EXECUTE dbo.usp_helper @param = 1") is False


def test_has_dynamic_sql_absent() -> None:
    assert has_dynamic_sql("INSERT INTO dbo.T1 SELECT * FROM dbo.T2") is False


def test_write_object_catalog_with_dynamic_sql_flag() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        refs = {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        }
        write_object_catalog(ddl_path, "procedures", "dbo.usp_dynamic", refs, dynamic_sql=True)
        loaded = load_proc_catalog(ddl_path, "dbo.usp_dynamic")
        assert loaded is not None
        assert loaded["has_dynamic_sql"] is True


def test_write_object_catalog_without_dynamic_sql_flag() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = Path(tmp)
        refs = {
            "tables": {"in_scope": [], "out_of_scope": []},
            "views": {"in_scope": [], "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": [], "out_of_scope": []},
        }
        write_object_catalog(ddl_path, "procedures", "dbo.usp_static", refs)
        loaded = load_proc_catalog(ddl_path, "dbo.usp_static")
        assert loaded is not None
        assert "has_dynamic_sql" not in loaded


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
    result = process_dmf_results(rows, database="MyDB")
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
    result = process_dmf_results(rows, database="MyDB")
    refs = result["dbo.usp_load"]
    assert len(refs["tables"]["in_scope"]) == 1
    assert refs["tables"]["out_of_scope"] == []
