"""Shared fixtures for discover command tests."""

from __future__ import annotations

import json
from pathlib import Path

_TESTS_DIR = Path(__file__).parent
_FLAT_FIXTURES = _TESTS_DIR / "fixtures" / "flat"
_UNPARSEABLE_FIXTURES = _TESTS_DIR / "fixtures" / "unparseable"
_LISTING_OBJECTS_EVAL_FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "evals" / "fixtures" / "analyzing-table" / "merge"
_SOURCE_TABLE_GUARD_FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "evals" / "fixtures" / "listing-objects" / "source-table-guard"
_CATALOG_FIXTURES = _TESTS_DIR.parent / "fixtures" / "catalog"

def _make_project_with_corrupt_catalog(tmp: Path, object_type: str, fqn: str) -> Path:
    """Set up a minimal project with one corrupt catalog file."""
    ddl_dir = tmp / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "tables.sql").write_text(
        "CREATE TABLE dbo.T (Id INT)\nGO\n", encoding="utf-8",
    )
    (ddl_dir / "procedures.sql").write_text(
        "CREATE PROCEDURE dbo.usp_test AS SELECT 1\nGO\n", encoding="utf-8",
    )
    cat_dir = tmp / "catalog" / object_type
    cat_dir.mkdir(parents=True)
    (cat_dir / f"{fqn}.json").write_text("{truncated", encoding="utf-8")
    return tmp

def _make_project_with_proc_view_refs(tmp: Path) -> Path:
    """Project where a proc references both a table and a view in its catalog."""
    ddl_dir = tmp / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "procedures.sql").write_text(
        "CREATE PROCEDURE dbo.usp_LoadData AS BEGIN SELECT 1 END\nGO\n",
        encoding="utf-8",
    )
    (ddl_dir / "tables.sql").write_text(
        "CREATE TABLE silver.FactSales (Id INT)\nGO\n", encoding="utf-8",
    )
    cat_dir = tmp / "catalog"
    (cat_dir / "procedures").mkdir(parents=True)
    (cat_dir / "tables").mkdir(parents=True)
    (cat_dir / "procedures" / "dbo.usp_loaddata.json").write_text(
        json.dumps({
            "references": {
                "tables": {
                    "in_scope": [
                        {
                            "schema": "silver", "name": "FactSales",
                            "is_selected": True, "is_updated": True,
                            "is_insert_all": False, "columns": [],
                        },
                    ],
                    "out_of_scope": [],
                },
                "views": {
                    "in_scope": [
                        {"schema": "dbo", "name": "vw_customer_dim", "is_selected": True, "is_updated": False},
                    ],
                    "out_of_scope": [],
                },
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            },
        }),
        encoding="utf-8",
    )
    (cat_dir / "tables" / "silver.factsales.json").write_text(
        json.dumps({
            "columns": [], "primary_keys": [], "unique_indexes": [], "foreign_keys": [],
            "auto_increment_columns": [], "change_capture": None, "sensitivity_classifications": [],
            "referenced_by": {
                "procedures": {"in_scope": [], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
            },
        }),
        encoding="utf-8",
    )
    return tmp

def _make_project_with_view_catalog(tmp: Path) -> Path:
    """Project with a view catalog entry that has referenced_by.procedures."""
    ddl_dir = tmp / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "views.sql").write_text(
        "CREATE VIEW dbo.vw_customer_dim AS SELECT Id FROM dbo.Customer\nGO\n",
        encoding="utf-8",
    )
    cat_dir = tmp / "catalog"
    (cat_dir / "views").mkdir(parents=True)
    (cat_dir / "views" / "dbo.vw_customer_dim.json").write_text(
        json.dumps({
            "schema": "dbo",
            "name": "vw_customer_dim",
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
            },
            "referenced_by": {
                "procedures": {
                    "in_scope": [
                        {
                            "schema": "dbo", "name": "usp_load_fact_sales",
                            "is_selected": True, "is_updated": False,
                        },
                    ],
                    "out_of_scope": [],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
            },
        }),
        encoding="utf-8",
    )
    return tmp

def _make_table_cat(root: Path, fqn: str, scoping: dict, extra: dict | None = None) -> Path:
    tables_dir = root / "catalog" / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    schema, name = fqn.split(".", 1)
    data: dict = {"schema": schema, "name": name, "scoping": scoping}
    if extra:
        data.update(extra)
    path = tables_dir / f"{fqn}.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path

def _make_proc_cat(root: Path, fqn: str) -> Path:
    """Create a minimal proc catalog file at catalog/procedures/<fqn>.json."""
    proc_dir = root / "catalog" / "procedures"
    proc_dir.mkdir(parents=True, exist_ok=True)
    schema, name = fqn.split(".", 1)
    data = {
        "schema": schema,
        "name": name,
        "references": {
            "tables": {"in_scope": [], "out_of_scope": []},
        },
    }
    path = proc_dir / f"{fqn}.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
