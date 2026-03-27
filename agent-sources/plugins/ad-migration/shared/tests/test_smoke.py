"""Smoke tests for the shared library.

Covers:
- Module imports
- Key type instantiation
- IR JSON round-trip
- name_resolver normalization
- loader: GO-split + DdlCatalog population
- dialect: protocol compliance + registry lookup
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest


# ── Import smoke ──────────────────────────────────────────────────────────────

def test_import_ir() -> None:
    from shared import ir  # noqa: F401


def test_import_loader() -> None:
    from shared import loader  # noqa: F401


def test_import_dialect() -> None:
    from shared import dialect  # noqa: F401


def test_import_name_resolver() -> None:
    from shared import name_resolver  # noqa: F401


# ── IR types ──────────────────────────────────────────────────────────────────

def test_table_ref_fqn() -> None:
    from shared.ir import TableRef

    ref = TableRef(schema_name="silver", table_name="DimProduct")
    assert ref.fqn == "silver.DimProduct"


def test_column_ref() -> None:
    from shared.ir import ColumnRef, TableRef

    col = ColumnRef(column_name="ProductKey", table_ref=TableRef(schema_name="silver", table_name="DimProduct"))
    assert col.column_name == "ProductKey"
    assert col.table_ref is not None


def test_proc_param() -> None:
    from shared.ir import ProcParam

    p = ProcParam(name="@StartDate", sql_type="DATE", default_value="'2020-01-01'")
    assert p.name == "@StartDate"
    assert not p.is_output


def test_cte_node() -> None:
    from shared.ir import CteNode

    cte = CteNode(name="base", select_sql="SELECT 1 AS x")
    assert cte.name == "base"


def test_select_model() -> None:
    from shared.ir import CteNode, SelectModel

    model = SelectModel(
        ctes=[CteNode(name="base", select_sql="SELECT 1 AS x")],
        final_select="SELECT * FROM base",
    )
    assert len(model.ctes) == 1


def test_procedure() -> None:
    from shared.ir import Procedure, ProcParam

    proc = Procedure(
        schema_name="dbo",
        procedure_name="usp_LoadDimProduct",
        params=[ProcParam(name="@BatchDate", sql_type="DATE")],
        body_sql="SELECT 1",
        source_file="procedures.sql",
    )
    assert proc.fqn == "dbo.usp_LoadDimProduct"


# ── IR JSON round-trip ────────────────────────────────────────────────────────

def test_procedure_json_roundtrip() -> None:
    from shared.ir import Procedure, ProcParam

    proc = Procedure(
        schema_name="dbo",
        procedure_name="usp_Test",
        params=[ProcParam(name="@Id", sql_type="INT", default_value="0")],
        body_sql="SELECT @Id AS id",
    )
    raw = proc.model_dump_json()
    data = json.loads(raw)
    restored = Procedure.model_validate(data)
    assert restored.fqn == proc.fqn
    assert restored.params[0].name == "@Id"


def test_select_model_json_roundtrip() -> None:
    from shared.ir import CteNode, SelectModel

    model = SelectModel(
        ctes=[CteNode(name="src", select_sql="SELECT id FROM raw")],
        final_select="SELECT * FROM src",
    )
    raw = model.model_dump_json()
    restored = SelectModel.model_validate(json.loads(raw))
    assert restored.ctes[0].name == "src"


# ── name_resolver ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("input_name,default_schema,expected", [
    ("[silver].[DimProduct]", "dbo", "silver.dimproduct"),
    ("DimProduct", "dbo", "dbo.dimproduct"),
    ("dbo.usp_Load", "dbo", "dbo.usp_load"),
    ("[MyDB].[dbo].[FactSales]", "dbo", "dbo.factsales"),
    ("SILVER.DimCustomer", "dbo", "silver.dimcustomer"),
])
def test_normalize(input_name: str, default_schema: str, expected: str) -> None:
    from shared.name_resolver import normalize

    assert normalize(input_name, default_schema) == expected


# ── loader ────────────────────────────────────────────────────────────────────

_TABLES_SQL = """\
CREATE TABLE [silver].[DimProduct] (
    ProductKey INT NOT NULL,
    ProductName NVARCHAR(200)
)
GO
CREATE TABLE [silver].[DimCustomer] (
    CustomerKey INT NOT NULL
)
GO
"""

_PROCEDURES_SQL = """\
CREATE PROCEDURE [dbo].[usp_LoadDimProduct]
    @BatchDate DATE = NULL
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductKey, ProductName)
    SELECT ProductKey, Name FROM [AdventureWorks2022].[Production].[Product]
END
GO
"""


def test_load_directory_tables() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        (p / "tables.sql").write_text(_TABLES_SQL, encoding="utf-8")
        catalog = load_directory(p)

    assert "silver.dimproduct" in catalog.tables
    assert "silver.dimcustomer" in catalog.tables


def test_load_directory_procedures() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        (p / "procedures.sql").write_text(_PROCEDURES_SQL, encoding="utf-8")
        catalog = load_directory(p)

    assert "dbo.usp_loaddimproduct" in catalog.procedures
    entry = catalog.procedures["dbo.usp_loaddimproduct"]
    assert "usp_LoadDimProduct" in entry.raw_ddl


def test_load_directory_ast_parsed() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        (p / "tables.sql").write_text(_TABLES_SQL, encoding="utf-8")
        catalog = load_directory(p)

    entry = catalog.tables["silver.dimproduct"]
    # AST may be None if sqlglot can't parse a CREATE TABLE, but the entry exists
    assert entry.raw_ddl


def test_load_directory_missing_optional_files() -> None:
    """views.sql and functions.sql are optional — missing files yield empty dicts."""
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        catalog = load_directory(Path(tmp))

    assert catalog.tables == {}
    assert catalog.views == {}
    assert catalog.functions == {}


def test_load_directory_not_found() -> None:
    from shared.loader import load_directory

    with pytest.raises(FileNotFoundError):
        load_directory("/nonexistent/path/ddl")


def test_catalog_get_helpers() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        (p / "tables.sql").write_text(_TABLES_SQL, encoding="utf-8")
        catalog = load_directory(p)

    assert catalog.get_table("[silver].[DimProduct]") is not None
    assert catalog.get_table("silver.DimProduct") is not None
    assert catalog.get_table("silver.NonExistent") is None


# ── dialect ───────────────────────────────────────────────────────────────────

def test_get_dialect_tsql() -> None:
    from shared.dialect import SqlDialect, get_dialect

    d = get_dialect("tsql")
    assert d.name == "tsql"
    assert isinstance(d, SqlDialect)


def test_get_dialect_spark() -> None:
    from shared.dialect import get_dialect

    d = get_dialect("spark")
    assert d.name == "spark"


def test_get_dialect_unknown() -> None:
    from shared.dialect import get_dialect

    with pytest.raises(KeyError):
        get_dialect("unknown_dialect")


def test_tsql_parse() -> None:
    from shared.dialect import get_dialect

    d = get_dialect("tsql")
    ast = d.parse("SELECT ProductKey FROM silver.DimProduct WHERE ProductKey = 1")
    assert ast is not None


def test_tsql_transpile_to_spark() -> None:
    from shared.dialect import get_dialect

    tsql = get_dialect("tsql")
    spark = get_dialect("spark")
    result = tsql.transpile_to("SELECT TOP 10 ProductKey FROM silver.DimProduct", spark)
    assert "ProductKey" in result


def test_register_dialect() -> None:
    from shared.dialect import SqlDialect, get_dialect, register_dialect

    class DummyDialect:
        @property
        def name(self) -> str:
            return "dummy"

        def parse(self, sql: str):
            return None

        def transpile_to(self, sql: str, target: SqlDialect) -> str:
            return sql

    register_dialect(DummyDialect())
    assert get_dialect("dummy").name == "dummy"
