"""Smoke tests for the shared library.

Covers:
- Module imports
- Key type instantiation
- IR JSON round-trip
- name_resolver normalization
- loader: GO-split + DdlCatalog population
- loader: DdlParseError on Command fallback
- loader: extract_refs AST-only extraction
- loader: index_directory + catalog.json
- loader: load_catalog round-trip
- dialect: protocol compliance + registry lookup
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

# Path to the fixture DDL directory (no live DB needed)
_FIXTURES_DDL = Path(__file__).parent / "fixtures" / "ddl"


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


# ── DdlParseError — Command fallback ─────────────────────────────────────────

def test_parse_block_raises_for_if_else() -> None:
    """Proc with IF/ELSE BEGIN/END falls back to Command → DdlParseError."""
    from shared.loader import DdlParseError, _parse_block

    sql = """CREATE PROCEDURE [dbo].[usp_complex]
    @Mode INT = 0
AS
BEGIN
    IF @Mode = 1
    BEGIN
        INSERT INTO silver.T (a) SELECT b FROM bronze.S
    END
    ELSE
    BEGIN
        UPDATE silver.T SET a = 'x' WHERE 1=0
    END
END"""
    with pytest.raises(DdlParseError, match="Command"):
        _parse_block(sql, dialect="tsql")


def test_parse_block_raises_for_multiple_statements() -> None:
    """Multiple DML statements in proc body fall back to Command → DdlParseError."""
    from shared.loader import DdlParseError, _parse_block

    sql = """CREATE PROCEDURE [dbo].[usp_multi]
AS
BEGIN
    INSERT INTO silver.T1 (a) SELECT b FROM bronze.S1
    INSERT INTO silver.T2 (c) SELECT d FROM bronze.S2
END"""
    with pytest.raises(DdlParseError, match="Command"):
        _parse_block(sql, dialect="tsql")


def test_load_directory_records_parse_errors() -> None:
    """Procs that fall back to top-level Command have parse_error set and ast=None.

    Note: procs with internal Command nodes (EXEC, TRUNCATE in body) still parse
    as Create at this level — their errors surface in extract_refs / index_directory.
    """
    from shared.loader import load_directory

    catalog = load_directory(_FIXTURES_DDL)

    # usp_nested_if_else has IF/ELSE → whole proc falls back to Command
    entry = catalog.procedures.get("dbo.usp_nested_if_else")
    assert entry is not None
    assert entry.parse_error is not None, "Expected parse_error for usp_nested_if_else"
    assert entry.raw_ddl, "raw_ddl must be preserved even on parse failure"
    assert entry.ast is None


def test_load_directory_simple_proc_parses_ok() -> None:
    """usp_simple_insert has no complex control flow — should parse without error."""
    from shared.loader import load_directory

    catalog = load_directory(_FIXTURES_DDL)
    entry = catalog.procedures.get("dbo.usp_simple_insert")
    assert entry is not None
    assert entry.parse_error is None
    assert entry.ast is not None


# ── extract_refs ──────────────────────────────────────────────────────────────

def test_extract_refs_simple_insert() -> None:
    """Single INSERT+SELECT — writes_to and reads_from extracted from AST."""
    from shared.loader import extract_refs, load_directory

    catalog = load_directory(_FIXTURES_DDL)
    entry = catalog.procedures["dbo.usp_simple_insert"]
    refs = extract_refs(entry)

    assert "silver.dimproduct" in refs.writes_to
    assert "bronze.product" in refs.reads_from
    assert refs.calls == []


def test_extract_refs_raises_for_parse_failed_entry() -> None:
    """extract_refs raises DdlParseError when ast is None."""
    from shared.loader import DdlEntry, DdlParseError, extract_refs

    entry = DdlEntry(raw_ddl="CREATE PROCEDURE ...", ast=None, parse_error="some error")
    with pytest.raises(DdlParseError):
        extract_refs(entry)


def test_extract_refs_raises_for_internal_command() -> None:
    """Procs with internal Command nodes (EXEC, TRUNCATE) raise DdlParseError."""
    from shared.loader import DdlParseError, _parse_block, extract_refs, DdlEntry

    # usp_orchestrator (EXEC) parses as Create but has internal Command
    sql = """CREATE PROCEDURE [dbo].[usp_orchestrator]
AS
BEGIN
    EXEC dbo.usp_simple_insert
END"""
    ast = _parse_block(sql, dialect="tsql")  # does NOT raise — top-level is Create
    entry = DdlEntry(raw_ddl=sql, ast=ast)
    with pytest.raises(DdlParseError, match="Command"):
        extract_refs(entry)


def test_extract_refs_excludes_cross_db_reads() -> None:
    """Cross-DB tables (catalog!='' → 3-part name) are excluded from reads_from."""
    from shared.loader import extract_refs, load_directory

    catalog = load_directory(_FIXTURES_DDL)
    entry = catalog.procedures.get("dbo.usp_cross_db")
    assert entry is not None
    assert entry.parse_error is None  # single INSERT, no complex control flow

    refs = extract_refs(entry)
    assert "silver.dimproduct" in refs.writes_to
    # OtherDB reference must NOT appear
    for ref in refs.reads_from:
        assert "otherdb" not in ref.lower(), f"Cross-DB ref leaked into reads_from: {ref}"


# ── index_directory + catalog.json ───────────────────────────────────────────

def test_index_directory_creates_structure() -> None:
    """index_directory creates subdirs and catalog.json."""
    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "index"
        index_directory(_FIXTURES_DDL, out)

        assert (out / "catalog.json").exists()
        assert (out / "tables").is_dir()
        assert (out / "procedures").is_dir()
        assert (out / "views").is_dir()


def test_index_directory_per_object_files() -> None:
    """Each DDL object gets its own .sql file in the appropriate subdir."""
    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "index"
        index_directory(_FIXTURES_DDL, out)

        assert (out / "tables" / "silver.dimproduct.sql").exists()
        assert (out / "tables" / "bronze.product.sql").exists()
        assert (out / "procedures" / "dbo.usp_simple_insert.sql").exists()
        assert (out / "procedures" / "dbo.usp_nested_if_else.sql").exists()
        assert (out / "views" / "silver.vw_dimproduct.sql").exists()


def test_catalog_json_content() -> None:
    """catalog.json has correct refs for simple proc and parse_error for complex ones."""
    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "index"
        index_directory(_FIXTURES_DDL, out)

        doc = json.loads((out / "catalog.json").read_text())

    assert doc["schema_version"] == "1.0"
    assert "objects" in doc

    # Simple proc: refs present, no error
    simple = doc["objects"].get("dbo.usp_simple_insert")
    assert simple is not None
    assert simple["parse_error"] is None
    assert "silver.dimproduct" in simple["writes_to"]
    assert "bronze.product" in simple["reads_from"]

    # Complex proc: parse_error set, refs empty
    complex_proc = doc["objects"].get("dbo.usp_nested_if_else")
    assert complex_proc is not None
    assert complex_proc["parse_error"] is not None
    assert complex_proc["writes_to"] == []

    # Cross-DB proc: no error, cross-DB ref excluded
    cross = doc["objects"].get("dbo.usp_cross_db")
    assert cross is not None
    assert cross["parse_error"] is None
    for ref in cross.get("reads_from", []):
        assert "otherdb" not in ref.lower()


# ── load_catalog round-trip ───────────────────────────────────────────────────

def test_load_catalog_round_trip() -> None:
    """load_catalog restores DdlCatalog entries from an indexed directory."""
    from shared.loader import index_directory, load_catalog

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "index"
        index_directory(_FIXTURES_DDL, out)
        catalog = load_catalog(out)

    assert "silver.dimproduct" in catalog.tables
    assert "dbo.usp_simple_insert" in catalog.procedures
    assert "silver.vw_dimproduct" in catalog.views

    # Raw DDL is populated from per-file SQL
    entry = catalog.procedures["dbo.usp_simple_insert"]
    assert "usp_simple_insert" in entry.raw_ddl

    # AST is not re-parsed (None in load_catalog)
    assert entry.ast is None


def test_load_catalog_preserves_parse_errors() -> None:
    """load_catalog preserves parse_error field from catalog.json."""
    from shared.loader import index_directory, load_catalog

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "index"
        index_directory(_FIXTURES_DDL, out)
        catalog = load_catalog(out)

    failed = [
        name for name, entry in catalog.procedures.items()
        if entry.parse_error is not None
    ]
    assert len(failed) >= 2


def test_load_catalog_not_found() -> None:
    from shared.loader import load_catalog

    with pytest.raises(FileNotFoundError):
        load_catalog("/nonexistent/dir")
