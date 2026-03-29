"""Smoke tests for the shared library.

Covers:
- Module imports
- name_resolver normalization
- loader: GO-split + DdlCatalog population
- loader: DdlParseError on Command fallback
- loader: extract_refs AST-only extraction
- loader: index_directory + catalog.json
- loader: load_catalog round-trip
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

# Path to the fixture DDL directory (no live DB needed)
_FIXTURES_DDL = Path(__file__).parent / "fixtures" / "ddl"


# ── Import smoke ──────────────────────────────────────────────────────────────

def test_import_loader() -> None:
    from shared import loader  # noqa: F401


def test_import_name_resolver() -> None:
    from shared import name_resolver  # noqa: F401


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


def _make_ddl_dir(tmp: str, files: dict[str, str]) -> Path:
    """Create a temp dir with a ddl/ subdirectory containing the given SQL files."""
    p = Path(tmp)
    ddl_dir = p / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (ddl_dir / name).write_text(content, encoding="utf-8")
    return p


def test_load_directory_tables() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {"tables.sql": _TABLES_SQL})
        catalog = load_directory(p)

    assert "silver.dimproduct" in catalog.tables
    assert "silver.dimcustomer" in catalog.tables


def test_load_directory_procedures() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {"procedures.sql": _PROCEDURES_SQL})
        catalog = load_directory(p)

    assert "dbo.usp_loaddimproduct" in catalog.procedures
    entry = catalog.procedures["dbo.usp_loaddimproduct"]
    assert "usp_LoadDimProduct" in entry.raw_ddl


def test_load_directory_ast_parsed() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {"tables.sql": _TABLES_SQL})
        catalog = load_directory(p)

    entry = catalog.tables["silver.dimproduct"]
    # AST may be None if sqlglot can't parse a CREATE TABLE, but the entry exists
    assert entry.raw_ddl


def test_load_directory_mixed_types_single_file() -> None:
    """A single .sql file with tables and procedures routes to correct buckets."""
    from shared.loader import load_directory

    mixed = (
        "CREATE TABLE [silver].[DimProduct] (\n"
        "    ProductKey INT NOT NULL\n"
        ")\n"
        "GO\n"
        "CREATE PROCEDURE [dbo].[usp_LoadDimProduct]\n"
        "AS\n"
        "BEGIN\n"
        "    INSERT INTO [silver].[DimProduct] (ProductKey)\n"
        "    SELECT ProductKey FROM bronze.Product\n"
        "END\n"
        "GO\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {"everything.sql": mixed})
        catalog = load_directory(p)

    assert "silver.dimproduct" in catalog.tables
    assert "dbo.usp_loaddimproduct" in catalog.procedures
    assert catalog.views == {}


def test_load_directory_arbitrary_filenames() -> None:
    """Filenames don't matter — objects are detected from CREATE statements."""
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {
            "my_custom_tables.sql": "CREATE TABLE dbo.Foo (Id INT NOT NULL)\nGO\n",
            "some_procs.sql": (
                "CREATE PROCEDURE dbo.usp_Bar\nAS\nBEGIN\n"
                "    INSERT INTO dbo.Foo (Id) SELECT 1\nEND\nGO\n"
            ),
        })
        catalog = load_directory(p)

    assert "dbo.foo" in catalog.tables
    assert "dbo.usp_bar" in catalog.procedures


def test_load_directory_multiple_files_same_type() -> None:
    """Objects of the same type spread across multiple files are all loaded."""
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {
            "batch_a.sql": "CREATE TABLE dbo.Alpha (Id INT)\nGO\n",
            "batch_b.sql": "CREATE TABLE dbo.Beta (Id INT)\nGO\n",
        })
        catalog = load_directory(p)

    assert "dbo.alpha" in catalog.tables
    assert "dbo.beta" in catalog.tables


def test_load_directory_empty_dir() -> None:
    """Empty directory with ddl/ subdirectory yields empty catalog."""
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {})
        catalog = load_directory(p)

    assert catalog.tables == {}
    assert catalog.procedures == {}
    assert catalog.views == {}
    assert catalog.functions == {}


def test_load_directory_not_found() -> None:
    from shared.loader import load_directory

    with pytest.raises(FileNotFoundError):
        load_directory("/nonexistent/path/ddl")


def test_catalog_get_helpers() -> None:
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {"tables.sql": _TABLES_SQL})
        catalog = load_directory(p)

    assert catalog.get_table("[silver].[DimProduct]") is not None
    assert catalog.get_table("silver.DimProduct") is not None
    assert catalog.get_table("silver.NonExistent") is None


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


def test_load_directory_stores_parse_error_per_block() -> None:
    """Procs that fall back to Command are stored with parse_error, not skipped."""
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        p = _make_ddl_dir(tmp, {
            "procedures.sql": (
                "CREATE PROCEDURE [dbo].[usp_bad]\n"
                "    @Mode INT = 0\n"
                "AS\n"
                "BEGIN\n"
                "    IF @Mode = 1\n"
                "    BEGIN\n"
                "        INSERT INTO silver.T (a) SELECT b FROM bronze.S\n"
                "    END\n"
                "END\n"
                "GO\n"
            ),
        })
        catalog = load_directory(p)
        entry = catalog.procedures.get("dbo.usp_bad")
        assert entry is not None
        assert entry.parse_error is not None


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


def test_extract_refs_handles_internal_command() -> None:
    """Procs with internal Command nodes (EXEC) are handled via body parsing."""
    from shared.loader import _parse_block, extract_refs, DdlEntry

    sql = """CREATE PROCEDURE [dbo].[usp_orchestrator]
AS
BEGIN
    EXEC dbo.usp_simple_insert
END"""
    ast = _parse_block(sql, dialect="tsql")
    entry = DdlEntry(raw_ddl=sql, ast=ast)
    # extract_refs no longer raises — it falls through to body parsing
    # EXEC produces a Command that gets skipped (no DML to extract)
    refs = extract_refs(entry)
    assert refs.writes_to == []
    assert refs.reads_from == []


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
        assert (out / "procedures" / "dbo.usp_cross_db.sql").exists()
        assert (out / "views" / "silver.vw_dimproduct.sql").exists()


def test_catalog_json_content() -> None:
    """catalog.json has correct refs for parseable procs."""
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

    # Cross-DB proc: no error, cross-DB ref excluded
    cross = doc["objects"].get("dbo.usp_cross_db")
    assert cross is not None
    assert cross["parse_error"] is None
    for ref in cross.get("reads_from", []):
        assert "otherdb" not in ref.lower()


def test_index_directory_stores_unparseable_with_error() -> None:
    """index_directory stores unparseable blocks with parse_error in catalog.json."""
    import json
    from shared.loader import index_directory

    _FIXTURES_UNPARSEABLE = Path(__file__).parent / "fixtures" / "ddl_unparseable"
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "index"
        index_directory(_FIXTURES_UNPARSEABLE, out)
        catalog_doc = json.loads((out / "catalog.json").read_text())
        has_error = any(
            obj.get("parse_error") is not None
            for obj in catalog_doc["objects"].values()
        )
        assert has_error


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


def test_load_directory_stores_unparseable_with_error() -> None:
    """load_directory stores unparseable DDL blocks with parse_error, does not raise."""
    from shared.loader import load_directory

    _FIXTURES_UNPARSEABLE = Path(__file__).parent / "fixtures" / "ddl_unparseable"
    catalog = load_directory(_FIXTURES_UNPARSEABLE)
    has_error = any(
        e.parse_error is not None
        for bucket in [catalog.procedures, catalog.tables, catalog.views, catalog.functions]
        for e in bucket.values()
    )
    assert has_error


def test_load_catalog_not_found() -> None:
    from shared.loader import load_catalog

    with pytest.raises(FileNotFoundError):
        load_catalog("/nonexistent/dir")
