"""Tests for ddl_mcp/server.py — semantic analysis and new tools.

Exercises the internal helper functions directly so tests don't need
a running MCP server process.
"""

import json
from pathlib import Path

import pytest

from shared.loader import load_directory

import server as ddl_server


# ── Fixtures ──────────────────────────────────────────────────────────────────

TABLES_SQL = """\
CREATE TABLE silver.DimProduct (
    ProductKey INT NOT NULL,
    Name NVARCHAR(100) NOT NULL,
    Color NVARCHAR(50) NULL,
    CONSTRAINT PK_DimProduct PRIMARY KEY (ProductKey)
)
GO
"""

# usp_load_DimProduct: writes to silver.DimProduct (INSERT target)
PROC_WRITER = """\
CREATE PROCEDURE silver.usp_load_DimProduct
AS
BEGIN
    INSERT INTO silver.DimProduct (ProductKey, Name, Color)
    SELECT ProductKey, Name, Color FROM bronze.StageProduct
END
GO
"""

# usp_read_DimProduct: reads from silver.DimProduct (SELECT source)
PROC_READER = """\
CREATE PROCEDURE silver.usp_read_DimProduct
AS
BEGIN
    SELECT ProductKey, Name FROM silver.DimProduct WHERE Color = 'Red'
END
GO
"""

# usp_comment_only: mentions silver.DimProduct only in a comment — no AST ref
PROC_COMMENT_ONLY = """\
CREATE PROCEDURE silver.usp_comment_only
AS
BEGIN
    -- This proc used to load silver.DimProduct but has been replaced
    SELECT 1
END
GO
"""

# usp_exec: EXEC causes parse error — should be skipped gracefully
PROC_EXEC = """\
CREATE PROCEDURE silver.usp_exec
AS
BEGIN
    EXEC silver.usp_load_DimProduct
END
GO
"""

FUNCTIONS_SQL = """\
CREATE FUNCTION dbo.fnGetDate()
RETURNS DATE
AS
BEGIN
    RETURN CAST(GETDATE() AS DATE)
END
GO
"""


@pytest.fixture()
def ddl_dir(tmp_path: Path) -> Path:
    """Temp DDL directory with tables, procedures, and functions."""
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()
    procs = PROC_WRITER + PROC_READER + PROC_COMMENT_ONLY + PROC_EXEC
    (ddl_dir / "tables.sql").write_text(TABLES_SQL, encoding="utf-8")
    (ddl_dir / "procedures.sql").write_text(procs, encoding="utf-8")
    (ddl_dir / "functions.sql").write_text(FUNCTIONS_SQL, encoding="utf-8")
    return tmp_path


@pytest.fixture()
def ddl_dir_no_functions(tmp_path: Path) -> Path:
    """Temp DDL directory without functions.sql."""
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "tables.sql").write_text(TABLES_SQL, encoding="utf-8")
    (ddl_dir / "procedures.sql").write_text(PROC_WRITER, encoding="utf-8")
    return tmp_path


# ── get_dependencies — AST semantics ─────────────────────────────────────────

def test_get_dependencies_includes_writers(ddl_dir: Path) -> None:
    """Procedure that INSERTs into the target table is returned."""
    catalog = load_directory(ddl_dir)
    from shared.loader import DdlParseError, extract_refs
    from shared.name_resolver import normalize

    target = normalize("silver.DimProduct")
    matches = []
    for proc_name, entry in catalog.procedures.items():
        try:
            refs = extract_refs(entry)
            if target in refs.reads_from or target in refs.writes_to:
                matches.append(proc_name)
        except DdlParseError:
            pass

    assert "silver.usp_load_dimproduct" in matches


def test_get_dependencies_includes_readers(ddl_dir: Path) -> None:
    """Procedure that SELECTs from the target table is returned."""
    catalog = load_directory(ddl_dir)
    from shared.loader import DdlParseError, extract_refs
    from shared.name_resolver import normalize

    target = normalize("silver.DimProduct")
    matches = []
    for proc_name, entry in catalog.procedures.items():
        try:
            refs = extract_refs(entry)
            if target in refs.reads_from or target in refs.writes_to:
                matches.append(proc_name)
        except DdlParseError:
            pass

    assert "silver.usp_read_dimproduct" in matches


def test_get_dependencies_ast_excludes_comment_only(ddl_dir: Path) -> None:
    """Procedure with table name only in a SQL comment is excluded by AST analysis.

    Text grep would match this proc; AST walk correctly excludes it.
    """
    catalog = load_directory(ddl_dir)
    from shared.loader import DdlParseError, extract_refs
    from shared.name_resolver import normalize

    target = normalize("silver.DimProduct")
    matches = []
    for proc_name, entry in catalog.procedures.items():
        try:
            refs = extract_refs(entry)
            if target in refs.reads_from or target in refs.writes_to:
                matches.append(proc_name)
        except DdlParseError:
            pass

    assert "silver.usp_comment_only" not in matches


def test_get_dependencies_exec_procs_need_enrich(ddl_dir: Path) -> None:
    """Procedures with static EXEC stay deterministic and request enrichment."""
    catalog = load_directory(ddl_dir)
    from shared.loader import extract_refs
    from shared.name_resolver import normalize

    exec_proc = normalize("silver.usp_exec")
    entry = catalog.procedures.get(exec_proc)
    assert entry is not None
    refs = extract_refs(entry)
    assert refs.needs_llm is False
    assert refs.writes_to == []
    assert refs.reads_from == []


# ── get_table_schema — structured JSON ───────────────────────────────────────

def test_get_table_schema_returns_json(ddl_dir: Path) -> None:
    """get_table_schema returns JSON with ddl and columns keys."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_table("silver.DimProduct")
    assert entry is not None

    result = json.loads(json.dumps({
        "ddl": entry.raw_ddl,
        "columns": ddl_server._parse_columns(entry),
    }))

    assert "ddl" in result
    assert "columns" in result
    assert "DimProduct" in result["ddl"]


def test_get_table_schema_column_count(ddl_dir: Path) -> None:
    """Column list has the expected number of entries."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_table("silver.DimProduct")
    assert entry is not None

    cols = ddl_server._parse_columns(entry)
    assert len(cols) == 3


def test_get_table_schema_column_names(ddl_dir: Path) -> None:
    """Column names are correctly extracted."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_table("silver.DimProduct")
    assert entry is not None

    names = [c["name"] for c in ddl_server._parse_columns(entry)]
    assert "ProductKey" in names
    assert "Name" in names
    assert "Color" in names


def test_get_table_schema_nullable_column(ddl_dir: Path) -> None:
    """Color (NULL) is nullable; Name (NOT NULL) is not."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_table("silver.DimProduct")
    assert entry is not None

    cols = {c["name"]: c for c in ddl_server._parse_columns(entry)}
    assert cols["Color"]["nullable"] is True
    assert cols["Name"]["nullable"] is False


# ── list_functions / get_function_body ───────────────────────────────────────

def test_list_functions_returns_names(ddl_dir: Path) -> None:
    """list_functions returns normalized function names from functions.sql."""
    catalog = load_directory(ddl_dir)
    assert "dbo.fngetdate" in catalog.functions


def test_list_functions_absent_file(ddl_dir_no_functions: Path) -> None:
    """list_functions returns empty dict when functions.sql is absent."""
    catalog = load_directory(ddl_dir_no_functions)
    assert catalog.functions == {}


def test_get_function_body_returns_ddl(ddl_dir: Path) -> None:
    """get_function_body returns the raw DDL for a known function."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_function("dbo.fnGetDate")
    assert entry is not None
    assert "fnGetDate" in entry.raw_ddl


def test_get_function_body_unknown(ddl_dir: Path) -> None:
    """get_function_body returns None for an unknown function name."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_function("dbo.fnDoesNotExist")
    assert entry is None
