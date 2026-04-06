"""Tests for ddl_mcp/server.py — semantic analysis and new tools.

Exercises the internal helper functions directly so tests don't need
a running MCP server process.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from shared.loader import load_directory

import server as ddl_server

_SHARED_DIR = Path(__file__).parents[4] / "plugin" / "lib"


def _skip_if_no_oracle() -> None:
    for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
        if not os.environ.get(var):
            pytest.skip(f"{var} not set")


def _git_init(path: Path) -> None:
    subprocess.run(["git", "init", str(path)], capture_output=True, check=True)


def _run_setup_ddl(args: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "setup-ddl", *args],
        cwd=str(_SHARED_DIR),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


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

VIEWS_SQL = """\
CREATE VIEW silver.vw_DimProductSummary AS
SELECT ProductKey, Name FROM silver.DimProduct WHERE Color IS NOT NULL
GO
"""


@pytest.fixture()
def ddl_dir(tmp_path: Path) -> Path:
    """Temp DDL directory with tables, procedures, functions, and views."""
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()
    procs = PROC_WRITER + PROC_READER + PROC_COMMENT_ONLY + PROC_EXEC
    (ddl_dir / "tables.sql").write_text(TABLES_SQL, encoding="utf-8")
    (ddl_dir / "procedures.sql").write_text(procs, encoding="utf-8")
    (ddl_dir / "functions.sql").write_text(FUNCTIONS_SQL, encoding="utf-8")
    (ddl_dir / "views.sql").write_text(VIEWS_SQL, encoding="utf-8")
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


# ── list_views / get_view_body ────────────────────────────────────────────────


def test_list_views_returns_names(ddl_dir: Path) -> None:
    """list_views returns normalized view names from views.sql."""
    catalog = load_directory(ddl_dir)
    assert "silver.vw_dimproductsummary" in catalog.views


def test_list_views_absent_file(ddl_dir_no_functions: Path) -> None:
    """list_views returns empty dict when views.sql is absent."""
    catalog = load_directory(ddl_dir_no_functions)
    assert catalog.views == {}


def test_get_view_body_returns_ddl(ddl_dir: Path) -> None:
    """get_view_body returns the raw DDL for a known view."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_view("silver.vw_DimProductSummary")
    assert entry is not None
    assert "vw_DimProductSummary" in entry.raw_ddl


def test_get_view_body_unknown(ddl_dir: Path) -> None:
    """get_view_body returns None for an unknown view name."""
    catalog = load_directory(ddl_dir)
    entry = catalog.get_view("silver.vw_DoesNotExist")
    assert entry is None


# ── Oracle dialect — column type rendering ────────────────────────────────────

ORACLE_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "oracle"


@pytest.fixture()
def oracle_ddl_dir() -> Path:
    """Path to the Oracle SH-schema fixture project root."""
    return ORACLE_FIXTURE_DIR


def test_parse_columns_oracle_varchar2(oracle_ddl_dir: Path) -> None:
    """VARCHAR2 column type is rendered as VARCHAR2 under the oracle dialect."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_table("SH.CUSTOMERS")
    assert entry is not None

    cols = {c["name"]: c for c in ddl_server._parse_columns(entry, dialect="oracle")}
    assert cols["CUST_FIRST_NAME"]["type"] == "VARCHAR2(20)"


def test_parse_columns_oracle_number(oracle_ddl_dir: Path) -> None:
    """NUMBER column type is rendered as NUMBER under the oracle dialect."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_table("SH.CUSTOMERS")
    assert entry is not None

    cols = {c["name"]: c for c in ddl_server._parse_columns(entry, dialect="oracle")}
    assert cols["CUST_ID"]["type"] == "NUMBER"


def test_parse_columns_oracle_char(oracle_ddl_dir: Path) -> None:
    """CHAR column type is rendered as CHAR(1) under the oracle dialect."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_table("SH.CUSTOMERS")
    assert entry is not None

    cols = {c["name"]: c for c in ddl_server._parse_columns(entry, dialect="oracle")}
    assert cols["CUST_GENDER"]["type"] == "CHAR(1)"


def test_parse_columns_oracle_no_tsql_types(oracle_ddl_dir: Path) -> None:
    """Oracle column types contain no T-SQL-specific type names."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_table("SH.CUSTOMERS")
    assert entry is not None

    tsql_types = {"NVARCHAR", "INT", "BIGINT", "BIT", "UNIQUEIDENTIFIER"}
    cols = ddl_server._parse_columns(entry, dialect="oracle")
    rendered_types = {c["type"] for c in cols}
    assert tsql_types.isdisjoint(rendered_types)


# ── Oracle dialect — integration tests (require Docker Oracle + SH schema) ────


@pytest.mark.oracle
def test_oracle_list_procedures(oracle_ddl_dir: Path) -> None:
    """list_procedures returns the SH.GET_PRODUCT_COUNT procedure from fixtures."""
    catalog = load_directory(oracle_ddl_dir)
    assert "sh.get_product_count" in catalog.procedures


@pytest.mark.oracle
def test_oracle_get_procedure_body(oracle_ddl_dir: Path) -> None:
    """get_procedure_body returns the raw DDL for the SH procedure."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_procedure("SH.GET_PRODUCT_COUNT")
    assert entry is not None
    assert "GET_PRODUCT_COUNT" in entry.raw_ddl


@pytest.mark.oracle
def test_oracle_get_table_schema_column_types(oracle_ddl_dir: Path) -> None:
    """get_table_schema for an Oracle project returns correct Oracle column types."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_table("SH.CUSTOMERS")
    assert entry is not None

    cols = {c["name"]: c for c in ddl_server._parse_columns(entry, dialect="oracle")}
    assert cols["CUST_FIRST_NAME"]["type"] == "VARCHAR2(20)"
    assert cols["CUST_ID"]["type"] == "NUMBER"
    assert cols["CUST_GENDER"]["type"] == "CHAR(1)"


# ── Oracle DDL loading — CREATE OR REPLACE + double-quoted names ──────────────


def test_oracle_or_replace_procedure_is_indexed(oracle_ddl_dir: Path) -> None:
    """CREATE OR REPLACE PROCEDURE "SH"."name" is indexed under the plain key."""
    catalog = load_directory(oracle_ddl_dir)
    assert "sh.get_product_count" in catalog.procedures


def test_oracle_or_replace_view_is_indexed(oracle_ddl_dir: Path) -> None:
    """CREATE OR REPLACE VIEW SH.name is indexed under the normalized key."""
    catalog = load_directory(oracle_ddl_dir)
    assert "sh.profits" in catalog.views


def test_oracle_get_view_body_returns_ddl(oracle_ddl_dir: Path) -> None:
    """get_view_body returns the raw DDL for an Oracle view."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_view("SH.PROFITS")
    assert entry is not None
    assert "PROFITS" in entry.raw_ddl


def test_oracle_double_quoted_name_lookup(oracle_ddl_dir: Path) -> None:
    """Procedure stored via double-quoted DDL is retrievable by plain name."""
    catalog = load_directory(oracle_ddl_dir)
    entry = catalog.get_procedure("SH.GET_PRODUCT_COUNT")
    assert entry is not None
    assert "GET_PRODUCT_COUNT" in entry.raw_ddl


# ── Oracle integration tests — require live Docker Oracle with SH schema ──────


@pytest.mark.oracle
class TestOracleLiveIntegration:
    """Integration tests against DDL extracted from the live Oracle SH schema.

    Requires ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN to be set and the
    SH schema to be present (standard Oracle sample schema).
    """

    def _extract_sh(self, tmp_path: Path) -> None:
        """Write partial manifest and run setup-ddl extract for SH schema."""
        (tmp_path / "manifest.json").write_text(
            json.dumps({"technology": "oracle", "dialect": "oracle"}),
            encoding="utf-8",
        )
        result = _run_setup_ddl([
            "extract", "--schemas", "SH", "--project-root", str(tmp_path),
        ])
        assert result.returncode == 0, f"setup-ddl extract failed: {result.stderr}"

    def test_get_table_schema_oracle_column_types(self, tmp_path: Path) -> None:
        """get_table_schema returns correct Oracle column types for real SH tables."""
        _skip_if_no_oracle()
        _git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        entry = catalog.get_table("SH.CUSTOMERS")
        assert entry is not None, "SH.CUSTOMERS not found in extracted DDL"

        cols = {c["name"].upper(): c for c in ddl_server._parse_columns(entry, dialect="oracle")}
        assert any(c["type"].startswith("VARCHAR2") for c in cols.values()), \
            "Expected VARCHAR2 columns in SH.CUSTOMERS"
        assert any(c["type"] == "NUMBER" or c["type"].startswith("NUMBER(") for c in cols.values()), \
            "Expected NUMBER columns in SH.CUSTOMERS"

    def test_list_procedures_returns_sh_procedures(self, tmp_path: Path) -> None:
        """list_procedures returns SH schema procedures from extracted DDL."""
        _skip_if_no_oracle()
        _git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        sh_procs = [k for k in catalog.procedures if k.startswith("sh.")]
        assert len(sh_procs) > 0, "No SH procedures found in extracted DDL"

    def test_get_procedure_body_returns_oracle_ddl(self, tmp_path: Path) -> None:
        """get_procedure_body returns non-empty DDL for an SH procedure."""
        _skip_if_no_oracle()
        _git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        sh_procs = [k for k in catalog.procedures if k.startswith("sh.")]
        assert sh_procs, "No SH procedures found"

        entry = catalog.procedures[sh_procs[0]]
        assert entry.raw_ddl.strip(), "Procedure body is empty"
