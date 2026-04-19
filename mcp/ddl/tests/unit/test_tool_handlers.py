"""Tests for DDL MCP tool dispatch handlers."""

import json
import logging
from pathlib import Path

import pytest

from ddl_mcp_support.loader import DdlEntry, load_directory
from ddl_mcp_support.tool_handlers import handle_tool_call


TABLES_SQL = """\
CREATE TABLE silver.DimProduct (
    ProductKey INT NOT NULL,
    Name NVARCHAR(100) NOT NULL,
    Color NVARCHAR(50) NULL,
    CONSTRAINT PK_DimProduct PRIMARY KEY (ProductKey)
)
GO
"""

PROC_WRITER = """\
CREATE PROCEDURE silver.usp_load_DimProduct
AS
BEGIN
    INSERT INTO silver.DimProduct (ProductKey, Name, Color)
    SELECT ProductKey, Name, Color FROM bronze.StageProduct
END
GO
"""

PROC_READER = """\
CREATE PROCEDURE silver.usp_read_DimProduct
AS
BEGIN
    SELECT ProductKey, Name FROM silver.DimProduct WHERE Color = 'Red'
END
GO
"""


class StaticContext:
    def __init__(self, project_root: Path, dialect: str = "tsql") -> None:
        self.project_root = project_root
        self._catalog = load_directory(project_root, dialect=dialect)
        self._dialect = dialect

    def catalog(self):
        return self._catalog

    def catalog_dialect(self) -> str:
        return self._dialect


@pytest.fixture()
def ddl_project(tmp_path: Path) -> Path:
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "tables.sql").write_text(TABLES_SQL, encoding="utf-8")
    (ddl_dir / "procedures.sql").write_text(PROC_WRITER + PROC_READER, encoding="utf-8")
    return tmp_path


def test_handle_list_tables_returns_sorted_table_names(ddl_project: Path) -> None:
    result = handle_tool_call("list_tables", {}, StaticContext(ddl_project))

    assert result[0].text == "silver.dimproduct"


def test_handle_get_table_schema_returns_ddl_and_columns(ddl_project: Path) -> None:
    result = handle_tool_call(
        "get_table_schema",
        {"name": "silver.DimProduct"},
        StaticContext(ddl_project),
    )

    payload = json.loads(result[0].text)
    assert "DimProduct" in payload["ddl"]
    columns = {column["name"]: column for column in payload["columns"]}
    assert set(columns) == {"ProductKey", "Name", "Color"}
    assert columns["ProductKey"]["nullable"] is False
    assert columns["Name"]["nullable"] is False
    assert columns["Color"]["nullable"] is True


def test_handle_get_dependencies_returns_readers_and_writers(ddl_project: Path) -> None:
    result = handle_tool_call(
        "get_dependencies",
        {"table_name": "silver.DimProduct"},
        StaticContext(ddl_project),
    )

    assert result[0].text == "silver.usp_load_dimproduct\nsilver.usp_read_dimproduct"


def test_handle_get_table_schema_reports_missing_name(ddl_project: Path) -> None:
    result = handle_tool_call("get_table_schema", {}, StaticContext(ddl_project))

    assert result[0].text == "Missing required argument: name"


def test_handle_get_dependencies_logs_skipped_procedures(
    ddl_project: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    context = StaticContext(ddl_project)
    context.catalog().procedures["silver.usp_bad_parse"] = DdlEntry(
        raw_ddl="CREATE PROCEDURE silver.usp_bad_parse AS",
        ast=None,
        parse_error="synthetic parse failure",
    )

    with caplog.at_level(logging.WARNING):
        result = handle_tool_call(
            "get_dependencies",
            {"table_name": "silver.DimProduct"},
            context,
        )

    assert result[0].text == "silver.usp_load_dimproduct\nsilver.usp_read_dimproduct"
    assert any(
        record.levelname == "WARNING"
        and "skip_procedure" in record.message
        and "silver.usp_bad_parse" in record.message
        and "silver.DimProduct" in record.message
        for record in caplog.records
    )
