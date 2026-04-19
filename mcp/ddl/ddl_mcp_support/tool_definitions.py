"""MCP tool schema declarations for the DDL server."""

from __future__ import annotations

from mcp import types


_DDL_PATH_SCHEMA: dict = {}


def tool_definitions() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_tables",
            description=(
                "List all table names (schema.name) found in the DDL directory."
            ),
            inputSchema={"type": "object", "properties": {**_DDL_PATH_SCHEMA}},
        ),
        types.Tool(
            name="get_table_schema",
            description=(
                "Return CREATE TABLE DDL and parsed column metadata as JSON. "
                "Response fields: ddl (raw DDL string), columns (list of "
                "{name, type, nullable, is_pk})."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Schema-qualified name, e.g. silver.DimProduct",
                    },
                    **_DDL_PATH_SCHEMA,
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="list_procedures",
            description=(
                "List all stored procedure names (schema.name) found in the DDL directory."
            ),
            inputSchema={"type": "object", "properties": {**_DDL_PATH_SCHEMA}},
        ),
        types.Tool(
            name="get_procedure_body",
            description="Return the CREATE PROCEDURE DDL for a specific stored procedure.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Schema-qualified name, e.g. silver.usp_load_DimProduct",
                    },
                    **_DDL_PATH_SCHEMA,
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="get_dependencies",
            description=(
                "Find all stored procedures whose body contains an AST-level reference "
                "to the given table (reads or writes). Uses sqlglot AST analysis - "
                "procedures whose bodies cannot be parsed (EXEC, MERGE, complex IF/ELSE) "
                "are excluded from results. "
                "Returns a newline-separated list of schema-qualified procedure names, "
                "or '(none)' if no references are found."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Schema-qualified table name, e.g. silver.DimProduct",
                    },
                    **_DDL_PATH_SCHEMA,
                },
                "required": ["table_name"],
            },
        ),
        types.Tool(
            name="list_views",
            description=(
                "List all view names (schema.name) found in the DDL directory. "
                "Returns '(none)' when no views are found."
            ),
            inputSchema={"type": "object", "properties": {**_DDL_PATH_SCHEMA}},
        ),
        types.Tool(
            name="get_view_body",
            description="Return the CREATE VIEW DDL for a specific view.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Schema-qualified name, e.g. silver.vw_DimDate",
                    },
                    **_DDL_PATH_SCHEMA,
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="list_functions",
            description=(
                "List all function names (schema.name) found in the DDL directory. "
                "Returns '(none)' when no functions are found."
            ),
            inputSchema={"type": "object", "properties": {**_DDL_PATH_SCHEMA}},
        ),
        types.Tool(
            name="get_function_body",
            description="Return the CREATE FUNCTION DDL for a specific function.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Schema-qualified name, e.g. dbo.fnGetDate",
                    },
                    **_DDL_PATH_SCHEMA,
                },
                "required": ["name"],
            },
        ),
    ]
