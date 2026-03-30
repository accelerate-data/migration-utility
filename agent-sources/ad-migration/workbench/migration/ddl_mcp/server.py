# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "mcp>=1.0",
#   "shared",
# ]
#
# [tool.uv.sources]
# shared = { path = "../shared" }
# ///
"""DDL file MCP server.

Reads DDL files extracted by the dacpac-extract sidecar and exposes them
as MCP tools for use by all analysis agents (scoping, profiling, etc.).

Usage (stdio mode):
    uv run server.py

DDL path resolution order (first wins):
    1. `ddl_path` parameter passed in the tool call arguments
    2. `DDL_PATH` environment variable

The resolved path must point to a directory containing one or more .sql files
with GO-delimited CREATE statements.  Object types (table, procedure, view,
function) are auto-detected from the DDL — filenames are not significant.
"""

import asyncio
import json
import os
from pathlib import Path

import sqlglot.expressions as exp
from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server
from shared.loader import DdlCatalog, DdlEntry, DdlParseError, extract_refs, load_directory, read_manifest
from shared.name_resolver import normalize

server = Server("ddl-mcp")

_catalog_cache: dict[Path, DdlCatalog] = {}

_DDL_PATH_SCHEMA = {
    "ddl_path": {
        "type": "string",
        "description": (
            "Absolute path to the DDL artifacts directory. "
            "Overrides the DDL_PATH environment variable when provided."
        ),
    }
}


# ── Path helpers ──────────────────────────────────────────────────────────────

def _ddl_path(override: str | None = None) -> Path:
    raw = (override or os.environ.get("DDL_PATH", "")).strip()
    if not raw:
        raise ValueError(
            "DDL path is not set. Pass ddl_path in tool arguments or set the "
            "DDL_PATH environment variable."
        )
    p = Path(raw)
    if not p.exists():
        raise FileNotFoundError(f"DDL path does not exist: {p}")
    return p


def _catalog(ddl_path: Path) -> DdlCatalog:
    resolved = ddl_path.resolve()
    if resolved not in _catalog_cache:
        manifest = read_manifest(ddl_path)
        _catalog_cache[resolved] = load_directory(ddl_path, dialect=manifest["dialect"])
    return _catalog_cache[resolved]


# ── Column metadata ───────────────────────────────────────────────────────────

def _parse_columns(entry: DdlEntry) -> list[dict]:
    """Parse column metadata from a CREATE TABLE AST entry.

    sqlglot represents both NULL and NOT NULL as NotNullColumnConstraint —
    distinguished by the allow_null arg (True = NULL, False/absent = NOT NULL).
    """
    if entry.ast is None:
        return []
    cols = []
    for col_def in entry.ast.find_all(exp.ColumnDef):
        is_pk = False
        is_not_null = False
        for constraint in col_def.constraints:
            kind = constraint.kind
            if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                is_pk = True
            elif isinstance(kind, exp.NotNullColumnConstraint):
                # allow_null=True means the keyword is NULL (explicitly nullable)
                # allow_null absent/False means NOT NULL
                if not kind.args.get("allow_null", False):
                    is_not_null = True
        cols.append({
            "name": col_def.name,
            "type": col_def.kind.sql(dialect="tsql") if col_def.kind else "UNKNOWN",
            "nullable": not (is_not_null or is_pk),
            "is_pk": is_pk,
        })
    return cols


# ── Tool handlers ─────────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
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
                "to the given table (reads or writes). Uses sqlglot AST analysis — "
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


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    ddl_path = _ddl_path(arguments.get("ddl_path"))
    catalog = _catalog(ddl_path)

    if name == "list_tables":
        return [types.TextContent(type="text", text="\n".join(sorted(catalog.tables)) or "(none)")]

    if name == "get_table_schema":
        entry = catalog.get_table(arguments["name"])
        if not entry:
            return [types.TextContent(type="text", text=f"Table not found: {arguments['name']}")]
        return [types.TextContent(type="text", text=json.dumps({
            "ddl": entry.raw_ddl,
            "columns": _parse_columns(entry),
        }))]

    if name == "list_procedures":
        return [types.TextContent(type="text", text="\n".join(sorted(catalog.procedures)) or "(none)")]

    if name == "get_procedure_body":
        entry = catalog.get_procedure(arguments["name"])
        if not entry:
            return [types.TextContent(type="text", text=f"Procedure not found: {arguments['name']}")]
        return [types.TextContent(type="text", text=entry.raw_ddl)]

    if name == "get_dependencies":
        target = normalize(arguments["table_name"])
        matches = []
        for proc_name, entry in catalog.procedures.items():
            try:
                refs = extract_refs(entry)
                if target in refs.reads_from or target in refs.writes_to:
                    matches.append(proc_name)
            except DdlParseError:
                pass  # cannot determine refs for EXEC/MERGE/complex IF-ELSE bodies
        return [types.TextContent(
            type="text",
            text="\n".join(sorted(matches)) if matches else "(none)",
        )]

    if name == "list_views":
        return [types.TextContent(type="text", text="\n".join(sorted(catalog.views)) or "(none)")]

    if name == "get_view_body":
        entry = catalog.get_view(arguments["name"])
        if not entry:
            return [types.TextContent(type="text", text=f"View not found: {arguments['name']}")]
        return [types.TextContent(type="text", text=entry.raw_ddl)]

    if name == "list_functions":
        return [types.TextContent(type="text", text="\n".join(sorted(catalog.functions)) or "(none)")]

    if name == "get_function_body":
        entry = catalog.get_function(arguments["name"])
        if not entry:
            return [types.TextContent(type="text", text=f"Function not found: {arguments['name']}")]
        return [types.TextContent(type="text", text=entry.raw_ddl)]

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(_main())
