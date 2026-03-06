# /// script
# requires-python = ">=3.11"
# dependencies = ["mcp>=1.0"]
# ///
"""DDL file MCP server.

Reads DDL files extracted by the dacpac-extract sidecar and exposes them
as MCP tools for use by all analysis agents (scoping, profiling, etc.).

Usage (stdio mode):
    DDL_PATH=/path/to/project/artifacts/ddl uv run server.py

DDL_PATH must point to a directory containing:
    tables.sql      CREATE TABLE statements, GO-separated
    procedures.sql  CREATE PROCEDURE statements, GO-separated
    views.sql       CREATE VIEW statements, GO-separated (optional)
    functions.sql   CREATE FUNCTION statements, GO-separated (optional)
"""

import asyncio
import os
import re
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

server = Server("ddl-mcp")


# ── Path helpers ──────────────────────────────────────────────────────────────

def _ddl_path() -> Path:
    raw = os.environ.get("DDL_PATH", "").strip()
    if not raw:
        raise ValueError(
            "DDL_PATH environment variable is not set. "
            "Set it to the artifacts/ddl/ directory of the target project."
        )
    p = Path(raw)
    if not p.exists():
        raise FileNotFoundError(f"DDL_PATH does not exist: {p}")
    return p


# ── SQL file parsing ──────────────────────────────────────────────────────────

def _split_blocks(sql: str) -> list[str]:
    """Split a SQL file into GO-delimited object blocks."""
    blocks = re.split(r"\nGO\b", sql, flags=re.IGNORECASE)
    return [b.strip() for b in blocks if b.strip()]


_NAME_RE = re.compile(
    r"CREATE\s+(?:OR\s+ALTER\s+)?"
    r"(?:TABLE|PROCEDURE|VIEW|FUNCTION)\s+"
    r"\[?(\w+)\]?\.\[?(\w+)\]?",
    re.IGNORECASE,
)


def _extract_name(block: str) -> str | None:
    m = _NAME_RE.search(block)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return None


def _load_file(ddl_path: Path, filename: str) -> dict[str, str]:
    """Return a lowercased-name → body mapping for objects in a DDL file."""
    f = ddl_path / filename
    if not f.exists():
        return {}
    blocks = _split_blocks(f.read_text(encoding="utf-8"))
    result: dict[str, str] = {}
    for block in blocks:
        name = _extract_name(block)
        if name:
            result[name.lower()] = block
    return result


def _normalise(name: str) -> str:
    """Lower-case and strip square brackets for map lookup."""
    return name.lower().replace("[", "").replace("]", "")


# ── Tool handlers ─────────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_tables",
            description=(
                "List all table names (schema.name) available in tables.sql."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="get_table_schema",
            description="Return the CREATE TABLE DDL for a specific table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Schema-qualified name, e.g. silver.DimProduct",
                    }
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="list_procedures",
            description=(
                "List all stored procedure names (schema.name) in procedures.sql."
            ),
            inputSchema={"type": "object", "properties": {}},
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
                    }
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="get_dependencies",
            description=(
                "Find all stored procedures whose body references the given table. "
                "Returns a newline-separated list of schema-qualified procedure names, "
                "or '(none)' if no references are found."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Schema-qualified table name, e.g. silver.DimProduct",
                    }
                },
                "required": ["table_name"],
            },
        ),
        types.Tool(
            name="list_views",
            description=(
                "List all view names (schema.name) available in views.sql. "
                "Returns '(none)' when views.sql is absent."
            ),
            inputSchema={"type": "object", "properties": {}},
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
                    }
                },
                "required": ["name"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    ddl_path = _ddl_path()

    if name == "list_tables":
        tables = _load_file(ddl_path, "tables.sql")
        return [types.TextContent(type="text", text="\n".join(sorted(tables)) or "(none)")]

    if name == "get_table_schema":
        tables = _load_file(ddl_path, "tables.sql")
        key = _normalise(arguments["name"])
        block = tables.get(key)
        if not block:
            return [types.TextContent(type="text", text=f"Table not found: {arguments['name']}")]
        return [types.TextContent(type="text", text=block)]

    if name == "list_procedures":
        procs = _load_file(ddl_path, "procedures.sql")
        return [types.TextContent(type="text", text="\n".join(sorted(procs)) or "(none)")]

    if name == "get_procedure_body":
        procs = _load_file(ddl_path, "procedures.sql")
        key = _normalise(arguments["name"])
        block = procs.get(key)
        if not block:
            return [types.TextContent(type="text", text=f"Procedure not found: {arguments['name']}")]
        return [types.TextContent(type="text", text=block)]

    if name == "get_dependencies":
        procs = _load_file(ddl_path, "procedures.sql")
        raw_name = arguments["table_name"]
        # match both bare name and schema-qualified name
        bare = _normalise(raw_name).split(".")[-1]
        matches = [
            proc_name
            for proc_name, body in procs.items()
            if bare in body.lower()
        ]
        return [types.TextContent(
            type="text",
            text="\n".join(sorted(matches)) if matches else "(none)",
        )]

    if name == "list_views":
        views = _load_file(ddl_path, "views.sql")
        return [types.TextContent(type="text", text="\n".join(sorted(views)) or "(none)")]

    if name == "get_view_body":
        views = _load_file(ddl_path, "views.sql")
        key = _normalise(arguments["name"])
        block = views.get(key)
        if not block:
            return [types.TextContent(type="text", text=f"View not found: {arguments['name']}")]
        return [types.TextContent(type="text", text=block)]

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
