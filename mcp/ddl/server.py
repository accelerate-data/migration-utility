# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "mcp>=1.0",
#   "sqlglot>=25.0,<26",
# ]
# ///
"""DDL file MCP server.

Reads DDL files extracted by setup-ddl and exposes them as MCP tools for
analysis agents.

Usage (stdio mode):
    uv run server.py

DDL path resolution order (first wins):
    1. `DDL_PATH` environment variable
    2. Current working directory
"""

import asyncio

from ddl_mcp_support.server_context import DdlServerContext
from ddl_mcp_support.tool_definitions import tool_definitions
from ddl_mcp_support.tool_handlers import handle_tool_call
from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server


server = Server("ddl-mcp")
_context = DdlServerContext()


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return tool_definitions()


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    return handle_tool_call(name, arguments, _context)


async def _main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(_main())
