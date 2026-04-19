"""Dispatch handlers for DDL MCP tools."""

from __future__ import annotations

import json
import logging
from typing import Protocol

from mcp import types

from ddl_mcp_support.loader import DdlCatalog, DdlParseError, extract_refs
from ddl_mcp_support.name_resolver import normalize
from ddl_mcp_support.server_context import parse_columns, require_argument


logger = logging.getLogger(__name__)


class ToolContext(Protocol):
    def catalog(self) -> DdlCatalog:
        """Return the current DDL catalog."""

    def catalog_dialect(self) -> str:
        """Return the dialect used to parse the current catalog."""


def handle_tool_call(
    name: str,
    arguments: dict | None,
    context: ToolContext,
) -> list[types.TextContent]:
    arguments = arguments or {}
    handlers = {
        "list_tables": handle_list_tables,
        "get_table_schema": handle_get_table_schema,
        "list_procedures": handle_list_procedures,
        "get_procedure_body": handle_get_procedure_body,
        "get_dependencies": handle_get_dependencies,
        "list_views": handle_list_views,
        "get_view_body": handle_get_view_body,
        "list_functions": handle_list_functions,
        "get_function_body": handle_get_function_body,
    }
    handler = handlers.get(name)
    if handler is None:
        return [_text(f"Unknown tool: {name}")]
    return handler(arguments, context)


def handle_list_tables(
    _arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    catalog = context.catalog()
    return [_text("\n".join(sorted(catalog.tables)) or "(none)")]


def handle_get_table_schema(
    arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    table_name = require_argument(arguments, "name")
    if table_name is None:
        return [_text("Missing required argument: name")]
    catalog = context.catalog()
    entry = catalog.get_table(table_name)
    if not entry:
        return [_text(f"Table not found: {table_name}")]
    dialect = context.catalog_dialect()
    return [_text(json.dumps({
        "ddl": entry.raw_ddl,
        "columns": parse_columns(entry, dialect),
    }))]


def handle_list_procedures(
    _arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    catalog = context.catalog()
    return [_text("\n".join(sorted(catalog.procedures)) or "(none)")]


def handle_get_procedure_body(
    arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    procedure_name = require_argument(arguments, "name")
    if procedure_name is None:
        return [_text("Missing required argument: name")]
    catalog = context.catalog()
    entry = catalog.get_procedure(procedure_name)
    if not entry:
        return [_text(f"Procedure not found: {procedure_name}")]
    return [_text(entry.raw_ddl)]


def handle_get_dependencies(
    arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    table_name = require_argument(arguments, "table_name")
    if table_name is None:
        return [_text("Missing required argument: table_name")]
    catalog = context.catalog()
    target = normalize(table_name)
    matches = []
    for proc_name, entry in catalog.procedures.items():
        try:
            refs = extract_refs(entry)
            if target in refs.reads_from or target in refs.writes_to:
                matches.append(proc_name)
        except DdlParseError as exc:
            logger.warning(
                "event=get_dependencies operation=skip_procedure procedure=%s table=%s error=%s",
                proc_name,
                table_name,
                exc,
            )
    return [_text("\n".join(sorted(matches)) if matches else "(none)")]


def handle_list_views(
    _arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    catalog = context.catalog()
    return [_text("\n".join(sorted(catalog.views)) or "(none)")]


def handle_get_view_body(
    arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    view_name = require_argument(arguments, "name")
    if view_name is None:
        return [_text("Missing required argument: name")]
    catalog = context.catalog()
    entry = catalog.get_view(view_name)
    if not entry:
        return [_text(f"View not found: {view_name}")]
    return [_text(entry.raw_ddl)]


def handle_list_functions(
    _arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    catalog = context.catalog()
    return [_text("\n".join(sorted(catalog.functions)) or "(none)")]


def handle_get_function_body(
    arguments: dict,
    context: ToolContext,
) -> list[types.TextContent]:
    function_name = require_argument(arguments, "name")
    if function_name is None:
        return [_text("Missing required argument: name")]
    catalog = context.catalog()
    entry = catalog.get_function(function_name)
    if not entry:
        return [_text(f"Function not found: {function_name}")]
    return [_text(entry.raw_ddl)]


def _text(value: str) -> types.TextContent:
    return types.TextContent(type="text", text=value)
