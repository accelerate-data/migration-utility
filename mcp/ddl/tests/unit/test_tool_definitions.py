"""Tests for DDL MCP tool schema declarations."""

from ddl_mcp_support.tool_definitions import tool_definitions


EXPECTED_TOOL_NAMES = [
    "list_tables",
    "get_table_schema",
    "list_procedures",
    "get_procedure_body",
    "get_dependencies",
    "list_views",
    "get_view_body",
    "list_functions",
    "get_function_body",
]


def test_tool_definitions_keep_existing_names_and_order() -> None:
    tools = tool_definitions()

    assert [tool.name for tool in tools] == EXPECTED_TOOL_NAMES


def test_tool_definitions_keep_required_name_schema() -> None:
    tools = {tool.name: tool for tool in tool_definitions()}

    for tool_name in [
        "get_table_schema",
        "get_procedure_body",
        "get_view_body",
        "get_function_body",
    ]:
        schema = tools[tool_name].inputSchema
        assert schema["type"] == "object"
        assert schema["required"] == ["name"]
        assert schema["properties"]["name"]["type"] == "string"


def test_tool_definitions_keep_required_table_name_schema() -> None:
    tools = {tool.name: tool for tool in tool_definitions()}
    schema = tools["get_dependencies"].inputSchema

    assert schema["type"] == "object"
    assert schema["required"] == ["table_name"]
    assert schema["properties"]["table_name"]["type"] == "string"


def test_list_tool_definitions_have_no_required_arguments() -> None:
    tools = {tool.name: tool for tool in tool_definitions()}

    for tool_name in ["list_tables", "list_procedures", "list_views", "list_functions"]:
        schema = tools[tool_name].inputSchema
        assert schema == {"type": "object", "properties": {}}
