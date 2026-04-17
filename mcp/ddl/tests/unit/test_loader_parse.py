"""Characterization coverage for standalone MCP parser support modules."""

from ddl_mcp_support.block_segmenter import (
    IfNode,
    TryCatchNode,
    WhileNode,
    segment_sql,
)
from ddl_mcp_support.loader import DdlEntry, extract_refs, parse_body_statements
from ddl_mcp_support.routing import scan_routing_flags


def _procedure(body: str) -> str:
    return f"""\
CREATE PROCEDURE silver.usp_test
AS
BEGIN
{body}
END
"""


def test_segment_sql_preserves_control_flow_shapes() -> None:
    nodes = segment_sql(
        """
IF @flag = 1
BEGIN
    INSERT INTO silver.Target (id) SELECT id FROM bronze.Source;
END
ELSE
BEGIN
    UPDATE silver.Target SET id = id;
END;
WHILE @flag < 3
BEGIN
    SET @flag = @flag + 1;
END;
BEGIN TRY
    SELECT id FROM silver.Target;
END TRY
BEGIN CATCH
    SELECT 1;
END CATCH;
"""
    )

    assert isinstance(nodes[0], IfNode)
    assert isinstance(nodes[1], WhileNode)
    assert isinstance(nodes[2], TryCatchNode)


def test_parse_body_statements_flattens_control_flow_leaves() -> None:
    statements, needs_llm = parse_body_statements(
        _procedure(
            """
    IF @flag = 1
        INSERT INTO silver.Target (id) SELECT id FROM bronze.Source;
    ELSE
        UPDATE silver.Target SET id = id;
    WHILE @flag < 3
        SELECT id FROM silver.Target;
"""
        )
    )

    assert needs_llm is False
    assert [type(stmt).__name__ for stmt in statements] == [
        "Insert",
        "Update",
        "Select",
    ]


def test_extract_refs_reads_body_with_trailing_comment_after_end() -> None:
    entry = DdlEntry(
        raw_ddl=_procedure(
            """
    INSERT INTO silver.Target (id)
    SELECT id FROM bronze.Source;
"""
        )
        + "-- trailing deployment comment",
        ast=None,
    )

    refs = extract_refs(entry)

    assert refs.needs_llm is False
    assert refs.writes_to == ["silver.target"]
    assert refs.reads_from == ["bronze.source"]


def test_scan_routing_flags_distinguishes_exec_forms() -> None:
    assert scan_routing_flags("EXEC silver.usp_child") == {
        "needs_llm": False,
        "needs_enrich": True,
        "mode": "call_graph_enrich",
        "routing_reasons": ["static_exec"],
    }
    assert scan_routing_flags("EXEC sp_executesql N'SELECT 1'") == {
        "needs_llm": False,
        "needs_enrich": False,
        "mode": "dynamic_sql_literal",
        "routing_reasons": ["dynamic_sql_literal"],
    }
    assert scan_routing_flags("EXEC sp_executesql @sql") == {
        "needs_llm": True,
        "needs_enrich": False,
        "mode": "llm_required",
        "routing_reasons": ["dynamic_sql_variable"],
    }


def test_parse_body_statements_skips_exec_leaves_and_marks_dynamic_variables() -> None:
    static_statements, static_needs_llm = parse_body_statements(
        _procedure("    EXEC silver.usp_child;")
    )
    dynamic_statements, dynamic_needs_llm = parse_body_statements(
        _procedure("    EXEC sp_executesql @sql;")
    )

    assert static_statements == []
    assert static_needs_llm is False
    assert dynamic_statements == []
    assert dynamic_needs_llm is True
