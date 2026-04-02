"""Direct tests for the T-SQL control-flow segmenter."""

from __future__ import annotations

import pytest

from shared.block_segmenter import (
    BlockNode,
    IfNode,
    SegmenterError,
    SegmenterLimitError,
    StatementNode,
    TryCatchNode,
    WhileNode,
    segment_sql,
)


def _collect_statement_sql(nodes: list[object]) -> list[str]:
    statements: list[str] = []
    for node in nodes:
        if isinstance(node, StatementNode):
            statements.append(node.sql)
        elif isinstance(node, BlockNode):
            statements.extend(_collect_statement_sql(node.children))
        elif isinstance(node, IfNode):
            statements.extend(_collect_statement_sql(node.true_branch))
            statements.extend(_collect_statement_sql(node.false_branch))
        elif isinstance(node, WhileNode):
            statements.extend(_collect_statement_sql(node.body))
        elif isinstance(node, TryCatchNode):
            statements.extend(_collect_statement_sql(node.try_branch))
            statements.extend(_collect_statement_sql(node.catch_branch))
    return statements


def test_segment_sql_if_else_begin_blocks() -> None:
    nodes = segment_sql(
        """
        IF EXISTS (SELECT 1 FROM dbo.Config)
        BEGIN
            INSERT INTO dbo.Target VALUES (1);
        END
        ELSE
        BEGIN
            UPDATE dbo.Target SET flag = 1;
        END
        """,
    )

    assert len(nodes) == 1
    if_node = nodes[0]
    assert isinstance(if_node, IfNode)
    assert "EXISTS" in if_node.condition_sql.upper()
    assert _collect_statement_sql(if_node.true_branch) == ["INSERT INTO dbo.Target VALUES (1)"]
    assert _collect_statement_sql(if_node.false_branch) == ["UPDATE dbo.Target SET flag = 1"]


def test_segment_sql_if_else_single_statement_branches() -> None:
    nodes = segment_sql(
        "IF @mode = 1 INSERT INTO dbo.Target VALUES (1) ELSE DELETE FROM dbo.Target WHERE id = 1;",
    )

    assert len(nodes) == 1
    if_node = nodes[0]
    assert isinstance(if_node, IfNode)
    assert _collect_statement_sql(if_node.true_branch) == ["INSERT INTO dbo.Target VALUES (1)"]
    assert _collect_statement_sql(if_node.false_branch) == ["DELETE FROM dbo.Target WHERE id = 1"]


def test_segment_sql_while_begin_block() -> None:
    nodes = segment_sql(
        """
        WHILE EXISTS (SELECT 1 FROM dbo.Queue)
        BEGIN
            DELETE FROM dbo.Queue WHERE processed = 1;
            INSERT INTO dbo.Audit VALUES ('done');
        END
        """,
    )

    assert len(nodes) == 1
    while_node = nodes[0]
    assert isinstance(while_node, WhileNode)
    assert "EXISTS" in while_node.condition_sql.upper()
    assert _collect_statement_sql(while_node.body) == [
        "DELETE FROM dbo.Queue WHERE processed = 1",
        "INSERT INTO dbo.Audit VALUES ('done')",
    ]


def test_segment_sql_while_single_statement_body() -> None:
    nodes = segment_sql("WHILE @i < 10 SET @i = @i + 1;")

    assert len(nodes) == 1
    while_node = nodes[0]
    assert isinstance(while_node, WhileNode)
    assert while_node.condition_sql == "@i < 10"
    assert _collect_statement_sql(while_node.body) == ["SET @i = @i + 1"]


def test_segment_sql_try_catch_nested_if() -> None:
    nodes = segment_sql(
        """
        BEGIN TRY
            IF EXISTS (SELECT 1 FROM dbo.Config)
            BEGIN
                INSERT INTO dbo.Target VALUES (1);
            END
        END TRY
        BEGIN CATCH
            INSERT INTO dbo.Errors VALUES ('failed');
        END CATCH
        """,
    )

    assert len(nodes) == 1
    try_node = nodes[0]
    assert isinstance(try_node, TryCatchNode)
    assert isinstance(try_node.try_branch[0], IfNode)
    assert _collect_statement_sql(try_node.try_branch) == ["INSERT INTO dbo.Target VALUES (1)"]
    assert _collect_statement_sql(try_node.catch_branch) == ["INSERT INTO dbo.Errors VALUES ('failed')"]


def test_segment_sql_ignores_keywords_in_strings_comments_and_identifiers() -> None:
    nodes = segment_sql(
        """
        INSERT INTO dbo.Log(message)
        SELECT 'IF BEGIN TRY END CATCH ELSE WHILE';
        -- IF ELSE BEGIN END should not be parsed as control flow
        SELECT [END], [ELSE] FROM dbo.Source;
        /* BEGIN TRY
           END CATCH
        */
        """,
    )

    assert [type(node).__name__ for node in nodes] == ["StatementNode", "StatementNode"]
    assert _collect_statement_sql(nodes) == [
        "INSERT INTO dbo.Log(message)\n        SELECT 'IF BEGIN TRY END CATCH ELSE WHILE'",
        "SELECT [END], [ELSE] FROM dbo.Source",
    ]


def test_segment_sql_case_when_does_not_split_on_end() -> None:
    nodes = segment_sql(
        """
        INSERT INTO dbo.Target (name)
        SELECT CASE
            WHEN ProductName IS NULL THEN 'Unknown'
            ELSE ProductName
        END
        FROM dbo.Source;
        """,
    )

    assert len(nodes) == 1
    assert isinstance(nodes[0], StatementNode)
    assert "CASE" in nodes[0].sql
    assert "FROM dbo.Source" in nodes[0].sql


def test_segment_sql_depth_guardrail() -> None:
    sql = """
    IF @a = 1
    BEGIN
        IF @b = 1
        BEGIN
            INSERT INTO dbo.Target VALUES (1);
        END
    END
    """

    with pytest.raises(SegmenterLimitError, match="maximum control-flow nesting depth exceeded"):
        segment_sql(sql, max_depth=1)


def test_segment_sql_node_count_guardrail() -> None:
    sql = "SET @a = 1; SET @b = 2; SET @c = 3;"

    with pytest.raises(SegmenterLimitError, match="maximum segmenter node count exceeded"):
        segment_sql(sql, max_nodes=2)


def test_segment_sql_rejects_malformed_try_catch() -> None:
    with pytest.raises(SegmenterError, match="BEGIN TRY block missing BEGIN CATCH"):
        segment_sql(
            """
            BEGIN TRY
                INSERT INTO dbo.Target VALUES (1);
            END TRY
            """,
        )
