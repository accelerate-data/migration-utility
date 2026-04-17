"""Public DDL loader facade for the standalone DDL MCP package."""
from __future__ import annotations

from ddl_mcp_support.block_segmenter import (
    BlockNode,
    IfNode,
    SegmentNode,
    SegmenterError,
    SegmenterLimitError,
    StatementNode,
    TryCatchNode,
    WhileNode,
    segment_sql,
)
from ddl_mcp_support.loader_data import DdlCatalog, DdlEntry, DdlParseError, ObjectRefs
from ddl_mcp_support.loader_io import load_directory, read_manifest
from ddl_mcp_support.loader_parse import (
    GO_RE,
    classify_statement,
    collect_refs_from_statements,
    extract_name,
    extract_refs,
    extract_type_bucket,
    parse_block,
    parse_body_statements,
    split_blocks,
)
from ddl_mcp_support.routing import DYNAMIC_EXEC_RE, scan_routing_flags
from ddl_mcp_support.tsql_utils import mask_tsql

__all__ = [
    "BlockNode",
    "DYNAMIC_EXEC_RE",
    "DdlCatalog",
    "DdlEntry",
    "DdlParseError",
    "GO_RE",
    "IfNode",
    "ObjectRefs",
    "SegmentNode",
    "SegmenterError",
    "SegmenterLimitError",
    "StatementNode",
    "TryCatchNode",
    "WhileNode",
    "classify_statement",
    "collect_refs_from_statements",
    "extract_name",
    "extract_refs",
    "extract_type_bucket",
    "load_directory",
    "mask_tsql",
    "parse_block",
    "parse_body_statements",
    "read_manifest",
    "scan_routing_flags",
    "segment_sql",
    "split_blocks",
]
