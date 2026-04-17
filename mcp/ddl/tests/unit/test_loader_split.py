"""Public loader facade contract for the standalone MCP support package."""

import importlib


def test_loader_reexports_focused_support_modules() -> None:
    """loader.py stays a small compatibility facade over focused support modules."""
    module_names = {
        "block_segmenter": "ddl_mcp_support.block_segmenter",
        "loader": "ddl_mcp_support.loader",
        "loader_data": "ddl_mcp_support.loader_data",
        "loader_io": "ddl_mcp_support.loader_io",
        "loader_parse": "ddl_mcp_support.loader_parse",
        "routing": "ddl_mcp_support.routing",
        "tsql_utils": "ddl_mcp_support.tsql_utils",
    }
    modules = {}
    missing_modules = []
    for key, module_name in module_names.items():
        try:
            modules[key] = importlib.import_module(module_name)
        except ModuleNotFoundError:
            missing_modules.append(module_name)

    assert missing_modules == []

    block_segmenter = modules["block_segmenter"]
    loader = modules["loader"]
    loader_data = modules["loader_data"]
    loader_io = modules["loader_io"]
    loader_parse = modules["loader_parse"]
    routing = modules["routing"]
    tsql_utils = modules["tsql_utils"]

    assert loader.DdlCatalog is loader_data.DdlCatalog
    assert loader.DdlEntry is loader_data.DdlEntry
    assert loader.DdlParseError is loader_data.DdlParseError
    assert loader.ObjectRefs is loader_data.ObjectRefs

    assert loader.mask_tsql is tsql_utils.mask_tsql
    assert loader.scan_routing_flags is routing.scan_routing_flags
    assert loader.segment_sql is block_segmenter.segment_sql
    assert loader.SegmenterError is block_segmenter.SegmenterError
    assert loader.SegmenterLimitError is block_segmenter.SegmenterLimitError

    assert loader.GO_RE is loader_parse.GO_RE
    assert loader.extract_refs is loader_parse.extract_refs
    assert loader.parse_body_statements is loader_parse.parse_body_statements
    assert loader.collect_refs_from_statements is loader_parse.collect_refs_from_statements

    assert loader.load_directory is loader_io.load_directory
    assert loader.read_manifest is loader_io.read_manifest
