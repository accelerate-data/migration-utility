"""DDL directory loader — public API facade.

Re-exports from loader_data, loader_parse, and loader_io so that existing
callers (``from shared.loader import DdlCatalog``, etc.) continue to work.
"""

from shared.loader_data import (  # noqa: F401 — re-export
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    ObjectNotFoundError,
    ObjectRefs,
    ProfileMissingError,
)
from shared.loader_io import (  # noqa: F401 — re-export
    index_directory,
    load_catalog,
    load_ddl,
    load_directory,
    read_manifest,
)
from shared.loader_parse import (  # noqa: F401 — re-export
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

__all__ = [
    "CatalogFileMissingError",
    "CatalogNotFoundError",
    "DdlCatalog",
    "DdlEntry",
    "DdlParseError",
    "GO_RE",
    "ObjectNotFoundError",
    "ObjectRefs",
    "ProfileMissingError",
    "classify_statement",
    "collect_refs_from_statements",
    "extract_name",
    "extract_refs",
    "extract_type_bucket",
    "index_directory",
    "load_catalog",
    "load_ddl",
    "load_directory",
    "parse_block",
    "parse_body_statements",
    "read_manifest",
    "split_blocks",
]
