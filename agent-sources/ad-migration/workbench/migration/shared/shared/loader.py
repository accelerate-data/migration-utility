"""DDL directory loader — public API facade.

Re-exports from loader_data, loader_parse, and loader_io so that existing
callers (``from shared.loader import DdlCatalog``, etc.) continue to work.
"""

from shared.loader_data import (  # noqa: F401 — re-export
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    ObjectRefs,
)
from shared.loader_io import (  # noqa: F401 — re-export
    index_directory,
    load_catalog,
    load_ddl,
    load_directory,
    read_manifest,
)
from shared.loader_parse import (  # noqa: F401 — re-export
    _collect_refs_from_statements,
    _parse_block,
    _parse_body_statements,
    classify_statement,
    extract_refs,
)

__all__ = [
    "CatalogNotFoundError",
    "DdlCatalog",
    "DdlEntry",
    "DdlParseError",
    "ObjectRefs",
    "classify_statement",
    "extract_refs",
    "index_directory",
    "load_catalog",
    "load_ddl",
    "load_directory",
    "read_manifest",
]
