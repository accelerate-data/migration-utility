"""Compatibility barrel for shared.loader_io support modules."""

from __future__ import annotations

from sqlglot import exp  # noqa: F401

from shared.env_config import resolve_catalog_dir, resolve_ddl_dir  # noqa: F401
from shared.loader_data import (  # noqa: F401
    CatalogLoadError,
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
)
from shared.loader_io_support.directory import (  # noqa: F401
    _DELIMITER_MAP,
    _SEMICOLON_RE,
    _load_file,
    load_ddl,
    load_directory,
)
from shared.loader_io_support.indexing import (  # noqa: F401
    _CATALOG_SCHEMA_VERSION,
    _write_per_object_files,
    index_directory,
    load_catalog,
)
from shared.loader_io_support.manifest import (  # noqa: F401
    _require_manifest_file,
    clear_manifest_sandbox,
    read_manifest,
    write_manifest_sandbox,
)
from shared.loader_parse import (  # noqa: F401
    GO_RE,
    extract_name,
    extract_refs,
    extract_type_bucket,
    parse_block,
    split_blocks,
)
from shared.name_resolver import normalize  # noqa: F401
from shared.runtime_config import (  # noqa: F401
    get_primary_dialect,
    get_runtime_role,
    set_runtime_role,
    validate_supported_technologies,
)
from shared.runtime_config_models import RuntimeRole  # noqa: F401

__all__ = [
    "_CATALOG_SCHEMA_VERSION",
    "_DELIMITER_MAP",
    "_SEMICOLON_RE",
    "_load_file",
    "_require_manifest_file",
    "_write_per_object_files",
    "CatalogLoadError",
    "CatalogNotFoundError",
    "DdlCatalog",
    "DdlEntry",
    "DdlParseError",
    "GO_RE",
    "RuntimeRole",
    "clear_manifest_sandbox",
    "exp",
    "extract_name",
    "extract_refs",
    "extract_type_bucket",
    "get_primary_dialect",
    "get_runtime_role",
    "index_directory",
    "load_catalog",
    "load_ddl",
    "load_directory",
    "normalize",
    "parse_block",
    "read_manifest",
    "resolve_catalog_dir",
    "resolve_ddl_dir",
    "set_runtime_role",
    "split_blocks",
    "validate_supported_technologies",
    "write_manifest_sandbox",
]
