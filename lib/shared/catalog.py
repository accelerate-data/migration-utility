"""Catalog JSON file I/O for per-object metadata extracted from sys.* views.

Reads and writes the ``catalog/`` subdirectory that setup-ddl
produces alongside the flat ``.sql`` DDL files.  Each object gets its own JSON
file keyed by normalized ``schema.name``.

Layout::

    <project-root>/
    └── catalog/
        ├── tables/<schema>.<table>.json
        ├── procedures/<schema>.<proc>.json
        ├── views/<schema>.<view>.json
        └── functions/<schema>.<function>.json

Table files carry catalog signals (PKs, FKs, identity, CDC, sensitivity) plus
``referenced_by`` (inbound references flipped from proc/view/function DMF data).
Proc/view/function files carry ``references`` (outbound references from the DMF).
"""

from __future__ import annotations

from shared.catalog_preservation import (
    restore_enriched_fields as restore_enriched_fields,
    snapshot_enriched_fields as snapshot_enriched_fields,
)
from shared.catalog_support.loaders import (
    load_function_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    read_selected_writer,
)
from shared.catalog_support.merge import load_and_merge_catalog, write_json
from shared.catalog_support.paths import (
    _catalog_dir,
    _object_path,
    detect_catalog_bucket,
    detect_object_type,
    has_catalog,
    resolve_catalog_path,
)
from shared.catalog_support.references import ensure_referenced_by, ensure_references
from shared.catalog_support.writers import (
    _write_catalog_json,
    write_function_catalog,
    write_proc_catalog,
    write_proc_statements,
    write_proc_table_slice,
    write_table_catalog,
    write_view_catalog,
)
