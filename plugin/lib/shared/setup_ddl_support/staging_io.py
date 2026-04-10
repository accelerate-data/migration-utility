"""Staging-file I/O helpers for setup-ddl."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.loader_data import CorruptJSONError


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CorruptJSONError(path, exc) from exc


def read_json_optional(path: Path) -> Any:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CorruptJSONError(path, exc) from exc


def load_staging_catalog_inputs(staging_dir: Path) -> dict[str, Any]:
    return {
        "table_columns_rows": read_json_optional(staging_dir / "table_columns.json"),
        "pk_unique_rows": read_json_optional(staging_dir / "pk_unique.json"),
        "fk_rows": read_json_optional(staging_dir / "foreign_keys.json"),
        "identity_rows": read_json_optional(staging_dir / "identity_columns.json"),
        "cdc_rows": read_json_optional(staging_dir / "cdc.json"),
        "ct_rows": read_json_optional(staging_dir / "change_tracking.json"),
        "sensitivity_rows": read_json_optional(staging_dir / "sensitivity.json"),
        "object_types_raw": read_json_optional(staging_dir / "object_types.json"),
        "proc_dmf_rows": read_json_optional(staging_dir / "proc_dmf.json"),
        "view_dmf_rows": read_json_optional(staging_dir / "view_dmf.json"),
        "func_dmf_rows": read_json_optional(staging_dir / "func_dmf.json"),
        "proc_params_rows": read_json_optional(staging_dir / "proc_params.json"),
        "definitions_rows": read_json_optional(staging_dir / "definitions.json"),
        "view_columns_rows": read_json_optional(staging_dir / "view_columns.json"),
        "mv_fqns_list": read_json_optional(staging_dir / "mv_fqns.json")
        or read_json_optional(staging_dir / "indexed_views.json")
        or [],
    }
