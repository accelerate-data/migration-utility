from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.catalog_support.paths import resolve_catalog_path
from shared.loader_data import CatalogLoadError


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    try:
        tmp_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(path)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise


def load_and_merge_catalog(
    project_root: Path,
    fqn: str,
    section_key: str,
    section_data: Any,
) -> dict[str, Any]:
    """Load a catalog file, merge a section into it, and write it back atomically.

    Auto-detects table vs view by checking catalog/views/ first, then catalog/tables/.
    Returns a confirmation dict with ``ok``, ``table``, ``status``, and ``catalog_path``.

    Raises CatalogFileMissingError if no catalog file exists for the FQN.
    Raises CatalogLoadError on corrupt JSON.
    """
    cat_path = resolve_catalog_path(project_root, fqn)

    try:
        existing = json.loads(cat_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(cat_path), exc) from exc

    existing[section_key] = section_data
    write_json(cat_path, existing)

    result: dict[str, Any] = {
        "ok": True,
        "table": fqn,
        "catalog_path": str(cat_path),
    }
    if isinstance(section_data, dict) and "status" in section_data:
        result["status"] = section_data["status"]
    return result
