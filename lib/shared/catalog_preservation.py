"""Catalog enriched-field preservation for re-extraction workflows."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.env_config import resolve_catalog_dir

logger = logging.getLogger(__name__)

# Keys preserved per bucket during re-extraction. ``refactor`` belongs only on
# procedure catalogs; never copy it from tables/views/functions.
_ENRICHED_KEYS_BY_BUCKET: dict[str, tuple[str, ...]] = {
    "tables": ("scoping", "profile", "excluded", "is_source", "is_seed"),
    "procedures": ("scoping", "profile", "refactor"),
    "views": ("scoping", "profile", "excluded"),
    "functions": ("scoping", "profile"),
}


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def snapshot_enriched_fields(project_root: Path) -> dict[str, dict[str, Any]]:
    """Snapshot LLM-enriched fields from all existing catalog files."""
    catalog_dir = resolve_catalog_dir(project_root)
    snapshot: dict[str, dict[str, Any]] = {}
    if not catalog_dir.is_dir():
        return snapshot
    for bucket, keys in _ENRICHED_KEYS_BY_BUCKET.items():
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for json_file in bucket_dir.glob("*.json"):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            enriched = {k: data[k] for k in keys if data.get(k) is not None}
            if enriched:
                snapshot[json_file.stem] = enriched
    return snapshot


def restore_enriched_fields(
    project_root: Path, snapshot: dict[str, dict[str, Any]]
) -> None:
    """Restore LLM-enriched fields into catalog files after re-extraction."""
    catalog_dir = resolve_catalog_dir(project_root)
    for fqn, enriched in snapshot.items():
        for bucket in ("tables", "procedures", "views", "functions"):
            path = catalog_dir / bucket / f"{fqn}.json"
            if not path.exists():
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            changed = False
            for key, value in enriched.items():
                if data.get(key) != value:
                    data[key] = value
                    changed = True
            if changed:
                _write_json(path, data)
            break
        else:
            logger.debug("event=catalog_restore_skip fqn=%s reason=not_found_after_reextract", fqn)
