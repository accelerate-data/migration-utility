from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import (
    load_and_merge_catalog,
    load_table_catalog,
    write_json as _write_catalog_json,
)
from shared.catalog_models import TableProfileSection, ViewProfileSection
from shared.env_config import resolve_catalog_dir
from shared.loader import CatalogFileMissingError, CatalogLoadError
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)


def derive_table_profile_status(section: TableProfileSection) -> str:
    """Derive the persisted status for a validated table profile."""
    resolved_kind = section.classification.resolved_kind if section.classification else None
    if resolved_kind == "seed":
        return "ok"
    if resolved_kind and section.primary_key is not None:
        return "ok"
    if resolved_kind:
        return "partial"
    return "error"


def derive_view_profile_status(section: ViewProfileSection) -> str:
    """Derive the persisted status for a validated view profile."""
    return "ok"


def _profile_payload_with_status(
    section: TableProfileSection | ViewProfileSection,
    status: str,
) -> dict[str, Any]:
    """Return the validated profile payload with derived status preserved."""
    return section.model_copy(update={"status": status}).model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
        exclude_unset=True,
    )


def _write_view_profile(project_root: Path, view_norm: str, profile_json: dict[str, Any]) -> dict[str, Any]:
    """Validate and merge a profile section into a view catalog file."""
    profile_section = ViewProfileSection.model_validate(profile_json)
    profile_payload = _profile_payload_with_status(
        profile_section,
        derive_view_profile_status(profile_section),
    )

    catalog_path = resolve_catalog_dir(project_root) / "views" / f"{view_norm}.json"
    if not catalog_path.exists():
        raise CatalogFileMissingError("view", view_norm)

    try:
        existing = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(catalog_path), exc) from exc
    except OSError as exc:
        logger.error("event=write_failed operation=read_catalog view=%s error=%s", view_norm, exc)
        raise

    existing["profile"] = profile_payload

    try:
        _write_catalog_json(catalog_path, existing)
    except OSError as exc:
        logger.error("event=write_failed operation=atomic_write view=%s error=%s", view_norm, exc)
        raise

    logger.info("event=write_complete view=%s catalog_path=%s", view_norm, catalog_path)
    return {
        "ok": True,
        "table": view_norm,
        "catalog_path": str(catalog_path),
    }


def run_write(project_root: Path, table: str, profile_json: dict[str, Any]) -> dict[str, Any]:
    """Validate and merge a profile section into a table or view catalog file.

    Auto-detects whether the FQN refers to a view (catalog/views/) or table
    (catalog/tables/). View path is checked first.

    Returns a confirmation dict on success.
    Raises ValueError on validation failure, OSError/json.JSONDecodeError on IO error.
    """
    if "status" in profile_json:
        raise ValueError("status must not be passed — determined by CLI")

    norm = normalize(table)

    view_catalog_path = resolve_catalog_dir(project_root) / "views" / f"{norm}.json"
    if view_catalog_path.exists():
        return _write_view_profile(project_root, norm, profile_json)

    existing_table = load_table_catalog(project_root, norm)
    if existing_table is None:
        raise CatalogFileMissingError("table", norm)

    profile_section = TableProfileSection.model_validate(profile_json)
    resolved_kind = (
        profile_section.classification.resolved_kind
        if profile_section.classification is not None
        else None
    )
    if existing_table.is_seed and resolved_kind != "seed":
        raise ValueError(f"seed table profiles must use seed classification for {norm}")
    if resolved_kind == "seed" and not existing_table.is_seed:
        raise ValueError(f"seed classification requires is_seed: true for {norm}")

    profile_payload = _profile_payload_with_status(
        profile_section,
        derive_table_profile_status(profile_section),
    )

    result = load_and_merge_catalog(project_root, norm, "profile", profile_payload)
    logger.info("event=write_complete table=%s catalog_path=%s", norm, result["catalog_path"])
    return result
