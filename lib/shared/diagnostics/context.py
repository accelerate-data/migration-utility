"""Catalog context construction helpers for diagnostics."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shared.loader import DdlCatalog, DdlEntry

logger = logging.getLogger(__name__)


@dataclass
class CatalogContext:
    """Read-only context bag passed to every diagnostic check function."""

    project_root: Path
    dialect: str
    fqn: str
    object_type: str
    catalog_data: dict[str, Any]
    known_fqns: dict[str, set[str]]
    ddl_entry: DdlEntry | None = None
    pass1_results: dict[str, list[Any]] | None = None
    package_members: set[str] | None = None


def load_package_members(project_root: Path) -> set[str] | None:
    """Load Oracle package members from staging/packages.json."""
    for subdir in ("staging", ".staging"):
        packages_path = project_root / subdir / "packages.json"
        if packages_path.exists():
            try:
                rows = json.loads(packages_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("event=load_package_members error=%s", exc)
                return None
            members: set[str] = set()
            for row in rows:
                schema = row.get("schema_name", "")
                member = row.get("member_name", "")
                if schema and member:
                    members.add(f"{schema}.{member}".lower())
            return members if members else None
    return None


def build_known_fqns(catalog_dir: Path) -> dict[str, set[str]]:
    """Glob catalog directories to build a set of known FQNs per bucket."""
    known: dict[str, set[str]] = {}
    for bucket in ("tables", "procedures", "views", "functions"):
        bucket_dir = catalog_dir / bucket
        if bucket_dir.is_dir():
            known[bucket] = {path.stem for path in bucket_dir.glob("*.json")}
        else:
            known[bucket] = set()
    return known


def build_ddl_lookup(ddl_catalog: DdlCatalog) -> dict[str, DdlEntry]:
    """Build a flat FQN -> DdlEntry lookup from a DdlCatalog."""
    lookup: dict[str, DdlEntry] = {}
    for bucket_name in ("tables", "procedures", "views", "functions"):
        for fqn, entry in getattr(ddl_catalog, bucket_name).items():
            lookup[fqn] = entry
    return lookup
