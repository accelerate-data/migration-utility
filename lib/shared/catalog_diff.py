"""Diff-aware classification of catalog objects for incremental reexport.

Compares fresh DDL hashes (computed from staging data) against hashes stored
in existing catalog JSON files, and classifies every object as unchanged,
changed, new, or removed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from shared.ddl_hash import hash_definition, hash_table_signals
from shared.env_config import resolve_catalog_dir
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)


@dataclass
class DiffResult:
    """Classification of objects after comparing fresh vs existing hashes."""

    unchanged: set[str] = field(default_factory=set)
    changed: set[str] = field(default_factory=set)
    new: set[str] = field(default_factory=set)
    removed: set[str] = field(default_factory=set)


def compute_object_hashes(
    definitions_rows: list[dict[str, Any]],
    table_signals: dict[str, dict[str, Any]],
    object_types: dict[str, str],
) -> dict[str, str]:
    """Compute fresh DDL hashes for every object from staging data.

    For procs/views/functions: hashes the ``definition`` field from
    ``definitions.json`` staging rows.  For tables: hashes the canonical
    JSON of the table-signals dict.

    Returns ``{normalized_fqn: sha256_hex}``.
    """
    hashes: dict[str, str] = {}

    # Hash definitions for procs/views/functions
    for row in definitions_rows:
        definition = row.get("definition")
        if definition:
            fqn = normalize(f"{row['schema_name']}.{row['object_name']}")
            hashes[fqn] = hash_definition(definition)

    # Hash table signals
    for fqn, signals in table_signals.items():
        hashes[normalize(fqn)] = hash_table_signals(signals)

    # Ensure all objects in object_types have an entry (even if empty hash)
    for fqn, bucket in object_types.items():
        norm = normalize(fqn)
        if norm not in hashes:
            if bucket == "tables" and norm in table_signals:
                hashes[norm] = hash_table_signals(table_signals[norm])
            # Procs/views/functions without a definition row get no hash —
            # they will be treated as new/changed on first encounter.

    return hashes


def load_existing_hashes(project_root: Path) -> dict[str, str | None]:
    """Scan all catalog JSON files and extract ``{fqn: ddl_hash}``.

    Objects without a ``ddl_hash`` field (pre-migration catalogs) map to
    ``None``, which causes them to be classified as *changed* so they are
    rewritten with a hash on the next run.
    """
    catalog_dir = resolve_catalog_dir(project_root)
    result: dict[str, str | None] = {}

    if not catalog_dir.is_dir():
        return result

    for bucket in ("tables", "procedures", "views", "functions"):
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for json_file in bucket_dir.glob("*.json"):
            fqn = json_file.stem  # already normalized (lowercase)
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                result[fqn] = data.get("ddl_hash")
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "event=load_existing_hash fqn=%s error=%s", fqn, exc,
                )
                result[fqn] = None

    return result


def classify_objects(
    fresh_hashes: dict[str, str],
    existing_hashes: dict[str, str | None],
) -> DiffResult:
    """Compare fresh vs existing hashes and classify each object.

    - *new*: in fresh but not in existing
    - *removed*: in existing but not in fresh
    - *changed*: in both but hash differs, or existing hash is ``None``
    - *unchanged*: in both with matching hash
    """
    fresh_fqns = set(fresh_hashes.keys())
    existing_fqns = set(existing_hashes.keys())

    result = DiffResult(
        new=fresh_fqns - existing_fqns,
        removed=existing_fqns - fresh_fqns,
    )

    for fqn in fresh_fqns & existing_fqns:
        existing_hash = existing_hashes[fqn]
        if existing_hash is not None and existing_hash == fresh_hashes[fqn]:
            result.unchanged.add(fqn)
        else:
            result.changed.add(fqn)

    return result
