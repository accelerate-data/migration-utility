"""Deterministic hashing of DDL content for diff-aware catalog reexport.

Provides hash functions for both procedural objects (procs, views, functions)
whose DDL comes from ``OBJECT_DEFINITION()``, and tables whose "definition"
is reconstructed from column/constraint metadata.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_definition(definition: str) -> str:
    """Normalize a T-SQL definition for deterministic hashing.

    Collapses all whitespace runs to a single space, strips leading/trailing
    whitespace, and lowercases.  Does NOT strip comments — they can carry
    semantic intent in T-SQL.
    """
    return _WHITESPACE_RE.sub(" ", definition).strip().lower()


def hash_definition(definition: str) -> str:
    """Return the SHA-256 hex digest of a normalized ``OBJECT_DEFINITION()`` body."""
    normalized = normalize_definition(definition)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _canonicalize(obj: Any) -> Any:
    """Recursively sort lists of dicts by their JSON representation.

    SQL Server may return rows in different order across extractions
    (e.g. after index rebuilds), so list order must not affect the hash.
    """
    if isinstance(obj, dict):
        return {k: _canonicalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        canonicalized = [_canonicalize(item) for item in obj]
        # Sort lists of dicts by their canonical JSON; leave primitive lists as-is
        if canonicalized and isinstance(canonicalized[0], dict):
            canonicalized.sort(key=lambda x: json.dumps(x, sort_keys=True, separators=(",", ":")))
        return canonicalized
    return obj


def hash_table_signals(signals: dict[str, Any]) -> str:
    """Return the SHA-256 hex digest of a canonical table-signals dict.

    The signals dict is serialized with sorted keys and compact separators
    to produce a deterministic string regardless of insertion order.
    Lists of dicts (columns, PKs, FKs, etc.) are sorted by their canonical
    JSON representation to absorb row-order differences from SQL Server.
    """
    stable = _canonicalize(signals)
    canonical = json.dumps(stable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
