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


def hash_table_signals(signals: dict[str, Any]) -> str:
    """Return the SHA-256 hex digest of a canonical table-signals dict.

    The signals dict is serialized with sorted keys and compact separators
    to produce a deterministic string regardless of insertion order.
    """
    canonical = json.dumps(signals, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
