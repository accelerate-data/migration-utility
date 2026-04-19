"""Graph traversal helpers for diagnostics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_catalog_json(catalog_dir: Path, bucket: str, fqn: str) -> dict[str, Any] | None:
    """Load a catalog JSON file by bucket and FQN."""
    p = catalog_dir / bucket / f"{fqn}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _get_dep_fqns(catalog_data: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract all direct dependency (fqn, bucket) pairs from references.*.in_scope."""
    deps: list[tuple[str, str]] = []
    refs = catalog_data.get("references", {})
    for bucket in ("tables", "views", "functions", "procedures"):
        for entry in refs.get(bucket, {}).get("in_scope", []):
            fqn = f"{entry['schema']}.{entry['name']}".lower()
            deps.append((fqn, bucket))
    return deps
