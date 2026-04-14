"""Shared helpers for setup_ddl unit tests."""

from __future__ import annotations

import json
from pathlib import Path


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
