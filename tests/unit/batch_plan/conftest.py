"""Shared helpers for batch_plan unit tests."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

_TESTS_DIR = Path(__file__).parent
_FIXTURES = _TESTS_DIR / "fixtures"


def _make_project(
    src: Path = _FIXTURES,
) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy fixtures to a temp dir."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    shutil.copytree(src, dst)
    return tmp, dst


def _make_empty_project() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Create a project with no catalog objects."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    dst.mkdir(parents=True)
    (dst / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
        encoding="utf-8",
    )
    (dst / "catalog" / "tables").mkdir(parents=True)
    return tmp, dst


def _write_table_cat(path: Path, fqn: str, scoping: dict, extra: dict | None = None) -> None:
    schema, name = fqn.split(".", 1)
    data: dict = {"schema": schema, "name": name, "scoping": scoping}
    if extra:
        data.update(extra)
    path.write_text(json.dumps(data), encoding="utf-8")


def _make_minimal_project(tmp_path: Path) -> Path:
    """Create a minimal project with catalog/tables/ dir."""
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
    )
    return tmp_path
