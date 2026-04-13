"""Centralised env-var and path resolution for migration-utility CLI tools."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path


def assert_git_repo(path: Path) -> None:
    """Raise RuntimeError if *path* is not inside a git repository."""
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=path,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Not inside a git repository: {path}. "
            "Migration artifacts must be tracked in git."
        )


def resolve_project_root(path: Path | None) -> Path:
    """Return *path* or CWD, then assert the result is inside a git repo."""
    resolved = path if path is not None else Path.cwd()
    assert_git_repo(resolved)
    return resolved


def resolve_dbt_project_path(project_root: Path) -> Path:
    """Return $DBT_PROJECT_PATH or <project_root>/dbt."""
    raw = os.environ.get("DBT_PROJECT_PATH", "").strip()
    return Path(raw) if raw else project_root / "dbt"


def resolve_ddl_dir(project_root: Path) -> Path:
    """Return $DDL_DIR or <project_root>/ddl."""
    raw = os.environ.get("DDL_DIR", "").strip()
    return Path(raw) if raw else project_root / "ddl"


def resolve_catalog_dir(project_root: Path) -> Path:
    """Return $CATALOG_DIR or <project_root>/catalog."""
    raw = os.environ.get("CATALOG_DIR", "").strip()
    return Path(raw) if raw else project_root / "catalog"
