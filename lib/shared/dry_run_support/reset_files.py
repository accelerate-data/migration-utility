"""Filesystem helpers for dry-run reset operations."""

from __future__ import annotations

import shutil
from pathlib import Path


def delete_if_present(path: Path) -> bool:
    if not path.exists():
        return False
    path.unlink()
    return True


def delete_tree_if_present(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return True


__all__ = ["delete_if_present", "delete_tree_if_present"]
