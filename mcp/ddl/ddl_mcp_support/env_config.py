"""Minimal environment helpers used by the standalone DDL MCP server."""
from __future__ import annotations

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
