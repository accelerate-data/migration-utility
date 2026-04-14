"""Git helpers for ad-migration CLI commands."""
from __future__ import annotations

import subprocess
from pathlib import Path


def stage_and_commit(files: list[Path], message: str, project_root: Path) -> bool:
    """Stage specific files and commit. Returns True if a commit was made.

    Returns False silently when there is nothing to commit.
    Raises RuntimeError on git failures.
    """
    try:
        subprocess.run(
            ["git", "add", "--"] + [str(f) for f in files],
            cwd=project_root,
            check=True,
            capture_output=True,
        )
        result = subprocess.run(
            ["git", "commit", "-m", message],
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
        if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
            return False
        raise subprocess.CalledProcessError(result.returncode, "git commit", result.stderr)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"git operation failed: {exc.stderr or exc}") from exc


def is_git_repo(project_root: Path) -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=project_root,
        capture_output=True,
    )
    return result.returncode == 0
