"""Git helpers for ad-migration CLI commands."""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


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
            text=True,
        )
        result = subprocess.run(
            ["git", "commit", "-m", message],
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            logger.debug(
                "event=git_commit status=success component=git_ops message=%r project_root=%s",
                message,
                project_root,
            )
            return True
        if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
            logger.debug(
                "event=git_commit status=nothing_to_commit component=git_ops project_root=%s",
                project_root,
            )
            return False
        raise subprocess.CalledProcessError(result.returncode, "git commit", result.stderr)
    except subprocess.CalledProcessError as exc:
        logger.error(
            "event=git_commit status=failure component=git_ops error=%s",
            exc.stderr or exc,
        )
        raise RuntimeError(f"git operation failed: {exc.stderr or exc}") from exc


def git_push(project_root: Path) -> bool:
    """Push current branch to remote. Returns True on success, False on failure.

    Never raises — push failures are soft errors that the caller handles with a warning.
    """
    result = subprocess.run(
        ["git", "push"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        logger.debug(
            "event=git_push status=success component=git_ops project_root=%s",
            project_root,
        )
        return True
    logger.warning(
        "event=git_push status=failure component=git_ops project_root=%s stderr=%s",
        project_root,
        result.stderr,
    )
    return False


def is_git_repo(project_root: Path) -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=project_root,
        capture_output=True,
    )
    return result.returncode == 0
