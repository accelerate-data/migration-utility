"""Regression coverage for the future root-level Claude plugin layout."""

from __future__ import annotations

import stat
from pathlib import Path


def _find_repo_root() -> Path:
    """Resolve the repository root from this test file."""
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()


def test_root_plugin_layout_matches_the_desired_structure() -> None:
    """The repository should expose the plugin layout at the repo root."""
    assert (REPO_ROOT / ".claude-plugin" / "plugin.json").is_file()
    assert (REPO_ROOT / ".codex-plugin" / "plugin.json").is_file()
    assert (REPO_ROOT / "commands").is_dir()
    assert (REPO_ROOT / "skills").is_dir()
    assert not (REPO_ROOT / "skills" / "git-checkpoints").exists()
    assert not (REPO_ROOT / "shared").exists()
    assert (REPO_ROOT / "scripts" / "worktree.sh").is_file()
    assert (REPO_ROOT / "scripts" / "stage-worktree.sh").is_file()
    assert (REPO_ROOT / "scripts" / "stage-pr.sh").is_file()
    assert (REPO_ROOT / "scripts" / "stage-pr-merge.sh").is_file()
    assert (REPO_ROOT / "scripts" / "stage-cleanup.sh").is_file()
    assert (REPO_ROOT / "scripts" / "README.md").is_file()
    assert (REPO_ROOT / "scripts" / "worktree.sh").stat().st_mode & stat.S_IXUSR
    assert (REPO_ROOT / "scripts" / "stage-worktree.sh").stat().st_mode & stat.S_IXUSR
    assert (REPO_ROOT / "scripts" / "stage-pr.sh").stat().st_mode & stat.S_IXUSR
    assert (REPO_ROOT / "scripts" / "stage-pr-merge.sh").stat().st_mode & stat.S_IXUSR
    assert (REPO_ROOT / "scripts" / "stage-cleanup.sh").stat().st_mode & stat.S_IXUSR
    assert (REPO_ROOT / "lib" / "pyproject.toml").is_file()
    assert (REPO_ROOT / "mcp" / "ddl" / "server.py").is_file()

    assert not (REPO_ROOT / "plugin").exists(), "Legacy plugin/ directory should be removed"


def test_removed_command_and_skill_surfaces_are_not_referenced_by_commands() -> None:
    """Plugin commands should not point users at removed wrapper surfaces."""
    command_text = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((REPO_ROOT / "commands").glob("*.md"))
    )

    assert not (REPO_ROOT / "commands" / "commit-push-pr.md").exists()
    assert not (REPO_ROOT / "commands" / "review-diagnostics.md").exists()
    assert not (REPO_ROOT / "skills" / "git-checkpoints").exists()
    assert "commit-push-pr" not in command_text
    assert "review-diagnostics" not in command_text
    assert "git-checkpoints" not in command_text
    assert "shared/scripts" not in command_text
    assert "scripts/stage-worktree.sh" in command_text
