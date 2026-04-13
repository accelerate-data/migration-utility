"""Regression coverage for the future root-level Claude plugin layout."""

from __future__ import annotations

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
    assert (REPO_ROOT / "commands").is_dir()
    assert (REPO_ROOT / "skills").is_dir()
    assert (REPO_ROOT / "lib" / "pyproject.toml").is_file()
    assert (REPO_ROOT / "mcp" / "ddl" / "server.py").is_file()

    assert not (REPO_ROOT / "plugin").exists(), "Legacy plugin/ directory should be removed"
