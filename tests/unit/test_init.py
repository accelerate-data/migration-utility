"""Tests for init.py CLI.

Unit tests verify scaffold-project and scaffold-hooks produce correct output.
Tests call run_* functions directly (testability pattern).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from shared.init import (
    CLAUDE_MD,
    GIT_WORKFLOW_MD,
    GITIGNORE_ENTRIES,
    PRE_COMMIT_HOOK,
    README_MD,
    REPO_MAP_JSON,
    run_scaffold_hooks,
    run_scaffold_project,
)


# ── scaffold-project ─────────────────────────────────────────────────────────


class TestScaffoldProject:
    def test_creates_all_files_in_empty_dir(self, tmp_path: Path) -> None:
        result = run_scaffold_project(tmp_path)
        assert "CLAUDE.md" in result["files_created"]
        assert "README.md" in result["files_created"]
        assert "repo-map.json" in result["files_created"]
        assert ".gitignore" in result["files_created"]
        assert ".envrc" in result["files_created"]
        assert ".claude/rules/git-workflow.md" in result["files_created"]
        assert result["files_updated"] == []
        assert result["files_skipped"] == []

        # Verify file contents
        assert (tmp_path / "CLAUDE.md").read_text() == CLAUDE_MD
        assert (tmp_path / "README.md").read_text() == README_MD
        repo_map = json.loads((tmp_path / "repo-map.json").read_text())
        assert repo_map == REPO_MAP_JSON
        assert ".mcp.json" in (tmp_path / ".gitignore").read_text()
        assert ".envrc" in (tmp_path / ".gitignore").read_text()
        assert "MSSQL_HOST" in (tmp_path / ".envrc").read_text()
        workflow = (tmp_path / ".claude" / "rules" / "git-workflow.md").read_text()
        assert "Worktree" in workflow
        assert "../worktrees" in workflow

    def test_idempotent_skips_existing_files(self, tmp_path: Path) -> None:
        # First run creates everything
        run_scaffold_project(tmp_path)
        # Second run skips everything
        result = run_scaffold_project(tmp_path)
        assert result["files_created"] == []
        assert result["files_updated"] == []
        assert len(result["files_skipped"]) == 6

    def test_merges_missing_gitignore_entries(self, tmp_path: Path) -> None:
        # Create a partial .gitignore
        (tmp_path / ".gitignore").write_text("# Custom\n.DS_Store\n")
        result = run_scaffold_project(tmp_path)
        updated = [f for f in result["files_updated"] if f.startswith(".gitignore")]
        assert len(updated) == 1
        content = (tmp_path / ".gitignore").read_text()
        # Original entry preserved
        assert "# Custom" in content
        # New entries added
        assert ".mcp.json" in content
        assert ".envrc" in content

    def test_reports_missing_claude_md_sections(self, tmp_path: Path) -> None:
        # Write a CLAUDE.md with only Domain section
        (tmp_path / "CLAUDE.md").write_text("# Project\n\n## Domain\n\nSome domain info.\n")
        result = run_scaffold_project(tmp_path)
        skipped = [f for f in result["files_skipped"] if f.startswith("CLAUDE.md")]
        assert len(skipped) == 1
        assert "missing sections" in skipped[0]

    def test_complete_claude_md_skipped_cleanly(self, tmp_path: Path) -> None:
        # Write a CLAUDE.md with all required sections
        (tmp_path / "CLAUDE.md").write_text(CLAUDE_MD)
        result = run_scaffold_project(tmp_path)
        assert "CLAUDE.md" in result["files_skipped"]


# ── scaffold-hooks ───────────────────────────────────────────────────────────


class TestScaffoldHooks:
    def test_creates_pre_commit_hook(self, tmp_path: Path) -> None:
        # Init a git repo so git config works
        subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        result = run_scaffold_hooks(tmp_path)
        assert result["hook_created"] is True
        assert result["hooks_path_configured"] is True

        hook_path = tmp_path / ".githooks" / "pre-commit"
        assert hook_path.exists()
        assert hook_path.stat().st_mode & 0o111  # executable
        assert "ANT_KEY_PAT" in hook_path.read_text()

    def test_idempotent_skips_existing_hook(self, tmp_path: Path) -> None:
        subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        run_scaffold_hooks(tmp_path)
        result = run_scaffold_hooks(tmp_path)
        assert result["hook_created"] is False
        assert result["hooks_path_configured"] is True

    def test_no_git_repo_still_creates_hook(self, tmp_path: Path) -> None:
        result = run_scaffold_hooks(tmp_path)
        assert result["hook_created"] is True
        # git config fails without a repo
        assert result["hooks_path_configured"] is False
        assert (tmp_path / ".githooks" / "pre-commit").exists()
