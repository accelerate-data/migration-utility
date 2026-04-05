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
    GITIGNORE_ENTRIES,
    SOURCE_REGISTRY,
    get_source_config,
    run_scaffold_hooks,
    run_scaffold_project,
)


# ── scaffold-project (sql_server default) ───────────────────────────────────


class TestScaffoldProject:
    def test_creates_all_files_in_empty_dir(self, tmp_path: Path) -> None:
        config = get_source_config("sql_server")
        result = run_scaffold_project(tmp_path)
        assert "CLAUDE.md" in result["files_created"]
        assert "README.md" in result["files_created"]
        assert "repo-map.json" in result["files_created"]
        assert ".gitignore" in result["files_created"]
        assert ".envrc" in result["files_created"]
        assert ".claude/rules/git-workflow.md" in result["files_created"]
        assert result["files_updated"] == []
        assert result["files_skipped"] == []

        # Verify file contents match sql_server templates
        assert (tmp_path / "CLAUDE.md").read_text() == config.claude_md_fn()
        assert (tmp_path / "README.md").read_text() == config.readme_md_fn()
        repo_map = json.loads((tmp_path / "repo-map.json").read_text())
        assert repo_map == config.repo_map_fn()
        assert ".mcp.json" in (tmp_path / ".gitignore").read_text()
        assert ".envrc" in (tmp_path / ".gitignore").read_text()
        assert "MSSQL_HOST" in (tmp_path / ".envrc").read_text()
        workflow = (tmp_path / ".claude" / "rules" / "git-workflow.md").read_text()
        assert "Worktree" in workflow
        assert "../worktrees" in workflow

    def test_idempotent_skips_existing_files(self, tmp_path: Path) -> None:
        run_scaffold_project(tmp_path)
        result = run_scaffold_project(tmp_path)
        assert result["files_created"] == []
        assert result["files_updated"] == []
        assert len(result["files_skipped"]) == 6

    def test_merges_missing_gitignore_entries(self, tmp_path: Path) -> None:
        (tmp_path / ".gitignore").write_text("# Custom\n.DS_Store\n")
        result = run_scaffold_project(tmp_path)
        updated = [f for f in result["files_updated"] if f.startswith(".gitignore")]
        assert len(updated) == 1
        content = (tmp_path / ".gitignore").read_text()
        assert "# Custom" in content
        assert ".mcp.json" in content
        assert ".envrc" in content

    def test_reports_missing_claude_md_sections(self, tmp_path: Path) -> None:
        (tmp_path / "CLAUDE.md").write_text("# Project\n\n## Domain\n\nSome domain info.\n")
        result = run_scaffold_project(tmp_path)
        skipped = [f for f in result["files_skipped"] if f.startswith("CLAUDE.md")]
        assert len(skipped) == 1
        assert "missing sections" in skipped[0]

    def test_complete_claude_md_skipped_cleanly(self, tmp_path: Path) -> None:
        config = get_source_config("sql_server")
        (tmp_path / "CLAUDE.md").write_text(config.claude_md_fn())
        result = run_scaffold_project(tmp_path)
        assert "CLAUDE.md" in result["files_skipped"]


# ── scaffold-project (oracle) ───────────────────────────────────────────────


class TestScaffoldProjectOracle:
    def test_creates_oracle_files(self, tmp_path: Path) -> None:
        result = run_scaffold_project(tmp_path, technology="oracle")
        assert "CLAUDE.md" in result["files_created"]
        assert ".envrc" in result["files_created"]

        # Oracle-specific content
        envrc = (tmp_path / ".envrc").read_text()
        assert "ORACLE_HOST" in envrc
        assert "ORACLE_PORT" in envrc
        assert "ORACLE_SERVICE" in envrc
        assert "ORACLE_USER" in envrc
        assert "ORACLE_PASSWORD" in envrc
        assert "MSSQL" not in envrc

        claude_md = (tmp_path / "CLAUDE.md").read_text()
        assert "Oracle" in claude_md
        assert "SQLcl" in claude_md
        assert "auto-connect" in claude_md

        readme = (tmp_path / "README.md").read_text()
        assert "Oracle" in readme
        assert "SQLcl" in readme
        assert "Java 11+" in readme

        repo_map = json.loads((tmp_path / "repo-map.json").read_text())
        assert "oracle_env_vars" in repo_map["notes_for_agents"]
        assert "mssql_env_vars" not in repo_map["notes_for_agents"]

    def test_invalid_technology_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unknown technology"):
            run_scaffold_project(tmp_path, technology="postgres")


# ── scaffold-hooks ───────────────────────────────────────────────────────────


class TestScaffoldHooks:
    def test_creates_pre_commit_hook(self, tmp_path: Path) -> None:
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
        assert result["hooks_path_configured"] is False
        assert (tmp_path / ".githooks" / "pre-commit").exists()

    def test_oracle_hook_blocks_oracle_creds(self, tmp_path: Path) -> None:
        run_scaffold_hooks(tmp_path, technology="oracle")
        hook_content = (tmp_path / ".githooks" / "pre-commit").read_text()
        assert "ORACLE_PASSWORD" in hook_content
        assert "ORACLE_HOST" in hook_content
        # Oracle hook should NOT check for MSSQL patterns
        assert "SA_PASSWORD" not in hook_content
        assert "MSSQL" not in hook_content


# ── source registry ─────────────────────────────────────────────────────────


class TestSourceRegistry:
    def test_registry_has_both_sources(self) -> None:
        assert "sql_server" in SOURCE_REGISTRY
        assert "oracle" in SOURCE_REGISTRY

    def test_get_source_config_returns_config(self) -> None:
        config = get_source_config("sql_server")
        assert config.slug == "sql_server"
        assert config.display_name == "SQL Server"

    def test_get_source_config_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown technology"):
            get_source_config("postgres")

    def test_each_source_has_callable_templates(self) -> None:
        for slug, config in SOURCE_REGISTRY.items():
            assert callable(config.claude_md_fn), f"{slug} claude_md_fn not callable"
            assert callable(config.readme_md_fn), f"{slug} readme_md_fn not callable"
            assert callable(config.envrc_fn), f"{slug} envrc_fn not callable"
            assert callable(config.repo_map_fn), f"{slug} repo_map_fn not callable"
            assert callable(config.pre_commit_hook_fn), f"{slug} pre_commit_hook_fn not callable"
            # Verify they return non-empty content
            assert len(config.claude_md_fn()) > 0
            assert len(config.readme_md_fn()) > 0
            assert len(config.envrc_fn()) > 0
            assert len(config.repo_map_fn()) > 0
            assert len(config.pre_commit_hook_fn()) > 0
