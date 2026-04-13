"""Tests for shared.env_config — git-repo enforcement and path resolution."""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from shared.env_config import (
    assert_git_repo,
    resolve_catalog_dir,
    resolve_dbt_project_path,
    resolve_ddl_dir,
    resolve_project_root,
)

# Path to the worktree — guaranteed to be inside a git repo.
_WORKTREE_ROOT = Path(__file__).resolve().parents[3]


class TestAssertGitRepo:
    def test_passes_inside_git_repo(self) -> None:
        """assert_git_repo does not raise when called from inside a git repo."""
        assert_git_repo(_WORKTREE_ROOT)  # should not raise

    def test_raises_outside_git_repo(self, tmp_path: Path) -> None:
        """assert_git_repo raises RuntimeError for a plain temp directory."""
        with pytest.raises(RuntimeError, match="Not inside a git repository"):
            assert_git_repo(tmp_path)

    def test_error_message_contains_path(self, tmp_path: Path) -> None:
        """The RuntimeError message includes the path that failed."""
        with pytest.raises(RuntimeError, match=str(tmp_path)):
            assert_git_repo(tmp_path)


class TestResolveProjectRoot:
    def test_returns_provided_path_inside_git_repo(self) -> None:
        """resolve_project_root returns the given path when it is a git repo."""
        result = resolve_project_root(_WORKTREE_ROOT)
        assert result == _WORKTREE_ROOT

    def test_returns_cwd_when_none_and_cwd_is_git_repo(self) -> None:
        """resolve_project_root falls back to CWD when None is passed."""
        with patch("shared.env_config.Path.cwd", return_value=_WORKTREE_ROOT):
            result = resolve_project_root(None)
        assert result == _WORKTREE_ROOT

    def test_raises_for_non_git_path(self, tmp_path: Path) -> None:
        """resolve_project_root raises RuntimeError for a non-git directory."""
        with pytest.raises(RuntimeError, match="Not inside a git repository"):
            resolve_project_root(tmp_path)

    def test_raises_when_cwd_is_not_git_repo(self, tmp_path: Path) -> None:
        """resolve_project_root raises when CWD is not a git repo."""
        with patch("shared.env_config.Path.cwd", return_value=tmp_path):
            with pytest.raises(RuntimeError, match="Not inside a git repository"):
                resolve_project_root(None)


class TestResolveDbtProjectPath:
    def test_returns_env_var_when_set(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """resolve_dbt_project_path returns $DBT_PROJECT_PATH when set."""
        monkeypatch.setenv("DBT_PROJECT_PATH", str(tmp_path))
        result = resolve_dbt_project_path(Path("/some/project"))
        assert result == tmp_path

    def test_returns_project_root_dbt_when_env_not_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_dbt_project_path returns <project_root>/dbt when env is unset."""
        monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)
        project_root = Path("/my/project")
        result = resolve_dbt_project_path(project_root)
        assert result == project_root / "dbt"

    def test_ignores_blank_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_dbt_project_path treats a blank env var as unset."""
        monkeypatch.setenv("DBT_PROJECT_PATH", "   ")
        project_root = Path("/my/project")
        result = resolve_dbt_project_path(project_root)
        assert result == project_root / "dbt"


class TestResolveDdlDir:
    def test_returns_env_var_when_set(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """resolve_ddl_dir returns $DDL_DIR when set."""
        monkeypatch.setenv("DDL_DIR", str(tmp_path))
        result = resolve_ddl_dir(Path("/some/project"))
        assert result == tmp_path

    def test_returns_project_root_ddl_when_env_not_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_ddl_dir returns <project_root>/ddl when env is unset."""
        monkeypatch.delenv("DDL_DIR", raising=False)
        project_root = Path("/my/project")
        result = resolve_ddl_dir(project_root)
        assert result == project_root / "ddl"

    def test_ignores_blank_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_ddl_dir treats a blank env var as unset."""
        monkeypatch.setenv("DDL_DIR", "  ")
        project_root = Path("/my/project")
        result = resolve_ddl_dir(project_root)
        assert result == project_root / "ddl"


class TestResolveCatalogDir:
    def test_returns_env_var_when_set(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """resolve_catalog_dir returns $CATALOG_DIR when set."""
        monkeypatch.setenv("CATALOG_DIR", str(tmp_path))
        result = resolve_catalog_dir(Path("/some/project"))
        assert result == tmp_path

    def test_returns_project_root_catalog_when_env_not_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_catalog_dir returns <project_root>/catalog when env is unset."""
        monkeypatch.delenv("CATALOG_DIR", raising=False)
        project_root = Path("/my/project")
        result = resolve_catalog_dir(project_root)
        assert result == project_root / "catalog"

    def test_ignores_blank_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_catalog_dir treats a blank env var as unset."""
        monkeypatch.setenv("CATALOG_DIR", "\t")
        project_root = Path("/my/project")
        result = resolve_catalog_dir(project_root)
        assert result == project_root / "catalog"
