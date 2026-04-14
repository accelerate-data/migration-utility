import subprocess
from pathlib import Path

import pytest

from shared.cli.git_ops import is_git_repo, stage_and_commit


def _init_repo(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, check=True, capture_output=True)


def test_is_git_repo_returns_true_inside_repo(tmp_path):
    _init_repo(tmp_path)
    assert is_git_repo(tmp_path) is True


def test_is_git_repo_returns_false_outside_repo(tmp_path):
    assert is_git_repo(tmp_path) is False


def test_stage_and_commit_returns_true_on_success(tmp_path):
    _init_repo(tmp_path)
    f = tmp_path / "readme.txt"
    f.write_text("hello")
    result = stage_and_commit([f], "test: initial", tmp_path)
    assert result is True


def test_stage_and_commit_returns_false_when_nothing_to_commit(tmp_path):
    _init_repo(tmp_path)
    f = tmp_path / "readme.txt"
    f.write_text("hello")
    stage_and_commit([f], "test: initial", tmp_path)
    # Second commit on the same unchanged file should be nothing-to-commit.
    result = stage_and_commit([f], "test: no changes", tmp_path)
    assert result is False


def test_stage_and_commit_raises_on_git_failure(tmp_path):
    # Not a git repo — git add will fail.
    f = tmp_path / "readme.txt"
    f.write_text("hello")
    with pytest.raises(RuntimeError, match="git operation failed"):
        stage_and_commit([f], "test: fail", tmp_path)
