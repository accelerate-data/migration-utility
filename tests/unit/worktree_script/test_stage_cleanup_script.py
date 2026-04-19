"""Tests for the shared stage cleanup helper."""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "shared" / "scripts" / "stage-cleanup.sh"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _base_env(tmp_path: Path) -> tuple[dict[str, str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "calls.log"

    _write_executable(
        bin_dir / "git",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "git $*" >> "{log_path}"
if [[ "$1" == "rev-parse" && "$2" == "--show-toplevel" ]]; then
  pwd
  exit 0
fi
if [[ "$1" == "worktree" && "$2" == "remove" ]]; then
  exit "${{FAKE_GIT_WORKTREE_REMOVE_EXIT:-0}}"
fi
if [[ "$1" == "show-ref" && "$2" == "--verify" && "$3" == "--quiet" ]]; then
  case "$4" in
    refs/heads/*)
      exit "${{FAKE_GIT_LOCAL_REF_EXISTS:-0}}"
      ;;
    refs/remotes/origin/*)
      exit "${{FAKE_GIT_REMOTE_REF_EXISTS:-0}}"
      ;;
  esac
fi
if [[ "$1" == "branch" && "$2" == "-d" ]]; then
  exit "${{FAKE_GIT_BRANCH_DELETE_EXIT:-0}}"
fi
if [[ "$1" == "push" && "$2" == "origin" && "$4" == "--delete" ]]; then
  exit "${{FAKE_GIT_PUSH_DELETE_EXIT:-0}}"
fi
if [[ "$1" == "push" && "$2" == "origin" && "$3" == "--delete" ]]; then
  exit "${{FAKE_GIT_PUSH_DELETE_EXIT:-0}}"
fi
if [[ "$1" == "worktree" && "$2" == "list" ]]; then
  if [[ "${{FAKE_GIT_WORKTREE_LIST_EXIT:-0}}" != "0" ]]; then
    exit "${{FAKE_GIT_WORKTREE_LIST_EXIT}}"
  fi
  cat "${{FAKE_GIT_WORKTREE_LIST:-/dev/null}}"
  exit 0
fi
echo "unexpected git invocation: $*" >&2
exit 99
""",
    )

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["FAKE_GIT_WORKTREE_LIST"] = str(tmp_path / "worktree-list.txt")
    env["FAKE_GIT_LOCAL_REF_EXISTS"] = "1"
    env["FAKE_GIT_REMOTE_REF_EXISTS"] = "1"
    (tmp_path / "worktree-list.txt").write_text("", encoding="utf-8")
    return env, log_path


def test_stage_cleanup_script_removes_worktree_and_branch(tmp_path: Path) -> None:
    """A present worktree and safe branch should be deleted in one pass."""
    env, log_path = _base_env(tmp_path)
    env["FAKE_GIT_LOCAL_REF_EXISTS"] = "0"
    env["FAKE_GIT_REMOTE_REF_EXISTS"] = "0"
    worktree_path = tmp_path / "worktrees" / "feature" / "migrate-mart" / "090-cleanup"
    worktree_path.mkdir(parents=True)
    (tmp_path / "worktree-list.txt").write_text(
        f"worktree {worktree_path}\nHEAD deadbeef\nbranch refs/heads/feature/migrate-mart/090-cleanup\n\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/090-cleanup", str(worktree_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "git worktree list --porcelain",
        f"git worktree remove {worktree_path}",
        "git show-ref --verify --quiet refs/heads/feature/migrate-mart/090-cleanup",
        "git show-ref --verify --quiet refs/remotes/origin/feature/migrate-mart/090-cleanup",
        "git branch -d feature/migrate-mart/090-cleanup",
        "git push origin --delete feature/migrate-mart/090-cleanup",
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "cleaned",
        "branch": "feature/migrate-mart/090-cleanup",
        "worktree_path": str(worktree_path),
    }


def test_stage_cleanup_script_is_idempotent_when_already_clean(tmp_path: Path) -> None:
    """Missing worktree and missing branch should report already_clean."""
    env, log_path = _base_env(tmp_path)
    worktree_path = tmp_path / "worktrees" / "feature" / "migrate-mart" / "091-cleanup"

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/091-cleanup", str(worktree_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "git worktree list --porcelain",
        "git show-ref --verify --quiet refs/heads/feature/migrate-mart/091-cleanup",
        "git show-ref --verify --quiet refs/remotes/origin/feature/migrate-mart/091-cleanup",
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "already_clean",
        "branch": "feature/migrate-mart/091-cleanup",
        "worktree_path": str(worktree_path),
    }


def test_stage_cleanup_script_blocks_when_listed_worktree_path_is_missing(tmp_path: Path) -> None:
    """A stale worktree reference should fail deterministically instead of cleaning."""
    env, log_path = _base_env(tmp_path)
    worktree_path = tmp_path / "worktrees" / "feature" / "migrate-mart" / "094-cleanup"
    (tmp_path / "worktree-list.txt").write_text(
        f"worktree {worktree_path}\nHEAD deadbeef\nbranch refs/heads/feature/migrate-mart/094-cleanup\n\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/094-cleanup", str(worktree_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "git worktree list --porcelain",
    ]
    payload = json.loads(result.stderr.strip())
    assert payload["status"] == "failed"
    assert payload["code"] == "WORKTREE_STALE_WORKTREE_REFERENCE"
    assert payload["branch"] == "feature/migrate-mart/094-cleanup"
    assert payload["worktree_path"] == str(worktree_path)


def test_stage_cleanup_script_fails_when_local_branch_deletion_fails(tmp_path: Path) -> None:
    """Branch deletion errors should not be reported as cleaned."""
    env, log_path = _base_env(tmp_path)
    env["FAKE_GIT_LOCAL_REF_EXISTS"] = "0"
    env["FAKE_GIT_REMOTE_REF_EXISTS"] = "1"
    env["FAKE_GIT_BRANCH_DELETE_EXIT"] = "1"
    worktree_path = tmp_path / "worktrees" / "feature" / "migrate-mart" / "092-cleanup"
    worktree_path.mkdir(parents=True)
    (tmp_path / "worktree-list.txt").write_text(
        f"worktree {worktree_path}\nHEAD deadbeef\nbranch refs/heads/feature/migrate-mart/092-cleanup\n\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/092-cleanup", str(worktree_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    payload = json.loads(result.stderr.strip())
    assert payload["status"] == "failed"
    assert payload["code"] == "LOCAL_BRANCH_DELETE_FAILED"
    assert payload["branch"] == "feature/migrate-mart/092-cleanup"
    assert payload["worktree_path"] == str(worktree_path)
    assert "git branch -d feature/migrate-mart/092-cleanup" in log_path.read_text(encoding="utf-8")


def test_stage_cleanup_script_reports_worktree_list_failure_as_json(tmp_path: Path) -> None:
    """Failed worktree list reads should return deterministic JSON instead of aborting."""
    env, _ = _base_env(tmp_path)
    env["FAKE_GIT_WORKTREE_LIST_EXIT"] = "23"

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/093-cleanup", str(tmp_path / "worktrees" / "feature" / "migrate-mart" / "093-cleanup")],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    payload = json.loads(result.stderr.strip())
    assert payload["status"] == "failed"
    assert payload["code"] == "WORKTREE_LIST_FAILED"
    assert payload["branch"] == "feature/migrate-mart/093-cleanup"
