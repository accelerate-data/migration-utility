"""Tests for the shared plugin worktree helper."""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "shared" / "scripts" / "worktree.sh"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _write_git_shim(bin_dir: Path, log_path: Path) -> None:
    _write_executable(
        bin_dir / "git",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "git $*" >> "{log_path}"

if [[ "$1" == "show-ref" ]]; then
  if [[ "${{FAKE_GIT_BRANCH_EXISTS:-0}}" == "1" ]]; then
    exit 0
  fi
  exit 1
fi

if [[ "$1" == "rev-parse" && "$2" == "--show-toplevel" ]]; then
  pwd
  exit 0
fi

if [[ "$1" == "worktree" && "$2" == "list" ]]; then
  if [[ -n "${{FAKE_GIT_WORKTREE_LIST:-}}" && -f "${{FAKE_GIT_WORKTREE_LIST}}" ]]; then
    cat "${{FAKE_GIT_WORKTREE_LIST}}"
  fi
  exit 0
fi

if [[ "$1" == "worktree" && "$2" == "add" ]]; then
  if [[ "$3" == "-b" ]]; then
    branch="$4"
    path="$5"
  else
    path="$3"
    branch="$4"
  fi
  mkdir -p "$path/lib" "$path/tests/evals"
  printf "[project]\\nname='x'\\n" > "$path/lib/pyproject.toml"
  printf "{{}}\\n" > "$path/tests/evals/package.json"
  if [[ "${{FAKE_EVAL_LOCKFILE:-1}}" == "1" ]]; then
    printf "{{\\n  \\"lockfileVersion\\": 3\\n}}\\n" > "$path/tests/evals/package-lock.json"
  fi
  printf "dotenv\\n" > "$path/.envrc"
  printf "worktree %s\\nHEAD deadbeef\\nbranch refs/heads/%s\\n\\n" "$path" "$branch" > "${{FAKE_GIT_WORKTREE_LIST_OUT:-/dev/null}}"
  exit 0
fi

echo "unexpected git invocation: $*" >&2
exit 99
""",
    )


def _base_env(
    tmp_path: Path,
    worktree_list_content: str = "",
    branch_exists: bool = False,
) -> tuple[dict[str, str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "calls.log"
    worktree_list_path = tmp_path / "worktree-list.txt"
    worktree_list_path.write_text(worktree_list_content, encoding="utf-8")
    worktree_list_out = tmp_path / "worktree-list-out.txt"
    worktree_base = tmp_path / "worktrees"

    _write_git_shim(bin_dir, log_path)
    _write_executable(
        bin_dir / "uv",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "uv $*" >> "{log_path}"
if [[ "${{FAKE_UV_FAIL_SYNC:-0}}" == "1" && "$1" == "sync" ]]; then
  exit 42
fi
if [[ "${{FAKE_UV_FAIL_RUN:-0}}" == "1" && "$1" == "run" ]]; then
  exit 17
fi
exit 0
""",
    )
    _write_executable(
        bin_dir / "npm",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "npm $*" >> "{log_path}"
if [[ "${{FAKE_NPM_FAIL:-0}}" == "1" ]]; then
  exit 23
fi
exit 0
""",
    )
    _write_executable(
        bin_dir / "direnv",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "direnv $*" >> "{log_path}"
if [[ "${{FAKE_DIRENV_FAIL:-0}}" == "1" ]]; then
  exit 19
fi
exit 0
""",
    )

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["WORKTREE_BASE_DIR"] = str(worktree_base)
    env["FAKE_GIT_BRANCH_EXISTS"] = "1" if branch_exists else "0"
    env["FAKE_GIT_WORKTREE_LIST"] = str(worktree_list_path)
    env["FAKE_GIT_WORKTREE_LIST_OUT"] = str(worktree_list_out)
    env["FAKE_EVAL_LOCKFILE"] = "1"
    return env, log_path


def test_worktree_script_creates_new_branch_and_bootstraps(tmp_path: Path) -> None:
    """New branches should create the worktree and run bootstrap steps."""
    env, log_path = _base_env(tmp_path)

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/040-profile", "040-profile", "feature/migrate-mart"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    expected_path = tmp_path / "worktrees" / "feature" / "migrate-mart" / "040-profile"
    assert expected_path.exists()
    assert not (expected_path / "tests" / "evals" / ".promptfoo").exists()
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "git show-ref --verify --quiet refs/heads/feature/migrate-mart/040-profile",
        "git worktree list --porcelain",
        f"git worktree add -b feature/migrate-mart/040-profile {expected_path} feature/migrate-mart",
        f"direnv allow {expected_path}",
        "uv sync --extra dev",
        "uv run python -c import pyodbc, oracledb, dbt.adapters.oracle, dbt.adapters.sqlserver",
        "npm ci --no-audit --no-fund",
    ]
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload == {
        "status": "ready",
        "branch": "feature/migrate-mart/040-profile",
        "base_branch": "feature/migrate-mart",
        "worktree_name": "040-profile",
        "worktree_path": str(expected_path),
        "reused": False,
    }


def test_worktree_script_attaches_existing_branch(tmp_path: Path) -> None:
    """Existing branches not checked out elsewhere should attach to a new worktree."""
    env, log_path = _base_env(tmp_path, branch_exists=True)

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/050-setup", "050-setup", "feature/migrate-mart"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    expected_path = tmp_path / "worktrees" / "feature" / "migrate-mart" / "050-setup"
    assert f"git worktree add {expected_path} feature/migrate-mart/050-setup" in log_path.read_text(encoding="utf-8")
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload["reused"] is False


def test_worktree_script_reuses_branch_checked_out_elsewhere(tmp_path: Path) -> None:
    """Existing checked-out branches should report the existing worktree as reusable state."""
    existing_path = tmp_path / "other" / "feature" / "migrate-mart" / "060-profile"
    worktree_list = (
        f"worktree {existing_path}\nHEAD deadbeef\nbranch refs/heads/feature/migrate-mart/060-profile\n\n"
    )
    env, _ = _base_env(tmp_path, worktree_list_content=worktree_list, branch_exists=True)

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/060-profile", "060-profile", "feature/migrate-mart"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload["status"] == "ready"
    assert payload["reused"] is True
    assert payload["existing_worktree_path"] == str(existing_path)
    assert payload["worktree_path"] == str(existing_path)
    assert "created worktree" not in result.stdout


def test_worktree_script_fails_when_uv_sync_fails(tmp_path: Path) -> None:
    """Bootstrap failures should stop with structured recovery output."""
    env, log_path = _base_env(tmp_path)
    env["FAKE_UV_FAIL_SYNC"] = "1"

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/070-sync-fails", "070-sync-fails", "feature/migrate-mart"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    payload = json.loads(result.stderr.strip())
    assert payload["code"] == "WORKTREE_UV_SYNC_FAILED"
    assert payload["can_retry"] is True
    assert "uv sync --extra dev" in payload["suggested_fix"]
    assert "npm install --no-audit --no-fund" not in log_path.read_text(encoding="utf-8")
    assert payload["base_branch"] == "feature/migrate-mart"
    assert payload["worktree_name"] == "070-sync-fails"


def test_worktree_script_fails_when_dependency_verification_fails(tmp_path: Path) -> None:
    """Dependency verification failures should produce structured guidance."""
    env, log_path = _base_env(tmp_path)
    env["FAKE_UV_FAIL_RUN"] = "1"

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/080-import-fails", "080-import-fails", "feature/migrate-mart"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    payload = json.loads(result.stderr.strip())
    assert payload["code"] == "WORKTREE_DEPENDENCY_VERIFICATION_FAILED"
    assert payload["can_retry"] is True
    assert "pyodbc" in payload["message"]
    assert "dbt.adapters.oracle" in payload["message"]
    assert log_path.read_text(encoding="utf-8").splitlines()[-1] == (
        "uv run python -c import pyodbc, oracledb, dbt.adapters.oracle, dbt.adapters.sqlserver"
    )
    assert payload["branch"] == "feature/migrate-mart/080-import-fails"


def test_worktree_script_falls_back_to_npm_install_without_lockfile(tmp_path: Path) -> None:
    """Worktree bootstrap should use npm install when the eval lockfile is absent."""
    env, log_path = _base_env(tmp_path)
    env["FAKE_EVAL_LOCKFILE"] = "0"

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/090-no-lockfile", "090-no-lockfile", "feature/migrate-mart"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines()[-1] == (
        "npm install --no-audit --no-fund"
    )
