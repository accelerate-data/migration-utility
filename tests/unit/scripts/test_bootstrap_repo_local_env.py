"""Tests for scripts/bootstrap_repo_local_env.py."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "bootstrap_repo_local_env.py"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _prepare_repo(repo_root: Path) -> None:
    (repo_root / "lib").mkdir(parents=True, exist_ok=True)
    (repo_root / "mcp" / "ddl").mkdir(parents=True, exist_ok=True)
    (repo_root / "tests" / "evals").mkdir(parents=True, exist_ok=True)
    (repo_root / "lib" / "pyproject.toml").write_text(
        "[project]\nname='shared'\n", encoding="utf-8"
    )
    (repo_root / "lib" / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (repo_root / "mcp" / "ddl" / "pyproject.toml").write_text(
        "[project]\nname='ddl-mcp'\n",
        encoding="utf-8",
    )
    (repo_root / "mcp" / "ddl" / "uv.lock").write_text(
        "version = 1\n", encoding="utf-8"
    )
    (repo_root / "tests" / "evals" / "package.json").write_text(
        '{"name":"evals"}\n', encoding="utf-8"
    )
    (repo_root / "tests" / "evals" / "package-lock.json").write_text(
        '{\n  "lockfileVersion": 3\n}\n',
        encoding="utf-8",
    )


def _write_shims(bin_dir: Path, log_path: Path) -> None:
    _write_executable(
        bin_dir / "uv",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "uv $*" >> "{log_path}"
if [[ "${{FAKE_UV_FAIL_LIB_SYNC:-0}}" == "1" && "${{PWD}}" == *"/lib" && "$1" == "sync" ]]; then
  exit 41
fi
if [[ "${{FAKE_UV_FAIL_LIB_VERIFY:-0}}" == "1" && "${{PWD}}" == *"/lib" && "$1" == "run" ]]; then
  exit 42
fi
if [[ "$1" == "sync" ]]; then
  mkdir -p .venv
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
  exit 43
fi
mkdir -p node_modules
exit 0
""",
    )


def _run(
    tmp_path: Path, *args: str, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    repo_root = tmp_path / "repo"
    if not repo_root.exists():
        repo_root.mkdir()
        _prepare_repo(repo_root)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    log_path = tmp_path / "calls.log"
    _write_shims(bin_dir, log_path)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    if extra_env:
        env.update(extra_env)

    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--repo-root", str(repo_root), *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_ensure_all_bootstraps_missing_envs_and_writes_stamps(tmp_path: Path) -> None:
    result = _run(tmp_path, "ensure", "all")

    assert result.returncode == 0
    repo_root = tmp_path / "repo"
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert log_lines == [
        "uv sync --extra dev",
        "uv run python -c import pyodbc, oracledb, dbt.adapters.oracle, dbt.adapters.sqlserver",
        "uv sync",
        "npm ci --no-audit --no-fund",
    ]
    assert (repo_root / "lib" / ".venv" / ".bootstrap-fingerprint.json").is_file()
    assert (
        repo_root / "mcp" / "ddl" / ".venv" / ".bootstrap-fingerprint.json"
    ).is_file()
    assert (
        repo_root / "tests" / "evals" / "node_modules" / ".bootstrap-fingerprint.json"
    ).is_file()


def test_ensure_all_skips_fresh_environments(tmp_path: Path) -> None:
    first = _run(tmp_path, "ensure", "all")
    assert first.returncode == 0

    second = _run(tmp_path, "ensure", "all", "--quiet")
    assert second.returncode == 0
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert log_lines == [
        "uv sync --extra dev",
        "uv run python -c import pyodbc, oracledb, dbt.adapters.oracle, dbt.adapters.sqlserver",
        "uv sync",
        "npm ci --no-audit --no-fund",
    ]


def test_ensure_detects_lockfile_drift_and_resyncs_only_changed_target(
    tmp_path: Path,
) -> None:
    first = _run(tmp_path, "ensure", "all")
    assert first.returncode == 0

    package_lock = tmp_path / "repo" / "tests" / "evals" / "package-lock.json"
    package_lock.write_text('{\n  "lockfileVersion": 4\n}\n', encoding="utf-8")

    second = _run(tmp_path, "ensure", "all", "--quiet")
    assert second.returncode == 0
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert log_lines == [
        "uv sync --extra dev",
        "uv run python -c import pyodbc, oracledb, dbt.adapters.oracle, dbt.adapters.sqlserver",
        "uv sync",
        "npm ci --no-audit --no-fund",
        "npm ci --no-audit --no-fund",
    ]


def test_ensure_returns_specific_code_for_lib_verify_failure(tmp_path: Path) -> None:
    result = _run(tmp_path, "ensure", "lib", extra_env={"FAKE_UV_FAIL_LIB_VERIFY": "1"})

    assert result.returncode == 11
    assert "dbt.adapters.oracle" in result.stderr


def test_ensure_falls_back_to_npm_install_without_lockfile(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _prepare_repo(repo_root)
    (repo_root / "tests" / "evals" / "package-lock.json").unlink()

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "calls.log"
    _write_shims(bin_dir, log_path)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--repo-root",
            str(repo_root),
            "ensure",
            "tests_evals",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines() == [
        "npm install --no-audit --no-fund"
    ]
    payload = json.loads(
        (
            repo_root
            / "tests"
            / "evals"
            / "node_modules"
            / ".bootstrap-fingerprint.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["target"] == "tests_evals"
