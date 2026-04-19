"""Tests for scripts/worktree.sh."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "worktree.sh"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _base_env(tmp_path: Path) -> tuple[dict[str, str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "calls.log"
    worktree_base = tmp_path / "worktrees"

    _write_executable(
        bin_dir / "git",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "git $*" >> "{log_path}"

if [[ "$1" == "show-ref" ]]; then
  exit 1
fi

if [[ "$1" == "worktree" && "$2" == "list" ]]; then
  exit 0
fi

if [[ "$1" == "worktree" && "$2" == "add" ]]; then
  path="$5"
  mkdir -p "$path/lib" "$path/tests/evals"
  printf "[project]\\nname='shared'\\n" > "$path/lib/pyproject.toml"
  printf "{{}}\\n" > "$path/tests/evals/package.json"
  printf "{{\\n  \\"lockfileVersion\\": 3\\n}}\\n" > "$path/tests/evals/package-lock.json"
  exit 0
fi

echo "unexpected git invocation: $*" >&2
exit 99
""",
    )
    _write_executable(
        bin_dir / "uv",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "uv $*" >> "{log_path}"
exit 0
""",
    )
    _write_executable(
        bin_dir / "npm",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "npm $*" >> "{log_path}"
exit 0
""",
    )

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["WORKTREE_BASE_DIR"] = str(worktree_base)
    return env, log_path


def test_maintainer_worktree_verifies_dbt_adapter_imports(tmp_path: Path) -> None:
    env, log_path = _base_env(tmp_path)

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/dbt-adapter-check"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert "uv run python -c import pyodbc, oracledb, dbt.adapters.oracle, dbt.adapters.sqlserver" in (
        log_path.read_text(encoding="utf-8").splitlines()
    )
