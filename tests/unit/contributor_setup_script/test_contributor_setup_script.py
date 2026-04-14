"""Tests for scripts/contributor-setup.sh."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "contributor-setup.sh"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _write_shims(bin_dir: Path, log_path: Path) -> None:
    _write_executable(
        bin_dir / "uname",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "${FAKE_UNAME:-Linux}"
""",
    )
    _write_executable(
        bin_dir / "git",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "git $*" >> "{log_path}"
if [[ "$1" == "--version" ]]; then
  echo "git version 2.47.0"
  exit 0
fi
exit 0
""",
    )
    _write_executable(
        bin_dir / "python3",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "python3 $*" >> "{log_path}"
if [[ "$1" == "--version" ]]; then
  echo "Python 3.11.9"
  exit 0
fi
exec "{sys.executable}" "$@"
""",
    )
    _write_executable(
        bin_dir / "uv",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "uv $*" >> "{log_path}"
if [[ "${{FAKE_UV_FAIL_LIB:-0}}" == "1" && "${{PWD}}" == *"/lib" ]]; then
  exit 41
fi
if [[ "${{FAKE_UV_FAIL_DDL:-0}}" == "1" && "${{PWD}}" == *"/mcp/ddl" ]]; then
  exit 42
fi
exit 0
""",
    )
    _write_executable(
        bin_dir / "node",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "node $*" >> "{log_path}"
echo "v22.0.0"
""",
    )
    _write_executable(
        bin_dir / "npm",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "npm $*" >> "{log_path}"
if [[ "$1" == "--version" ]]; then
  echo "10.0.0"
  exit 0
fi
if [[ "${{FAKE_NPM_FAIL:-0}}" == "1" && "$1" == "install" ]]; then
  exit 43
fi
exit 0
""",
    )
    _write_executable(
        bin_dir / "direnv",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "direnv $*" >> "{log_path}"
echo "2.35.0"
""",
    )
    _write_executable(
        bin_dir / "markdownlint",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "markdownlint $*" >> "{log_path}"
echo "0.0.0"
""",
    )
    _write_executable(
        bin_dir / "toolbox",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "toolbox $*" >> "{log_path}"
if [[ "${{FAKE_MISSING_TOOLBOX:-0}}" == "1" ]]; then
  exit 127
fi
echo "toolbox 1.0.0"
""",
    )
    _write_executable(
        bin_dir / "sql",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "sql $*" >> "{log_path}"
if [[ "${{FAKE_MISSING_SQL:-0}}" == "1" ]]; then
  exit 127
fi
echo "SQLcl: Release 24.1"
""",
    )
    _write_executable(
        bin_dir / "java",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "java $*" >> "{log_path}"
if [[ "${{FAKE_JAVA_VERSION:-17}}" == "bad" ]]; then
  echo 'java version "1.8.0_401"' >&2
  exit 0
fi
echo 'openjdk version "${{FAKE_JAVA_VERSION:-17}}.0.10"'
""",
    )
    _write_executable(
        bin_dir / "docker",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "{log_path}"
if [[ "${{FAKE_DOCKER_MISSING:-0}}" == "1" ]]; then
  exit 127
fi

if [[ "$1" == "--version" ]]; then
  echo "Docker version 27.0.0"
  exit 0
fi

if [[ "$1" == "info" ]]; then
  if [[ "${{FAKE_DOCKER_INFO_FAIL:-0}}" == "1" ]]; then
    exit 1
  fi
  echo "Server Version: 27.0.0"
  exit 0
fi

if [[ "$1" == "start" ]]; then
  if [[ "$2" == "sql-test" && "${{FAKE_SQL_CONTAINER_FAIL:-0}}" == "1" ]]; then
    exit 1
  fi
  if [[ "$2" == "oracle-test" && "${{FAKE_ORACLE_CONTAINER_FAIL:-0}}" == "1" ]]; then
    exit 1
  fi
  echo "$2"
  exit 0
fi

if [[ "$1" == "inspect" ]]; then
  if [[ "$4" == "sql-test" && "${{FAKE_SQL_CONTAINER_FAIL:-0}}" == "1" ]]; then
    exit 1
  fi
  if [[ "$4" == "oracle-test" && "${{FAKE_ORACLE_CONTAINER_FAIL:-0}}" == "1" ]]; then
    exit 1
  fi
  echo "true"
  exit 0
fi

if [[ "$1" == "exec" ]]; then
  container="$2"
  if [[ "$container" == "sql-test" && "${{FAKE_SQL_EXEC_FAIL:-0}}" == "1" ]]; then
    exit 1
  fi
  if [[ "$container" == "oracle-test" && "${{FAKE_ORACLE_EXEC_FAIL:-0}}" == "1" ]]; then
    exit 1
  fi
  echo "1"
  exit 0
fi

exit 0
""",
    )


def _prepare_repo(repo_root: Path) -> None:
    (repo_root / "lib").mkdir(parents=True, exist_ok=True)
    (repo_root / "mcp" / "ddl").mkdir(parents=True, exist_ok=True)
    (repo_root / "tests" / "evals").mkdir(parents=True, exist_ok=True)
    (repo_root / "lib" / "pyproject.toml").write_text("[project]\nname='shared'\n", encoding="utf-8")
    (repo_root / "mcp" / "ddl" / "pyproject.toml").write_text(
        "[project]\nname='ddl-mcp'\n",
        encoding="utf-8",
    )
    (repo_root / "tests" / "evals" / "package.json").write_text("{}", encoding="utf-8")


def _run_script(tmp_path: Path, *args: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _prepare_repo(repo_root)

    script_target = repo_root / "scripts" / "contributor-setup.sh"
    script_target.parent.mkdir(parents=True, exist_ok=True)
    script_target.write_text(SCRIPT_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    script_target.chmod(script_target.stat().st_mode | stat.S_IXUSR)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "calls.log"
    _write_shims(bin_dir, log_path)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["HOME"] = str(tmp_path / "home")
    if extra_env:
      env.update(extra_env)

    return subprocess.run(
        [str(script_target), *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def _parse_trailing_json(stdout: str) -> dict[str, object]:
    lines = [line for line in stdout.strip().splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_show_mode_reports_ready_when_docker_and_sql_server_work(tmp_path: Path) -> None:
    result = _run_script(
        tmp_path,
        "show",
        extra_env={
            "FAKE_MISSING_SQL": "1",
            "FAKE_SQL_CONTAINER_FAIL": "0",
            "FAKE_SQL_EXEC_FAIL": "0",
            "FAKE_ORACLE_CONTAINER_FAIL": "1",
        },
    )

    assert result.returncode == 0
    payload = _parse_trailing_json(result.stdout)
    assert payload["status"] == "ready"
    assert payload["mode"] == "show"
    assert payload["checks"]["docker"]["status"] == "ok"
    assert payload["checks"]["sql_server"]["status"] == "ok"
    assert payload["checks"]["oracle"]["status"] in {"manual_action", "blocked"}
    assert payload["summary"]["working_backends"] == ["sql_server"]
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert "uv sync --extra dev" not in log_lines
    assert "uv sync" not in log_lines
    assert "npm install --no-audit --no-fund" not in log_lines
    assert "docker start sql-test" not in log_lines
    assert "docker start oracle-test" not in log_lines


def test_fix_mode_bootstraps_repo_local_envs(tmp_path: Path) -> None:
    result = _run_script(tmp_path)

    assert result.returncode == 0
    payload = _parse_trailing_json(result.stdout)
    assert payload["status"] == "ready"
    assert payload["mode"] == "fix"
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert log_lines.count("uv sync --extra dev") == 1
    assert log_lines.count("uv sync") == 1
    assert "npm install --no-audit --no-fund" in log_lines
    assert "docker info" in log_lines


def test_fix_mode_reports_blocked_when_docker_daemon_unavailable(tmp_path: Path) -> None:
    result = _run_script(tmp_path, extra_env={"FAKE_DOCKER_INFO_FAIL": "1"})

    assert result.returncode != 0
    payload = _parse_trailing_json(result.stdout)
    assert payload["status"] == "blocked"
    assert payload["checks"]["docker"]["status"] == "blocked"
    assert payload["checks"]["sql_server"]["status"] == "skipped"
    assert payload["checks"]["oracle"]["status"] == "skipped"
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert "docker start sql-test" not in log_lines
    assert "docker start oracle-test" not in log_lines


def test_show_mode_rejects_unsupported_platform(tmp_path: Path) -> None:
    result = _run_script(tmp_path, "show", extra_env={"FAKE_UNAME": "MINGW64_NT-10.0"})

    assert result.returncode != 0
    payload = _parse_trailing_json(result.stdout)
    assert payload["status"] == "blocked"
    assert payload["checks"]["platform"]["status"] == "blocked"
    assert "Windows" in payload["checks"]["platform"]["message"]
    assert payload["checks"]["repo_bootstrap"]["status"] == "skipped"
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert "docker info" not in log_lines


def test_print_step_handles_quoted_json_messages() -> None:
    result = subprocess.run(
        [
            "bash",
            "-lc",
            (
                'eval "$(sed -n \'2,/^platform_check=""/p\' '
                f'"{SCRIPT_PATH}" | sed \'$d\')"; '
                'payload=$(json_check ok \'value with "quotes"\'); '
                'print_step platform "$payload"'
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == 'platform: ok - value with "quotes"'


def test_fix_mode_reports_partial_when_repo_bootstrap_fails_but_checks_continue(tmp_path: Path) -> None:
    result = _run_script(
        tmp_path,
        extra_env={
            "FAKE_UV_FAIL_DDL": "1",
            "FAKE_MISSING_SQL": "1",
            "FAKE_ORACLE_CONTAINER_FAIL": "1",
        },
    )

    assert result.returncode != 0
    payload = _parse_trailing_json(result.stdout)
    assert payload["status"] == "partially_ready"
    assert payload["checks"]["repo_bootstrap"]["status"] == "manual_action"
    assert payload["checks"]["sql_server"]["status"] == "ok"
    assert payload["summary"]["manual_actions"] == [
        "Repair repo-local bootstrap failures and rerun ./scripts/contributor-setup.sh."
    ]
    log_lines = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
    assert "docker info" in log_lines
