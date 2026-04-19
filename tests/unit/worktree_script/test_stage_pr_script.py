"""Tests for the shared stage PR helper."""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "shared" / "scripts" / "stage-pr.sh"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _base_env(tmp_path: Path, *, existing_pr_json: str = "[]") -> tuple[dict[str, str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "calls.log"
    pr_view_path = tmp_path / "pr-view.json"
    pr_view_path.write_text(existing_pr_json, encoding="utf-8")

    _write_executable(
        bin_dir / "git",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "git $*" >> "{log_path}"
if [[ "$1" == "rev-parse" && "$2" == "--show-toplevel" ]]; then
  pwd
  exit 0
fi
if [[ "$1" == "push" ]]; then
  exit "${{FAKE_GIT_PUSH_EXIT:-0}}"
fi
echo "unexpected git invocation: $*" >&2
exit 99
""",
    )
    _write_executable(
        bin_dir / "gh",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "gh $*" >> "{log_path}"
if [[ "$1" == "pr" && "$2" == "list" ]]; then
  cat "{pr_view_path}"
  exit 0
fi
if [[ "$1" == "pr" && "$2" == "create" ]]; then
  printf '%s\\n' "${{FAKE_GH_CREATE_URL:-https://github.com/example/repo/pull/101}}"
  exit "${{FAKE_GH_CREATE_EXIT:-0}}"
fi
if [[ "$1" == "pr" && "$2" == "edit" ]]; then
  printf '%s\\n' "${{FAKE_GH_EDIT_URL:-https://github.com/example/repo/pull/101}}"
  exit "${{FAKE_GH_EDIT_EXIT:-0}}"
fi
if [[ "$1" == "pr" && "$2" == "view" ]]; then
  printf '%s\\n' "${{FAKE_GH_VIEW_JSON}}"
  exit "${{FAKE_GH_VIEW_EXIT:-0}}"
fi
echo "unexpected gh invocation: $*" >&2
exit 99
""",
    )

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["FAKE_GH_VIEW_JSON"] = '{"number": 101, "url": "https://github.com/example/repo/pull/101"}'
    return env, log_path


def test_stage_pr_script_creates_pr_from_branch_and_body_file(tmp_path: Path) -> None:
    """A branch without an open PR should push and create one deterministically."""
    env, log_path = _base_env(tmp_path)
    body_file = tmp_path / "body.md"
    body_file.write_text("body text\n", encoding="utf-8")

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/080-pr", "main", "Stage PR", str(body_file)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    created_url = body_file.resolve()
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "git push --force-with-lease --set-upstream origin feature/migrate-mart/080-pr",
        "gh pr list --head feature/migrate-mart/080-pr --base main --json number,url --limit 1",
        f"gh pr create --title Stage PR --body-file {created_url} --base main --head feature/migrate-mart/080-pr",
        "gh pr view feature/migrate-mart/080-pr --json number,url",
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "created",
        "branch": "feature/migrate-mart/080-pr",
        "base_branch": "main",
        "pr_number": 101,
        "pr_url": "https://github.com/example/repo/pull/101",
    }


def test_stage_pr_script_reports_push_failure_as_json(tmp_path: Path) -> None:
    """Git push failures should return deterministic JSON on stderr."""
    env, _ = _base_env(tmp_path)
    env["FAKE_GIT_PUSH_EXIT"] = "42"
    body_file = tmp_path / "body.md"
    body_file.write_text("body text\n", encoding="utf-8")

    result = subprocess.run(
        [str(SCRIPT_PATH), "feature/migrate-mart/081-pr", "main", "Stage PR", str(body_file)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    payload = json.loads(result.stderr.strip())
    assert payload["status"] == "failed"
    assert payload["branch"] == "feature/migrate-mart/081-pr"
    assert payload["base_branch"] == "main"
    assert payload["code"] == "GIT_PUSH_FAILED"
