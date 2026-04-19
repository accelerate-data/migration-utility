"""Tests for the shared stage PR merge helper."""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "shared" / "scripts" / "stage-pr-merge.sh"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _base_env(tmp_path: Path, *, pr_json: str) -> tuple[dict[str, str], Path]:
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
echo "unexpected git invocation: $*" >&2
exit 99
""",
    )
    _write_executable(
        bin_dir / "gh",
        f"""#!/usr/bin/env bash
set -euo pipefail
echo "gh $*" >> "{log_path}"
if [[ "$1" == "pr" && "$2" == "view" ]]; then
  if [[ "${{FAKE_GH_VIEW_EXIT:-0}}" != "0" ]]; then
    exit "${{FAKE_GH_VIEW_EXIT}}"
  fi
  printf '%s\\n' '{pr_json}'
  exit 0
fi
if [[ "$1" == "pr" && "$2" == "merge" ]]; then
  exit "${{FAKE_GH_MERGE_EXIT:-0}}"
fi
echo "unexpected gh invocation: $*" >&2
exit 99
""",
    )

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    return env, log_path


def test_stage_pr_merge_script_reports_checks_pending_before_merge(tmp_path: Path) -> None:
    """Pending checks should block merge and emit a deterministic status."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":101,"url":"https://github.com/example/repo/pull/101","mergeStateStatus":"BLOCKED","statusCheckRollup":[{"state":"PENDING"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "101", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        'gh pr view 101 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup',
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "checks_pending",
        "pr_number": 101,
        "pr_url": "https://github.com/example/repo/pull/101",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_merges_clean_pr(tmp_path: Path) -> None:
    """A clean PR should merge non-force and return a merged status."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":102,"url":"https://github.com/example/repo/pull/102","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "https://github.com/example/repo/pull/102", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "gh pr view https://github.com/example/repo/pull/102 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup",
        "gh pr merge https://github.com/example/repo/pull/102 --merge --delete-branch=false",
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merged",
        "pr_number": 102,
        "pr_url": "https://github.com/example/repo/pull/102",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_blocks_when_pr_base_branch_differs(tmp_path: Path) -> None:
    """A PR opened against a different base should not be merged into the requested base."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":103,"url":"https://github.com/example/repo/pull/103","baseRefName":"release","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "103", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        'gh pr view 103 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup',
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merge_conflict",
        "pr_number": 103,
        "pr_url": "https://github.com/example/repo/pull/103",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_blocks_when_merge_state_is_non_mergeable(tmp_path: Path) -> None:
    """Blocked merge states should return merge_conflict without calling gh pr merge."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":105,"url":"https://github.com/example/repo/pull/105","mergeStateStatus":"BLOCKED","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "105", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        'gh pr view 105 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup',
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merge_conflict",
        "pr_number": 105,
        "pr_url": "https://github.com/example/repo/pull/105",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_blocks_when_pr_is_closed(tmp_path: Path) -> None:
    """Closed PRs should not be merged even if their merge state is clean."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"CLOSED","number":106,"url":"https://github.com/example/repo/pull/106","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "106", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        'gh pr view 106 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup',
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merge_conflict",
        "pr_number": 106,
        "pr_url": "https://github.com/example/repo/pull/106",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_blocks_when_pr_is_draft(tmp_path: Path) -> None:
    """Draft PRs should not be merged even if their merge state is clean."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","isDraft":true,"number":107,"url":"https://github.com/example/repo/pull/107","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "107", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        'gh pr view 107 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup',
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merge_conflict",
        "pr_number": 107,
        "pr_url": "https://github.com/example/repo/pull/107",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_preserves_url_input_when_merging(tmp_path: Path) -> None:
    """URL inputs should be passed through to gh commands unchanged."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":108,"url":"https://github.com/example/repo/pull/108","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "https://github.com/example/fork/pull/108", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "gh pr view https://github.com/example/fork/pull/108 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup",
        "gh pr merge https://github.com/example/fork/pull/108 --merge --delete-branch=false",
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merged",
        "pr_number": 108,
        "pr_url": "https://github.com/example/repo/pull/108",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_accepts_query_string_url_input(tmp_path: Path) -> None:
    """Query-string PR URLs should be normalized for the PR number but preserved for gh."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":109,"url":"https://github.com/example/repo/pull/109?expand=1","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "https://github.com/example/fork/pull/109?expand=1", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "gh pr view https://github.com/example/fork/pull/109?expand=1 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup",
        "gh pr merge https://github.com/example/fork/pull/109?expand=1 --merge --delete-branch=false",
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merged",
        "pr_number": 109,
        "pr_url": "https://github.com/example/repo/pull/109?expand=1",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_accepts_fragment_url_input(tmp_path: Path) -> None:
    """Fragment PR URLs should be normalized for the PR number but preserved for gh."""
    env, log_path = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":110,"url":"https://github.com/example/repo/pull/110#issuecomment-1","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )

    result = subprocess.run(
        [str(SCRIPT_PATH), "https://github.com/example/fork/pull/110#issuecomment-1", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "git rev-parse --show-toplevel",
        "gh pr view https://github.com/example/fork/pull/110#issuecomment-1 --json state,number,url,baseRefName,isDraft,mergeStateStatus,statusCheckRollup",
        "gh pr merge https://github.com/example/fork/pull/110#issuecomment-1 --merge --delete-branch=false",
    ]
    payload = json.loads(result.stdout.strip())
    assert payload == {
        "status": "merged",
        "pr_number": 110,
        "pr_url": "https://github.com/example/repo/pull/110#issuecomment-1",
        "base_branch": "main",
    }


def test_stage_pr_merge_script_reports_view_failure_as_json(tmp_path: Path) -> None:
    """Failed PR reads should return deterministic JSON instead of aborting."""
    env, _ = _base_env(
        tmp_path,
        pr_json='{"state":"OPEN","number":104,"url":"https://github.com/example/repo/pull/104","mergeStateStatus":"CLEAN","statusCheckRollup":[{"state":"SUCCESS"}]}',
    )
    env["FAKE_GH_VIEW_EXIT"] = "17"

    result = subprocess.run(
        [str(SCRIPT_PATH), "104", "main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    payload = json.loads(result.stderr.strip())
    assert payload["status"] == "failed"
    assert payload["code"] == "GH_PR_VIEW_FAILED"
    assert payload["pr_number"] == 104
    assert payload["base_branch"] == "main"
