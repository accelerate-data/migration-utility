from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests import helpers


def test_run_setup_ddl_cli_requires_project_root() -> None:
    with pytest.raises(ValueError, match="requires --project-root"):
        helpers.run_setup_ddl_cli(["list-databases"])


def test_run_setup_ddl_cli_passes_through_when_project_root_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_run_python_module(module: str, args: list[str], *, cwd: Path, timeout: int) -> subprocess.CompletedProcess:
        captured["module"] = module
        captured["args"] = args
        captured["cwd"] = cwd
        captured["timeout"] = timeout
        return subprocess.CompletedProcess([module, *args], 0, stdout="{}", stderr="")

    monkeypatch.setattr(helpers, "run_python_module", fake_run_python_module)

    result = helpers.run_setup_ddl_cli(
        ["list-databases", "--project-root", str(tmp_path)],
        timeout=12,
    )

    assert result.returncode == 0
    assert captured["module"] == "shared.setup_ddl"
    assert captured["args"] == ["list-databases", "--project-root", str(tmp_path)]
    assert captured["cwd"] == helpers.SHARED_LIB_DIR
    assert captured["timeout"] == 12
