from __future__ import annotations

import shutil
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def ddl_path(tmp_path: Path) -> Path:
    """Copy the migrate fixtures to a temp directory and return the path."""
    dest = tmp_path / "project"
    shutil.copytree(FIXTURES, dest)
    return dest

@pytest.fixture()
def dbt_project(tmp_path: Path) -> Path:
    """Create a minimal dbt project in a temp directory."""
    dbt = tmp_path / "dbt"
    dbt.mkdir()
    (dbt / "dbt_project.yml").write_text(
        "name: 'test_project'\nversion: '1.0.0'\nconfig-version: 2\n"
    )
    (dbt / "models" / "marts").mkdir(parents=True)
    return dbt
