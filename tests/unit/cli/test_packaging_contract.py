"""Packaging contract for shared, public, and internal distributions."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()


def _read_pyproject(relative_path: str) -> dict[str, object]:
    with (REPO_ROOT / relative_path).open("rb") as handle:
        return tomllib.load(handle)


def _copy_packaging_tree(dst_root: Path) -> Path:
    repo_root = dst_root / "repo"
    repo_root.mkdir()
    shutil.copytree(REPO_ROOT / "lib", repo_root / "lib")
    shutil.copytree(REPO_ROOT / "packages", repo_root / "packages")
    return repo_root


def _build_wheel(project_dir: Path) -> Path:
    shutil.rmtree(project_dir / "dist", ignore_errors=True)
    subprocess.run(["uv", "build"], cwd=project_dir, check=True, capture_output=True, text=True)
    wheels = sorted((project_dir / "dist").glob("*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def _wheel_members(wheel_path: Path) -> set[str]:
    with zipfile.ZipFile(wheel_path) as archive:
        return set(archive.namelist())


def _run_installed_version_check(tmp_path: Path, shared_wheel: Path, public_wheel: Path) -> str:
    venv_dir = tmp_path / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True, capture_output=True, text=True)
    python = venv_dir / "bin" / "python"
    subprocess.run([str(python), "-m", "pip", "install", str(shared_wheel), str(public_wheel)], check=True, capture_output=True, text=True)
    result = subprocess.run([str(venv_dir / "bin" / "ad-migration"), "--version"], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def test_packaging_contract_matches_the_split_distribution_layout(tmp_path: Path) -> None:
    shared = _read_pyproject("lib/pyproject.toml")
    public = _read_pyproject("packages/ad-migration-cli/pyproject.toml")
    internal = _read_pyproject("packages/ad-migration-internal/pyproject.toml")

    assert shared["project"]["name"] == "ad-migration-shared"
    assert "scripts" not in shared["project"]

    assert public["project"]["name"] == "ad-migration-cli"
    assert public["project"]["scripts"] == {
        "ad-migration": "ad_migration_cli.main:app",
    }

    assert internal["project"]["name"] == "ad-migration-internal"
    assert "ad-migration" not in internal["project"].get("scripts", {})
    assert internal["project"]["scripts"] == {
        "discover": "ad_migration_internal.entrypoints:discover_app",
        "setup-ddl": "ad_migration_internal.entrypoints:setup_ddl_app",
    }

    repo_root = _copy_packaging_tree(tmp_path)
    public_wheel = _build_wheel(repo_root / "packages" / "ad-migration-cli")
    shared_wheel = _build_wheel(repo_root / "lib")
    internal_wheel = _build_wheel(repo_root / "packages" / "ad-migration-internal")

    public_members = _wheel_members(public_wheel)
    assert "ad_migration_cli/__init__.py" in public_members
    assert "ad_migration_cli/main.py" in public_members
    assert "ad_migration_cli-0.1.0.dist-info/entry_points.txt" in public_members

    internal_members = _wheel_members(internal_wheel)
    assert "ad_migration_internal/__init__.py" in internal_members
    assert "ad_migration_internal/entrypoints.py" in internal_members
    assert "ad_migration_internal-0.1.0.dist-info/entry_points.txt" in internal_members

    assert _run_installed_version_check(tmp_path, shared_wheel, public_wheel) == "0.1.0"
