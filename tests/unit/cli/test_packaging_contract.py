"""Packaging contract for shared, public, and internal distributions."""

from __future__ import annotations

import shutil
import subprocess
import sys
import json
import os
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


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "__PYVENV_LAUNCHER__"):
        env.pop(key, None)
    return env


def _build_wheel(project_dir: Path) -> Path:
    shutil.rmtree(project_dir / "dist", ignore_errors=True)
    subprocess.run(["uv", "build"], cwd=project_dir, check=True, capture_output=True, text=True, env=_subprocess_env())
    wheels = sorted((project_dir / "dist").glob("*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def _wheel_members(wheel_path: Path) -> set[str]:
    with zipfile.ZipFile(wheel_path) as archive:
        return set(archive.namelist())


def _install_wheels(tmp_path: Path, *wheels: Path) -> Path:
    venv_dir = tmp_path / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True, capture_output=True, text=True, env=_subprocess_env())
    python = venv_dir / "bin" / "python"
    subprocess.run([str(python), "-m", "pip", "install", *map(str, wheels)], check=True, capture_output=True, text=True, env=_subprocess_env(), cwd=tmp_path)
    return venv_dir


def _venv_site_packages(venv_dir: Path) -> Path:
    result = subprocess.run(
        [str(venv_dir / "bin" / "python"), "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"],
        check=True,
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        cwd=venv_dir.parent,
    )
    return Path(result.stdout.strip())


def _installed_module_paths(venv_dir: Path) -> dict[str, Path]:
    result = subprocess.run(
        [
            str(venv_dir / "bin" / "python"),
            "-c",
            (
                "import importlib, json; "
                "mods = {"
                "'shared.cli.main': importlib.import_module('shared.cli.main').__file__, "
                "'ad_migration_cli.main': importlib.import_module('ad_migration_cli.main').__file__, "
                "'ad_migration_internal.entrypoints': importlib.import_module('ad_migration_internal.entrypoints').__file__"
                "}; "
                "print(json.dumps(mods))"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        cwd=venv_dir.parent,
    )
    return {name: Path(path) for name, path in json.loads(result.stdout).items()}


def _run_installed_smoke(venv_dir: Path) -> tuple[str, str]:
    version = subprocess.run(
        [str(venv_dir / "bin" / "ad-migration"), "--version"],
        check=True,
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        cwd=venv_dir.parent,
    ).stdout.strip()
    discover_help = subprocess.run(
        [str(venv_dir / "bin" / "discover"), "--help"],
        check=True,
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        cwd=venv_dir.parent,
    ).stdout
    return version, discover_help


def test_packaging_contract_matches_the_split_distribution_layout(tmp_path: Path) -> None:
    shared = _read_pyproject("lib/pyproject.toml")
    public = _read_pyproject("packages/ad-migration-cli/pyproject.toml")
    internal = _read_pyproject("packages/ad-migration-internal/pyproject.toml")

    assert shared["project"]["name"] == "ad-migration-shared"
    assert "scripts" not in shared["project"]

    assert public["project"]["name"] == "ad-migration-cli"
    assert public["project"]["dependencies"] == ["ad-migration-shared[export,oracle]==0.1.0"]
    assert public["project"]["scripts"] == {
        "ad-migration": "ad_migration_cli.main:app",
    }

    assert internal["project"]["name"] == "ad-migration-internal"
    assert internal["project"]["dependencies"] == ["ad-migration-shared==0.1.0"]
    assert "ad-migration" not in internal["project"].get("scripts", {})
    assert internal["project"]["scripts"] == {
        "catalog-enrich": "ad_migration_internal.entrypoints:catalog_enrich_app",
        "discover": "ad_migration_internal.entrypoints:discover_app",
        "generate-sources": "ad_migration_internal.entrypoints:generate_sources_app",
        "init": "ad_migration_internal.entrypoints:init_app",
        "migrate": "ad_migration_internal.entrypoints:migrate_app",
        "migrate-util": "ad_migration_internal.entrypoints:migrate_util_app",
        "profile": "ad_migration_internal.entrypoints:profile_app",
        "refactor": "ad_migration_internal.entrypoints:refactor_app",
        "setup-ddl": "ad_migration_internal.entrypoints:setup_ddl_app",
        "test-harness": "ad_migration_internal.entrypoints:test_harness_app",
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

    venv_dir = _install_wheels(tmp_path, shared_wheel, public_wheel, internal_wheel)
    site_packages = _venv_site_packages(venv_dir)
    installed_paths = _installed_module_paths(venv_dir)
    assert installed_paths["shared.cli.main"].is_relative_to(site_packages)
    assert installed_paths["ad_migration_cli.main"].is_relative_to(site_packages)
    assert installed_paths["ad_migration_internal.entrypoints"].is_relative_to(site_packages)

    version, discover_help = _run_installed_smoke(venv_dir)
    assert version == "0.1.0"
    assert "Usage:" in discover_help

    doctor_help = subprocess.run(
        [str(venv_dir / "bin" / "ad-migration"), "doctor", "drivers", "--help"],
        check=True,
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        cwd=venv_dir.parent,
    ).stdout
    assert "Usage:" in doctor_help
