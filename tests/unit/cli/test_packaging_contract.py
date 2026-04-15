"""Packaging contract for shared, public, and internal distributions."""

from __future__ import annotations

import tomllib
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


def test_packaging_contract_matches_the_split_distribution_layout() -> None:
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
