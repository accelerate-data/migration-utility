"""Unit coverage for the Homebrew formula renderer."""

from __future__ import annotations

import sys
from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()
sys.path.insert(0, str(REPO_ROOT))

from scripts.update_homebrew_tap import render_formula


def test_render_formula_includes_shared_resource() -> None:
    formula = render_formula(
        version="0.1.0",
        cli_url="https://example.test/ad_migration_cli-0.1.0.tar.gz",
        cli_sha256="1" * 64,
        shared_url="https://example.test/ad_migration_shared-0.1.0.tar.gz",
        shared_sha256="2" * 64,
    )

    assert 'depends_on "freetds"' in formula
    assert 'depends_on "unixodbc"' in formula
    assert 'depends_on "maturin" => :build' in formula
    assert 'depends_on "rust" => :build' in formula
    assert 'resource "ad-migration-shared"' in formula
    assert 'resource "pydantic"' in formula
    assert 'resource "pydantic-core"' in formula
    assert 'resource "typer"' in formula
    assert "ad_migration_shared-0.1.0.tar.gz" in formula
    assert "pydantic_core-" in formula
    assert ".tar.gz" in formula
    assert "virtualenv_install_with_resources" in formula
    assert 'assert_match "0.1.0\\n", shell_output("#{bin}/ad-migration --version")' in formula
