"""Regression checks for package/plugin version consistency audits."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()


def _load_checker():
    script_path = REPO_ROOT / "scripts" / "check_version_consistency.py"
    spec = importlib.util.spec_from_file_location("check_version_consistency", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_version_consistency_checker_accepts_current_repo() -> None:
    checker = _load_checker()

    assert checker.check_version_consistency() == []


def test_version_consistency_checker_uses_plugin_version_as_source_of_truth(
    monkeypatch,
) -> None:
    checker = _load_checker()

    def fake_pyproject(path: str) -> dict:
        if path == "lib/pyproject.toml":
            return {"project": {"version": "0.1.0"}}
        if path == "packages/ad-migration-cli/pyproject.toml":
            return {
                "project": {
                    "version": "0.1.0",
                    "dependencies": [
                        "ad-migration-shared[export,oracle,sql-server]==0.1.0"
                    ],
                }
            }
        if path == "packages/ad-migration-internal/pyproject.toml":
            return {
                "project": {
                    "version": "0.1.0",
                    "dependencies": [
                        "ad-migration-shared[export,oracle,sql-server]==0.1.0"
                    ],
                }
            }
        if path == "mcp/ddl/pyproject.toml":
            return {"project": {"version": "0.1.0"}}
        raise AssertionError(path)

    monkeypatch.setattr(checker, "_read_plugin_manifest", lambda: {"version": "0.1.2"})
    monkeypatch.setattr(checker, "_read_pyproject", fake_pyproject)

    errors = checker.check_version_consistency()

    assert "lib/pyproject.toml has version 0.1.0, expected 0.1.2" in errors
    assert (
        "packages/ad-migration-cli/pyproject.toml has version 0.1.0, expected 0.1.2"
        in errors
    )
    assert (
        "packages/ad-migration-internal/pyproject.toml has version 0.1.0, expected 0.1.2"
        in errors
    )
    assert "mcp/ddl/pyproject.toml has version 0.1.0, expected 0.1.2" in errors
    assert (
        "packages/ad-migration-cli/pyproject.toml dependency pins 0.1.0, expected 0.1.2"
        in errors
    )
    assert (
        "packages/ad-migration-internal/pyproject.toml dependency pins 0.1.0, expected 0.1.2"
        in errors
    )
