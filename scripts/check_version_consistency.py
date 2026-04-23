#!/usr/bin/env python3
"""Check release-facing package versions match the plugin manifests."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATHS = (
    "lib/pyproject.toml",
    "packages/ad-migration-cli/pyproject.toml",
    "packages/ad-migration-internal/pyproject.toml",
    "mcp/ddl/pyproject.toml",
)
SHARED_PACKAGE = "ad-migration-shared[export,oracle,sql-server]"


def _read_pyproject(path: str) -> dict:
    return tomllib.loads((REPO_ROOT / path).read_text(encoding="utf-8"))


def _read_plugin_manifest(path: str) -> dict:
    return json.loads((REPO_ROOT / path / "plugin.json").read_text())


def _dependency_pin(pyproject: dict, package: str) -> str | None:
    prefix = f"{package}=="
    for dependency in pyproject["project"].get("dependencies", []):
        if dependency.startswith(prefix):
            return dependency.removeprefix(prefix)
    return None


def check_version_consistency() -> list[str]:
    claude_plugin = _read_plugin_manifest(".claude-plugin")
    codex_plugin = _read_plugin_manifest(".codex-plugin")
    public_cli = _read_pyproject("packages/ad-migration-cli/pyproject.toml")
    internal_cli = _read_pyproject("packages/ad-migration-internal/pyproject.toml")

    plugin_version = claude_plugin["version"]
    package_versions = {
        path: _read_pyproject(path)["project"]["version"] for path in PYPROJECT_PATHS
    }
    plugin_versions = {
        ".claude-plugin/plugin.json": claude_plugin["version"],
        ".codex-plugin/plugin.json": codex_plugin["version"],
    }
    dependency_pins = {
        "packages/ad-migration-cli/pyproject.toml dependency": _dependency_pin(
            public_cli, SHARED_PACKAGE
        ),
        "packages/ad-migration-internal/pyproject.toml dependency": _dependency_pin(
            internal_cli, SHARED_PACKAGE
        ),
    }

    errors: list[str] = []
    for label, version in plugin_versions.items():
        if version != plugin_version:
            errors.append(f"{label} has version {version}, expected {plugin_version}")

    for label, version in package_versions.items():
        if version != plugin_version:
            errors.append(f"{label} has version {version}, expected {plugin_version}")

    for label, version in dependency_pins.items():
        if version != plugin_version:
            errors.append(f"{label} pins {version}, expected {plugin_version}")

    return errors


def main() -> int:
    errors = check_version_consistency()
    if errors:
        for error in errors:
            print(f"::error::{error}", file=sys.stderr)
        return 1

    version = _read_plugin_manifest(".claude-plugin")["version"]
    print(f"Version consistency audit passed for {version}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
