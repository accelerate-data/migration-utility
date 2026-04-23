"""Regression checks for Claude/Codex plugin manifest validation."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()


def _load_validator():
    script_path = REPO_ROOT / "scripts" / "validate_plugin_manifests.py"
    spec = importlib.util.spec_from_file_location("validate_plugin_manifests", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _manifest(version: str = "1.2.3", **overrides: Any) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "name": "ad-migration",
        "description": "Migrate stored procedures to dbt models.",
        "version": version,
        "license": "Elastic-2.0",
        "author": {"name": "Accelerate Data", "url": "https://github.com/accelerate-data"},
        "repository": "https://github.com/accelerate-data/migration-utility",
        "keywords": ["migration", "dbt"],
    }
    manifest.update(overrides)
    return manifest


def _write_fixture_repo(
    root: Path,
    *,
    codex_manifest: dict[str, Any] | None = None,
    surface_doc: str | None = "commands/ are Claude-only.",
) -> None:
    claude_manifest = _manifest()
    codex_manifest = (
        _manifest(
            skills="./skills/",
            mcpServers="./.mcp.json",
            interface={"displayName": "AD Migration"},
        )
        if codex_manifest is None
        else codex_manifest
    )

    (root / ".claude-plugin").mkdir()
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(claude_manifest), encoding="utf-8"
    )
    if codex_manifest:
        (root / ".codex-plugin").mkdir()
        (root / ".codex-plugin" / "plugin.json").write_text(
            json.dumps(codex_manifest), encoding="utf-8"
        )

    (root / "skills").mkdir()
    (root / "commands").mkdir()
    (root / ".mcp.json").write_text(json.dumps({"mcpServers": {}}), encoding="utf-8")
    if surface_doc is not None:
        doc_dir = root / "docs" / "reference" / "codex-plugin-surface"
        doc_dir.mkdir(parents=True)
        (doc_dir / "README.md").write_text(surface_doc, encoding="utf-8")


def test_manifest_validator_accepts_current_repo() -> None:
    validator = _load_validator()

    assert validator.validate_plugin_manifests() == []


def test_manifest_validator_accepts_valid_manifest_pair(tmp_path: Path) -> None:
    validator = _load_validator()
    _write_fixture_repo(tmp_path)

    assert validator.validate_plugin_manifests(tmp_path) == []


def test_manifest_validator_rejects_missing_codex_manifest(tmp_path: Path) -> None:
    validator = _load_validator()
    _write_fixture_repo(tmp_path, codex_manifest={})

    errors = validator.validate_plugin_manifests(tmp_path)

    assert any("Codex manifest is missing" in error for error in errors)


def test_manifest_validator_rejects_missing_codex_version(tmp_path: Path) -> None:
    validator = _load_validator()
    codex_manifest = _manifest(
        skills="./skills/",
        interface={"displayName": "AD Migration"},
    )
    del codex_manifest["version"]
    _write_fixture_repo(tmp_path, codex_manifest=codex_manifest)

    errors = validator.validate_plugin_manifests(tmp_path)

    assert "Codex manifest is missing required field: version" in errors


def test_manifest_validator_rejects_shared_field_mismatch(tmp_path: Path) -> None:
    validator = _load_validator()
    codex_manifest = _manifest(
        repository="https://github.com/accelerate-data/other",
        skills="./skills/",
        interface={"displayName": "AD Migration"},
    )
    _write_fixture_repo(tmp_path, codex_manifest=codex_manifest)

    errors = validator.validate_plugin_manifests(tmp_path)

    assert "Claude and Codex manifests disagree on shared field: repository" in errors


def test_manifest_validator_rejects_unsupported_skills_path(tmp_path: Path) -> None:
    validator = _load_validator()
    codex_manifest = _manifest(
        skills="./plugin-skills/",
        interface={"displayName": "AD Migration"},
    )
    _write_fixture_repo(tmp_path, codex_manifest=codex_manifest)

    errors = validator.validate_plugin_manifests(tmp_path)

    assert "Codex manifest skills must point to ./skills/" in errors


def test_manifest_validator_requires_claude_only_command_documentation(tmp_path: Path) -> None:
    validator = _load_validator()
    _write_fixture_repo(tmp_path, surface_doc="commands/ are supported everywhere.")

    errors = validator.validate_plugin_manifests(tmp_path)

    assert "Codex surface doc must document commands/ as Claude-only" in errors
