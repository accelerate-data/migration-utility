#!/usr/bin/env python3
"""Validate Claude and Codex plugin manifests as one plugin contract."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:[-+][0-9A-Za-z.-]+)?$")

CLAUDE_REQUIRED_FIELDS = (
    "name",
    "description",
    "version",
    "license",
    "author",
    "repository",
    "keywords",
)
CODEX_REQUIRED_FIELDS = (
    "name",
    "description",
    "version",
    "license",
    "author",
    "repository",
    "keywords",
    "skills",
    "interface",
)
SHARED_FIELDS = (
    "name",
    "description",
    "version",
    "license",
    "author",
    "repository",
)


def _load_json(
    path: Path, label: str, errors: list[str], repo_root: Path
) -> dict[str, Any] | None:
    if not path.is_file():
        errors.append(f"{label} manifest is missing: {path.relative_to(repo_root)}")
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        errors.append(f"{label} manifest is invalid JSON: {exc}")
        return None
    if not isinstance(data, dict):
        errors.append(f"{label} manifest must be a JSON object")
        return None
    return data


def _missing_required(manifest: dict[str, Any], fields: tuple[str, ...], label: str) -> list[str]:
    return [
        f"{label} manifest is missing required field: {field}"
        for field in fields
        if manifest.get(field) in (None, "", [], {})
    ]


def _allowed_difference(manifest: dict[str, Any], field: str) -> str | None:
    allowed = manifest.get("allowedClaudeManifestDifferences", {})
    if not isinstance(allowed, dict):
        return None
    rationale = allowed.get(field)
    if isinstance(rationale, str) and rationale.strip():
        return rationale
    return None


def validate_plugin_manifests(repo_root: Path = REPO_ROOT) -> list[str]:
    claude_manifest_path = repo_root / ".claude-plugin" / "plugin.json"
    codex_manifest_path = repo_root / ".codex-plugin" / "plugin.json"
    codex_surface_doc_path = repo_root / "docs/reference/codex-plugin-surface/README.md"

    errors: list[str] = []
    claude = _load_json(claude_manifest_path, "Claude", errors, repo_root)
    codex = _load_json(codex_manifest_path, "Codex", errors, repo_root)
    if claude is None or codex is None:
        return errors

    errors.extend(_missing_required(claude, CLAUDE_REQUIRED_FIELDS, "Claude"))
    errors.extend(_missing_required(codex, CODEX_REQUIRED_FIELDS, "Codex"))

    for label, manifest in (("Claude", claude), ("Codex", codex)):
        version = manifest.get("version")
        if not isinstance(version, str) or not SEMVER_RE.match(version):
            errors.append(f"{label} manifest version must be semver")

    for field in SHARED_FIELDS:
        if claude.get(field) != codex.get(field) and not _allowed_difference(codex, field):
            errors.append(f"Claude and Codex manifests disagree on shared field: {field}")

    skills_path = codex.get("skills")
    if skills_path != "./skills/":
        errors.append("Codex manifest skills must point to ./skills/")
    elif not (repo_root / "skills").is_dir():
        errors.append("Codex manifest declares ./skills/ but skills/ is missing")

    mcp_path = codex.get("mcpServers")
    if mcp_path is not None:
        if mcp_path != "./.mcp.json":
            errors.append("Codex manifest mcpServers must point to ./.mcp.json")
        elif not (repo_root / ".mcp.json").is_file():
            errors.append("Codex manifest declares ./.mcp.json but .mcp.json is missing")

    if (repo_root / "commands").is_dir():
        if not codex_surface_doc_path.is_file():
            errors.append("Codex surface doc is missing required commands/ rationale")
        else:
            surface_doc = codex_surface_doc_path.read_text(encoding="utf-8")
            if "commands/" not in surface_doc or "Claude-only" not in surface_doc:
                errors.append("Codex surface doc must document commands/ as Claude-only")

    return errors


def main() -> int:
    errors = validate_plugin_manifests()
    if errors:
        for error in errors:
            print(f"::error::{error}", file=sys.stderr)
        return 1

    print("Plugin manifest validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
