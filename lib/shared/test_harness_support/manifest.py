"""Manifest and backend resolution helpers for test_harness."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, NoReturn

import typer
from pydantic import ValidationError

from shared.loader_io import read_manifest
from shared.output_models import TestSpec
from shared.runtime_config import get_primary_technology, get_runtime_role, get_sandbox_name
from shared.sandbox import get_backend
from shared.sandbox.base import SandboxBackend

_ENV_ASSIGNMENT_RE = re.compile(r"^(?:export\s+)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=")


def _error_exit(
    code: str,
    message: str,
    exc: Exception | None = None,
    *,
    exit_code: int = 1,
) -> NoReturn:
    """Emit a JSON error and raise typer.Exit."""
    typer.echo(json.dumps({"status": "error", "errors": [
        {"code": code, "message": message}
    ]}))
    if exc is not None:
        raise typer.Exit(code=exit_code) from exc
    raise typer.Exit(code=exit_code)


def _validate_test_spec(spec_data: dict[str, Any]) -> TestSpec:
    """Validate a test spec via Pydantic and raise ValueError on failure."""
    try:
        return TestSpec.model_validate(spec_data)
    except ValidationError as exc:
        raise ValueError(f"Test spec validation failed: {exc}") from exc


def _dotenv_contains_key(project_root: Path, key: str) -> bool:
    env_file = project_root / ".env"
    if not env_file.exists():
        return False
    try:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            match = _ENV_ASSIGNMENT_RE.match(line.strip())
            if match and match.group("name") == key:
                return True
    except OSError:
        return False
    return False


def _password_env_names(manifest: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for role_name in ("source", "target", "sandbox"):
        role = get_runtime_role(manifest, role_name)
        if role is not None and role.connection.password_env:
            names.append(role.connection.password_env)
    return names


def _missing_env_guidance(project_root: Path, message: str, manifest: dict[str, Any]) -> str:
    referenced = [
        key for key in _password_env_names(manifest)
        if key in message
    ]
    dotenv_keys = [
        key for key in referenced
        if not os.environ.get(key) and _dotenv_contains_key(project_root, key)
    ]
    if not dotenv_keys:
        return message

    joined = ", ".join(dotenv_keys)
    return (
        f"{message}\n\n"
        f"{joined} {'is' if len(dotenv_keys) == 1 else 'are'} defined in .env, "
        "but this Claude session does not have the value loaded. Restart Claude from "
        "the migration project directory after direnv has loaded the environment, then rerun the command."
    )


def _load_manifest(project_root: Path) -> dict[str, Any]:
    """Load manifest.json via shared loader, converting errors to typer.Exit."""
    try:
        manifest = read_manifest(project_root)
    except ValueError as exc:
        _error_exit("MANIFEST_INVALID", str(exc), exc)
    except OSError as exc:
        _error_exit(
            "MANIFEST_READ_ERROR",
            f"Cannot read manifest.json: {exc}",
            exc,
            exit_code=2,
        )

    if get_primary_technology(manifest) is None:
        _error_exit(
            "MISSING_TECHNOLOGY",
            (
                "manifest.json does not define a supported runtime technology. "
                f"Configure runtime.source, runtime.target, or runtime.sandbox at {project_root}."
            ),
        )
    return manifest


def _create_backend(manifest: dict[str, Any], project_root: Path | None = None) -> SandboxBackend:
    """Instantiate the sandbox backend for the manifest's technology."""
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        _error_exit(
            "SANDBOX_RUNTIME_NOT_CONFIGURED",
            "manifest.json is missing runtime.sandbox. Run /setup-sandbox first.",
        )
    technology = sandbox_role.technology
    backend_cls = get_backend(technology)

    try:
        return backend_cls.from_env(manifest)
    except ValueError as exc:
        message = (
            _missing_env_guidance(project_root, str(exc), manifest)
            if project_root is not None
            else str(exc)
        )
        _error_exit("MISSING_ENV_VARS", message, exc)


def _resolve_sandbox_db(project_root: Path) -> tuple[str, dict[str, Any]]:
    """Read sandbox runtime from manifest or fail with SANDBOX_NOT_CONFIGURED."""
    manifest = _load_manifest(project_root)
    sandbox_name = get_sandbox_name(manifest)
    if not sandbox_name:
        _error_exit(
            "SANDBOX_NOT_CONFIGURED",
            "No sandbox configured in manifest.json. Run /setup-sandbox first.",
        )
    return sandbox_name, manifest
