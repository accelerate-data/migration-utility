"""Manifest and backend resolution helpers for test_harness."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, NoReturn

import typer
from pydantic import ValidationError

from shared.loader_io import read_manifest
from shared.output_models import TestSpec
from shared.runtime_config import get_primary_technology, get_runtime_role, get_sandbox_name
from shared.sandbox import get_backend
from shared.sandbox.base import SandboxBackend


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
            f"manifest.json is missing required 'technology' key at {project_root}",
        )
    return manifest


def _create_backend(manifest: dict[str, Any]) -> SandboxBackend:
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
        _error_exit("MISSING_ENV_VARS", str(exc), exc)


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
