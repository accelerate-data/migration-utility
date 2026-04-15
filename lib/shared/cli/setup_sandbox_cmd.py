"""setup-sandbox command — provision sandbox database from manifest config."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import typer

from shared.cli.env_check import require_sandbox_vars
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, print_table, success
from shared.loader_io import write_manifest_sandbox
from shared.runtime_config import get_extracted_schemas, get_runtime_role, set_runtime_role
from shared.runtime_config_models import RuntimeConnection, RuntimeRole
from shared.sandbox.base import SandboxBackend
from shared.test_harness_support.manifest import _create_backend as _th_create_backend
from shared.test_harness_support.manifest import _load_manifest as _th_load_manifest

logger = logging.getLogger(__name__)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    """Thin wrapper around test_harness_support._load_manifest for patching."""
    return _th_load_manifest(project_root)


def _create_backend(manifest: dict[str, Any]) -> SandboxBackend:
    """Thin wrapper around test_harness_support._create_backend for patching."""
    return _th_create_backend(manifest)


def _get_schemas(manifest: dict[str, Any]) -> list[str]:
    """Return extracted schemas from manifest."""
    return get_extracted_schemas(manifest)


def _get_sandbox_technology(manifest: dict[str, Any]) -> str:
    """Read sandbox technology from manifest. Exits 1 if runtime.sandbox is absent."""
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        error("manifest.json is missing runtime.sandbox. Run init-ad-migration first.")
        raise typer.Exit(code=1)
    return sandbox_role.technology


def _write_sandbox_connection_to_manifest(
    root: Path, manifest: dict[str, Any], technology: str
) -> dict[str, Any]:
    """Read SANDBOX_* env vars and write runtime.sandbox.connection into manifest.json."""
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        raise ValueError("manifest.json is missing runtime.sandbox")

    if technology == "sql_server":
        connection = RuntimeConnection(
            host=os.environ.get("SANDBOX_MSSQL_HOST") or None,
            port=os.environ.get("SANDBOX_MSSQL_PORT") or None,
            user=os.environ.get("SANDBOX_MSSQL_USER") or None,
            password_env="SANDBOX_MSSQL_PASSWORD",
            driver=os.environ.get("MSSQL_DRIVER", "FreeTDS") or None,
        )
    elif technology == "oracle":
        connection = RuntimeConnection(
            host=os.environ.get("SANDBOX_ORACLE_HOST") or None,
            port=os.environ.get("SANDBOX_ORACLE_PORT") or None,
            service=os.environ.get("SANDBOX_ORACLE_SERVICE") or None,
            user=os.environ.get("SANDBOX_ORACLE_USER") or None,
            password_env="SANDBOX_ORACLE_PASSWORD",
        )
    else:
        raise ValueError(f"Unsupported sandbox technology: {technology}")

    updated_role = RuntimeRole(
        technology=sandbox_role.technology,
        dialect=sandbox_role.dialect,
        connection=connection,
        schemas=sandbox_role.schemas,
    )
    updated_manifest = set_runtime_role(manifest, "sandbox", updated_role)
    manifest_path = root / "manifest.json"
    manifest_path.write_text(
        json.dumps(updated_manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "event=sandbox_connection_written component=setup_sandbox_cmd technology=%s",
        technology,
    )
    return updated_manifest


def _write_sandbox_to_manifest(project_root: Path, sandbox_database: str) -> None:
    """Persist sandbox database name into manifest.json."""
    write_manifest_sandbox(project_root, sandbox_database)


def setup_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Provision sandbox schema from manifest runtime.sandbox configuration."""
    root = project_root if project_root is not None else Path.cwd()

    manifest = _load_manifest(root)
    technology = _get_sandbox_technology(manifest)
    require_sandbox_vars(technology)
    manifest = _write_sandbox_connection_to_manifest(root, manifest, technology)

    schemas = _get_schemas(manifest)

    if not yes:
        confirmed = typer.confirm(
            f"Create sandbox database cloning schemas: {', '.join(schemas) or '(none)'}?"
        )
        if not confirmed:
            console.print("Aborted.")
            raise typer.Exit(code=0)

    backend = _create_backend(manifest)

    console.print(f"Provisioning sandbox for schemas: [bold]{', '.join(schemas)}[/bold]...")
    with console.status("Running sandbox_up..."):
        with cli_error_handler("provisioning sandbox database"):
            result = backend.sandbox_up(schemas=schemas)

    logger.info(
        "event=sandbox_up status=%s sandbox_database=%s tables=%d views=%d procedures=%d errors=%d",
        result.status,
        result.sandbox_database,
        len(result.tables_cloned),
        len(result.views_cloned),
        len(result.procedures_cloned),
        len(result.errors),
    )

    if result.status == "error":
        if result.errors:
            for entry in result.errors:
                error(f"[{entry.code}] {entry.message}")
        raise typer.Exit(code=1)

    _write_sandbox_to_manifest(root, result.sandbox_database)

    print_table(
        "Sandbox Setup",
        [
            ("Database", result.sandbox_database),
            ("Tables cloned", str(len(result.tables_cloned))),
            ("Views cloned", str(len(result.views_cloned))),
            ("Procedures cloned", str(len(result.procedures_cloned))),
            ("Status", result.status),
        ],
        columns=("", ""),
    )
