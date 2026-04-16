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
from shared.cli.output import console, error, print_table, remind_review_and_commit, success
from shared.loader_io import write_manifest_sandbox
from shared.runtime_config import get_extracted_schemas, get_runtime_role, set_runtime_role
from shared.runtime_config_models import RuntimeConnection, RuntimeRole
from shared.output_models.sandbox import SandboxUpOutput
from shared.sandbox.base import SandboxBackend
from shared.test_harness_support.manifest import _create_backend as _th_create_backend
from shared.test_harness_support.manifest import _load_manifest as _th_load_manifest

logger = logging.getLogger(__name__)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    """Thin wrapper around test_harness_support._load_manifest for patching."""
    return _th_load_manifest(project_root)


def _create_backend(manifest: dict[str, Any], project_root: Path | None = None) -> SandboxBackend:
    """Thin wrapper around test_harness_support._create_backend for patching."""
    return _th_create_backend(manifest, project_root)


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


def _get_configured_sandbox_name(manifest: dict[str, Any]) -> str | None:
    """Return only the canonical runtime sandbox name."""
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        return None
    if sandbox_role.technology == "sql_server":
        return sandbox_role.connection.database
    if sandbox_role.technology == "oracle":
        return sandbox_role.connection.schema_name
    return None


def _build_sandbox_connection_manifest(
    manifest: dict[str, Any], technology: str
) -> dict[str, Any]:
    """Return a manifest with refreshed sandbox connection metadata."""
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        raise ValueError("manifest.json is missing runtime.sandbox")

    if technology == "sql_server":
        connection = RuntimeConnection(
            host=os.environ.get("SANDBOX_MSSQL_HOST") or None,
            port=os.environ.get("SANDBOX_MSSQL_PORT") or None,
            database=sandbox_role.connection.database,
            user=os.environ.get("SANDBOX_MSSQL_USER") or None,
            password_env="SANDBOX_MSSQL_PASSWORD",
            driver=os.environ.get("MSSQL_DRIVER", "FreeTDS") or None,
        )
    elif technology == "oracle":
        connection = RuntimeConnection(
            host=os.environ.get("SANDBOX_ORACLE_HOST") or None,
            port=os.environ.get("SANDBOX_ORACLE_PORT") or None,
            service=os.environ.get("SANDBOX_ORACLE_SERVICE") or None,
            schema_name=sandbox_role.connection.schema_name,
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
    return set_runtime_role(manifest, "sandbox", updated_role)


def _write_sandbox_connection_to_manifest(
    root: Path, manifest: dict[str, Any], technology: str
) -> dict[str, Any]:
    """Read SANDBOX_* env vars and write runtime.sandbox.connection into manifest.json."""
    updated_manifest = _build_sandbox_connection_manifest(manifest, technology)
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
    require_sandbox_vars(technology, root)
    manifest = _build_sandbox_connection_manifest(manifest, technology)

    schemas = _get_schemas(manifest)

    if not yes:
        confirmed = typer.confirm(
            f"Create or reset sandbox database cloning schemas: {', '.join(schemas) or '(none)'}?"
        )
        if not confirmed:
            console.print("Aborted.")
            raise typer.Exit(code=0)

    backend = _create_backend(manifest, root)
    sandbox_database = _get_configured_sandbox_name(manifest)
    operation = "create"

    if sandbox_database:
        console.print(f"Checking sandbox: [bold]{sandbox_database}[/bold]...")
        with console.status("Checking existing sandbox..."):
            with cli_error_handler("checking existing sandbox database"):
                status = backend.sandbox_status(sandbox_database, schemas=schemas)

        if status.status == "error":
            if status.errors:
                for entry in status.errors:
                    error(f"[{entry.code}] {entry.message}")
            raise typer.Exit(code=1)

        if status.exists and status.has_content is True:
            operation = "reuse"
            console.print(
                f"Sandbox [bold]{sandbox_database}[/bold] already exists with cloned content; reusing it."
            )
            result = SandboxUpOutput(
                sandbox_database=sandbox_database,
                status="ok",
                tables_cloned=[],
                views_cloned=[],
                procedures_cloned=[],
                errors=[],
            )
        elif status.exists:
            operation = "repair"
            console.print(
                f"Sandbox [bold]{sandbox_database}[/bold] exists without cloned content; repairing it..."
            )
            with console.status("Running sandbox_reset..."):
                with cli_error_handler("resetting sandbox database"):
                    result = backend.sandbox_reset(sandbox_database, schemas=schemas)
        else:
            console.print(
                f"Configured sandbox [bold]{sandbox_database}[/bold] was not found; creating a new sandbox..."
            )
            with console.status("Running sandbox_up..."):
                with cli_error_handler("provisioning sandbox database"):
                    result = backend.sandbox_up(schemas=schemas)
    else:
        operation = "create"
        console.print(f"Provisioning sandbox for schemas: [bold]{', '.join(schemas)}[/bold]...")
        with console.status("Running sandbox_up..."):
            with cli_error_handler("provisioning sandbox database"):
                result = backend.sandbox_up(schemas=schemas)

    logger.info(
        "event=sandbox_setup operation=%s status=%s sandbox_database=%s tables=%d views=%d procedures=%d errors=%d",
        operation,
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

    _write_sandbox_connection_to_manifest(root, manifest, technology)
    _write_sandbox_to_manifest(root, result.sandbox_database)

    print_table(
        "Sandbox Setup",
        [
            ("Database", result.sandbox_database),
            ("Operation", operation),
            ("Tables cloned", str(len(result.tables_cloned))),
            ("Views cloned", str(len(result.views_cloned))),
            ("Procedures cloned", str(len(result.procedures_cloned))),
            ("Status", result.status),
        ],
        columns=("", ""),
    )
    remind_review_and_commit()
