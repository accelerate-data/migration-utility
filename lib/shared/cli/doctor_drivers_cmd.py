"""Driver readiness checks for the public ad-migration runtime."""

from __future__ import annotations

import importlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import typer

from shared.cli import output
from shared.init import SOURCE_REGISTRY
from shared.runtime_config_models import ManifestModel, RuntimeRole

logger = logging.getLogger(__name__)

TECHNOLOGY_DRIVER_MODULES = {
    "oracle": "oracledb",
    "sql_server": "pyodbc",
}
ROLE_NAMES = ("source", "sandbox", "target")
MAINTAINER_REMEDIATION = (
    "Fix the public CLI package or Homebrew formula resources so this driver is "
    "bundled with the installed ad-migration runtime."
)


@dataclass(frozen=True)
class DriverRequirement:
    """One Python driver required by a supported technology."""

    technology: str
    driver_module: str
    roles: list[str]


@dataclass(frozen=True)
class DriverCheckResult:
    """Import check result for one required driver."""

    technology: str
    driver_module: str
    roles: list[str]
    importable: bool
    remediation: str | None = None
    error: str | None = None


def _load_manifest(project_root: Path) -> ManifestModel | None:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return None
    with manifest_path.open("r", encoding="utf-8") as handle:
        return ManifestModel.model_validate(json.load(handle))


def _configured_role_technologies(manifest: ManifestModel | None) -> dict[str, list[str]]:
    technologies: dict[str, list[str]] = {}
    if manifest is None or manifest.runtime is None:
        return technologies
    for role_name in ROLE_NAMES:
        role = getattr(manifest.runtime, role_name)
        if not isinstance(role, RuntimeRole):
            continue
        technologies.setdefault(role.technology, []).append(role_name)
    return technologies


def resolve_driver_requirements(project_root: Path) -> list[DriverRequirement]:
    """Resolve driver checks for all supported technologies plus configured role metadata."""

    manifest = _load_manifest(project_root)
    configured_roles = _configured_role_technologies(manifest)
    requirements: list[DriverRequirement] = []
    for technology in sorted(SOURCE_REGISTRY):
        driver_module = TECHNOLOGY_DRIVER_MODULES.get(technology)
        if driver_module is None:
            continue
        requirements.append(
            DriverRequirement(
                technology=technology,
                driver_module=driver_module,
                roles=configured_roles.get(technology, []),
            )
        )
    return requirements


def _check_requirement(requirement: DriverRequirement) -> DriverCheckResult:
    try:
        importlib.import_module(requirement.driver_module)
    except ImportError as exc:
        return DriverCheckResult(
            technology=requirement.technology,
            driver_module=requirement.driver_module,
            roles=requirement.roles,
            importable=False,
            remediation=MAINTAINER_REMEDIATION,
            error=str(exc),
        )
    return DriverCheckResult(
        technology=requirement.technology,
        driver_module=requirement.driver_module,
        roles=requirement.roles,
        importable=True,
    )


def _result_payload(results: list[DriverCheckResult]) -> dict[str, object]:
    return {
        "status": "ok" if all(result.importable for result in results) else "error",
        "drivers": [
            {
                "technology": result.technology,
                "driver_module": result.driver_module,
                "roles": result.roles,
                "importable": result.importable,
                "remediation": result.remediation,
                "error": result.error,
            }
            for result in results
        ],
    }


def _print_human_results(results: list[DriverCheckResult]) -> None:
    rows: list[tuple[str, str, str, str]] = []
    for result in results:
        role_text = ", ".join(result.roles) if result.roles else "supported"
        status = "importable" if result.importable else "missing"
        remediation = "" if result.importable else MAINTAINER_REMEDIATION
        rows.append((result.technology, result.driver_module, role_text, status))
        if remediation:
            output.error(f"{result.driver_module}: {remediation}")
    output.print_table(
        "Public CLI Driver Readiness",
        rows,
        columns=("Technology", "Driver", "Roles", "Status"),
    )


def drivers(
    project_root: Path = typer.Option(
        Path("."),
        "--project-root",
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Migration project root containing manifest.json.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Print machine-readable JSON output.",
    ),
) -> None:
    """Check supported backend Python drivers in the public CLI runtime."""

    logger.info(
        "event=doctor_drivers component=public_cli operation=check status=start project_root=%s",
        project_root,
    )
    try:
        requirements = resolve_driver_requirements(project_root)
    except Exception as exc:
        logger.exception(
            "event=doctor_drivers component=public_cli operation=load_manifest status=error project_root=%s",
            project_root,
        )
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}, indent=2))
        else:
            output.error(f"Could not read runtime manifest: {exc}")
        raise typer.Exit(1) from exc

    results = [_check_requirement(requirement) for requirement in requirements]
    payload = _result_payload(results)
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
    else:
        _print_human_results(results)

    if payload["status"] != "ok":
        logger.error(
            "event=doctor_drivers component=public_cli operation=check status=error project_root=%s",
            project_root,
        )
        raise typer.Exit(1)

    logger.info(
        "event=doctor_drivers component=public_cli operation=check status=success project_root=%s",
        project_root,
    )
