"""generate_sources.py — Build and write dbt sources.yml from catalog.

Only tables explicitly marked ``is_source: true`` are registered as dbt
sources. Tables with ``resolved`` status are procedure targets that will
become dbt models (referenced via ``ref()``). Tables with
``scoping.status == "no_writer_found"`` but no ``is_source`` flag appear in
the ``unconfirmed`` list — they need user confirmation via ``/add-source-tables``
or the ``/setup-target`` confirmation flow before they are included.

Exit codes:
    0  success
    1  domain failure (incomplete scoping with --strict)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import typer
from shared.cli_utils import emit
from shared.env_config import (
    resolve_catalog_dir,
    resolve_project_root,
)
from shared.generate_sources_support.candidates import (
    collect_source_candidates,
    list_confirmed_source_tables_from_dir,
    validate_source_namespace,
    validate_staging_contract_types,
)
from shared.generate_sources_support.sources_yaml import build_sources_yaml
from shared.generate_sources_support.staging import write_staging_artifacts
from shared.output_models.generate_sources import GenerateSourcesOutput
from shared.runtime_config import get_runtime_role

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


def _resolve_physical_source_schema(
    project_root: Path,
    source_schema_override: str | None = None,
) -> str | None:
    if source_schema_override:
        return source_schema_override
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    target_role = get_runtime_role(manifest, "target")
    if target_role is None or target_role.schemas is None:
        return None
    return target_role.schemas.source


def list_confirmed_source_tables(project_root: Path) -> list[str]:
    """Return confirmed source-table FQNs from persisted catalog state."""
    catalog_dir = resolve_catalog_dir(project_root)
    return list_confirmed_source_tables_from_dir(catalog_dir / "tables")


def generate_sources(
    project_root: Path,
    *,
    source_schema_override: str | None = None,
    require_staging_contract_types: bool = False,
) -> GenerateSourcesOutput:
    """Build sources.yml content from catalog tables.

    Only tables with ``is_source: true`` are included. Tables with
    ``resolved`` status are excluded (they become dbt models). Tables with
    ``scoping.status == "no_writer_found"`` but no ``is_source`` flag are
    listed in ``unconfirmed`` — they need explicit confirmation before
    being included.

    When a target source schema override is configured, logical source names
    remain grouped by extracted schema while the emitted dbt ``schema:``
    points at the configured physical target schema.
    """
    catalog_dir = resolve_catalog_dir(project_root)
    tables_dir = catalog_dir / "tables"

    if not tables_dir.is_dir():
        return GenerateSourcesOutput(
            sources=None,
            included=[],
            excluded=[],
            unconfirmed=[],
            incomplete=[],
        )

    candidates = collect_source_candidates(tables_dir)
    if not candidates.source_tables:
        return GenerateSourcesOutput(
            sources=None,
            included=candidates.included,
            excluded=candidates.excluded,
            unconfirmed=candidates.unconfirmed,
            incomplete=candidates.incomplete,
        )

    namespace_error = validate_source_namespace(candidates)
    if namespace_error is not None:
        return namespace_error

    if require_staging_contract_types:
        contract_type_error = validate_staging_contract_types(candidates)
        if contract_type_error is not None:
            return contract_type_error

    physical_source_schema = _resolve_physical_source_schema(
        project_root,
        source_schema_override,
    )
    sources_dict = build_sources_yaml(
        candidates.source_tables,
        physical_source_schema=physical_source_schema,
    )
    return GenerateSourcesOutput(
        sources=sources_dict,
        included=candidates.included,
        excluded=candidates.excluded,
        unconfirmed=candidates.unconfirmed,
        incomplete=candidates.incomplete,
    )


def write_sources_yml(
    project_root: Path,
    *,
    source_schema_override: str | None = None,
    require_staging_contract_types: bool = False,
) -> GenerateSourcesOutput:
    """Generate sources.yml and write it to the dbt project."""
    result = generate_sources(
        project_root,
        source_schema_override=source_schema_override,
        require_staging_contract_types=require_staging_contract_types,
    )
    return write_staging_artifacts(project_root, result)


@app.command()
def main(
    project_root: Path | None = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
    write: bool = typer.Option(
        False, "--write", help="Write sources.yml to dbt project",
    ),
    strict: bool = typer.Option(
        False, "--strict", help="Exit 1 if any table has incomplete scoping (not yet analyzed)",
    ),
) -> None:
    """Generate sources.yml from catalog tables.

    Only tables with is_source=true are included. Tables with 'resolved'
    status are excluded (they become dbt models). Tables with
    'no_writer_found' but no is_source flag appear in 'unconfirmed'.
    """
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    try:
        if write:
            result = write_sources_yml(root)
        else:
            result = generate_sources(root)
    except OSError as exc:
        logger.error("event=generate_sources_io_error error=%s", exc)
        emit({"error": "IO_ERROR", "message": str(exc)})
        raise typer.Exit(code=2) from exc

    if result.error:
        emit(result)
        raise typer.Exit(code=1)

    if strict and result.incomplete:
        emit({
            "error": "INCOMPLETE_SCOPING",
            "message": f"{len(result.incomplete)} tables have incomplete scoping",
            "incomplete": result.incomplete,
        })
        raise typer.Exit(code=1)

    emit(result)
