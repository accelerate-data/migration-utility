"""generate_sources.py — Build and write dbt sources.yml from catalog.

Only tables explicitly marked ``is_source: true`` are registered as dbt
sources. Tables with ``resolved`` status are procedure targets that will
become dbt models (referenced via ``ref()``). Tables with
``scoping.status == "no_writer_found"`` but no ``is_source`` flag appear in
the ``unconfirmed`` list — they need user confirmation via ``/add-source-tables``
or the ``/init-dbt`` confirmation flow before they are included.

Exit codes:
    0  success
    1  domain failure (incomplete scoping with --strict)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import typer
import yaml

from shared.cli_utils import emit
from shared.env_config import (
    resolve_catalog_dir,
    resolve_dbt_project_path,
    resolve_project_root,
)
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


def generate_sources(
    project_root: Path,
    *,
    source_schema_override: str | None = None,
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

    Returns a dict with:
      - ``sources``: the YAML-serialisable sources dict (or None if empty)
      - ``included``: list of table FQNs included as sources (is_source: true)
      - ``excluded``: list of table FQNs excluded (resolved targets)
      - ``unconfirmed``: list of no_writer_found FQNs without is_source flag
      - ``incomplete``: list of table FQNs with incomplete scoping
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

    included: list[str] = []
    excluded: list[str] = []
    unconfirmed: list[str] = []
    incomplete: list[str] = []

    physical_source_schema = _resolve_physical_source_schema(
        project_root,
        source_schema_override,
    )

    # logical schema_name → list of table names
    sources_by_schema: dict[str, list[str]] = {}

    for table_file in sorted(tables_dir.glob("*.json")):
        try:
            cat = json.loads(table_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "event=generate_sources_skip_file path=%s reason=parse_error",
                table_file,
            )
            continue

        schema = cat.get("schema", "").lower()
        name = cat.get("name", "")
        fqn = f"{schema}.{name.lower()}"

        scoping = cat.get("scoping") or {}
        status = scoping.get("status")

        if cat.get("excluded"):
            continue
        if cat.get("is_source") is True:
            included.append(fqn)
            sources_by_schema.setdefault(schema, []).append(name)
        elif status == "resolved":
            excluded.append(fqn)
        elif status == "no_writer_found":
            unconfirmed.append(fqn)
        else:
            incomplete.append(fqn)

    if not sources_by_schema:
        return GenerateSourcesOutput(
            sources=None,
            included=included,
            excluded=excluded,
            unconfirmed=unconfirmed,
            incomplete=incomplete,
        )

    source_entries = []
    for schema_name in sorted(sources_by_schema):
        tables = [
            {"name": t, "description": f"{t} from source system"}
            for t in sorted(sources_by_schema[schema_name])
        ]
        source_entry: dict[str, Any] = {
            "name": schema_name,
            "description": f"Source tables from {schema_name} schema",
            "tables": tables,
        }
        if physical_source_schema:
            source_entry["schema"] = physical_source_schema
        source_entries.append(source_entry)

    sources_dict: dict[str, Any] = {"version": 2, "sources": source_entries}

    return GenerateSourcesOutput(
        sources=sources_dict,
        included=included,
        excluded=excluded,
        unconfirmed=unconfirmed,
        incomplete=incomplete,
    )


def write_sources_yml(
    project_root: Path,
    *,
    source_schema_override: str | None = None,
) -> GenerateSourcesOutput:
    """Generate sources.yml and write it to the dbt project.

    Returns the generate_sources result dict with an added ``path`` field.
    """
    result = generate_sources(
        project_root,
        source_schema_override=source_schema_override,
    )
    dbt_root = resolve_dbt_project_path(project_root)
    sources_path = dbt_root / "models" / "staging" / "sources.yml"

    if result.sources is None:
        return result.model_copy(update={"path": None})

    sources_path.parent.mkdir(parents=True, exist_ok=True)
    with sources_path.open("w", encoding="utf-8") as f:
        yaml.dump(
            result.sources,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    return result.model_copy(update={"path": str(sources_path)})


@app.command()
def main(
    project_root: Optional[Path] = typer.Option(
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

    if strict and result.incomplete:
        emit({
            "error": "INCOMPLETE_SCOPING",
            "message": f"{len(result.incomplete)} tables have incomplete scoping",
            "incomplete": result.incomplete,
        })
        raise typer.Exit(code=1)

    emit(result)
