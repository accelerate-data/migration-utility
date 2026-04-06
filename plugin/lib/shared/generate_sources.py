"""generate_sources.py — Build and write dbt sources.yml from catalog.

Only tables with ``scoping.status == "no_writer_found"`` are registered as
dbt sources. Tables with ``resolved`` status are procedure targets that will
become dbt models (referenced via ``ref()``).

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

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


def generate_sources(project_root: Path) -> dict[str, Any]:
    """Build sources.yml content from catalog tables.

    Only tables with ``scoping.status == "no_writer_found"`` are included.
    Tables with ``resolved`` status are procedure targets and will become
    dbt models (referenced via ``ref()``).

    Returns a dict with:
      - ``sources``: the YAML-serialisable sources dict (or None if empty)
      - ``included``: list of table FQNs included as sources
      - ``excluded``: list of table FQNs excluded (resolved targets)
      - ``incomplete``: list of table FQNs with incomplete scoping
    """
    catalog_dir = resolve_catalog_dir(project_root)
    tables_dir = catalog_dir / "tables"

    if not tables_dir.is_dir():
        return {"sources": None, "included": [], "excluded": [], "incomplete": []}

    included: list[str] = []
    excluded: list[str] = []
    incomplete: list[str] = []

    # schema_name → list of table names
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

        if status == "no_writer_found":
            included.append(fqn)
            sources_by_schema.setdefault(schema, []).append(name)
        elif status == "resolved":
            excluded.append(fqn)
        else:
            incomplete.append(fqn)

    if not sources_by_schema:
        return {
            "sources": None,
            "included": included,
            "excluded": excluded,
            "incomplete": incomplete,
        }

    source_entries = []
    for schema_name in sorted(sources_by_schema):
        tables = [
            {"name": t, "description": f"{t} from source system"}
            for t in sorted(sources_by_schema[schema_name])
        ]
        source_entries.append({
            "name": schema_name,
            "description": f"Source tables from {schema_name} schema",
            "tables": tables,
        })

    sources_dict: dict[str, Any] = {"version": 2, "sources": source_entries}

    return {
        "sources": sources_dict,
        "included": included,
        "excluded": excluded,
        "incomplete": incomplete,
    }


def write_sources_yml(project_root: Path) -> dict[str, Any]:
    """Generate sources.yml and write it to the dbt project.

    Returns the generate_sources result dict with an added ``path`` field.
    """
    result = generate_sources(project_root)
    dbt_root = resolve_dbt_project_path(project_root)
    sources_path = dbt_root / "models" / "staging" / "sources.yml"

    if result["sources"] is None:
        result["path"] = None
        return result

    sources_path.parent.mkdir(parents=True, exist_ok=True)
    with sources_path.open("w", encoding="utf-8") as f:
        yaml.dump(
            result["sources"],
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    result["path"] = str(sources_path)
    return result


@app.command()
def main(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
    write: bool = typer.Option(
        False, "--write", help="Write sources.yml to dbt project",
    ),
    strict: bool = typer.Option(
        False, "--strict", help="Exit 1 if any table has incomplete scoping",
    ),
) -> None:
    """Generate sources.yml from catalog tables with scoping filter.

    Only tables with scoping.status == 'no_writer_found' are included.
    Tables with 'resolved' status are excluded (they become dbt models).
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

    if strict and result["incomplete"]:
        emit({
            "error": "INCOMPLETE_SCOPING",
            "message": f"{len(result['incomplete'])} tables have incomplete scoping",
            "incomplete": result["incomplete"],
        })
        raise typer.Exit(code=1)

    emit(result)
