"""CLI helpers for the setup-ddl skill.

Each subcommand either accepts raw JSON (MCP query results saved by the agent)
and performs deterministic processing, or connects directly to the source DB
for discovery.

Usage (via uv):
    uv run --project <shared> setup-ddl extract --database <db> --schemas silver,bronze [--project-root <dir>]
    uv run --project <shared> setup-ddl assemble-modules --input <json> --project-root <dir> --type procedures
    uv run --project <shared> setup-ddl assemble-tables --input <json> --project-root <dir>
    uv run --project <shared> setup-ddl write-catalog --staging-dir <dir> --project-root <dir> --database <name>
    uv run --project <shared> setup-ddl write-manifest --project-root <dir> --technology sql_server --database <name> --schemas bronze,silver
    uv run --project <shared> setup-ddl list-databases --project-root <dir>
    uv run --project <shared> setup-ddl list-schemas --project-root <dir> [--database <name>]

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure (invalid type, unknown technology, unsupported operation)
    2  IO, parse, or connection error
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import typer

from shared.loader_data import CorruptJSONError
from shared.setup_ddl_core import (
    UnsupportedOperationError,
    run_assemble_modules,
    run_assemble_tables,
    run_extract,
    run_list_databases,
    run_list_schemas,
    run_read_handoff,
    run_write_catalog,
    run_write_manifest,
    run_write_partial_manifest,
)

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── CLI wrappers ─────────────────────────────────────────────────────────────


@app.command("assemble-modules")
def assemble_modules(
    input: Path = typer.Option(..., help="JSON file with [{schema_name, object_name, definition}]"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
    type: str = typer.Option(..., help="Object type: procedures, views, or functions"),
) -> None:
    """Assemble a GO-delimited .sql file from OBJECT_DEFINITION results."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_assemble_modules(input, project_root, type)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2 if isinstance(exc, CorruptJSONError) else 1) from exc
    typer.echo(json.dumps(result))


@app.command("assemble-tables")
def assemble_tables(
    input: Path = typer.Option(..., help="JSON file with column metadata rows"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
) -> None:
    """Build CREATE TABLE statements from sys.columns metadata and write tables.sql."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_assemble_tables(input, project_root)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2 if isinstance(exc, CorruptJSONError) else 1) from exc
    typer.echo(json.dumps(result))


@app.command("write-catalog")
def write_catalog(
    staging_dir: Path = typer.Option(..., help="Directory with staging JSON files from MCP queries"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
    database: str = typer.Option(..., help="Source database name"),
) -> None:
    """Process staging JSON files and write all catalog JSON files.

    Expected staging files (saved by the agent from MCP query results):
      table_columns.json, pk_unique.json, foreign_keys.json,
      identity_columns.json, cdc.json, change_tracking.json (optional),
      sensitivity.json (optional), object_types.json, routing_flags.json (optional),
      proc_params.json (optional),
      proc_dmf.json, view_dmf.json, func_dmf.json
    """
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_write_catalog(staging_dir, project_root, database)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2 if isinstance(exc, CorruptJSONError) else 1) from exc
    typer.echo(json.dumps(result))


@app.command("write-manifest")
def write_manifest(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
    technology: str = typer.Option(..., help="Source technology: sql_server, snowflake, oracle, duckdb"),
    database: str = typer.Option(..., help="Source database name"),
    schemas: str = typer.Option(..., help="Comma-separated list of extracted schemas"),
) -> None:
    """Write manifest.json to the project root."""
    if project_root is None:
        project_root = Path.cwd()
    schema_list = [s.strip() for s in schemas.split(",")]
    try:
        result = run_write_manifest(project_root, technology, database, schema_list)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result))


@app.command("write-partial-manifest")
def write_partial_manifest(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root directory (defaults to CWD)"
    ),
    technology: str = typer.Option(..., help="Source technology: sql_server, snowflake, oracle, duckdb"),
    prereqs_json: Optional[str] = typer.Option(
        None, "--prereqs-json",
        help="JSON object of validated prerequisite results (env_vars, tools)",
    ),
) -> None:
    """Write a partial manifest.json with technology, dialect, and optional prereqs."""
    if project_root is None:
        project_root = Path.cwd()

    prereqs: dict[str, Any] | None = None
    if prereqs_json is not None:
        try:
            prereqs = json.loads(prereqs_json)
        except json.JSONDecodeError as exc:
            typer.echo(f"Invalid --prereqs-json: {exc}", err=True)
            raise typer.Exit(1) from exc

    try:
        result = run_write_partial_manifest(project_root, technology, prereqs=prereqs)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result))


@app.command("read-handoff")
def read_handoff(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing manifest.json (defaults to CWD)",
    ),
) -> None:
    """Read init_handoff from manifest.json. Returns {skip: true/false, handoff: ...}."""
    if project_root is None:
        project_root = Path.cwd()
    handoff = run_read_handoff(project_root)
    if handoff is not None:
        typer.echo(json.dumps({"skip": True, "handoff": handoff}))
    else:
        typer.echo(json.dumps({"skip": False}))


@app.command("list-databases")
def list_databases(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing manifest.json (defaults to CWD)"
    ),
) -> None:
    """List user databases on the source system (SQL Server only)."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_list_databases(project_root)
    except UnsupportedOperationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps(result))


@app.command("list-schemas")
def list_schemas(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing manifest.json (defaults to CWD)"
    ),
    database: Optional[str] = typer.Option(
        None, "--database",
        help="Source database name (required for SQL Server)"
    ),
) -> None:
    """List schemas with object counts on the source system."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_list_schemas(project_root, database)
    except UnsupportedOperationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps(result))


@app.command("extract")
def extract(
    database: Optional[str] = typer.Option(
        None, "--database",
        help="Source database name (required for SQL Server; ignored for Oracle and optional for DuckDB)",
    ),
    schemas: str = typer.Option(
        ..., "--schemas",
        help="Comma-separated list of schemas to extract",
    ),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing manifest.json (defaults to CWD)",
    ),
) -> None:
    """Connect to the source DB and extract DDL, catalog, and manifest in one pass.

    Reads technology from manifest.json. Runs all extraction queries internally,
    writes ddl/, catalog/, and manifest.json, then runs catalog-enrich.
    Preserves any existing LLM-enriched catalog fields (scoping, profile, refactor).
    """
    if project_root is None:
        project_root = Path.cwd()
    schema_list = [s.strip() for s in schemas.split(",") if s.strip()]
    try:
        result = run_extract(project_root, database, schema_list)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps(result))


if __name__ == "__main__":
    app()
