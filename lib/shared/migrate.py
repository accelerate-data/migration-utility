"""migrate.py — dbt model context assembly and artifact writer.

Standalone CLI with two subcommands:

    context   Assemble migration context from catalog profile, resolved
              statements, proc body, and DDL columns.
    write     Validate and write generated model SQL + schema YAML to a
              dbt project.

Requires catalog files from setup-ddl + profile from profiler.
All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain/validation failure
    2  IO error
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import typer

from shared.catalog import (
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
)
from shared.loader import (
    CatalogFileMissingError,
    CatalogNotFoundError,
    DdlParseError,
    ProfileMissingError,
    load_directory,
)
from shared.name_resolver import fqn_parts, normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


def _model_name_from_table(table_fqn: str) -> str:
    """Derive a dbt model name from a table FQN.

    ``silver.dim_customer`` → ``stg_dim_customer``
    ``dbo.fact_sales`` → ``stg_fact_sales``
    """
    _, name = fqn_parts(normalize(table_fqn))
    return f"stg_{name}"


# ── Materialization derivation ────────────────────────────────────────────────


def derive_materialization(profile: dict[str, Any]) -> str:
    """Derive dbt materialization from profile classification and watermark.

    Rules:
    - ``dim_scd2`` classification → ``snapshot``
    - No watermark (or watermark column is null/empty) → ``table``
    - Otherwise → ``incremental``
    """
    classification = profile.get("classification", "")
    if classification == "dim_scd2":
        return "snapshot"
    watermark = profile.get("watermark")
    if watermark and watermark.get("column"):
        return "incremental"
    return "table"


# ── Schema test derivation ────────────────────────────────────────────────────


def derive_schema_tests(profile: dict[str, Any]) -> dict[str, Any]:
    """Build dbt schema test specs from profile answers.

    Returns a dict with:
    - ``entity_integrity``: PK columns → unique + not_null
    - ``referential_integrity``: FK columns → relationships
    - ``recency``: watermark column → recency test (incremental only)
    - ``pii``: sensitive columns → meta tags
    """
    tests: dict[str, Any] = {}

    # Entity integrity from primary key
    pk = profile.get("primary_key")
    if pk and pk.get("columns"):
        tests["entity_integrity"] = [
            {"column": col, "tests": ["unique", "not_null"]}
            for col in pk["columns"]
        ]

    # Referential integrity from foreign keys
    fks = profile.get("foreign_keys", [])
    if fks:
        ri_tests = []
        for fk in fks:
            col = fk.get("column", "")
            ref_table = fk.get("references_table", "")
            ref_col = fk.get("references_column", "")
            ref_schema = fk.get("references_schema", "")
            if col and ref_table:
                # Build ref() target from schema + table
                ref_name = normalize(f"{ref_schema}.{ref_table}") if ref_schema else ref_table
                model_ref = f"ref('{_model_name_from_table(ref_name)}')"
                ri_tests.append({
                    "column": col,
                    "to": model_ref,
                    "field": ref_col or col,
                })
        if ri_tests:
            tests["referential_integrity"] = ri_tests

    # Recency from watermark
    watermark = profile.get("watermark")
    if watermark and watermark.get("column"):
        tests["recency"] = {"column": watermark["column"]}

    # PII from sensitivity / pii_actions
    pii_actions = profile.get("pii_actions", [])
    if pii_actions:
        tests["pii"] = [
            {"column": p.get("column", ""), "action": p.get("action", "mask")}
            for p in pii_actions
            if p.get("column")
        ]

    return tests


# ── Context assembly ──────────────────────────────────────────────────────────


def _load_table_profile(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Load the profile section from a table catalog file."""
    cat = load_table_catalog(project_root, table_fqn)
    if cat is None:
        raise CatalogFileMissingError("table", table_fqn)
    profile = cat.get("profile")
    if profile is None:
        raise ProfileMissingError(table_fqn)
    return profile


def _load_proc_statements(project_root: Path, writer_fqn: str) -> list[dict[str, Any]]:
    """Load resolved statements from a procedure catalog file."""
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        raise CatalogFileMissingError("procedure", writer_fqn)
    statements = cat.get("statements")
    if statements is None:
        raise CatalogFileMissingError("procedure statements", writer_fqn)
    return statements


def _load_proc_body(project_root: Path, writer_fqn: str) -> str:
    """Load the raw DDL body of a procedure from the DDL directory."""
    catalog = load_directory(project_root)
    entry = catalog.get_procedure(writer_fqn)
    if entry is None:
        raise CatalogFileMissingError("procedure DDL", writer_fqn)
    return entry.raw_ddl


def _load_table_columns(project_root: Path, table_fqn: str) -> list[dict[str, Any]]:
    """Load column list from the table catalog file."""
    cat = load_table_catalog(project_root, table_fqn)
    if cat and cat.get("columns"):
        return cat["columns"]
    return []


def _collect_source_tables(
    project_root: Path, writer_fqn: str,
) -> list[str]:
    """Collect source tables from the writer procedure's references."""
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        return []
    refs = cat.get("references", {})
    tables_in_scope = refs.get("tables", {}).get("in_scope", [])
    sources = []
    for t in tables_in_scope:
        if t.get("is_selected") and not t.get("is_updated"):
            sources.append(normalize(f"{t['schema']}.{t['name']}"))
    return sorted(set(sources))


def run_context(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str,
) -> dict[str, Any]:
    """Assemble migration context for a single table/writer pair."""
    table_norm = normalize(table_fqn)
    writer_norm = normalize(writer_fqn)

    if not has_catalog(project_root):
        raise CatalogNotFoundError(project_root)

    profile = _load_table_profile(project_root, table_norm)
    statements = _load_proc_statements(project_root, writer_norm)
    proc_body = _load_proc_body(project_root, writer_norm)
    columns = _load_table_columns(project_root, table_norm)
    source_tables = _collect_source_tables(project_root, writer_norm)
    materialization = derive_materialization(profile)
    schema_tests = derive_schema_tests(profile)

    return {
        "table": table_norm,
        "writer": writer_norm,
        "profile": profile,
        "materialization": materialization,
        "statements": statements,
        "proc_body": proc_body,
        "columns": columns,
        "source_tables": source_tables,
        "schema_tests": schema_tests,
    }


# ── Write artifacts ───────────────────────────────────────────────────────────


def run_write(
    table_fqn: str,
    project_root: Path,
    dbt_project_path: Path,
    model_sql: str,
    schema_yml: str,
) -> dict[str, Any]:
    """Validate and write model SQL + schema YAML to a dbt project.

    Returns ``{"written": [...], "status": "ok"}``.

    Raises typer.Exit:
        code 1 — validation failure (empty SQL, etc.)
        code 2 — IO error (project path missing, write failure)
    """
    table_norm = normalize(table_fqn)
    model_name = _model_name_from_table(table_norm)

    # Validation
    if not model_sql or not model_sql.strip():
        raise ValueError("model SQL is empty")

    if not dbt_project_path.is_dir():
        raise FileNotFoundError(f"dbt project path does not exist: {dbt_project_path}")

    dbt_project_yml = dbt_project_path / "dbt_project.yml"
    if not dbt_project_yml.exists():
        raise FileNotFoundError(f"no dbt_project.yml in {dbt_project_path}")

    staging_dir = dbt_project_path / "models" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    sql_path = staging_dir / f"{model_name}.sql"
    yml_path = staging_dir / f"_{model_name}.yml"

    written: list[str] = []
    sql_path.write_text(model_sql, encoding="utf-8")
    written.append(str(sql_path.relative_to(dbt_project_path)))

    if schema_yml and schema_yml.strip():
        yml_path.write_text(schema_yml, encoding="utf-8")
        written.append(str(yml_path.relative_to(dbt_project_path)))

    return {"written": written, "status": "ok"}


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command()
def context(
    table: str = typer.Option(..., help="Fully-qualified target table name (schema.table)"),
    writer: str = typer.Option(..., help="Fully-qualified writer procedure name (schema.proc)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
) -> None:
    """Assemble migration context from catalog + DDL."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_context(project_root, table, writer)
    except (CatalogFileMissingError, ProfileMissingError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def write(
    table: str = typer.Option(..., help="Fully-qualified target table name (schema.table)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    dbt_project_path: Path = typer.Option(..., help="Path to dbt project root"),
    model_sql: str = typer.Option(..., help="Generated dbt model SQL"),
    schema_yml: str = typer.Option("", help="Generated schema YAML"),
) -> None:
    """Write generated dbt model SQL + schema YAML to dbt project."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_write(table, project_root, dbt_project_path, model_sql, schema_yml)
    except ValueError as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    _emit(result)


if __name__ == "__main__":
    app()
