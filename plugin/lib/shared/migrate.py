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

from shared.output_models import MigrateContextOutput, MigrateWriteOutput

from shared.catalog import (
    has_catalog,
    load_and_merge_catalog,
    load_proc_catalog,
    load_table_catalog,
    read_selected_writer,
    write_json as _write_catalog_json,
)
from shared.cli_utils import emit
from shared.context_helpers import (
    collect_source_tables,
    load_object_columns,
    load_proc_body,
    load_proc_statements,
    load_source_columns,
    load_table_columns,
    load_table_profile,
)
from shared.env_config import resolve_catalog_dir, resolve_dbt_project_path, resolve_project_root
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
    ProfileMissingError,
)
from shared.name_resolver import fqn_parts, model_name_from_table, normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Materialization derivation ────────────────────────────────────────────────


def derive_materialization(profile: Any) -> str:
    """Derive dbt materialization from profile classification and watermark.

    Rules:
    - ``dim_scd2`` classification → ``snapshot``
    - No watermark (or watermark column is null/empty) → ``table``
    - Otherwise → ``incremental``
    """
    _get = profile.get if isinstance(profile, dict) else lambda k, d=None: getattr(profile, k, d)
    classification = _get("classification") or {}
    if isinstance(classification, dict) and classification.get("resolved_kind") == "dim_scd2":
        return "snapshot"
    watermark = _get("watermark")
    if watermark and (watermark.get("column") if isinstance(watermark, dict) else getattr(watermark, "column", None)):
        return "incremental"
    return "table"


# ── Schema test derivation ────────────────────────────────────────────────────


def derive_schema_tests(profile: Any) -> dict[str, Any]:
    """Build dbt schema test specs from profile answers.

    Returns a dict with:
    - ``entity_integrity``: PK columns → unique + not_null
    - ``referential_integrity``: FK columns → relationships
    - ``recency``: watermark column → recency test (incremental only)
    - ``pii``: sensitive columns → meta tags
    """
    _get = profile.get if isinstance(profile, dict) else lambda k, d=None: getattr(profile, k, d)
    tests: dict[str, Any] = {}

    # Entity integrity from primary key
    pk = _get("primary_key")
    if pk and (pk.get("columns") if isinstance(pk, dict) else getattr(pk, "columns", None)):
        cols = pk.get("columns") if isinstance(pk, dict) else pk.columns
        tests["entity_integrity"] = [
            {"column": col, "tests": ["unique", "not_null"]}
            for col in cols
        ]

    # Referential integrity from foreign keys
    fks = _get("foreign_keys") or []
    if fks:
        ri_tests = []
        for fk in fks:
            col = fk.get("column", "") if isinstance(fk, dict) else getattr(fk, "column", "")
            ref_relation = fk.get("references_source_relation", "") if isinstance(fk, dict) else getattr(fk, "references_source_relation", "")
            ref_col = fk.get("references_column", "") if isinstance(fk, dict) else getattr(fk, "references_column", "")
            if col and ref_relation:
                model_ref = f"ref('{model_name_from_table(ref_relation)}')"
                ri_tests.append({
                    "column": col,
                    "to": model_ref,
                    "field": ref_col or col,
                })
        if ri_tests:
            tests["referential_integrity"] = ri_tests

    # Recency from watermark
    watermark = _get("watermark")
    if watermark and (watermark.get("column") if isinstance(watermark, dict) else getattr(watermark, "column", None)):
        col = watermark.get("column") if isinstance(watermark, dict) else watermark.column
        tests["recency"] = {"column": col}

    # PII from sensitivity / pii_actions
    pii_actions = _get("pii_actions") or []
    if pii_actions:
        tests["pii"] = [
            {"column": p.get("column", "") if isinstance(p, dict) else getattr(p, "column", ""),
             "suggested_action": p.get("suggested_action", "mask") if isinstance(p, dict) else getattr(p, "suggested_action", "mask")}
            for p in pii_actions
            if (p.get("column") if isinstance(p, dict) else getattr(p, "column", None))
        ]

    return tests


# ── Context assembly ──────────────────────────────────────────────────────────


def _classify_proc(project_root: Path, writer_fqn: str) -> bool:
    """Return True if the procedure needs LLM analysis."""
    from shared.catalog import load_proc_catalog
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        raise CatalogFileMissingError("procedure", writer_fqn)
    return cat.mode == "llm_required"


def _load_refactored_sql(project_root: Path, table_fqn: str) -> str | None:
    """Load refactored_sql from the writer procedure's catalog.

    Resolves the writer via ``scoping.selected_writer`` on the table catalog,
    then reads ``refactor.refactored_sql`` from the procedure catalog.

    Returns ``None`` when the refactor section is absent or incomplete.
    The generating-model guard (``check_refactor_complete``) ensures this
    field is always populated before the generating-model skill runs.
    """
    writer_fqn = read_selected_writer(project_root, table_fqn)
    if not writer_fqn:
        return None
    cat = load_proc_catalog(project_root, normalize(writer_fqn))
    if cat is None:
        return None
    if not cat.refactor:
        return None
    return cat.refactor.refactored_sql or None



def run_context(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str | None = None,
) -> MigrateContextOutput:
    """Assemble migration context for a single table/writer pair.

    If *writer_fqn* is not provided, reads ``scoping.selected_writer``
    from the table catalog.  Raises ``ValueError`` if neither is available.
    """
    table_norm = normalize(table_fqn)
    if not writer_fqn:
        writer_fqn = read_selected_writer(project_root, table_norm)
        if not writer_fqn:
            raise ValueError(
                f"No writer provided and no scoping.selected_writer in catalog for {table_norm}"
            )
    writer_norm = normalize(writer_fqn)

    if not has_catalog(project_root):
        raise CatalogNotFoundError(project_root)

    profile = load_table_profile(project_root, table_norm)
    statements = load_proc_statements(project_root, writer_norm)
    needs_llm = _classify_proc(project_root, writer_norm)
    proc_body = load_proc_body(project_root, writer_norm)
    columns = load_table_columns(project_root, table_norm)
    source_tables = collect_source_tables(project_root, writer_norm)
    source_columns = {
        fqn: load_object_columns(project_root, fqn) for fqn in source_tables
    }
    materialization = derive_materialization(profile)
    schema_tests = derive_schema_tests(profile)
    refactored_sql = _load_refactored_sql(project_root, table_norm)

    return MigrateContextOutput(
        table=table_norm,
        writer=writer_norm,
        needs_llm=needs_llm,
        profile=profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        materialization=materialization,
        statements=statements,
        proc_body=proc_body,
        columns=columns,
        source_tables=source_tables,
        source_columns=source_columns,
        schema_tests=schema_tests,
        refactored_sql=refactored_sql,
    )


# ── Write artifacts ───────────────────────────────────────────────────────────


def _atomic_write(path: Path, content: str) -> None:
    """Write content to path via tmp-then-rename for crash safety."""
    tmp_path = path.with_name(path.name + ".tmp")
    try:
        tmp_path.write_text(content, encoding="utf-8")
        tmp_path.replace(path)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise


def run_write(
    table_fqn: str,
    project_root: Path,
    dbt_project_path: Path,
    model_sql: str,
    schema_yml: str,
) -> MigrateWriteOutput:
    """Validate and write model SQL + schema YAML to a dbt project.

    Returns ``{"written": [...], "status": "ok"}``.

    Raises typer.Exit:
        code 1 — validation failure (empty SQL, etc.)
        code 2 — IO error (project path missing, write failure)
    """
    table_norm = normalize(table_fqn)
    model_name = model_name_from_table(table_norm)

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
    _atomic_write(sql_path, model_sql)
    written.append(str(sql_path.relative_to(dbt_project_path)))

    if schema_yml and schema_yml.strip():
        _atomic_write(yml_path, schema_yml)
        written.append(str(yml_path.relative_to(dbt_project_path)))

    return MigrateWriteOutput(written=written, status="ok")


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command()
def context(
    table: str = typer.Option(..., help="Fully-qualified target table name (schema.table)"),
    writer: Optional[str] = typer.Option(None, help="Fully-qualified writer procedure name (reads from catalog scoping section if omitted)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
) -> None:
    """Assemble migration context from catalog + DDL."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_context(project_root, table, writer)
    except (CatalogFileMissingError, ProfileMissingError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=1) from exc
    except (ValueError, FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def write(
    table: str = typer.Option(..., help="Fully-qualified target table name (schema.table)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    dbt_project_path: Optional[Path] = typer.Option(None, "--dbt-project-path", help="Path to dbt project (default: $DBT_PROJECT_PATH or ./dbt)"),
    model_sql: str = typer.Option("", help="Generated dbt model SQL (inline string)"),
    schema_yml: str = typer.Option("", help="Generated schema YAML (inline string)"),
    model_sql_file: Optional[Path] = typer.Option(None, "--model-sql-file", help="Path to file containing generated dbt model SQL"),
    schema_yml_file: Optional[Path] = typer.Option(None, "--schema-yml-file", help="Path to file containing generated schema YAML"),
) -> None:
    """Write generated dbt model SQL + schema YAML to dbt project."""
    if model_sql_file:
        model_sql = model_sql_file.read_text(encoding="utf-8")
    if schema_yml_file:
        schema_yml = schema_yml_file.read_text(encoding="utf-8")
    if not model_sql:
        logger.error("event=write_failed table=%s error=no model SQL provided (use --model-sql or --model-sql-file)", table)
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    if dbt_project_path is None:
        dbt_project_path = resolve_dbt_project_path(project_root)
    try:
        result = run_write(table, project_root, dbt_project_path, model_sql, schema_yml)
    except ValueError as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


# ── Write generate summary ────────────────────────────────────────────────────


def run_write_generate(
    project_root: Path,
    table_fqn: str,
    model_path: str,
    compiled: bool,
    tests_passed: bool,
    test_count: int,
    schema_yml: bool,
    warnings: list[dict[str, Any]] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Validate generate output and write generate section to catalog.

    Reads the dbt model file from disk to verify it exists.
    Determines status: model exists + compiled + tests passed → ok; otherwise → error.
    Writes the generate section to the table or view catalog file.
    """
    norm = normalize(table_fqn)

    # Check model file exists — model_path is relative to dbt project root
    dbt_root = resolve_dbt_project_path(project_root)
    model_file = dbt_root / model_path
    file_exists = model_file.exists()

    # Determine status
    status = "ok" if file_exists and compiled and tests_passed else "error"

    # Build generate section
    generate: dict[str, Any] = {
        "status": status,
        "model_path": model_path,
        "schema_yml": schema_yml,
        "compiled": compiled,
        "tests_passed": tests_passed,
        "test_count": test_count,
        "warnings": warnings or [],
        "errors": errors or [],
    }

    result = load_and_merge_catalog(project_root, norm, "generate", generate)
    logger.info("event=write_generate_complete table=%s status=%s", norm, status)
    return result


@app.command("write-catalog")
def write_catalog_cmd(
    table: str = typer.Option(..., help="Fully-qualified table/view name"),
    model_path: str = typer.Option(..., "--model-path", help="Relative path to dbt model SQL file"),
    compiled: bool = typer.Option(..., help="Whether dbt compile succeeded"),
    tests_passed: bool = typer.Option(..., "--tests-passed", help="Whether dbt test passed"),
    test_count: int = typer.Option(0, "--test-count", help="Number of dbt tests executed"),
    schema_yml_flag: bool = typer.Option(False, "--schema-yml", help="Whether schema YAML entry exists"),
    warnings_json: Optional[str] = typer.Option(None, "--warnings", help="JSON array of warning diagnostics"),
    errors_json: Optional[str] = typer.Option(None, "--errors", help="JSON array of error diagnostics"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Write model generation summary to catalog with CLI-determined status."""
    root = resolve_project_root(project_root)

    parsed_warnings: list[dict[str, Any]] | None = None
    parsed_errors: list[dict[str, Any]] | None = None
    if warnings_json:
        try:
            parsed_warnings = json.loads(warnings_json)
        except json.JSONDecodeError as exc:
            logger.error("event=write_catalog_failed operation=parse_warnings table=%s error=%s", table, exc)
            raise typer.Exit(code=1) from exc
    if errors_json:
        try:
            parsed_errors = json.loads(errors_json)
        except json.JSONDecodeError as exc:
            logger.error("event=write_catalog_failed operation=parse_errors table=%s error=%s", table, exc)
            raise typer.Exit(code=1) from exc

    try:
        result = run_write_generate(
            project_root=root,
            table_fqn=table,
            model_path=model_path,
            compiled=compiled,
            tests_passed=tests_passed,
            test_count=test_count,
            schema_yml=schema_yml_flag,
            warnings=parsed_warnings,
            errors=parsed_errors,
        )
    except (CatalogFileMissingError, CatalogLoadError) as exc:
        logger.error("event=write_catalog_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=1) from exc
    except OSError as exc:
        logger.error("event=write_catalog_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
