"""refactor.py -- Refactoring context assembly, catalog write-back, and diff logic.

Standalone CLI with two subcommands:

    context  Assemble all deterministic context needed for LLM SQL refactoring.
    write    Validate and merge a refactor section into the writer procedure's catalog.

Also exposes ``symmetric_diff`` for comparing two row-dict lists.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain/validation failure
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import typer

from shared.catalog import (
    load_table_catalog,
    read_selected_writer,
    write_json as _write_catalog_json,
)
from shared.context_helpers import (
    collect_source_tables,
    load_proc_body,
    load_proc_statements,
    load_table_columns,
    load_table_profile,
    load_test_spec,
    sandbox_metadata,
)
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
)
from shared.cli_utils import emit
from shared.env_config import resolve_catalog_dir, resolve_project_root
from shared.name_resolver import fqn_parts, normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Constants ────────────────────────────────────────────────────────────────

REFACTOR_STATUSES = frozenset({"ok", "partial", "error"})


# ── Helpers ──────────────────────────────────────────────────────────────────


# ── Symmetric diff ───────────────────────────────────────────────────────────


def _row_to_key(row: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """Convert a row dict to a hashable key for multiset comparison.

    All values are stringified to handle type mismatches (e.g. Decimal vs str).
    """
    return tuple(sorted((k, str(v)) for k, v in row.items()))


def symmetric_diff(
    rows_a: list[dict[str, Any]],
    rows_b: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compute the symmetric difference of two row-dict lists.

    Uses multiset (Counter) comparison to correctly handle duplicate rows.

    Returns::

        {
            "equivalent": bool,
            "a_minus_b": list[dict],  # rows in A but not in B
            "b_minus_a": list[dict],  # rows in B but not in A
            "a_count": int,
            "b_count": int,
        }
    """
    keys_a = [_row_to_key(r) for r in rows_a]
    keys_b = [_row_to_key(r) for r in rows_b]

    counter_a = Counter(keys_a)
    counter_b = Counter(keys_b)

    # Multiset difference: elements in A not accounted for in B
    a_minus_b_counter = counter_a - counter_b
    b_minus_a_counter = counter_b - counter_a

    # Reconstruct row dicts from keys
    def _keys_to_rows(counter: Counter[tuple[tuple[str, str], ...]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, count in counter.items():
            row = dict(key)
            for _ in range(count):
                rows.append(row)
        return rows

    a_minus_b = _keys_to_rows(a_minus_b_counter)
    b_minus_a = _keys_to_rows(b_minus_a_counter)

    return {
        "equivalent": len(a_minus_b) == 0 and len(b_minus_a) == 0,
        "a_minus_b": a_minus_b,
        "b_minus_a": b_minus_a,
        "a_count": len(rows_a),
        "b_count": len(rows_b),
    }


# ── Context assembly ─────────────────────────────────────────────────────────


def run_context(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str | None = None,
) -> dict[str, Any]:
    """Assemble refactoring context for a single table/writer pair.

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

    profile = load_table_profile(project_root, table_norm)
    statements = load_proc_statements(project_root, writer_norm)
    proc_body = load_proc_body(project_root, writer_norm)
    columns = load_table_columns(project_root, table_norm)
    source_tables = collect_source_tables(project_root, writer_norm)
    test_spec = load_test_spec(project_root, table_norm)
    sandbox = sandbox_metadata(project_root)

    logger.info(
        "event=context_assembled table=%s writer=%s source_tables=%d test_scenarios=%d",
        table_norm, writer_norm, len(source_tables),
        len(test_spec.get("unit_tests", [])) if test_spec else 0,
    )

    return {
        "table": table_norm,
        "writer": writer_norm,
        "proc_body": proc_body,
        "profile": profile,
        "statements": statements,
        "columns": columns,
        "source_tables": source_tables,
        "test_spec": test_spec,
        "sandbox": sandbox,
    }


# ── Write validation and merge ───────────────────────────────────────────────


def _validate_refactor(refactor: dict[str, Any]) -> list[str]:
    """Validate a refactor dict. Returns a list of error messages (empty = valid)."""
    errors: list[str] = []

    status = refactor.get("status")
    if status is None:
        errors.append("missing required field: status")
    elif status not in REFACTOR_STATUSES:
        errors.append(f"invalid status: {status!r}, must be one of {sorted(REFACTOR_STATUSES)}")

    extracted = refactor.get("extracted_sql")
    if status == "ok" and (not extracted or not extracted.strip()):
        errors.append("extracted_sql is required when status is 'ok'")

    refactored = refactor.get("refactored_sql")
    if status == "ok" and (not refactored or not refactored.strip()):
        errors.append("refactored_sql is required when status is 'ok'")

    return errors


def run_write(
    project_root: Path,
    table_fqn: str,
    extracted_sql: str,
    refactored_sql: str,
    status: str,
) -> dict[str, Any]:
    """Validate and merge a refactor section into the writer procedure's catalog.

    Resolves the writer from the table catalog's ``scoping.selected_writer``,
    then persists the refactor block on the procedure catalog file.

    Returns a confirmation dict on success.
    Raises ValueError on validation failure, OSError/json.JSONDecodeError on IO error.
    """
    table_norm = normalize(table_fqn)

    refactor_data: dict[str, Any] = {
        "status": status,
        "extracted_sql": " ".join(extracted_sql.split()),
        "refactored_sql": " ".join(refactored_sql.split()),
    }

    errors = _validate_refactor(refactor_data)
    if errors:
        raise ValueError(f"Refactor validation failed for {table_norm}: {'; '.join(errors)}")

    # Resolve writer from table catalog
    writer_fqn = read_selected_writer(project_root, table_norm)
    if not writer_fqn:
        raise ValueError(
            f"No scoping.selected_writer in table catalog for {table_norm}"
        )
    writer_norm = normalize(writer_fqn)

    # Load existing procedure catalog file
    catalog_path = resolve_catalog_dir(project_root) / "procedures" / f"{writer_norm}.json"
    if not catalog_path.exists():
        raise CatalogFileMissingError("procedure", writer_norm)

    try:
        existing = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(catalog_path), exc) from exc
    except OSError as exc:
        logger.error(
            "event=write_failed operation=read_catalog table=%s writer=%s error=%s",
            table_norm, writer_norm, exc,
        )
        raise

    # Merge refactor section onto procedure catalog
    existing["refactor"] = refactor_data

    try:
        _write_catalog_json(catalog_path, existing)
    except OSError as exc:
        logger.error(
            "event=write_failed operation=atomic_write table=%s writer=%s error=%s",
            table_norm, writer_norm, exc,
        )
        raise

    logger.info(
        "event=write_complete table=%s writer=%s catalog_path=%s",
        table_norm, writer_norm, catalog_path,
    )
    return {
        "ok": True,
        "table": table_norm,
        "writer": writer_norm,
        "catalog_path": str(catalog_path),
    }


# ── CLI commands ─────────────────────────────────────────────────────────────


@app.command()
def context(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    writer: Optional[str] = typer.Option(None, help="Fully-qualified writer procedure name (reads from catalog scoping section if omitted)"),
) -> None:
    """Assemble refactoring context for a table + writer pair."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_context(project_root, table, writer)
    except CatalogFileMissingError as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=1) from exc
    except (ValueError, FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def write(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    status: str = typer.Option(..., help="Refactor status: ok, partial, or error"),
    extracted_sql: str = typer.Option("", help="Extracted core SQL string"),
    extracted_sql_file: Optional[Path] = typer.Option(None, "--extracted-sql-file", help="Path to file containing extracted core SQL"),
    refactored_sql: str = typer.Option("", help="Refactored SQL string"),
    refactored_sql_file: Optional[Path] = typer.Option(None, "--refactored-sql-file", help="Path to file containing refactored SQL"),
) -> None:
    """Validate and merge a refactor section into the writer procedure's catalog."""
    if extracted_sql_file:
        extracted_sql = extracted_sql_file.read_text(encoding="utf-8")
    if refactored_sql_file:
        refactored_sql = refactored_sql_file.read_text(encoding="utf-8")
    if not extracted_sql and not refactored_sql:
        logger.error("event=write_failed table=%s error=no SQL provided", table)
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)

    try:
        result = run_write(project_root, table, extracted_sql, refactored_sql, status)
    except (ValueError, CatalogFileMissingError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
