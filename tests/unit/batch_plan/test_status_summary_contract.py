"""Tests for deterministic /status summary dashboard fields."""

from __future__ import annotations

import json
from pathlib import Path

from shared.batch_plan import build_batch_plan
from shared.diagnostic_reviews import (
    ReviewedDiagnostic,
    diagnostic_identity,
    write_reviewed_diagnostic,
)


def _write_manifest(path: Path, *, target: bool = False, sandbox: bool = False) -> None:
    runtime: dict[str, object] = {
        "source": {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {"database": "SourceDb", "schema": "dbo"},
        },
    }
    if target:
        runtime["target"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {"database": "TargetDb", "schema": "bronze"},
        }
    if sandbox:
        runtime["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {"database": "SandboxDb", "schema": "dbo"},
        }
    (path / "manifest.json").write_text(
        json.dumps({"technology": "sql_server", "dialect": "tsql", "runtime": runtime}),
        encoding="utf-8",
    )


def _write_table(path: Path, fqn: str, payload: dict[str, object]) -> None:
    schema, name = fqn.split(".", 1)
    data = {"schema": schema, "name": name}
    data.update(payload)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_status_summary_rows_exclude_source_and_seed_tables(tmp_path: Path) -> None:
    _write_manifest(tmp_path)
    table_dir = tmp_path / "catalog" / "tables"
    proc_dir = tmp_path / "catalog" / "procedures"
    table_dir.mkdir(parents=True)
    proc_dir.mkdir(parents=True)

    _write_table(
        table_dir / "silver.dim_customer.json",
        "silver.dim_customer",
        {
            "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_customer"},
            "profile": {"status": "ok"},
        },
    )
    _write_table(
        table_dir / "bronze.product.json",
        "bronze.product",
        {"is_source": True},
    )
    _write_table(
        table_dir / "bronze.currency.json",
        "bronze.currency",
        {"is_seed": True},
    )
    (proc_dir / "dbo.usp_load_customer.json").write_text(
        json.dumps({
            "schema": "dbo",
            "name": "usp_load_customer",
            "statements": [{"action": "migrate", "source": "ast", "sql": ""}],
            "mode": "deterministic",
            "routing_reasons": [],
        }),
        encoding="utf-8",
    )

    result = build_batch_plan(tmp_path)

    assert [row.fqn for row in result.status_summary.pipeline_rows] == [
        "silver.dim_customer"
    ]
    assert result.summary.source_tables == 1
    assert result.summary.seed_tables == 1


def test_status_summary_marks_test_gen_setup_block(tmp_path: Path) -> None:
    _write_manifest(tmp_path)
    table_dir = tmp_path / "catalog" / "tables"
    proc_dir = tmp_path / "catalog" / "procedures"
    table_dir.mkdir(parents=True)
    proc_dir.mkdir(parents=True)
    _write_table(
        table_dir / "silver.dim_customer.json",
        "silver.dim_customer",
        {
            "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_customer"},
            "profile": {"status": "ok"},
        },
    )
    (proc_dir / "dbo.usp_load_customer.json").write_text(
        json.dumps({
            "schema": "dbo",
            "name": "usp_load_customer",
            "statements": [{"action": "migrate", "source": "ast", "sql": ""}],
            "mode": "deterministic",
            "routing_reasons": [],
        }),
        encoding="utf-8",
    )

    result = build_batch_plan(tmp_path)

    row = result.status_summary.pipeline_rows[0]
    assert row.test_gen == "setup-blocked"
    assert row.refactor == "blocked"
    assert row.migrate == "blocked"
    assert result.status_summary.next_action.command == "!ad-migration setup-target"


def test_status_summary_diagnostics_counts_without_messages(tmp_path: Path) -> None:
    _write_manifest(tmp_path, target=True, sandbox=True)
    table_dir = tmp_path / "catalog" / "tables"
    table_dir.mkdir(parents=True)
    active_warning = {
        "code": "PARSE_ERROR",
        "message": "active warning still needs review",
        "severity": "warning",
    }
    resolved_warning = {
        "code": "MULTI_TABLE_WRITE",
        "message": "proc also writes dim.other",
        "severity": "warning",
    }
    active_error = {
        "code": "PARSE_ERROR",
        "message": "DDL failed to parse near token",
        "severity": "error",
    }
    _write_table(
        table_dir / "dim.dim_address.json",
        "dim.dim_address",
        {
            "errors": [active_error],
            "warnings": [active_warning, resolved_warning],
            "scoping": {"status": "no_writer_found"},
        },
    )
    identity = diagnostic_identity(
        "dim.dim_address",
        resolved_warning,
        object_type="table",
    )
    write_reviewed_diagnostic(
        tmp_path,
        ReviewedDiagnostic(
            **identity.model_dump(),
            status="accepted",
            reason="Reviewed table slice.",
            evidence=["catalog/tables/dim.dim_address.json"],
        ),
    )

    result = build_batch_plan(tmp_path)

    assert result.status_summary.diagnostic_rows[0].fqn == "dim.dim_address"
    assert result.status_summary.diagnostic_rows[0].errors_unresolved == 1
    assert result.status_summary.diagnostic_rows[0].warnings_unresolved == 1
    assert result.status_summary.diagnostic_rows[0].warnings_resolved == 1
    assert result.status_summary.diagnostic_rows[0].details_command == "/status dim.dim_address"
    serialized = result.status_summary.model_dump_json()
    assert "DDL failed to parse near token" not in serialized
    assert "active warning still needs review" not in serialized
    assert "proc also writes dim.other" not in serialized
