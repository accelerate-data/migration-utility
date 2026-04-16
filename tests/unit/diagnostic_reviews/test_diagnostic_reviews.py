"""Tests for reviewed catalog diagnostic support."""

from __future__ import annotations

import json
from pathlib import Path

from shared.diagnostic_reviews import (
    DiagnosticIdentity,
    ReviewedDiagnostic,
    diagnostic_identity,
    load_reviewed_diagnostics,
    partition_reviewed_warnings,
    write_reviewed_diagnostic,
)


def test_diagnostic_identity_includes_fqn_code_and_message_hash() -> None:
    entry = {
        "code": "PARSE_ERROR",
        "message": "dynamic SQL reduced parse confidence",
        "severity": "warning",
    }

    identity = diagnostic_identity("gold.rpt_sales_by_category", entry)

    assert identity.fqn == "gold.rpt_sales_by_category"
    assert identity.code == "PARSE_ERROR"
    assert identity.message_hash.startswith("sha256:")
    assert identity.message_hash == diagnostic_identity("gold.rpt_sales_by_category", dict(entry)).message_hash
    changed = diagnostic_identity(
        "gold.rpt_sales_by_category",
        {**entry, "message": "different message"},
    )
    assert changed.message_hash != identity.message_hash


def test_write_and_load_reviewed_diagnostic_round_trips(tmp_path: Path) -> None:
    identity = DiagnosticIdentity(
        fqn="dim.dim_address",
        object_type="table",
        code="MULTI_TABLE_WRITE",
        message_hash="sha256:abc123",
    )

    written = write_reviewed_diagnostic(
        tmp_path,
        ReviewedDiagnostic(
            **identity.model_dump(),
            status="accepted",
            reason="Reviewed table-specific slice; multi-table proc is intentional.",
            evidence=[
                "catalog/tables/dim.dim_address.json",
                "catalog/procedures/dbo.usp_load_dim_address_and_credit_card.json",
            ],
        ),
    )

    assert written == tmp_path / "catalog" / "diagnostic-reviews.json"
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "1.0"
    assert payload["reviews"][0]["fqn"] == "dim.dim_address"
    loaded = load_reviewed_diagnostics(tmp_path)
    assert loaded[0].reason == "Reviewed table-specific slice; multi-table proc is intentional."


def test_partition_reviewed_warnings_hides_only_matching_accepted_reviews(tmp_path: Path) -> None:
    active = [
        {
            "code": "MULTI_TABLE_WRITE",
            "message": "proc writes another table",
            "severity": "warning",
        },
        {
            "code": "PARSE_ERROR",
            "message": "parser fallback",
            "severity": "warning",
        },
    ]
    first_identity = diagnostic_identity("dim.dim_address", active[0], object_type="table")
    write_reviewed_diagnostic(
        tmp_path,
        ReviewedDiagnostic(
            **first_identity.model_dump(),
            status="accepted",
            reason="Intentional multi-table writer.",
            evidence=[],
        ),
    )

    visible, hidden = partition_reviewed_warnings(
        tmp_path,
        fqn="dim.dim_address",
        object_type="table",
        warnings=active,
    )

    assert [warning["code"] for warning in visible] == ["PARSE_ERROR"]
    assert hidden == 1


def test_partition_reviewed_warnings_does_not_hide_changed_message(tmp_path: Path) -> None:
    old_entry = {
        "code": "MULTI_TABLE_WRITE",
        "message": "old warning text",
        "severity": "warning",
    }
    identity = diagnostic_identity("dim.dim_address", old_entry, object_type="table")
    write_reviewed_diagnostic(
        tmp_path,
        ReviewedDiagnostic(
            **identity.model_dump(),
            status="accepted",
            reason="Old review.",
            evidence=[],
        ),
    )

    visible, hidden = partition_reviewed_warnings(
        tmp_path,
        fqn="dim.dim_address",
        object_type="table",
        warnings=[{**old_entry, "message": "new warning text"}],
    )

    assert len(visible) == 1
    assert hidden == 0
