"""Tests for profile.py -- profiling context assembly and catalog write-back.

Tests import shared.profile core functions directly (not via subprocess) to keep
execution fast and test coverage clear.  Run via uv to ensure shared is
importable: uv run --project <shared> pytest tests/ad-migration/migration/
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from shared import profile

_TESTS_DIR = Path(__file__).parent
_PROFILE_FIXTURES = _TESTS_DIR / "fixtures" / "profile"


def _make_writable_copy() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy profile fixtures to a temp dir so write tests can mutate them."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "profile"
    shutil.copytree(_PROFILE_FIXTURES, dst)
    return tmp, dst


# ── Context: rich catalog signals ────────────────────────────────────────────


def test_context_rich_catalog_all_signals_present() -> None:
    """Context with rich catalog returns all catalog signals."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    assert result["table"] == "silver.factsales"
    assert result["writer"] == "dbo.usp_load_fact_sales"

    signals = result["catalog_signals"]
    assert len(signals["primary_keys"]) == 1
    assert signals["primary_keys"][0]["columns"] == ["sale_id"]
    assert len(signals["foreign_keys"]) == 1
    assert signals["foreign_keys"][0]["columns"] == ["customer_key"]
    assert len(signals["auto_increment_columns"]) == 1
    assert signals["auto_increment_columns"][0]["column"] == "sale_id"
    assert signals["change_capture"]["enabled"] is True
    assert signals["change_capture"]["mechanism"] == "cdc"
    assert len(signals["sensitivity_classifications"]) == 1
    assert signals["sensitivity_classifications"][0]["column"] == "customer_email"


def test_context_rich_catalog_columns() -> None:
    """Context includes column list from table catalog."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    col_names = [c["name"] for c in result["columns"]]
    assert "sale_id" in col_names
    assert "customer_key" in col_names
    assert "load_date" in col_names


def test_context_rich_catalog_writer_references() -> None:
    """Context includes writer procedure references."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    refs = result["writer_references"]
    table_refs = refs["tables"]["in_scope"]
    ref_names = [f"{t['schema']}.{t['name']}" for t in table_refs]
    assert any("FactSales" in n for n in ref_names)


def test_context_rich_catalog_proc_body() -> None:
    """Context includes proc body text."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    assert "INSERT INTO" in result["proc_body"]
    assert "silver.FactSales" in result["proc_body"]


# ── Context: bare catalog (no constraints) ───────────────────────────────────


def test_context_bare_catalog_empty_arrays() -> None:
    """Context with bare catalog returns empty arrays, no errors."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimCustomer", "dbo.usp_merge_dim_customer",
    )
    signals = result["catalog_signals"]
    assert signals["primary_keys"] == []
    assert signals["foreign_keys"] == []
    assert signals["auto_increment_columns"] == []
    assert signals["unique_indexes"] == []
    assert signals["change_capture"] is None
    assert signals["sensitivity_classifications"] == []


# ── Context: related procedures ──────────────────────────────────────────────


def test_context_related_procedures_included() -> None:
    """Context with writer that has EXEC chains includes related proc bodies."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimCustomer", "dbo.usp_merge_dim_customer",
    )
    related = result["related_procedures"]
    assert len(related) >= 1
    related_names = [r["procedure"] for r in related]
    assert "dbo.usp_helper_log" in related_names
    helper = next(r for r in related if r["procedure"] == "dbo.usp_helper_log")
    assert "proc_body" in helper
    assert "INSERT INTO" in helper["proc_body"]


# ── Context: error paths ────────────────────────────────────────────────────


def test_context_missing_table_catalog_exits_1() -> None:
    """Context with nonexistent table exits 1."""
    from typer import Exit

    with pytest.raises(Exit) as exc_info:
        profile.run_context(
            _PROFILE_FIXTURES, "dbo.NonexistentTable", "dbo.usp_load_fact_sales",
        )
    assert exc_info.value.exit_code == 1


def test_context_missing_proc_catalog_exits_1() -> None:
    """Context with nonexistent writer proc exits 1."""
    from typer import Exit

    with pytest.raises(Exit) as exc_info:
        profile.run_context(
            _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_nonexistent_proc",
        )
    assert exc_info.value.exit_code == 1


def test_context_missing_proc_body_returns_empty_string() -> None:
    """Context with valid catalog but missing DDL body returns empty proc_body."""
    tmp, ddl_path = _make_writable_copy()
    try:
        # Create a proc catalog file for a proc that has no DDL body
        proc_dir = ddl_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True, exist_ok=True)
        ghost_proc = {
            "schema": "dbo",
            "name": "usp_ghost",
            "references": {"tables": {"in_scope": []}, "procedures": {"in_scope": []}},
        }
        (proc_dir / "dbo.usp_ghost.json").write_text(
            json.dumps(ghost_proc, indent=2), encoding="utf-8",
        )
        result = profile.run_context(ddl_path, "silver.FactSales", "dbo.usp_ghost")
        assert result["proc_body"] == ""
    finally:
        tmp.cleanup()


# ── Write: valid profile ─────────────────────────────────────────────────────


def test_write_valid_profile_merges() -> None:
    """Write valid profile merges into catalog file."""
    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "rationale": "Pure INSERT with no UPDATE or DELETE.",
                "source": "llm",
            },
            "primary_key": {
                "columns": ["sale_id"],
                "primary_key_type": "surrogate",
                "source": "catalog",
            },
        }
        result = profile.run_write(ddl_path, "silver.FactSales", valid_profile)
        assert result["ok"] is True

        # Verify catalog file was updated
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        assert "profile" in cat
        assert cat["profile"]["status"] == "ok"
        assert cat["profile"]["classification"]["resolved_kind"] == "fact_transaction"
    finally:
        tmp.cleanup()


# ── Write: missing required field ────────────────────────────────────────────


def test_write_missing_required_field_exits_1() -> None:
    """Write with missing required field exits 1."""
    from typer import Exit

    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "writer": "dbo.usp_load_fact_sales",
            # missing "status"
        }
        with pytest.raises(Exit) as exc_info:
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
        assert exc_info.value.exit_code == 1
    finally:
        tmp.cleanup()


# ── Write: invalid enum value ────────────────────────────────────────────────


def test_write_invalid_enum_exits_1() -> None:
    """Write with invalid enum value exits 1."""
    from typer import Exit

    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "invalid_kind",
                "source": "llm",
            },
        }
        with pytest.raises(Exit) as exc_info:
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
        assert exc_info.value.exit_code == 1
    finally:
        tmp.cleanup()


def test_write_invalid_fk_type_exits_1() -> None:
    """Write with invalid FK type exits 1."""
    from typer import Exit

    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "foreign_keys": [
                {
                    "column": "customer_key",
                    "fk_type": "invalid_type",
                    "source": "llm",
                }
            ],
        }
        with pytest.raises(Exit) as exc_info:
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
        assert exc_info.value.exit_code == 1
    finally:
        tmp.cleanup()


def test_write_invalid_suggested_action_exits_1() -> None:
    """Write with invalid suggested action exits 1."""
    from typer import Exit

    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "pii_actions": [
                {
                    "column": "email",
                    "suggested_action": "encrypt",
                    "source": "llm",
                }
            ],
        }
        with pytest.raises(Exit) as exc_info:
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
        assert exc_info.value.exit_code == 1
    finally:
        tmp.cleanup()


def test_write_invalid_source_exits_1() -> None:
    """Write with invalid source enum exits 1."""
    from typer import Exit

    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "source": "invalid_source",
            },
        }
        with pytest.raises(Exit) as exc_info:
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
        assert exc_info.value.exit_code == 1
    finally:
        tmp.cleanup()


# ── Write: nonexistent catalog ───────────────────────────────────────────────


def test_write_nonexistent_catalog_exits_2() -> None:
    """Write to nonexistent catalog file exits 2."""
    from typer import Exit

    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_nonexistent",
        }
        with pytest.raises(Exit) as exc_info:
            profile.run_write(ddl_path, "dbo.NonexistentTable", valid_profile)
        assert exc_info.value.exit_code == 2
    finally:
        tmp.cleanup()


# ── Write: idempotent ────────────────────────────────────────────────────────


def test_write_idempotent() -> None:
    """Running write twice with the same profile produces identical catalog."""
    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "rationale": "Pure INSERT.",
                "source": "llm",
            },
            "primary_key": {
                "columns": ["sale_id"],
                "primary_key_type": "surrogate",
                "source": "catalog",
            },
            "watermark": {
                "column": "load_date",
                "rationale": "WHERE load_date > @batch_date in proc.",
                "source": "llm",
            },
        }
        profile.run_write(ddl_path, "silver.FactSales", valid_profile)
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        first = cat_path.read_text(encoding="utf-8")

        profile.run_write(ddl_path, "silver.FactSales", valid_profile)
        second = cat_path.read_text(encoding="utf-8")

        assert first == second
    finally:
        tmp.cleanup()
