from __future__ import annotations

import json
import subprocess

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from shared import profile
from shared.catalog import load_table_catalog
from shared.loader import CatalogFileMissingError
from tests.unit.profile.helpers import _make_writable_copy

_cli_runner = CliRunner()


def test_write_valid_profile_merges() -> None:
    """Write valid profile merges into catalog file."""
    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
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
            "natural_key": {
                "columns": ["order_number"],
                "source": "catalog+llm",
                "rationale": "Order number is the business identifier.",
            },
            "watermark": {
                "column": "load_date",
                "source": "llm",
                "rationale": "Writer filters by load date.",
            },
            "foreign_keys": [
                {
                    "column": "customer_key",
                    "fk_type": "standard",
                    "references_source_relation": "silver.dimcustomer",
                    "references_column": "customer_key",
                    "source": "catalog",
                    "rationale": "Declared customer FK.",
                }
            ],
            "pii_actions": [
                {
                    "column": "customer_email",
                    "suggested_action": "mask",
                    "entity": "email",
                    "source": "catalog",
                    "rationale": "Catalog marks this as email.",
                }
            ],
            "warnings": [{"code": "LOW_CONFIDENCE", "message": "Limited evidence.", "severity": "warning"}],
            "errors": [{"code": "PROFILE_FAILED", "message": "Profile failed.", "severity": "error"}],
        }
        result = profile.run_write(ddl_path, "silver.FactSales", valid_profile)
        assert result["ok"] is True

        # Verify catalog file was updated
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        assert "profile" in cat
        assert cat["profile"]["status"] == "ok"
        assert "writer" not in cat["profile"]
        assert cat["profile"]["classification"]["resolved_kind"] == "fact_transaction"
        assert cat["profile"]["natural_key"]["columns"] == ["order_number"]
        assert cat["profile"]["watermark"]["column"] == "load_date"
        assert cat["profile"]["foreign_keys"][0]["references_source_relation"] == "silver.dimcustomer"
        assert cat["profile"]["pii_actions"][0]["entity"] == "email"
        assert cat["profile"]["warnings"][0]["severity"] == "warning"
        assert cat["profile"]["errors"][0]["severity"] == "error"
    finally:
        tmp.cleanup()

@pytest.mark.parametrize(
    ("profile_json", "expected_status"),
    [
        (
            {
                "classification": {"resolved_kind": "fact_transaction", "source": "llm"},
                "primary_key": {"columns": ["sale_id"], "primary_key_type": "surrogate", "source": "catalog"},
            },
            "ok",
        ),
        ({"classification": {"resolved_kind": "fact_transaction", "source": "llm"}}, "partial"),
        ({}, "error"),
        ({"classification": {"resolved_kind": "seed", "source": "catalog"}}, "ok"),
    ],
)
def test_derive_table_profile_status(profile_json: dict[str, object], expected_status: str) -> None:
    """Table profile status derivation is isolated from write-back."""
    section = profile.TableProfileSection.model_validate(profile_json)

    assert profile.derive_table_profile_status(section) == expected_status

def test_derive_view_profile_status() -> None:
    """View profile status derivation is isolated from write-back."""
    section = profile.ViewProfileSection.model_validate({
        "classification": "stg",
        "rationale": "Single-source pass-through.",
        "source": "llm",
    })

    assert profile.derive_view_profile_status(section) == "ok"

def test_write_seed_profile_allowed_for_seed_table() -> None:
    """Seed tables can persist a seed classification profile."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_seed"] = True
        cat["is_source"] = False
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = profile.run_write(
            root,
            "silver.FactSales",
            {"classification": {"resolved_kind": "seed", "source": "catalog", "rationale": "Static seed data."}},
        )
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result["table"] == "silver.factsales"
        assert written["profile"]["status"] == "ok"
        assert written["profile"]["classification"]["resolved_kind"] == "seed"

def test_write_non_seed_profile_rejected_for_seed_table() -> None:
    """Seed tables cannot be overwritten with writer-driven profile content."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_seed"] = True
        cat["is_source"] = False
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        with pytest.raises(ValueError, match="seed table profiles must use seed classification"):
            profile.run_write(
                root,
                "silver.FactSales",
                {
                    "classification": {
                        "resolved_kind": "fact_transaction",
                        "rationale": "Fact table.",
                        "source": "llm",
                    },
                    "primary_key": {
                        "columns": ["sale_id"],
                        "primary_key_type": "surrogate",
                        "source": "catalog",
                    },
                },
            )

def test_write_seed_profile_rejected_for_non_seed_table() -> None:
    """Non-seed tables cannot be profiled with seed classification."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(ValueError, match="seed classification requires is_seed"):
            profile.run_write(
                root,
                "silver.FactSales",
                {"classification": {"resolved_kind": "seed", "source": "catalog"}},
            )

def test_write_legacy_writer_field_raises() -> None:
    """Write rejects the legacy persisted writer field."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "rationale": "test",
                "source": "llm",
            },
        }
        with pytest.raises(ValidationError, match="writer"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()

def test_write_invalid_enum_raises() -> None:
    """Write with invalid enum value raises model validation error."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "classification": {
                "resolved_kind": "invalid_kind",
                "source": "llm",
            },
        }
        with pytest.raises(ValidationError, match="classification.resolved_kind"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()

def test_write_invalid_fk_type_raises() -> None:
    """Write with invalid FK type raises model validation error."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "foreign_keys": [
                {
                    "column": "customer_key",
                    "fk_type": "invalid_type",
                    "source": "llm",
                }
            ],
        }
        with pytest.raises(ValidationError, match="foreign_keys.0.fk_type"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()

def test_write_invalid_suggested_action_raises() -> None:
    """Write with invalid suggested action raises model validation error."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "pii_actions": [
                {
                    "column": "email",
                    "suggested_action": "encrypt",
                    "source": "llm",
                }
            ],
        }
        with pytest.raises(ValidationError, match="pii_actions.0.suggested_action"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()

def test_write_invalid_source_raises() -> None:
    """Write with invalid source enum raises model validation error."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "classification": {
                "resolved_kind": "fact_transaction",
                "source": "invalid_source",
            },
        }
        with pytest.raises(ValidationError, match="classification.source"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()

def test_write_nonexistent_catalog_raises() -> None:
    """Write to nonexistent catalog file raises CatalogFileMissingError."""
    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile: dict[str, object] = {}
        with pytest.raises(CatalogFileMissingError):
            profile.run_write(ddl_path, "dbo.NonexistentTable", valid_profile)
    finally:
        tmp.cleanup()

def test_write_idempotent() -> None:
    """Running write twice with the same profile produces identical catalog."""
    import copy

    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
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
        profile.run_write(ddl_path, "silver.FactSales", copy.deepcopy(valid_profile))
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        first = cat_path.read_text(encoding="utf-8")

        profile.run_write(ddl_path, "silver.FactSales", copy.deepcopy(valid_profile))
        second = cat_path.read_text(encoding="utf-8")

        assert first == second
    finally:
        tmp.cleanup()

def test_write_cli_emits_error_json_on_validation_failure() -> None:
    """write CLI emits structured error JSON to stdout and exits 1 on validation failure."""
    tmp, ddl_path = _make_writable_copy()
    try:
        subprocess.run(["git", "init"], cwd=ddl_path, capture_output=True, check=True)
        bad_profile = json.dumps({"writer": "dbo.usp_load_fact_sales"})
        result = _cli_runner.invoke(
            profile.app,
            ["write", "--project-root", str(ddl_path), "--table", "silver.FactSales", "--profile", bad_profile],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ok"] is False
        assert "error" in output
        assert output["table"] == "silver.factsales"
    finally:
        tmp.cleanup()

def test_write_corrupt_existing_table_catalog_exit_2() -> None:
    """write with corrupt existing table catalog exits code 2."""
    tmp, ddl_path = _make_writable_copy()
    try:
        subprocess.run(["git", "init"], cwd=ddl_path, capture_output=True, check=True)
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        cat_path.write_text("{truncated", encoding="utf-8")
        good_profile = json.dumps({})
        result = _cli_runner.invoke(
            profile.app,
            ["write", "--project-root", str(ddl_path), "--table", "silver.FactSales", "--profile", good_profile],
        )
        assert result.exit_code == 2
    finally:
        tmp.cleanup()

def test_write_legacy_catalog_profile_writer_fails_loudly() -> None:
    """Legacy table catalogs with profile.writer fail validation on load."""
    tmp, ddl_path = _make_writable_copy()
    try:
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["profile"] = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "source": "llm",
            },
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        with pytest.raises(ValidationError, match="writer"):
            load_table_catalog(ddl_path, "silver.FactSales")
    finally:
        tmp.cleanup()

def test_write_invalid_profile_json_arg_exit_2() -> None:
    """write with invalid JSON string argument exits code 2."""
    tmp, ddl_path = _make_writable_copy()
    try:
        subprocess.run(["git", "init"], cwd=ddl_path, capture_output=True, check=True)
        result = _cli_runner.invoke(
            profile.app,
            ["write", "--project-root", str(ddl_path), "--table", "silver.FactSales", "--profile", "{not json"],
        )
        assert result.exit_code == 2
    finally:
        tmp.cleanup()
