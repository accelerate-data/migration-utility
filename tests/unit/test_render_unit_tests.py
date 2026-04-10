"""Unit tests for ``migrate render-unit-tests`` CLI subcommand."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from shared.migrate import run_render_unit_tests
from shared.output_models import RenderUnitTestsOutput


def _minimal_spec(
    item_id: str = "silver.dimcustomer",
    unit_tests: list | None = None,
) -> dict:
    """Build a minimal valid TestSpec dict."""
    if unit_tests is None:
        unit_tests = [
            {
                "name": "merge_insert_new_row",
                "target_table": "silver.DimCustomer",
                "procedure": "silver.usp_load_dimcustomer",
                "given": [
                    {"table": "bronze.CustomerRaw", "rows": [{"CustomerID": 1, "Name": "Alice"}]},
                ],
                "expect": {"rows": [{"CustomerKey": 1, "CustomerID": 1, "Name": "Alice"}]},
            },
            {
                "name": "merge_update_existing",
                "target_table": "silver.DimCustomer",
                "procedure": "silver.usp_load_dimcustomer",
                "given": [
                    {"table": "bronze.CustomerRaw", "rows": [{"CustomerID": 1, "Name": "Bob"}]},
                    {"table": "silver.DimCustomer", "rows": [{"CustomerKey": 1, "CustomerID": 1, "Name": "Alice"}]},
                ],
                "expect": {"rows": [{"CustomerKey": 1, "CustomerID": 1, "Name": "Bob"}]},
            },
        ]
    return {
        "item_id": item_id,
        "object_type": "table",
        "status": "ok",
        "coverage": "complete",
        "branch_manifest": [
            {"id": "merge_insert", "statement_index": 0, "description": "MERGE WHEN NOT MATCHED", "scenarios": ["merge_insert_new_row"]},
            {"id": "merge_update", "statement_index": 0, "description": "MERGE WHEN MATCHED", "scenarios": ["merge_update_existing"]},
        ],
        "unit_tests": unit_tests,
        "uncovered_branches": [],
        "warnings": [],
        "validation": {"passed": True, "issues": []},
        "errors": [],
    }


def _view_spec() -> dict:
    """Build a view-based test spec."""
    return {
        "item_id": "silver.vw_customer_summary",
        "object_type": "view",
        "status": "ok",
        "coverage": "complete",
        "branch_manifest": [
            {"id": "where_active", "statement_index": 0, "description": "WHERE IsActive = 1", "scenarios": ["active_filter"]},
        ],
        "unit_tests": [
            {
                "name": "active_filter",
                "sql": "SELECT * FROM silver.vw_customer_summary",
                "given": [
                    {"table": "silver.DimCustomer", "rows": [{"CustomerKey": 1, "IsActive": 1}]},
                ],
                "expect": {"rows": [{"CustomerKey": 1, "IsActive": 1}]},
            },
        ],
        "uncovered_branches": [],
        "warnings": [],
        "validation": {"passed": True, "issues": []},
        "errors": [],
    }


class TestRunRenderUnitTests:
    def test_renders_table_proc_scenarios(self, tmp_path: Path) -> None:
        """Proc-based test spec scenarios are translated to dbt unit tests."""
        spec_path = tmp_path / "test-specs" / "silver.dimcustomer.json"
        spec_path.parent.mkdir(parents=True)
        spec_path.write_text(json.dumps(_minimal_spec()), encoding="utf-8")
        yml_path = tmp_path / "dbt" / "models" / "staging" / "_stg_dimcustomer.yml"

        result = run_render_unit_tests(tmp_path, "silver.DimCustomer", "stg_dimcustomer", spec_path, yml_path)

        assert isinstance(result, RenderUnitTestsOutput)
        assert result.tests_rendered == 2
        assert result.model_name == "stg_dimcustomer"
        assert result.errors == []

        schema = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
        assert schema["version"] == 2
        model = schema["models"][0]
        assert model["name"] == "stg_dimcustomer"
        unit_tests = model["unit_tests"]
        assert len(unit_tests) == 2
        assert unit_tests[0]["name"] == "merge_insert_new_row"
        assert unit_tests[0]["model"] == "stg_dimcustomer"
        assert unit_tests[0]["given"][0]["input"] == "source('bronze', 'CustomerRaw')"
        assert unit_tests[1]["given"][1]["input"] == "source('silver', 'DimCustomer')"

    def test_renders_view_scenarios(self, tmp_path: Path) -> None:
        """View-based test spec scenarios use same source() mapping."""
        spec_path = tmp_path / "test-specs" / "silver.vw_customer_summary.json"
        spec_path.parent.mkdir(parents=True)
        spec_path.write_text(json.dumps(_view_spec()), encoding="utf-8")
        yml_path = tmp_path / "dbt" / "models" / "_vw_customer_summary.yml"

        result = run_render_unit_tests(tmp_path, "silver.vw_customer_summary", "vw_customer_summary", spec_path, yml_path)

        assert result.tests_rendered == 1
        schema = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
        ut = schema["models"][0]["unit_tests"][0]
        assert ut["given"][0]["input"] == "source('silver', 'DimCustomer')"

    def test_preserves_existing_schema_tests(self, tmp_path: Path) -> None:
        """Existing schema tests in YAML are preserved when unit tests are added."""
        spec_path = tmp_path / "test-specs" / "silver.dimcustomer.json"
        spec_path.parent.mkdir(parents=True)
        spec_path.write_text(json.dumps(_minimal_spec()), encoding="utf-8")
        yml_path = tmp_path / "schema.yml"

        existing_schema = {
            "version": 2,
            "models": [
                {
                    "name": "stg_dimcustomer",
                    "description": "Customer dimension",
                    "columns": [
                        {"name": "CustomerKey", "tests": ["unique", "not_null"]},
                    ],
                },
            ],
        }
        yml_path.write_text(yaml.dump(existing_schema), encoding="utf-8")

        result = run_render_unit_tests(tmp_path, "silver.DimCustomer", "stg_dimcustomer", spec_path, yml_path)

        assert result.tests_rendered == 2
        schema = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
        model = schema["models"][0]
        assert model["description"] == "Customer dimension"
        assert model["columns"][0]["tests"] == ["unique", "not_null"]
        assert len(model["unit_tests"]) == 2

    def test_empty_spec_returns_zero(self, tmp_path: Path) -> None:
        """Spec with no unit tests returns tests_rendered=0 with warning."""
        spec_path = tmp_path / "test-specs" / "silver.empty.json"
        spec_path.parent.mkdir(parents=True)
        spec_path.write_text(json.dumps(_minimal_spec(unit_tests=[])), encoding="utf-8")
        yml_path = tmp_path / "schema.yml"

        result = run_render_unit_tests(tmp_path, "silver.empty", "stg_empty", spec_path, yml_path)

        assert result.tests_rendered == 0
        assert len(result.warnings) == 1
        assert result.warnings[0].code == "NO_UNIT_TESTS"

    def test_handles_missing_expect(self, tmp_path: Path) -> None:
        """Scenarios without expect (pre-ground-truth) are still rendered."""
        no_expect_tests = [
            {
                "name": "insert_new",
                "procedure": "silver.usp_load",
                "given": [{"table": "bronze.Raw", "rows": [{"id": 1}]}],
            },
        ]
        spec_path = tmp_path / "test-specs" / "silver.t.json"
        spec_path.parent.mkdir(parents=True)
        spec_path.write_text(json.dumps(_minimal_spec(item_id="silver.t", unit_tests=no_expect_tests)), encoding="utf-8")
        yml_path = tmp_path / "schema.yml"

        result = run_render_unit_tests(tmp_path, "silver.t", "stg_t", spec_path, yml_path)

        assert result.tests_rendered == 1
        schema = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
        ut = schema["models"][0]["unit_tests"][0]
        assert "expect" not in ut

    def test_spec_not_found(self, tmp_path: Path) -> None:
        """Missing spec file returns error."""
        result = run_render_unit_tests(
            tmp_path, "silver.missing", "stg_missing",
            tmp_path / "nonexistent.json",
            tmp_path / "schema.yml",
        )
        assert result.tests_rendered == 0
        assert len(result.errors) == 1
        assert result.errors[0].code == "SPEC_NOT_FOUND"
