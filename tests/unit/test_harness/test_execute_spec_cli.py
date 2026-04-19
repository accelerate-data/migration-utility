from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch


from shared.loader_io import write_manifest_sandbox
from shared.output_models.sandbox import ErrorEntry, ExecuteSpecOutput, TestHarnessExecuteOutput
from tests.unit.test_harness.helpers import _cli_env, _write_fixture_manifest, _write_test_spec


class TestCLIExecuteSpec:
    """E2E: invoke execute-spec via CliRunner, verify expect.rows written back."""

    def test_execute_spec_writes_expect_rows(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_00000000000B")

        unit_tests = [
            {
                "name": "test_merge_matched",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load_DimProduct]",
                "given": [
                    {"table": "[bronze].[Product]", "rows": [{"id": 1}]},
                ],
            }
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.return_value = TestHarnessExecuteOutput(
            scenario_name="test_merge_matched",
            status="ok",
            ground_truth_rows=[{"ProductKey": 1, "Name": "Widget"}],
            row_count=1,
            errors=[],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["ok"] == 1
        assert output["failed"] == 0

        # Verify spec file was updated with expect.rows
        updated_spec = json.loads(spec_path.read_text())
        assert updated_spec["unit_tests"][0]["expect"] == {
            "rows": [{"ProductKey": 1, "Name": "Widget"}],
        }

    def test_execute_spec_partial_failure(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_00000000000B")

        unit_tests = [
            {
                "name": "test_ok",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 1}]}],
            },
            {
                "name": "test_fail",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 2}]}],
            },
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.side_effect = [
            TestHarnessExecuteOutput(
                scenario_name="test_ok",
                status="ok",
                ground_truth_rows=[{"id": 1}],
                row_count=1,
                errors=[],
            ),
            TestHarnessExecuteOutput(
                scenario_name="test_fail",
                status="error",
                ground_truth_rows=[],
                row_count=0,
                errors=[ErrorEntry(code="SCENARIO_FAILED", message="insert failed")],
            ),
        ]

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["ok"] == 1
        assert output["failed"] == 1

    def test_execute_spec_all_fail_exits_1(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_00000000000B")

        unit_tests = [
            {
                "name": "test_fail",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 1}]}],
            },
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.return_value = TestHarnessExecuteOutput(
            scenario_name="test_fail",
            status="error",
            ground_truth_rows=[],
            row_count=0,
            errors=[ErrorEntry(code="SCENARIO_FAILED", message="connection refused")],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 1

    def test_execute_spec_output_model(self) -> None:
        result = ExecuteSpecOutput.model_validate({
            "schema_version": "1.0",
            "sandbox_database": "SBX_ABC123000000",
            "spec_path": "test-specs/silver.dimproduct.json",
            "total": 2,
            "ok": 1,
            "failed": 1,
            "results": [
                {
                    "scenario_name": "test_merge_matched",
                    "status": "ok",
                    "row_count": 1,
                    "errors": [],
                },
                {
                    "scenario_name": "test_merge_not_matched",
                    "status": "error",
                    "row_count": 0,
                    "errors": [{"code": "SCENARIO_FAILED", "message": "insert failed"}],
                },
            ],
        })
        assert result.ok == 1
        assert result.failed == 1
        assert len(result.results) == 2
