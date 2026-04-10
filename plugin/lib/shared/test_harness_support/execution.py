"""Execution helpers for the test harness CLI."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.output_models import (
    ErrorEntry,
    ExecuteSpecOutput,
    ExecuteSpecResult,
    TestHarnessExecuteOutput,
)
from shared.sandbox.base import SandboxBackend

logger = logging.getLogger(__name__)


def load_json_file(path: Path, *, not_found_code: str, invalid_code: str, kind: str) -> dict[str, Any]:
    """Read a required JSON file and raise ValueError with a stable code prefix."""
    try:
        with path.open() as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise ValueError(f"{not_found_code}: {kind} file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{invalid_code}: {kind} file is not valid JSON: {exc}") from exc


def run_execute_spec(
    backend: SandboxBackend,
    sandbox_db: str,
    spec_path: Path,
) -> ExecuteSpecOutput:
    """Bulk-execute all scenarios in a test spec and write expect.rows back."""
    spec_data = load_json_file(
        spec_path,
        not_found_code="SPEC_NOT_FOUND",
        invalid_code="SPEC_INVALID_JSON",
        kind="Test spec",
    )
    unit_tests = spec_data.get("unit_tests", [])
    if not unit_tests:
        raise ValueError("SPEC_EMPTY: Test spec has no unit_tests entries")

    results: list[ExecuteSpecResult] = []
    ok_count = 0
    failed_count = 0

    for test_entry in unit_tests:
        try:
            if "procedure" in test_entry:
                scenario = {
                    "name": test_entry["name"],
                    "target_table": test_entry["target_table"],
                    "procedure": test_entry["procedure"],
                    "given": test_entry["given"],
                }
                exec_result = backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario)
            else:
                exec_result = backend.execute_select(
                    sandbox_db=sandbox_db,
                    sql=test_entry["sql"],
                    fixtures=test_entry["given"],
                )
                exec_result = exec_result.model_copy(
                    update={"scenario_name": test_entry["name"]},
                )
        except (ValueError, KeyError) as exc:
            exec_result = TestHarnessExecuteOutput(
                scenario_name=test_entry.get("name", "unknown"),
                status="error",
                ground_truth_rows=[],
                row_count=0,
                errors=[ErrorEntry(code="EXECUTE_INVALID_INPUT", message=str(exc))],
            )

        results.append(ExecuteSpecResult(
            scenario_name=exec_result.scenario_name,
            status=exec_result.status,
            row_count=exec_result.row_count,
            errors=exec_result.errors,
        ))

        if exec_result.status == "ok":
            test_entry["expect"] = {"rows": exec_result.ground_truth_rows}
            ok_count += 1
        else:
            test_entry.pop("expect", None)
            failed_count += 1
            logger.warning(
                "event=scenario_failed command=execute_spec sandbox_db=%s scenario=%s errors=%s",
                sandbox_db, test_entry["name"], exec_result.errors,
            )

    with spec_path.open("w") as handle:
        json.dump(spec_data, handle, indent=2)

    return ExecuteSpecOutput(
        sandbox_database=sandbox_db,
        spec_path=str(spec_path),
        total=len(unit_tests),
        ok=ok_count,
        failed=failed_count,
        results=results,
    )


def run_compare_sql(
    backend: SandboxBackend,
    sandbox_db: str,
    sql_a_file: Path,
    sql_b_file: Path,
    spec_path: Path,
) -> dict[str, Any]:
    """Compare two SQL SELECT statements for equivalence per test scenario."""
    for label, path in (("A", sql_a_file), ("B", sql_b_file)):
        if not path.exists():
            raise ValueError(f"SQL_FILE_NOT_FOUND: SQL file {label} not found: {path}")

    try:
        sql_a = sql_a_file.read_text(encoding="utf-8")
        sql_b = sql_b_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"SQL_FILE_READ_ERROR: Cannot read SQL file: {exc}") from exc

    spec_data = load_json_file(
        spec_path,
        not_found_code="SPEC_NOT_FOUND",
        invalid_code="SPEC_INVALID_JSON",
        kind="Test spec",
    )
    unit_tests = spec_data.get("unit_tests", [])
    if not unit_tests:
        raise ValueError("SPEC_EMPTY: Test spec has no unit_tests entries")

    results: list[dict[str, Any]] = []
    passed_count = 0
    failed_count = 0

    for test_entry in unit_tests:
        fixtures = test_entry.get("given", [])
        scenario_name = test_entry.get("name", "unnamed")

        try:
            result = backend.compare_two_sql(
                sandbox_db=sandbox_db,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )
        except (ValueError, KeyError) as exc:
            result = {
                "status": "error",
                "equivalent": False,
                "a_count": 0,
                "b_count": 0,
                "a_minus_b": [],
                "b_minus_a": [],
                "errors": [{"code": "COMPARE_INVALID_INPUT", "message": str(exc)}],
            }

        result["scenario_name"] = scenario_name
        results.append(result)

        if result.get("equivalent"):
            passed_count += 1
        else:
            failed_count += 1
            logger.warning(
                "event=compare_scenario_failed command=compare_sql sandbox_db=%s scenario=%s errors=%s",
                sandbox_db, scenario_name, result.get("errors"),
            )

    return {
        "schema_version": "1.0",
        "sandbox_database": sandbox_db,
        "total": len(unit_tests),
        "passed": passed_count,
        "failed": failed_count,
        "results": results,
    }
