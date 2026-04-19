from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

_TESTS_DIR = Path(__file__).parent
_REFACTOR_FIXTURES = _TESTS_DIR / "fixtures"


def _make_writable_copy() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy refactor fixtures to a temp dir so write tests can mutate them."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "refactor"
    shutil.copytree(_REFACTOR_FIXTURES, dst)
    return tmp, dst

def _semantic_review(*, passed: bool = True, issues: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "passed": passed,
        "checks": {
            "source_tables": {"passed": passed, "summary": "source tables match"},
            "output_columns": {"passed": passed, "summary": "output columns match"},
            "joins": {"passed": passed, "summary": "joins match"},
            "filters": {"passed": passed, "summary": "filters match"},
            "aggregation_grain": {"passed": passed, "summary": "aggregation grain matches"},
        },
        "issues": issues or [],
    }

def _compare_sql_result(*, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "sandbox_database": "SBX_ABC123000000",
        "total": 2,
        "passed": 2 if passed else 1,
        "failed": 0 if passed else 1,
        "results": [
            {"scenario_name": "scenario_a", "status": "ok", "equivalent": True, "a_count": 1, "b_count": 1, "a_minus_b": [], "b_minus_a": []},
            {
                "scenario_name": "scenario_b",
                "status": "ok" if passed else "error",
                "equivalent": passed,
                "a_count": 1,
                "b_count": 1,
                "a_minus_b": [] if passed else [{"CustomerID": "42"}],
                "b_minus_a": [],
                "errors": [] if passed else [{"code": "ROW_DIFF", "message": "rows differ"}],
            },
        ],
    }
