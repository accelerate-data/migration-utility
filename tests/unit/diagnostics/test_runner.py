from __future__ import annotations

import json
from pathlib import Path

from shared.catalog import write_json
from shared.diagnostics.context import CatalogContext
from shared.diagnostics.registry import DiagnosticResult, DiagnosticRegistry
from shared.diagnostics.runner import run_checks, run_diagnostics, write_results


def test_run_checks_suppresses_broken_check(tmp_path: Path) -> None:
    catalog_dir = tmp_path / "catalog"
    (catalog_dir / "procedures").mkdir(parents=True)
    (catalog_dir / "procedures" / "dbo.usp_load.json").write_text("{}", encoding="utf-8")
    registry = DiagnosticRegistry()

    def broken(_ctx: CatalogContext) -> None:
        raise RuntimeError("boom")

    registry.register(broken, code="BROKEN", objects=["procedure"], dialects=("tsql",), severity="warning", pass_number=1)

    results, suppressed = run_checks(
        catalog_dir=catalog_dir,
        project_root=tmp_path,
        dialect="tsql",
        known_fqns={"tables": set(), "procedures": {"dbo.usp_load"}, "views": set(), "functions": set()},
        ddl_lookup={},
        pass_number=1,
        registry=registry,
    )

    assert results == {}
    assert suppressed == 1


def test_write_results_replaces_warning_and_error_sections(tmp_path: Path) -> None:
    catalog_dir = tmp_path / "catalog"
    (catalog_dir / "procedures").mkdir(parents=True)
    path = catalog_dir / "procedures" / "dbo.usp_load.json"
    write_json(path, {"warnings": [{"code": "OLD"}], "errors": []})

    warnings_added, errors_added = write_results(
        catalog_dir,
        {
            "dbo.usp_load": [
                DiagnosticResult("WARN", "warning", "warning"),
                DiagnosticResult("ERR", "error", "error"),
            ]
        },
    )

    assert (warnings_added, errors_added) == (1, 1)
    data = json.loads(path.read_text(encoding="utf-8"))
    assert [item["code"] for item in data["warnings"]] == ["WARN"]
    assert [item["code"] for item in data["errors"]] == ["ERR"]


def test_run_diagnostics_imports_from_runner_module(tmp_path: Path) -> None:
    assert run_diagnostics(tmp_path, dialect="tsql") == {
        "objects_checked": 0,
        "warnings_added": 0,
        "errors_added": 0,
    }
