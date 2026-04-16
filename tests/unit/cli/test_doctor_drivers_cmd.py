from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.doctor_drivers_cmd import resolve_driver_requirements
from shared.cli.main import app


runner = CliRunner()


def _write_manifest(project_root: Path, runtime: dict[str, object]) -> None:
    (project_root / "manifest.json").write_text(
        json.dumps({"schema_version": "1", "runtime": runtime}),
        encoding="utf-8",
    )


def test_resolve_driver_requirements_checks_all_supported_technologies_without_manifest(
    tmp_path: Path,
) -> None:
    requirements = resolve_driver_requirements(tmp_path)

    assert [requirement.technology for requirement in requirements] == ["oracle", "sql_server"]
    assert {requirement.driver_module for requirement in requirements} == {"oracledb", "pyodbc"}
    assert all(requirement.roles == [] for requirement in requirements)


def test_resolve_driver_requirements_includes_configured_runtime_roles(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        {
            "source": {"technology": "sql_server", "dialect": "tsql"},
            "sandbox": {"technology": "sql_server", "dialect": "tsql"},
            "target": {"technology": "oracle", "dialect": "oracle"},
        },
    )

    requirements = resolve_driver_requirements(tmp_path)

    by_driver = {requirement.driver_module: requirement for requirement in requirements}
    assert by_driver["pyodbc"].roles == ["source", "sandbox"]
    assert by_driver["oracledb"].roles == ["target"]


def test_doctor_drivers_json_succeeds_when_all_supported_drivers_import(tmp_path: Path) -> None:
    with patch("shared.cli.doctor_drivers_cmd.importlib.import_module") as import_module:
        result = runner.invoke(app, ["doctor", "drivers", "--project-root", str(tmp_path), "--json"])

    assert result.exit_code == 0, result.output
    import_module.assert_any_call("pyodbc")
    import_module.assert_any_call("oracledb")
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert {driver["driver_module"] for driver in payload["drivers"]} == {"pyodbc", "oracledb"}
    assert all(driver["importable"] is True for driver in payload["drivers"])


def test_doctor_drivers_checks_all_supported_drivers_even_when_manifest_has_one_role(
    tmp_path: Path,
) -> None:
    _write_manifest(tmp_path, {"target": {"technology": "sql_server", "dialect": "tsql"}})

    with patch("shared.cli.doctor_drivers_cmd.importlib.import_module") as import_module:
        result = runner.invoke(app, ["doctor", "drivers", "--project-root", str(tmp_path), "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    by_driver = {driver["driver_module"]: driver for driver in payload["drivers"]}
    assert by_driver["pyodbc"]["roles"] == ["target"]
    assert by_driver["oracledb"]["roles"] == []
    import_module.assert_any_call("pyodbc")
    import_module.assert_any_call("oracledb")


def test_doctor_drivers_missing_driver_exits_one_with_maintainer_remediation(
    tmp_path: Path,
) -> None:
    def _import_driver(name: str) -> object:
        if name == "oracledb":
            raise ImportError("missing oracle driver")
        return object()

    with patch("shared.cli.doctor_drivers_cmd.importlib.import_module", side_effect=_import_driver):
        result = runner.invoke(app, ["doctor", "drivers", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
    assert "oracledb" in result.output
    assert "Fix the public CLI package or Homebrew formula resources" in result.output
    assert "pip install" not in result.output
    assert "uv pip install" not in result.output


def test_doctor_drivers_missing_sql_server_driver_exits_one_with_remediation(
    tmp_path: Path,
) -> None:
    def _import_driver(name: str) -> object:
        if name == "pyodbc":
            raise ImportError("missing sql server driver")
        return object()

    with patch("shared.cli.doctor_drivers_cmd.importlib.import_module", side_effect=_import_driver):
        result = runner.invoke(app, ["doctor", "drivers", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
    assert "pyodbc" in result.output
    assert "Fix the public CLI package or Homebrew formula resources" in result.output
    assert "pip install" not in result.output
    assert "uv pip install" not in result.output


def test_doctor_drivers_json_failure_reports_driver_remediation(tmp_path: Path) -> None:
    def _import_driver(name: str) -> object:
        if name == "pyodbc":
            raise ImportError("missing sql server driver")
        return object()

    with patch("shared.cli.doctor_drivers_cmd.importlib.import_module", side_effect=_import_driver):
        result = runner.invoke(app, ["doctor", "drivers", "--project-root", str(tmp_path), "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    by_driver = {driver["driver_module"]: driver for driver in payload["drivers"]}
    assert by_driver["pyodbc"]["importable"] is False
    assert by_driver["pyodbc"]["remediation"] == (
        "Fix the public CLI package or Homebrew formula resources so this driver is "
        "bundled with the installed ad-migration runtime."
    )
    assert by_driver["oracledb"]["importable"] is True
    assert "pip install" not in result.stdout
    assert "uv pip install" not in result.stdout
