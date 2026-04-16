"""Artifact writing and unit-test rendering helpers for migrate."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import yaml

from shared.catalog import load_and_merge_catalog
from shared.catalog_models import DiagnosticsEntry
from shared.env_config import resolve_dbt_project_path
from shared.name_resolver import model_name_from_table, normalize
from shared.output_models import MigrateWriteOutput, RenderUnitTestsOutput, TestSpec

logger = logging.getLogger(__name__)


def _atomic_write(path: Path, content: str) -> None:
    """Write content to path via tmp-then-rename for crash safety."""
    tmp_path = path.with_name(path.name + ".tmp")
    try:
        tmp_path.write_text(content, encoding="utf-8")
        tmp_path.replace(path)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise


def _merge_named_yaml(existing_text: str | None, new_text: str, section: str) -> str:
    existing = yaml.safe_load(existing_text) if existing_text else None
    incoming = yaml.safe_load(new_text) if new_text.strip() else None

    schema: dict[str, Any] = existing if isinstance(existing, dict) else {"version": 2}
    if "version" not in schema:
        schema["version"] = 2

    entries = schema.setdefault(section, [])
    if not isinstance(entries, list):
        entries = []
        schema[section] = entries

    incoming_models = []
    if isinstance(incoming, dict) and isinstance(incoming.get(section), list):
        incoming_models = [
            model for model in incoming[section]
            if isinstance(model, dict) and model.get("name")
        ]
    elif section == "snapshots" and isinstance(incoming, dict) and isinstance(incoming.get("models"), list):
        incoming_models = [
            model for model in incoming["models"]
            if isinstance(model, dict) and model.get("name")
        ]

    for incoming_model in incoming_models:
        incoming_name = incoming_model["name"]
        for index, existing_model in enumerate(entries):
            if isinstance(existing_model, dict) and existing_model.get("name") == incoming_name:
                entries[index] = incoming_model
                break
        else:
            entries.append(incoming_model)

    return yaml.dump(schema, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _merge_model_yaml(existing_text: str | None, new_text: str) -> str:
    return _merge_named_yaml(existing_text, new_text, "models")


def _merge_snapshot_yaml(existing_text: str | None, new_text: str) -> str:
    return _merge_named_yaml(existing_text, new_text, "snapshots")


def _snapshot_name(model_sql: str, fallback: str) -> str:
    match = re.search(r"{%\s*snapshot\s+([A-Za-z_][A-Za-z0-9_]*)\s*%}", model_sql)
    return match.group(1) if match else fallback


def run_write(
    table_fqn: str,
    project_root: Path,
    dbt_project_path: Path,
    model_sql: str,
    schema_yml: str,
) -> MigrateWriteOutput:
    """Validate and write model SQL + schema YAML to a dbt project."""
    table_norm = normalize(table_fqn)
    model_name = model_name_from_table(table_norm)

    if not model_sql or not model_sql.strip():
        raise ValueError("model SQL is empty")
    if not dbt_project_path.is_dir():
        raise FileNotFoundError(f"dbt project path does not exist: {dbt_project_path}")

    dbt_project_yml = dbt_project_path / "dbt_project.yml"
    if not dbt_project_yml.exists():
        raise FileNotFoundError(f"no dbt_project.yml in {dbt_project_path}")

    is_snapshot = model_sql.lstrip().startswith("{% snapshot")
    if is_snapshot:
        snapshot_name = _snapshot_name(model_sql, model_name)
        artifact_dir = dbt_project_path / "snapshots"
        sql_path = artifact_dir / f"{snapshot_name}.sql"
        yml_path = artifact_dir / "_snapshots__models.yml"
    else:
        artifact_dir = dbt_project_path / "models" / "marts"
        sql_path = artifact_dir / f"{model_name}.sql"
        yml_path = artifact_dir / "_marts__models.yml"

    artifact_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    _atomic_write(sql_path, model_sql)
    written.append(str(sql_path.relative_to(dbt_project_path)))

    if schema_yml and schema_yml.strip():
        existing_yml = yml_path.read_text(encoding="utf-8") if yml_path.exists() else None
        merged_yml = (
            _merge_snapshot_yaml(existing_yml, schema_yml)
            if is_snapshot
            else _merge_model_yaml(existing_yml, schema_yml)
        )
        _atomic_write(yml_path, merged_yml)
        written.append(str(yml_path.relative_to(dbt_project_path)))

    return MigrateWriteOutput(written=written, status="ok")


def run_write_generate(
    project_root: Path,
    table_fqn: str,
    model_path: str,
    compiled: bool,
    tests_passed: bool,
    test_count: int,
    schema_yml: bool,
    warnings: list[dict[str, Any]] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Validate generate output and write generate section to catalog."""
    norm = normalize(table_fqn)
    dbt_root = resolve_dbt_project_path(project_root)
    model_file = dbt_root / model_path
    file_exists = model_file.exists()
    status = "ok" if file_exists and compiled and tests_passed else "error"

    generate: dict[str, Any] = {
        "status": status,
        "model_path": model_path,
        "schema_yml": schema_yml,
        "compiled": compiled,
        "tests_passed": tests_passed,
        "test_count": test_count,
        "warnings": warnings or [],
        "errors": errors or [],
    }

    result = load_and_merge_catalog(project_root, norm, "generate", generate)
    logger.info("event=write_generate_complete table=%s status=%s", norm, status)
    return result


def _spec_given_to_dbt_input(table_ref: str) -> str:
    """Convert a test-spec given table reference to dbt unit test input."""
    parts = table_ref.split(".", 1)
    if len(parts) == 2:
        return f"source('{parts[0]}', '{parts[1]}')"
    return f"source('{table_ref}', '{table_ref}')"


def _unit_test_to_dbt(entry: dict[str, Any], model_name: str) -> dict[str, Any]:
    """Translate one test-spec UnitTestEntry dict to a dbt unit test dict."""
    dbt_test: dict[str, Any] = {
        "name": entry["name"],
        "model": model_name,
        "given": [],
    }
    for given in entry.get("given", []):
        dbt_test["given"].append({
            "input": _spec_given_to_dbt_input(given["table"]),
            "rows": given.get("rows", []),
        })
    expect = entry.get("expect")
    if expect and expect.get("rows"):
        dbt_test["expect"] = {"rows": expect["rows"]}
    return dbt_test


def run_render_unit_tests(
    project_root: Path,
    table_fqn: str,
    model_name: str,
    spec_path: Path,
    schema_yml_path: Path,
) -> RenderUnitTestsOutput:
    """Translate test-spec scenarios into dbt unit tests and write schema YAML."""
    import yaml

    norm = normalize(table_fqn)
    warnings: list[DiagnosticsEntry] = []

    if not spec_path.is_file():
        return RenderUnitTestsOutput(
            tests_rendered=0,
            model_name=model_name,
            errors=[DiagnosticsEntry(
                code="SPEC_NOT_FOUND",
                severity="error",
                message=f"Test spec not found: {spec_path}",
            )],
        )

    spec_data = json.loads(spec_path.read_text(encoding="utf-8"))
    spec = TestSpec(**spec_data)

    dbt_unit_tests = []
    for ut in spec.unit_tests:
        ut_dict = ut.model_dump(mode="json", exclude_none=True)
        dbt_unit_tests.append(_unit_test_to_dbt(ut_dict, model_name))

    if not dbt_unit_tests:
        return RenderUnitTestsOutput(
            tests_rendered=0,
            model_name=model_name,
            warnings=[DiagnosticsEntry(
                code="NO_UNIT_TESTS",
                severity="warning",
                message=f"Test spec {spec.item_id} has no unit tests to render",
            )],
        )

    schema: dict[str, Any] = {"version": 2, "models": []}
    if schema_yml_path.is_file():
        existing = yaml.safe_load(schema_yml_path.read_text(encoding="utf-8"))
        if isinstance(existing, dict):
            schema = existing

    models = schema.setdefault("models", [])
    model_entry = None
    for model in models:
        if isinstance(model, dict) and model.get("name") == model_name:
            model_entry = model
            break
    if model_entry is None:
        model_entry = {"name": model_name}
        models.append(model_entry)

    model_entry["unit_tests"] = dbt_unit_tests

    schema_yml_path.parent.mkdir(parents=True, exist_ok=True)
    schema_yml_path.write_text(
        yaml.dump(schema, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    logger.info(
        "event=render_unit_tests table=%s model=%s tests_rendered=%d",
        norm, model_name, len(dbt_unit_tests),
    )
    return RenderUnitTestsOutput(
        tests_rendered=len(dbt_unit_tests),
        model_name=model_name,
        warnings=warnings,
    )
