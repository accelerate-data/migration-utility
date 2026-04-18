"""generate_sources.py — Build and write dbt sources.yml from catalog.

Only tables explicitly marked ``is_source: true`` are registered as dbt
sources. Tables with ``resolved`` status are procedure targets that will
become dbt models (referenced via ``ref()``). Tables with
``scoping.status == "no_writer_found"`` but no ``is_source`` flag appear in
the ``unconfirmed`` list — they need user confirmation via ``/add-source-tables``
or the ``/setup-target`` confirmation flow before they are included.

Exit codes:
    0  success
    1  domain failure (incomplete scoping with --strict)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import typer
from shared.cli_utils import emit
from shared.dbt_artifacts import dump_schema_yaml
from shared.dbt_artifacts import replace_model_unit_tests
from shared.env_config import (
    resolve_catalog_dir,
    resolve_dbt_project_path,
    resolve_project_root,
)
from shared.name_resolver import model_name_from_table, normalize
from shared.output_models.generate_sources import GenerateSourcesOutput
from shared.runtime_config import get_runtime_role

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)

_BRONZE_SOURCE_NAME = "bronze"


@dataclass
class _SourceCandidates:
    """Classified catalog tables for sources.yml generation."""

    source_tables: list[dict[str, Any]] = field(default_factory=list)
    included: list[str] = field(default_factory=list)
    excluded: list[str] = field(default_factory=list)
    unconfirmed: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)


def _default_source_freshness() -> dict[str, dict[str, int | str]]:
    return {
        "warn_after": {"count": 24, "period": "hour"},
        "error_after": {"count": 48, "period": "hour"},
    }


def _resolve_physical_source_schema(
    project_root: Path,
    source_schema_override: str | None = None,
) -> str | None:
    if source_schema_override:
        return source_schema_override
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    target_role = get_runtime_role(manifest, "target")
    if target_role is None or target_role.schemas is None:
        return None
    return target_role.schemas.source


def list_confirmed_source_tables(project_root: Path) -> list[str]:
    """Return confirmed source-table FQNs from persisted catalog state."""
    catalog_dir = resolve_catalog_dir(project_root)
    tables_dir = catalog_dir / "tables"
    if not tables_dir.is_dir():
        return []

    included: list[str] = []
    for table_file in sorted(tables_dir.glob("*.json")):
        try:
            cat = json.loads(table_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "event=generate_sources_skip_file path=%s reason=parse_error",
                table_file,
            )
            continue

        if cat.get("excluded") or cat.get("is_source") is not True:
            continue

        schema = cat.get("schema", "").lower()
        name = cat.get("name", "")
        included.append(f"{schema}.{name.lower()}")

    return included


def _column_data_type(column: dict[str, Any]) -> str | None:
    value = column.get("sql_type") or column.get("data_type") or column.get("type")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _column_sql_type(column: dict[str, Any]) -> str | None:
    value = column.get("sql_type")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _append_test(tests: list[Any], test: Any) -> None:
    if test not in tests:
        tests.append(test)


def _present_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _source_loaded_at_field(cat: dict[str, Any], columns: list[dict[str, Any]]) -> str | None:
    profile = cat.get("profile")
    if not isinstance(profile, dict):
        return None
    watermark = profile.get("watermark")
    if not isinstance(watermark, dict):
        return None
    column = str(watermark.get("column", "")).strip()
    if not column:
        watermark_columns = watermark.get("columns")
        if isinstance(watermark_columns, list) and watermark_columns:
            column = str(watermark_columns[0]).strip()
    if not column:
        return None
    emitted_columns = {str(entry["name"]).lower(): str(entry["name"]) for entry in columns}
    return emitted_columns.get(column.lower())


def _single_column_constraint_columns(constraints: list[Any]) -> set[str]:
    columns: set[str] = set()
    for constraint in constraints:
        if not isinstance(constraint, dict):
            continue
        constraint_columns = constraint.get("columns")
        if not isinstance(constraint_columns, list) or len(constraint_columns) != 1:
            continue
        column = str(constraint_columns[0]).strip()
        if column:
            columns.add(column.lower())
    return columns


def _relationship_tests_by_column(
    cat: dict[str, Any],
    confirmed_sources: dict[str, tuple[str, str]],
) -> dict[str, list[dict[str, Any]]]:
    tests_by_column: dict[str, list[dict[str, Any]]] = {}
    for fk in cat.get("foreign_keys", []):
        if not isinstance(fk, dict):
            continue
        columns = fk.get("columns")
        referenced_columns = fk.get("referenced_columns")
        referenced_schema = _present_string(fk.get("referenced_schema"))
        referenced_table = _present_string(fk.get("referenced_table"))
        if (
            not isinstance(columns, list)
            or not isinstance(referenced_columns, list)
            or len(columns) != 1
            or len(referenced_columns) != 1
            or not referenced_schema
            or not referenced_table
        ):
            continue
        referenced_fqn = f"{referenced_schema.lower()}.{referenced_table.lower()}"
        emitted_source = confirmed_sources.get(referenced_fqn)
        if emitted_source is None:
            continue
        local_column = _present_string(columns[0])
        referenced_column = _present_string(referenced_columns[0])
        if not local_column or not referenced_column:
            continue
        emitted_source_name, emitted_table_name = emitted_source
        test = {
            "relationships": {
                "to": f"source('{emitted_source_name}', '{emitted_table_name}')",
                "field": referenced_column,
            }
        }
        tests_by_column.setdefault(local_column.lower(), []).append(test)
    return tests_by_column


def _build_source_columns(
    cat: dict[str, Any],
    confirmed_sources: dict[str, tuple[str, str]],
) -> list[dict[str, Any]]:
    columns: list[dict[str, Any]] = []
    unique_columns = _single_column_constraint_columns(cat.get("primary_keys", []))
    unique_columns.update(_single_column_constraint_columns(cat.get("unique_indexes", [])))
    relationships_by_column = _relationship_tests_by_column(cat, confirmed_sources)
    for column in cat.get("columns", []):
        name = column.get("name")
        if not name:
            continue
        entry: dict[str, Any] = {"name": str(name)}
        data_type = _column_data_type(column)
        if data_type:
            entry["data_type"] = data_type
        tests: list[Any] = []
        if column.get("is_nullable") is False:
            _append_test(tests, "not_null")
        if str(name).lower() in unique_columns:
            _append_test(tests, "unique")
        for relationship_test in relationships_by_column.get(str(name).lower(), []):
            _append_test(tests, relationship_test)
        if tests:
            entry["tests"] = tests
        columns.append(entry)
    return columns


def _collect_source_candidates(tables_dir: Path) -> _SourceCandidates:
    """Read catalog table files and classify source-generation candidates."""
    candidates = _SourceCandidates()
    for table_file in sorted(tables_dir.glob("*.json")):
        try:
            cat = json.loads(table_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "event=generate_sources_skip_file path=%s reason=parse_error",
                table_file,
            )
            continue

        schema = cat.get("schema", "").lower()
        name = cat.get("name", "")
        fqn = f"{schema}.{name.lower()}"

        scoping = cat.get("scoping") or {}
        status = scoping.get("status")

        if cat.get("excluded"):
            continue
        if cat.get("is_seed") is True:
            continue
        if cat.get("is_source") is True:
            candidates.included.append(fqn)
            candidates.source_tables.append(cat)
        elif status == "resolved":
            candidates.excluded.append(fqn)
        elif status == "no_writer_found":
            candidates.unconfirmed.append(fqn)
        else:
            candidates.incomplete.append(fqn)
    return candidates


def _validate_source_namespace(candidates: _SourceCandidates) -> GenerateSourcesOutput | None:
    """Ensure confirmed source table names are unique in the bronze namespace."""
    source_name_to_fqn: dict[str, str] = {}
    for cat in candidates.source_tables:
        schema_name = str(cat.get("schema", "")).lower()
        table_name = str(cat.get("name", ""))
        fqn = f"{schema_name}.{table_name.lower()}"
        source_table_name = table_name.lower()
        existing_fqn = source_name_to_fqn.get(source_table_name)
        if existing_fqn is not None and existing_fqn != fqn:
            message = (
                "Confirmed source tables must have unique names under the bronze "
                f"source namespace: {existing_fqn}, {fqn}"
            )
            logger.error(
                "event=generate_sources_duplicate_source_name table=%s existing=%s duplicate=%s",
                source_table_name,
                existing_fqn,
                fqn,
            )
            return GenerateSourcesOutput(
                sources=None,
                included=candidates.included,
                excluded=candidates.excluded,
                unconfirmed=candidates.unconfirmed,
                incomplete=candidates.incomplete,
                error="SOURCE_NAME_COLLISION",
                message=message,
            )
        source_name_to_fqn[source_table_name] = fqn
    return None


def _validate_staging_contract_types(candidates: _SourceCandidates) -> GenerateSourcesOutput | None:
    """Ensure generated staging contracts use target-normalized catalog types."""
    for cat in candidates.source_tables:
        schema_name = str(cat.get("schema", "")).lower()
        table_name = str(cat.get("name", ""))
        for column in cat.get("columns", []):
            if not isinstance(column, dict) or not column.get("name"):
                continue
            if _column_sql_type(column):
                continue
            column_name = str(column["name"])
            fqn = f"{schema_name}.{table_name}.{column_name}"
            message = (
                f"Cannot generate staging contract for {fqn}: catalog column is "
                "missing target-normalized sql_type"
            )
            logger.error(
                "event=generate_sources_staging_contract_type_missing table=%s column=%s",
                f"{schema_name}.{table_name}",
                column_name,
            )
            return GenerateSourcesOutput(
                sources=None,
                included=candidates.included,
                excluded=candidates.excluded,
                unconfirmed=candidates.unconfirmed,
                incomplete=candidates.incomplete,
                error="STAGING_CONTRACT_TYPE_MISSING",
                message=message,
            )
    return None


def _build_sources_yaml(
    source_tables: list[dict[str, Any]],
    *,
    physical_source_schema: str | None,
) -> dict[str, Any]:
    """Build the YAML-serializable dbt sources document."""
    confirmed_sources: dict[str, tuple[str, str]] = {}
    for cat in source_tables:
        schema_name = str(cat.get("schema", "")).lower()
        table_name = str(cat.get("name", ""))
        confirmed_sources[f"{schema_name}.{table_name.lower()}"] = (
            _BRONZE_SOURCE_NAME,
            table_name,
        )

    tables = []
    for cat in sorted(
        source_tables,
        key=lambda item: str(item.get("name", "")).lower(),
    ):
        table_name = str(cat.get("name", ""))
        table_entry: dict[str, Any] = {
            "name": table_name,
            "description": f"{table_name} from source system",
        }
        columns = _build_source_columns(cat, confirmed_sources)
        if columns:
            table_entry["columns"] = columns
        loaded_at_field = _source_loaded_at_field(cat, columns)
        if loaded_at_field:
            table_entry["loaded_at_field"] = loaded_at_field
            table_entry["freshness"] = _default_source_freshness()
        tables.append(table_entry)

    source_entry: dict[str, Any] = {
        "name": _BRONZE_SOURCE_NAME,
        "description": "Confirmed source tables available in the bronze layer",
        "tables": tables,
    }
    if physical_source_schema:
        source_entry["schema"] = physical_source_schema

    return {"version": 2, "sources": [source_entry]}


def generate_sources(
    project_root: Path,
    *,
    source_schema_override: str | None = None,
    require_staging_contract_types: bool = False,
) -> GenerateSourcesOutput:
    """Build sources.yml content from catalog tables.

    Only tables with ``is_source: true`` are included. Tables with
    ``resolved`` status are excluded (they become dbt models). Tables with
    ``scoping.status == "no_writer_found"`` but no ``is_source`` flag are
    listed in ``unconfirmed`` — they need explicit confirmation before
    being included.

    When a target source schema override is configured, logical source names
    remain grouped by extracted schema while the emitted dbt ``schema:``
    points at the configured physical target schema.
    """
    catalog_dir = resolve_catalog_dir(project_root)
    tables_dir = catalog_dir / "tables"

    if not tables_dir.is_dir():
        return GenerateSourcesOutput(
            sources=None,
            included=[],
            excluded=[],
            unconfirmed=[],
            incomplete=[],
        )

    candidates = _collect_source_candidates(tables_dir)
    if not candidates.source_tables:
        return GenerateSourcesOutput(
            sources=None,
            included=candidates.included,
            excluded=candidates.excluded,
            unconfirmed=candidates.unconfirmed,
            incomplete=candidates.incomplete,
        )

    namespace_error = _validate_source_namespace(candidates)
    if namespace_error is not None:
        return namespace_error

    if require_staging_contract_types:
        contract_type_error = _validate_staging_contract_types(candidates)
        if contract_type_error is not None:
            return contract_type_error

    physical_source_schema = _resolve_physical_source_schema(
        project_root,
        source_schema_override,
    )
    sources_dict = _build_sources_yaml(
        candidates.source_tables,
        physical_source_schema=physical_source_schema,
    )
    return GenerateSourcesOutput(
        sources=sources_dict,
        included=candidates.included,
        excluded=candidates.excluded,
        unconfirmed=candidates.unconfirmed,
        incomplete=candidates.incomplete,
    )


def _staging_model_name(table_name: str) -> str:
    return f"stg_bronze__{model_name_from_table(normalize(table_name))}"


def _staging_model_columns(columns: list[Any]) -> list[dict[str, Any]]:
    staging_columns: list[dict[str, Any]] = []
    for column in columns:
        if not isinstance(column, dict) or not column.get("name"):
            continue
        staging_column = {
            key: value
            for key, value in column.items()
            if key not in {"tests", "data_tests"}
        }
        staging_columns.append(staging_column)
    return staging_columns


def _sample_unit_test_value(column: dict[str, Any]) -> Any:
    column_name = str(column.get("name", "value")).strip().lower() or "value"
    data_type = str(column.get("data_type", "")).upper()
    if any(token in data_type for token in ("CHAR", "TEXT", "STRING", "CLOB")):
        return f"sample_{column_name}"
    if any(token in data_type for token in ("DATE", "TIME")):
        return "2020-01-01 00:00:00"
    if any(token in data_type for token in ("DECIMAL", "NUMERIC", "NUMBER", "FLOAT", "DOUBLE", "REAL")):
        return 1.0
    if any(token in data_type for token in ("INT", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT")):
        return 1
    if "BOOL" in data_type or data_type == "BIT":
        return True
    return f"sample_{column_name}"


def _staging_unit_test_for_table(table_name: str, columns: list[dict[str, Any]]) -> dict[str, Any]:
    model_name = _staging_model_name(table_name)
    row = {str(column["name"]): _sample_unit_test_value(column) for column in columns}
    return {
        "name": f"test_{model_name}_passthrough",
        "model": model_name,
        "given": [
            {
                "input": f"source('bronze', '{table_name}')",
                "rows": [row],
            }
        ],
        "expect": {
            "rows": [row],
        },
    }


def _render_staging_wrapper(table_name: str) -> str:
    return (
        "with\n"
        "\n"
        "source as (\n"
        "\n"
        f"    select * from {{{{ source('bronze', '{table_name}') }}}}\n"
        "\n"
        "),\n"
        "\n"
        "final as (\n"
        "\n"
        "    select * from source\n"
        "\n"
        ")\n"
        "\n"
        "select * from final\n"
    )


def _staging_models_from_sources(sources: dict[str, Any]) -> dict[str, Any]:
    source_entries = sources.get("sources")
    if not isinstance(source_entries, list):
        return {"version": 2, "models": []}

    schema: dict[str, Any] = {"version": 2, "models": []}
    models: list[dict[str, Any]] = schema["models"]
    for source in source_entries:
        if not isinstance(source, dict):
            continue
        for table in source.get("tables", []):
            if not isinstance(table, dict) or not table.get("name"):
                continue
            table_name = str(table["name"])
            model_entry: dict[str, Any] = {
                "name": _staging_model_name(table_name),
                "description": f"Pass-through staging wrapper for bronze.{table_name}",
                "config": {"contract": {"enforced": True}},
            }
            columns = table.get("columns")
            model_columns: list[dict[str, Any]] = []
            if isinstance(columns, list) and columns:
                model_columns = _staging_model_columns(columns)
                if model_columns:
                    model_entry["columns"] = model_columns
            models.append(model_entry)
            replace_model_unit_tests(
                schema,
                model_name=model_entry["name"],
                unit_tests=[_staging_unit_test_for_table(table_name, model_columns)],
            )
    return schema


def _cleanup_stale_staging_wrappers(staging_dir: Path, expected_wrapper_names: set[str]) -> list[Path]:
    """Remove stale generated bronze staging wrappers from previous source runs."""
    if not staging_dir.is_dir():
        return []
    removed_paths: list[Path] = []
    for wrapper_path in staging_dir.glob("stg_bronze__*.sql"):
        if wrapper_path.name in expected_wrapper_names:
            continue
        wrapper_path.unlink()
        removed_paths.append(wrapper_path)
        logger.info(
            "event=generate_sources_removed_stale_wrapper path=%s",
            wrapper_path,
        )
    return removed_paths


def _dump_yaml(path: Path, data: dict[str, Any]) -> None:
    path.write_text(dump_schema_yaml(data), encoding="utf-8")


def _remove_source_artifacts(staging_dir: Path, yaml_paths: tuple[Path, ...]) -> list[Path]:
    removed_paths = _cleanup_stale_staging_wrappers(staging_dir, set())
    for stale_yaml_path in yaml_paths:
        if stale_yaml_path.exists():
            stale_yaml_path.unlink()
            removed_paths.append(stale_yaml_path)
            logger.info(
                "event=generate_sources_removed_stale_yaml path=%s",
                stale_yaml_path,
            )
    return removed_paths


def _iter_source_table_names(sources: dict[str, Any]):
    for source in sources.get("sources", []):
        if not isinstance(source, dict):
            continue
        for table in source.get("tables", []):
            if not isinstance(table, dict) or not table.get("name"):
                continue
            yield str(table["name"])


def _source_selectors_from_sources(sources: dict[str, Any]) -> list[str]:
    selectors: list[str] = []
    for source in sources.get("sources", []):
        if not isinstance(source, dict) or not source.get("name"):
            continue
        source_name = str(source["name"])
        for table in source.get("tables", []):
            if not isinstance(table, dict) or not table.get("name"):
                continue
            selectors.append(f"source:{source_name}.{table['name']}")
    return selectors


def _write_staging_wrapper_files(staging_dir: Path, sources: dict[str, Any]) -> set[str]:
    expected_wrapper_names: set[str] = set()
    for table_name in _iter_source_table_names(sources):
        wrapper_name = f"{_staging_model_name(table_name)}.sql"
        expected_wrapper_names.add(wrapper_name)
        wrapper_path = staging_dir / wrapper_name
        wrapper_path.write_text(_render_staging_wrapper(table_name), encoding="utf-8")
    return expected_wrapper_names


def write_sources_yml(
    project_root: Path,
    *,
    source_schema_override: str | None = None,
) -> GenerateSourcesOutput:
    """Generate sources.yml and write it to the dbt project.

    Returns the generate_sources result dict with an added ``path`` field.
    """
    result = generate_sources(
        project_root,
        source_schema_override=source_schema_override,
        require_staging_contract_types=True,
    )
    dbt_root = resolve_dbt_project_path(project_root)
    staging_dir = dbt_root / "models" / "staging"
    sources_path = staging_dir / "_staging__sources.yml"
    models_path = staging_dir / "_staging__models.yml"

    if result.sources is None:
        if result.error:
            return result.model_copy(update={"path": None, "written_paths": []})
        removed_paths = _remove_source_artifacts(staging_dir, (sources_path, models_path))
        return result.model_copy(
            update={
                "path": None,
                "written_paths": [str(path.relative_to(project_root)) for path in removed_paths],
            }
        )

    staging_dir.mkdir(parents=True, exist_ok=True)
    _dump_yaml(sources_path, result.sources)
    staging_models = _staging_models_from_sources(result.sources)
    _dump_yaml(models_path, staging_models)
    expected_wrapper_names = _write_staging_wrapper_files(staging_dir, result.sources)
    removed_paths = _cleanup_stale_staging_wrappers(staging_dir, expected_wrapper_names)
    generated_model_names = [
        str(model["name"])
        for model in staging_models.get("models", [])
        if isinstance(model, dict) and model.get("name")
    ]
    written_paths = [
        str(sources_path.relative_to(project_root)),
        str(models_path.relative_to(project_root)),
        *(
            str((staging_dir / wrapper_name).relative_to(project_root))
            for wrapper_name in sorted(expected_wrapper_names)
        ),
        *(str(path.relative_to(project_root)) for path in removed_paths),
    ]

    return result.model_copy(
        update={
            "path": str(sources_path),
            "written_paths": written_paths,
            "generated_model_names": generated_model_names,
            "generated_source_selectors": _source_selectors_from_sources(result.sources),
        }
    )


@app.command()
def main(
    project_root: Path | None = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
    write: bool = typer.Option(
        False, "--write", help="Write sources.yml to dbt project",
    ),
    strict: bool = typer.Option(
        False, "--strict", help="Exit 1 if any table has incomplete scoping (not yet analyzed)",
    ),
) -> None:
    """Generate sources.yml from catalog tables.

    Only tables with is_source=true are included. Tables with 'resolved'
    status are excluded (they become dbt models). Tables with
    'no_writer_found' but no is_source flag appear in 'unconfirmed'.
    """
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    try:
        if write:
            result = write_sources_yml(root)
        else:
            result = generate_sources(root)
    except OSError as exc:
        logger.error("event=generate_sources_io_error error=%s", exc)
        emit({"error": "IO_ERROR", "message": str(exc)})
        raise typer.Exit(code=2) from exc

    if result.error:
        emit(result)
        raise typer.Exit(code=1)

    if strict and result.incomplete:
        emit({
            "error": "INCOMPLETE_SCOPING",
            "message": f"{len(result.incomplete)} tables have incomplete scoping",
            "incomplete": result.incomplete,
        })
        raise typer.Exit(code=1)

    emit(result)
