"""Generated staging wrappers and YAML artifacts for dbt sources."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.dbt_artifacts import dump_schema_yaml, replace_model_unit_tests
from shared.env_config import resolve_dbt_project_path
from shared.name_resolver import model_name_from_table, normalize
from shared.output_models.generate_sources import GenerateSourcesOutput

logger = logging.getLogger(__name__)


def staging_model_name(table_name: str) -> str:
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
    model_name = staging_model_name(table_name)
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


def render_staging_wrapper(table_name: str) -> str:
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


def staging_models_from_sources(sources: dict[str, Any]) -> dict[str, Any]:
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
                "name": staging_model_name(table_name),
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


def source_selectors_from_sources(sources: dict[str, Any]) -> list[str]:
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
        wrapper_name = f"{staging_model_name(table_name)}.sql"
        expected_wrapper_names.add(wrapper_name)
        wrapper_path = staging_dir / wrapper_name
        wrapper_path.write_text(render_staging_wrapper(table_name), encoding="utf-8")
    return expected_wrapper_names


def write_staging_artifacts(
    project_root: Path,
    result: GenerateSourcesOutput,
) -> GenerateSourcesOutput:
    """Write generated dbt source and staging artifacts for a sources result."""
    dbt_root = resolve_dbt_project_path(project_root)
    staging_dir = dbt_root / "models" / "staging"
    sources_path = staging_dir / "_staging__sources.yml"
    models_path = staging_dir / "_staging__models.yml"

    if result.sources is None:
        removed_paths = _remove_source_artifacts(staging_dir, (sources_path, models_path))
        return result.model_copy(
            update={
                "path": None,
                "written_paths": [str(path.relative_to(project_root)) for path in removed_paths],
            }
        )

    staging_dir.mkdir(parents=True, exist_ok=True)
    _dump_yaml(sources_path, result.sources)
    staging_models = staging_models_from_sources(result.sources)
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
            "generated_source_selectors": source_selectors_from_sources(result.sources),
        }
    )
