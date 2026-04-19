"""dbt sources YAML construction from confirmed catalog tables."""

from __future__ import annotations

from typing import Any

BRONZE_SOURCE_NAME = "bronze"


def default_source_freshness() -> dict[str, dict[str, int | str]]:
    return {
        "warn_after": {"count": 24, "period": "hour"},
        "error_after": {"count": 48, "period": "hour"},
    }


def _column_data_type(column: dict[str, Any]) -> str | None:
    value = column.get("sql_type") or column.get("data_type") or column.get("type")
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


def build_source_columns(
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


def build_sources_yaml(
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
            BRONZE_SOURCE_NAME,
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
        columns = build_source_columns(cat, confirmed_sources)
        if columns:
            table_entry["columns"] = columns
        loaded_at_field = _source_loaded_at_field(cat, columns)
        if loaded_at_field:
            table_entry["loaded_at_field"] = loaded_at_field
            table_entry["freshness"] = default_source_freshness()
        tables.append(table_entry)

    source_entry: dict[str, Any] = {
        "name": BRONZE_SOURCE_NAME,
        "description": "Confirmed source tables available in the bronze layer",
        "tables": tables,
    }
    if physical_source_schema:
        source_entry["schema"] = physical_source_schema

    return {"version": 2, "sources": [source_entry]}
