"""Target source-table spec loading and materialization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from shared.dbops import ColumnSpec, get_dbops
from shared.target_setup_support.runtime import get_target_source_schema, require_target_role


@dataclass(frozen=True)
class TargetTableSpec:
    """One source-backed table that should exist on the target."""

    logical_schema: str
    physical_schema: str
    table_name: str
    columns: list[ColumnSpec]

    @property
    def fqn(self) -> str:
        return f"{self.physical_schema}.{self.table_name}"


@dataclass(frozen=True)
class TargetApplyResult:
    """Outcome of applying source-backed target tables."""

    physical_schema: str
    desired_tables: list[str]
    created_tables: list[str]
    existing_tables: list[str]


def load_target_source_table_specs(
    project_root: Path,
    *,
    include_fallback_columns: bool = True,
) -> list[TargetTableSpec]:
    """Return confirmed source tables mapped to the configured target source schema."""
    target_schema = get_target_source_schema(project_root)
    tables_dir = project_root / "catalog" / "tables"
    if not tables_dir.is_dir():
        return []

    specs: list[TargetTableSpec] = []
    for table_file in sorted(tables_dir.glob("*.json")):
        payload = json.loads(table_file.read_text(encoding="utf-8"))
        if payload.get("excluded") or payload.get("is_source") is not True:
            continue
        logical_schema = str(payload.get("schema", ""))
        table_name = str(payload.get("name", ""))
        if not logical_schema or not table_name:
            continue
        columns = []
        for column in payload.get("columns", []):
            source_type = (
                column.get("sql_type")
                or column.get("data_type")
                or column.get("type")
                or "VARCHAR"
            )
            columns.append(
                ColumnSpec(
                    name=column["name"],
                    source_type=str(source_type),
                    nullable=bool(column.get("is_nullable", True)),
                )
            )
        if include_fallback_columns and not columns:
            columns.append(ColumnSpec(name="id", source_type="BIGINT", nullable=False))
        specs.append(
            TargetTableSpec(
                logical_schema=logical_schema,
                physical_schema=target_schema,
                table_name=table_name,
                columns=columns,
            )
        )
    return specs


def apply_target_source_tables(project_root: Path) -> TargetApplyResult:
    """Ensure confirmed source tables exist on the configured target schema."""
    target_role = require_target_role(project_root)
    target_schema = get_target_source_schema(project_root)
    adapter = get_dbops(target_role.technology).from_role(
        target_role,
        project_root=project_root,
    )
    desired_specs = load_target_source_table_specs(project_root)

    adapter.ensure_source_schema(target_schema)
    existing = adapter.list_source_tables(target_schema)

    created_tables: list[str] = []
    existing_tables: list[str] = []
    for spec in desired_specs:
        if spec.table_name.lower() in existing:
            existing_tables.append(spec.fqn)
            continue
        adapter.create_source_table(spec.physical_schema, spec.table_name, spec.columns)
        created_tables.append(spec.fqn)

    return TargetApplyResult(
        physical_schema=target_schema,
        desired_tables=[spec.fqn for spec in desired_specs],
        created_tables=created_tables,
        existing_tables=existing_tables,
    )
