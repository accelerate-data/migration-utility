"""Catalog seed-table specs for target setup."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from shared.name_resolver import model_name_from_table, normalize


@dataclass(frozen=True)
class SeedColumnSpec:
    """One documented column for a dbt seed."""

    name: str
    data_type: str | None = None


@dataclass(frozen=True)
class SeedTableSpec:
    """One catalog seed table that should be exported as a dbt seed CSV."""

    logical_schema: str
    table_name: str
    columns: list[SeedColumnSpec]

    @property
    def fqn(self) -> str:
        return normalize(f"{self.logical_schema}.{self.table_name}")

    @property
    def seed_name(self) -> str:
        return model_name_from_table(self.fqn)


def load_seed_table_specs(project_root: Path) -> list[SeedTableSpec]:
    tables_dir = project_root / "catalog" / "tables"
    if not tables_dir.is_dir():
        return []

    specs: list[SeedTableSpec] = []
    seen_seed_names: dict[str, str] = {}
    for table_file in sorted(tables_dir.glob("*.json")):
        payload = json.loads(table_file.read_text(encoding="utf-8"))
        if payload.get("excluded") or payload.get("is_seed") is not True:
            continue
        logical_schema = str(payload.get("schema", ""))
        table_name = str(payload.get("name", ""))
        if not logical_schema or not table_name:
            continue
        fqn = normalize(f"{logical_schema}.{table_name}")
        seed_name = model_name_from_table(fqn)
        if existing_fqn := seen_seed_names.get(seed_name):
            raise ValueError(
                f"Seed table {fqn!r} maps to dbt seed name {seed_name!r}, "
                f"which is already used by {existing_fqn!r}."
            )
        seen_seed_names[seed_name] = fqn
        columns = []
        for column in payload.get("columns", []):
            if not isinstance(column, dict) or not column.get("name"):
                continue
            data_type = (
                column.get("sql_type")
                or column.get("data_type")
                or column.get("type")
            )
            columns.append(
                SeedColumnSpec(
                    name=str(column["name"]),
                    data_type=str(data_type) if data_type else None,
                )
            )
        specs.append(
            SeedTableSpec(
                logical_schema=logical_schema,
                table_name=table_name,
                columns=columns,
            )
        )
    return specs
