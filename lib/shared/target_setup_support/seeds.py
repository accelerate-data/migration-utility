"""Seed-table export and materialization helpers."""

from __future__ import annotations

import json
import logging
import subprocess
from csv import writer as csv_writer
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import yaml

from shared.dbops import get_dbops
from shared.name_resolver import model_name_from_table, normalize
from shared.target_setup_support.runtime import require_source_role

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class SeedExportResult:
    """Outcome of exporting dbt seed CSV files from source tables."""

    files: list[str]
    csv_files: list[str]
    row_counts: dict[str, int]
    written_paths: list[str]


@dataclass(frozen=True)
class DbtSeedResult:
    """Outcome of invoking dbt seed for exported seed CSV files."""

    ran: bool
    command: list[str]


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


def render_seed_csv(columns: list[str], rows: list[tuple[object, ...]]) -> str:
    buffer = StringIO()
    writer = csv_writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue()


def render_seeds_yml(seed_specs: list[SeedTableSpec]) -> str:
    seeds = []
    for spec in seed_specs:
        columns = []
        for column in spec.columns:
            entry = {"name": column.name}
            if column.data_type:
                entry["data_type"] = column.data_type
            columns.append(entry)
        seed_entry: dict[str, object] = {"name": spec.seed_name}
        if columns:
            seed_entry["columns"] = columns
        seeds.append(seed_entry)

    return yaml.dump(
        {"version": 2, "seeds": seeds},
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )


def export_seed_tables(project_root: Path) -> SeedExportResult:
    """Export confirmed seed catalog tables from source DB into dbt seed CSVs."""
    seed_specs = load_seed_table_specs(project_root)
    if not seed_specs:
        return SeedExportResult(files=[], csv_files=[], row_counts={}, written_paths=[])

    source_role = require_source_role(project_root)
    adapter = get_dbops(source_role.technology).from_role(
        source_role,
        project_root=project_root,
    )
    dbt_root = project_root / "dbt"
    seeds_dir = dbt_root / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)

    files: list[str] = []
    csv_files: list[str] = []
    written_paths: list[str] = []
    row_counts: dict[str, int] = {}
    for spec in seed_specs:
        columns, rows = adapter.read_table_rows(
            spec.logical_schema,
            spec.table_name,
            [column.name for column in spec.columns],
        )
        seed_path = seeds_dir / f"{spec.seed_name}.csv"
        content = render_seed_csv(columns, rows)
        relative_path = str(seed_path.relative_to(project_root))
        if not seed_path.exists() or seed_path.read_text(encoding="utf-8") != content:
            seed_path.write_text(content, encoding="utf-8")
            written_paths.append(relative_path)
        files.append(relative_path)
        csv_files.append(relative_path)
        row_counts[spec.fqn] = len(rows)
        logger.info(
            "event=export_seed_table component=target_setup table=%s seed_file=%s rows=%d status=success",
            spec.fqn,
            relative_path,
            len(rows),
        )

    seeds_yml_path = seeds_dir / "_seeds.yml"
    seeds_yml_content = render_seeds_yml(seed_specs)
    seeds_yml_relative_path = str(seeds_yml_path.relative_to(project_root))
    if not seeds_yml_path.exists() or seeds_yml_path.read_text(encoding="utf-8") != seeds_yml_content:
        seeds_yml_path.write_text(seeds_yml_content, encoding="utf-8")
        written_paths.append(seeds_yml_relative_path)
    files.append(seeds_yml_relative_path)

    return SeedExportResult(files=files, csv_files=csv_files, row_counts=row_counts, written_paths=written_paths)


def materialize_seed_tables(project_root: Path, seed_files: list[str]) -> DbtSeedResult:
    """Run dbt seed so exported seed CSVs are materialized in the target schema."""
    seed_csv_files = [seed_file for seed_file in seed_files if seed_file.endswith(".csv")]
    if not seed_csv_files:
        return DbtSeedResult(ran=False, command=[])

    dbt_root = project_root / "dbt"
    command = [
        "dbt",
        "seed",
        "--project-dir",
        str(dbt_root),
        "--profiles-dir",
        str(dbt_root),
        "--target",
        "dev",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=dbt_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ValueError("dbt executable not found on PATH; install dbt before running setup-target for seeds.") from exc
    if completed.returncode != 0:
        details = (completed.stderr or completed.stdout or "").strip()
        message = "dbt seed failed while materializing seed tables"
        if details:
            message = f"{message}: {details}"
        raise ValueError(message)

    logger.info(
        "event=dbt_seed_complete component=target_setup seed_files=%d status=success",
        len(seed_csv_files),
    )
    return DbtSeedResult(ran=True, command=command)
