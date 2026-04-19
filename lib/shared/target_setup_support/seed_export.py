"""Seed-table export orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from shared.dbops import get_dbops
from shared.target_setup_support.runtime import require_source_role
from shared.target_setup_support.seed_rendering import render_seed_csv, render_seeds_yml
from shared.target_setup_support.seed_specs import load_seed_table_specs

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SeedExportResult:
    """Outcome of exporting dbt seed CSV files from source tables."""

    files: list[str]
    csv_files: list[str]
    row_counts: dict[str, int]
    written_paths: list[str]


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
