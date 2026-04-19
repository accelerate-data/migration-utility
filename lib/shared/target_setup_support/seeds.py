"""Seed-table export and materialization compatibility facade."""

from __future__ import annotations

from shared.target_setup_support.dbt_commands import MISSING_DBT_REMEDIATION
from shared.target_setup_support.seed_commands import (
    DbtSeedResult,
    materialize_seed_tables,
)
from shared.target_setup_support.seed_export import (
    SeedExportResult,
    export_seed_tables,
)
from shared.target_setup_support.seed_rendering import (
    render_seed_csv,
    render_seeds_yml,
)
from shared.target_setup_support.seed_specs import (
    SeedColumnSpec,
    SeedTableSpec,
    load_seed_table_specs,
)

__all__ = [
    "DbtSeedResult",
    "MISSING_DBT_REMEDIATION",
    "SeedColumnSpec",
    "SeedExportResult",
    "SeedTableSpec",
    "export_seed_tables",
    "load_seed_table_specs",
    "materialize_seed_tables",
    "render_seed_csv",
    "render_seeds_yml",
]
