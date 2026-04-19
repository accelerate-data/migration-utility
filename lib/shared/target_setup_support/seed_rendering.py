"""dbt seed file rendering helpers."""

from __future__ import annotations

from csv import writer as csv_writer
from io import StringIO

import yaml

from shared.target_setup_support.seed_specs import SeedTableSpec


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
