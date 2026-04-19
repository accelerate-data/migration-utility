from __future__ import annotations

import json
from pathlib import Path

from shared.diagnostics.context import CatalogContext, build_ddl_lookup, build_known_fqns, load_package_members
from shared.loader_data import DdlCatalog, DdlEntry


def test_build_known_fqns_collects_catalog_file_stems(tmp_path: Path) -> None:
    catalog_dir = tmp_path / "catalog"
    (catalog_dir / "tables").mkdir(parents=True)
    (catalog_dir / "procedures").mkdir()
    (catalog_dir / "tables" / "silver.dimcustomer.json").write_text("{}", encoding="utf-8")
    (catalog_dir / "procedures" / "dbo.usp_load.json").write_text("{}", encoding="utf-8")

    assert build_known_fqns(catalog_dir) == {
        "tables": {"silver.dimcustomer"},
        "procedures": {"dbo.usp_load"},
        "views": set(),
        "functions": set(),
    }


def test_build_ddl_lookup_flattens_catalog_buckets() -> None:
    entry = DdlEntry(raw_ddl="select 1", ast=None)
    catalog = DdlCatalog(tables={"silver.dimcustomer": entry})

    assert build_ddl_lookup(catalog) == {"silver.dimcustomer": entry}


def test_load_package_members_reads_staging_package_members(tmp_path: Path) -> None:
    staging = tmp_path / ".staging"
    staging.mkdir()
    (staging / "packages.json").write_text(
        json.dumps([{"schema_name": "PKG", "member_name": "DoWork"}]),
        encoding="utf-8",
    )

    assert load_package_members(tmp_path) == {"pkg.dowork"}


def test_catalog_context_is_importable_from_context_module(tmp_path: Path) -> None:
    ctx = CatalogContext(
        project_root=tmp_path,
        dialect="tsql",
        fqn="dbo.usp_load",
        object_type="procedure",
        catalog_data={},
        known_fqns={},
    )

    assert ctx.fqn == "dbo.usp_load"
