from __future__ import annotations

import json
from pathlib import Path

from shared.catalog_preservation import restore_enriched_fields, snapshot_enriched_fields


def test_snapshot_and_restore_enriched_fields_preserves_only_allowed_sections(tmp_path: Path) -> None:
    catalog_dir = tmp_path / "catalog"
    (catalog_dir / "tables").mkdir(parents=True)
    (catalog_dir / "procedures").mkdir()
    table_path = catalog_dir / "tables" / "silver.dimcustomer.json"
    proc_path = catalog_dir / "procedures" / "dbo.usp_load_dimcustomer.json"

    table_path.write_text(
        json.dumps(
            {
                "schema": "silver",
                "name": "DimCustomer",
                "scoping": {"status": "resolved"},
                "profile": {"status": "ok"},
                "refactor": {"status": "must not be preserved"},
            }
        ),
        encoding="utf-8",
    )
    proc_path.write_text(
        json.dumps(
            {
                "schema": "dbo",
                "name": "usp_load_dimcustomer",
                "refactor": {"status": "ok"},
            }
        ),
        encoding="utf-8",
    )

    snapshot = snapshot_enriched_fields(tmp_path)
    assert snapshot["silver.dimcustomer"] == {
        "scoping": {"status": "resolved"},
        "profile": {"status": "ok"},
    }
    assert snapshot["dbo.usp_load_dimcustomer"] == {"refactor": {"status": "ok"}}

    table_path.write_text(json.dumps({"schema": "silver", "name": "DimCustomer"}), encoding="utf-8")
    proc_path.write_text(json.dumps({"schema": "dbo", "name": "usp_load_dimcustomer"}), encoding="utf-8")

    restore_enriched_fields(tmp_path, snapshot)

    restored_table = json.loads(table_path.read_text(encoding="utf-8"))
    restored_proc = json.loads(proc_path.read_text(encoding="utf-8"))
    assert restored_table["scoping"] == {"status": "resolved"}
    assert restored_table["profile"] == {"status": "ok"}
    assert "refactor" not in restored_table
    assert restored_proc["refactor"] == {"status": "ok"}
