"""Tests for catalog enriched-field preservation across re-extraction."""

from __future__ import annotations

import json
from pathlib import Path

from shared.catalog import restore_enriched_fields, snapshot_enriched_fields


def test_snapshot_preserves_seed_flag_for_table_catalogs(tmp_path: Path) -> None:
    """Seed-table ownership survives catalog re-extraction."""
    table_dir = tmp_path / "catalog" / "tables"
    table_dir.mkdir(parents=True)
    table_path = table_dir / "silver.lookup.json"
    table_path.write_text(
        json.dumps({
            "schema": "silver",
            "name": "lookup",
            "is_seed": True,
            "is_source": False,
            "scoping": {"status": "no_writer_found"},
        }),
        encoding="utf-8",
    )

    snapshot = snapshot_enriched_fields(tmp_path)
    table_path.write_text(
        json.dumps({
            "schema": "silver",
            "name": "lookup",
        }),
        encoding="utf-8",
    )

    restore_enriched_fields(tmp_path, snapshot)

    restored = json.loads(table_path.read_text(encoding="utf-8"))
    assert restored["is_seed"] is True
    assert restored["is_source"] is False

