"""Oracle integration coverage for diff-aware catalog reexport."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.helpers import run_setup_ddl_cli as _run_cli
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    assert_manifest_has_runtime_roles,
    build_oracle_dsn,
    write_source_target_sandbox_manifest,
)

oracledb = pytest.importorskip(
    "oracledb",
    reason="oracledb not installed - skipping Oracle catalog diff integration tests",
)

pytestmark = pytest.mark.oracle


def _run_extract(project_root: Path) -> dict[str, object]:
    write_source_target_sandbox_manifest(project_root, source_technology="oracle")
    result = _run_cli([
        "extract",
        "--schemas",
        ORACLE_MIGRATION_SCHEMA,
        "--project-root",
        str(project_root),
    ], timeout=120)
    assert result.returncode == 0, result.stderr
    assert_manifest_has_runtime_roles(project_root)
    return json.loads(result.stdout)


def _connect_source_schema() -> oracledb.Connection:
    return oracledb.connect(
        user=os.environ["SOURCE_ORACLE_USER"],
        password=os.environ["SOURCE_ORACLE_PASSWORD"],
        dsn=build_oracle_dsn(),
    )


def _drop_view_if_exists(cursor: oracledb.Cursor, schema: str, view_name: str) -> None:
    try:
        cursor.execute(f'DROP VIEW "{schema}"."{view_name}"')
    except oracledb.DatabaseError:
        pass


@pytest.mark.usefixtures("oracle_extract_env")
class TestDiffAwareReexportOracleIntegration:
    def test_identical_reextract_preserves_enriched_sections(self, tmp_path, oracle_extract_env):
        counts1 = _run_extract(tmp_path)
        assert counts1["new"] > 0

        table_files = list((tmp_path / "catalog" / "tables").glob("*.json"))
        assert table_files
        enriched_table = table_files[0]
        data = json.loads(enriched_table.read_text(encoding="utf-8"))
        data["scoping"] = {"status": "resolved", "selected_writer": "test.usp_fake"}
        enriched_table.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

        counts2 = _run_extract(tmp_path)

        assert counts2["unchanged"] == counts1["new"]
        assert counts2["changed"] == 0
        assert counts2["new"] == 0
        assert counts2["removed"] == 0

        rerun_data = json.loads(enriched_table.read_text(encoding="utf-8"))
        assert rerun_data.get("scoping", {}).get("status") == "resolved"

    def test_view_catalog_has_sql_columns_and_hash(self, tmp_path, oracle_extract_env):
        counts = _run_extract(tmp_path)
        assert counts["views"] > 0

        view_files = list((tmp_path / "catalog" / "views").glob("*.json"))
        assert view_files
        view_catalog = json.loads(view_files[0].read_text(encoding="utf-8"))

        assert view_catalog["sql"].startswith("CREATE OR REPLACE VIEW")
        assert len(view_catalog["ddl_hash"]) == 64
        assert view_catalog["columns"]
        for column in view_catalog["columns"]:
            assert "name" in column
            assert "sql_type" in column
            assert "is_nullable" in column

    def test_changed_view_detected_as_changed(self, tmp_path, oracle_extract_env):
        conn = _connect_source_schema()
        cursor = conn.cursor()
        view_name = "SILVER_VW_DIFF_TEST"
        _drop_view_if_exists(cursor, ORACLE_MIGRATION_SCHEMA, view_name)

        try:
            try:
                cursor.execute(
                    f'CREATE VIEW "{ORACLE_MIGRATION_SCHEMA}"."{view_name}" AS '
                    "SELECT 1 AS DIFF_MARKER FROM DUAL"
                )
            except oracledb.DatabaseError as exc:
                pytest.skip(f"Oracle source user cannot create test view: {exc}")

            counts1 = _run_extract(tmp_path)
            assert counts1["new"] > 0

            cursor.execute(
                f'CREATE OR REPLACE VIEW "{ORACLE_MIGRATION_SCHEMA}"."{view_name}" AS '
                "SELECT 2 AS DIFF_MARKER FROM DUAL"
            )

            counts2 = _run_extract(tmp_path)

            assert counts2["changed"] >= 1
            assert counts2["new"] == 0
            assert counts2["removed"] == 0
        finally:
            _drop_view_if_exists(cursor, ORACLE_MIGRATION_SCHEMA, view_name)
            conn.close()
