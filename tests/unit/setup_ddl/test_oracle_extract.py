"""Tests for Oracle-specific schema processing and extract helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")


ORACLE_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "oracle"


# ── Unit: Oracle schema processing (no DB required) ──────────────────────────


class TestOracleSchemaProcessing:
    def test_groups_by_owner_from_fixture(self):
        from shared.setup_ddl_support.manifest import build_oracle_schema_summary
        rows = json.loads((ORACLE_FIXTURE_DIR / "list_schemas.json").read_text(encoding="utf-8"))
        summary = build_oracle_schema_summary(rows)
        owners = {entry["owner"] for entry in summary}
        assert "SH" in owners
        sh_entry = next(e for e in summary if e["owner"] == "SH")
        assert sh_entry["tables"] > 0

    def test_empty_input_returns_empty_list(self):
        from shared.setup_ddl_support.manifest import build_oracle_schema_summary
        assert build_oracle_schema_summary([]) == []

    def test_sorted_by_owner(self):
        from shared.setup_ddl_support.manifest import build_oracle_schema_summary
        rows = [
            {"OWNER": "ZZ", "OBJECT_TYPE": "TABLE", "OBJECT_NAME": "T1"},
            {"OWNER": "AA", "OBJECT_TYPE": "TABLE", "OBJECT_NAME": "T2"},
            {"OWNER": "MM", "OBJECT_TYPE": "TABLE", "OBJECT_NAME": "T3"},
        ]
        summary = build_oracle_schema_summary(rows)
        owners = [e["owner"] for e in summary]
        assert owners == sorted(owners)

    def test_lowercase_keys_handled(self):
        from shared.setup_ddl_support.manifest import build_oracle_schema_summary
        rows = [
            {"owner": "SH", "object_type": "TABLE", "object_name": "SALES"},
            {"owner": "SH", "object_type": "TABLE", "object_name": "COSTS"},
        ]
        summary = build_oracle_schema_summary(rows)
        assert len(summary) == 1
        assert summary[0]["owner"] == "SH"
        assert summary[0]["tables"] == 2


# ── Unit: oracle_extract helpers (fixture-based, no live DB) ─────────────────


class TestExtractOracleUnit:
    def test_pk_uk_fixture_has_primary_keys(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_pk_uk.json").read_text())
        pk_rows = [r for r in rows if r["is_primary_key"] == 1]
        assert len(pk_rows) > 0, "SH schema should have primary key constraints"

    def test_fk_fixture_has_foreign_keys(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_fk.json").read_text())
        assert len(rows) > 0, "SH schema should have foreign key constraints"
        constraint_names = {r["constraint_name"] for r in rows}
        assert any("FK" in name or "fk" in name.lower() for name in constraint_names)

    def test_table_columns_fixture_has_sh_tables(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_tab_columns.json").read_text())
        table_names = {r["table_name"] for r in rows}
        assert "CUSTOMERS" in table_names
        assert "SALES" in table_names

    def test_table_columns_have_required_fields(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_tab_columns.json").read_text())
        required = {"schema_name", "table_name", "column_name", "column_id",
                    "type_name", "max_length", "precision", "scale",
                    "is_nullable", "is_identity"}
        for row in rows[:5]:
            assert required.issubset(set(row.keys())), f"Missing fields in row: {row.keys()}"

    def test_object_types_fixture_maps_to_sql_server_codes(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "object_types.json").read_text())
        valid_types = {"U", "V", "P", "FN"}
        for row in rows:
            assert row["type"] in valid_types, f"Unexpected type code: {row['type']}"

    def test_dependencies_have_dmf_shape(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "dependencies_view.json").read_text())
        if not rows:
            return  # SH may have no view deps; skip rather than fail
        required = {
            "referencing_schema", "referencing_name", "referenced_schema",
            "referenced_entity", "referenced_minor_name", "referenced_class_desc",
            "is_selected", "is_updated", "is_insert_all",
        }
        for row in rows:
            assert required.issubset(set(row.keys()))
            assert row["is_selected"] is False
            assert row["is_updated"] is False

    def test_pk_rows_feed_apply_pk_unique(self, tmp_path):
        """Verify PK rows from Oracle fixture are consumed correctly by _apply_pk_unique_rows."""
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.setup_ddl_support.staging import apply_pk_unique_rows

        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_pk_uk.json").read_text())
        signals: dict = {}
        apply_pk_unique_rows(signals, rows)
        assert len(signals) > 0
        for fqn, sig in signals.items():
            for pk in sig.get("primary_keys", []):
                assert "constraint_name" in pk
                assert "columns" in pk
                assert len(pk["columns"]) > 0

    def test_fk_rows_feed_apply_fk_rows(self, tmp_path):
        """Verify FK rows from Oracle fixture are consumed correctly by _apply_fk_rows."""
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.setup_ddl_support.staging import apply_fk_rows

        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_fk.json").read_text())
        signals: dict = {}
        apply_fk_rows(signals, rows)
        assert len(signals) > 0
        for fqn, sig in signals.items():
            for fk in sig.get("foreign_keys", []):
                assert "constraint_name" in fk
                assert "referenced_table" in fk

    def test_definitions_fixture_has_clean_view_format(self):
        """Oracle definitions fixture uses ALL_VIEWS-reconstructed format, not DBMS_METADATA."""
        rows = json.loads((ORACLE_FIXTURE_DIR / "definitions.json").read_text())
        view_rows = [r for r in rows if r["object_name"] == "PROFITS"]
        assert len(view_rows) == 1
        defn = view_rows[0]["definition"]
        assert defn.startswith("CREATE OR REPLACE VIEW SH.PROFITS AS")
        assert "FORCE" not in defn
        assert "EDITIONABLE" not in defn

    def test_extract_view_ddl_reconstructs_from_all_views(self):
        """_extract_view_ddl builds CREATE OR REPLACE VIEW DDL from ALL_VIEWS rows."""
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.oracle_extract import _extract_view_ddl
        from unittest.mock import MagicMock

        all_views_rows = json.loads((ORACLE_FIXTURE_DIR / "all_views.json").read_text())

        # Build a mock connection whose cursor returns the ALL_VIEWS fixture rows
        mock_cur = MagicMock()
        mock_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
        mock_cur.fetchall.return_value = [
            (r["OWNER"], r["VIEW_NAME"], r["TEXT"]) for r in all_views_rows
        ]
        mock_conn = MagicMock()
        # Use side_effect so any unexpected second cursor() call raises StopIteration
        # rather than silently returning mock_cur and masking fallback bugs.
        mock_conn.cursor.side_effect = [mock_cur]

        result = _extract_view_ddl(mock_conn, ["SH"])

        assert len(result) == 1
        row = result[0]
        assert row["schema_name"] == "SH"
        assert row["object_name"] == "PROFITS"
        assert row["definition"].startswith("CREATE OR REPLACE VIEW SH.PROFITS AS\n")
        assert "channel_id" in row["definition"]

    def test_extract_view_ddl_falls_back_on_empty_text(self):
        """_extract_view_ddl falls back to DBMS_METADATA when ALL_VIEWS.TEXT is empty."""
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.oracle_extract import _extract_view_ddl
        from unittest.mock import MagicMock

        fallback_ddl = "CREATE OR REPLACE VIEW SH.PROFITS AS SELECT 1 FROM DUAL"

        mock_main_cur = MagicMock()
        mock_main_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
        mock_main_cur.fetchall.return_value = [("SH", "PROFITS", "")]

        mock_ddl_cur = MagicMock()
        clob = MagicMock()
        clob.read.return_value = fallback_ddl
        mock_ddl_cur.fetchone.return_value = (clob,)

        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = [mock_main_cur, mock_ddl_cur]

        result = _extract_view_ddl(mock_conn, ["SH"])

        assert len(result) == 1
        assert result[0]["definition"] == fallback_ddl

    def test_extract_view_ddl_falls_back_on_truncated_text(self):
        """_extract_view_ddl falls back to DBMS_METADATA when TEXT is exactly 32,767 bytes.

        oracledb thin mode silently truncates LONG columns at 32,767 bytes — the result
        arrives at exactly that boundary with no error signal. We treat any TEXT that is
        exactly 32,767 characters as potentially truncated and use DBMS_METADATA instead.
        """
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.oracle_extract import _extract_view_ddl
        from unittest.mock import MagicMock

        fallback_ddl = "CREATE OR REPLACE VIEW SH.PROFITS AS SELECT 1 FROM DUAL"
        truncated_text = "x" * 32767  # exactly at the LONG truncation boundary

        mock_main_cur = MagicMock()
        mock_main_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
        mock_main_cur.fetchall.return_value = [("SH", "PROFITS", truncated_text)]

        mock_ddl_cur = MagicMock()
        clob = MagicMock()
        clob.read.return_value = fallback_ddl
        mock_ddl_cur.fetchone.return_value = (clob,)

        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = [mock_main_cur, mock_ddl_cur]

        result = _extract_view_ddl(mock_conn, ["SH"])

        assert len(result) == 1
        assert result[0]["definition"] == fallback_ddl
