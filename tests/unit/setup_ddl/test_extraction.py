"""Tests for extraction: DMF queries, extract validation, error handling."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")

from shared.sqlserver_extract import _run_dmf_queries
from tests.helpers import run_setup_ddl_cli as _run_cli


def test_run_dmf_queries_escapes_single_quotes(tmp_path) -> None:
    conn = MagicMock()
    list_cursor = MagicMock()
    dmf_cursor = MagicMock()

    list_cursor.fetchall.return_value = [("o'brien", "proc'name")]
    dmf_cursor.fetchall.return_value = []

    conn.cursor.side_effect = [list_cursor, dmf_cursor]

    _run_dmf_queries(
        conn=conn,
        schemas=["dbo"],
        object_type_filter="P",
        staging_dir=tmp_path,
        filename="proc_dmf.json",
    )

    dmf_sql = dmf_cursor.execute.call_args.args[0]
    assert "'o''brien' AS referencing_schema" in dmf_sql
    assert "'proc''name' AS referencing_name" in dmf_sql


def test_run_dmf_queries_scopes_object_list_to_selected_schemas(tmp_path) -> None:
    conn = MagicMock()
    list_cursor = MagicMock()

    list_cursor.fetchall.return_value = []
    conn.cursor.return_value = list_cursor

    _run_dmf_queries(
        conn=conn,
        schemas=["dbo", "gold"],
        object_type_filter="P",
        staging_dir=tmp_path,
        filename="proc_dmf.json",
    )

    list_sql = list_cursor.execute.call_args.args[0]
    assert "o.type = 'P'" in list_sql
    assert "SCHEMA_NAME(o.schema_id) IN ('dbo', 'gold')" in list_sql


class _FakeSqlCursor:
    def __init__(self, failure_map: dict[str, Exception] | None = None) -> None:
        self.failure_map = failure_map or {}
        self.description = []
        self.last_sql = ""

    def execute(self, sql: str):
        self.last_sql = sql
        for needle, exc in self.failure_map.items():
            if needle in sql:
                raise exc
        return self

    def fetchall(self):
        return []

    def close(self) -> None:
        return None


class _FakeSqlConn:
    def __init__(self, failure_map: dict[str, Exception] | None = None) -> None:
        self.failure_map = failure_map or {}

    def cursor(self):
        return _FakeSqlCursor(self.failure_map)

    def close(self) -> None:
        return None


class _RecordingSqlCursor(_FakeSqlCursor):
    def __init__(self, executed_sql: list[str]) -> None:
        super().__init__()
        self.executed_sql = executed_sql

    def execute(self, sql: str):
        self.executed_sql.append(sql)
        return super().execute(sql)


class _RecordingSqlConn:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def cursor(self):
        return _RecordingSqlCursor(self.executed_sql)

    def close(self) -> None:
        return None


def test_run_extract_fails_loudly_on_manifest_read_error(tmp_path: Path) -> None:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
    from shared import setup_ddl
    from shared.setup_ddl_support import extract as setup_ddl_extract

    (tmp_path / "manifest.json").write_text(
        '{"technology": "sql_server", "dialect": "tsql"}',
        encoding="utf-8",
    )

    with patch.object(setup_ddl_extract, "read_manifest_strict", side_effect=ValueError("manifest.json is not valid JSON: boom")):
        with pytest.raises(ValueError, match="manifest.json is not valid JSON"):
            setup_ddl.run_extract(tmp_path, database="MigrationTest", schemas=["dbo"])


def test_assemble_ddl_reports_only_files_written_this_run(tmp_path: Path) -> None:
    from shared.setup_ddl_support.extract import assemble_ddl_from_staging

    project_root = tmp_path / "project"
    staging_dir = tmp_path / "staging"
    (project_root / "ddl").mkdir(parents=True)
    staging_dir.mkdir()
    (project_root / "ddl" / "functions.sql").write_text("-- stale\n", encoding="utf-8")
    (staging_dir / "object_types.json").write_text(
        json.dumps([{"schema_name": "dbo", "name": "usp_load", "type": "P"}]),
        encoding="utf-8",
    )
    (staging_dir / "definitions.json").write_text(
        json.dumps(
            [
                {
                    "schema_name": "dbo",
                    "object_name": "usp_load",
                    "definition": "CREATE PROCEDURE dbo.usp_load AS SELECT 1",
                }
            ]
        ),
        encoding="utf-8",
    )

    written_paths = assemble_ddl_from_staging(staging_dir, project_root)

    assert written_paths == ["ddl/procedures.sql"]


def test_run_extract_reports_catalog_files_mutated_after_catalog_write(tmp_path: Path) -> None:
    from shared.output_models.catalog_enrich import CatalogEnrichOutput
    from shared.setup_ddl_support import extract as setup_ddl_extract

    catalog_path = tmp_path / "catalog" / "procedures" / "dbo.usp_load.json"

    def fake_write_catalog(_staging_dir: Path, project_root: Path, _db_name: str) -> dict[str, object]:
        catalog_path.parent.mkdir(parents=True)
        catalog_path.write_text(
            json.dumps({"schema": "dbo", "name": "usp_load"}),
            encoding="utf-8",
        )
        return {"tables": 0, "procedures": 1, "views": 0, "functions": 0, "written_paths": []}

    def fake_run_diagnostics(project_root: Path, dialect: str) -> dict[str, int]:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        payload["warnings"] = [{"code": "TEST", "message": "diagnostic"}]
        catalog_path.write_text(json.dumps(payload), encoding="utf-8")
        return {"objects_checked": 1, "warnings_added": 1, "errors_added": 0}

    with (
        patch.object(setup_ddl_extract, "require_technology", return_value="sql_server"),
        patch.object(setup_ddl_extract, "read_manifest_strict", return_value={}),
        patch.object(setup_ddl_extract, "get_connection_identity", return_value={}),
        patch.object(setup_ddl_extract, "identity_changed", return_value=False),
        patch.object(setup_ddl_extract, "run_db_extraction"),
        patch.object(setup_ddl_extract, "assemble_ddl_from_staging", return_value=["ddl/procedures.sql"]),
        patch.object(setup_ddl_extract, "run_write_manifest"),
        patch.object(setup_ddl_extract, "run_write_catalog", side_effect=fake_write_catalog),
        patch("shared.catalog.snapshot_enriched_fields", return_value={}),
        patch("shared.catalog.restore_enriched_fields"),
        patch(
            "shared.catalog_enrich.enrich_catalog",
            return_value=CatalogEnrichOutput(tables_augmented=0, procedures_augmented=0, entries_added=0),
        ),
        patch("shared.diagnostics.run_diagnostics", side_effect=fake_run_diagnostics),
    ):
        result = setup_ddl_extract.run_extract(tmp_path, database="MigrationTest", schemas=["dbo"])

    assert "catalog/procedures/dbo.usp_load.json" in result["written_paths"]


def test_run_extract_reports_catalog_files_marked_stale_on_identity_change(tmp_path: Path) -> None:
    from shared.output_models.catalog_enrich import CatalogEnrichOutput
    from shared.setup_ddl_support import extract as setup_ddl_extract

    catalog_path = tmp_path / "catalog" / "procedures" / "dbo.usp_load.json"
    catalog_path.parent.mkdir(parents=True)
    catalog_path.write_text(
        json.dumps({"schema": "dbo", "name": "usp_load"}),
        encoding="utf-8",
    )

    with (
        patch.object(setup_ddl_extract, "require_technology", return_value="sql_server"),
        patch.object(setup_ddl_extract, "read_manifest_strict", return_value={}),
        patch.object(setup_ddl_extract, "get_connection_identity", return_value={}),
        patch.object(setup_ddl_extract, "identity_changed", return_value=True),
        patch.object(setup_ddl_extract, "run_db_extraction"),
        patch.object(setup_ddl_extract, "assemble_ddl_from_staging", return_value=[]),
        patch.object(setup_ddl_extract, "run_write_manifest"),
        patch.object(
            setup_ddl_extract,
            "run_write_catalog",
            return_value={"tables": 0, "procedures": 0, "views": 0, "functions": 0, "written_paths": []},
        ),
        patch("shared.catalog.snapshot_enriched_fields", return_value={}),
        patch("shared.catalog.restore_enriched_fields"),
        patch(
            "shared.catalog_enrich.enrich_catalog",
            return_value=CatalogEnrichOutput(tables_augmented=0, procedures_augmented=0, entries_added=0),
        ),
        patch("shared.diagnostics.run_diagnostics", return_value={"objects_checked": 0, "warnings_added": 0, "errors_added": 0}),
    ):
        result = setup_ddl_extract.run_extract(tmp_path, database="MigrationTest", schemas=["dbo"])

    assert "catalog/procedures/dbo.usp_load.json" in result["written_paths"]


def test_sqlserver_optional_metadata_unsupported_feature_writes_empty_files(tmp_path: Path) -> None:
    from shared import sqlserver_extract

    conn = _FakeSqlConn(
        {
            "FROM sys.change_tracking_tables": RuntimeError("Invalid object name 'sys.change_tracking_tables'"),
            "FROM sys.sensitivity_classifications": RuntimeError("Invalid object name 'sys.sensitivity_classifications'"),
        }
    )

    with (
        patch.object(sqlserver_extract, "_sql_server_connect", return_value=conn),
        patch.object(sqlserver_extract, "_rows_to_dicts", return_value=[]),
    ):
        sqlserver_extract.run_sqlserver_extraction(tmp_path, database="MigrationTest", schemas=["dbo"])

    assert json.loads((tmp_path / "change_tracking.json").read_text(encoding="utf-8")) == []
    assert json.loads((tmp_path / "sensitivity.json").read_text(encoding="utf-8")) == []


def test_sqlserver_pk_unique_query_skips_filtered_and_included_index_columns(tmp_path: Path) -> None:
    """Unique-test metadata should only include globally unique key columns."""
    from shared import sqlserver_extract

    conn = _RecordingSqlConn()

    with (
        patch.object(sqlserver_extract, "_sql_server_connect", return_value=conn),
        patch.object(sqlserver_extract, "_rows_to_dicts", return_value=[]),
    ):
        sqlserver_extract.run_sqlserver_extraction(tmp_path, database="MigrationTest", schemas=["dbo"])

    pk_unique_sql = next(sql for sql in conn.executed_sql if "sys.indexes i" in sql)
    assert "i.has_filter = 0" in pk_unique_sql
    assert "ic.key_ordinal > 0" in pk_unique_sql
    assert "ic.is_included_column = 0" in pk_unique_sql


def test_sqlserver_extraction_uses_shared_runner_for_metadata_specs(tmp_path: Path) -> None:
    from shared import sqlserver_extract

    conn = _RecordingSqlConn()
    spec_calls: dict[str, bool] = {}
    dmf_calls: list[tuple[str, str]] = []

    def record_spec(conn_arg, spec, staging_dir_arg, schemas_arg):
        assert conn_arg is conn
        assert staging_dir_arg == tmp_path
        assert schemas_arg == ["dbo"]
        spec_calls[spec.filename] = spec.optional

    def record_dmf(conn_arg, schemas_arg, object_type_filter, staging_dir_arg, filename):
        assert conn_arg is conn
        assert schemas_arg == ["dbo"]
        assert staging_dir_arg == tmp_path
        dmf_calls.append((object_type_filter, filename))

    with (
        patch.object(sqlserver_extract, "_sql_server_connect", return_value=conn),
        patch.object(sqlserver_extract, "_run_sqlserver_query_spec", side_effect=record_spec),
        patch.object(sqlserver_extract, "_run_dmf_queries", side_effect=record_dmf),
    ):
        sqlserver_extract.run_sqlserver_extraction(tmp_path, database="MigrationTest", schemas=["dbo"])

    assert spec_calls == {
        "table_columns.json": False,
        "pk_unique.json": False,
        "foreign_keys.json": False,
        "identity_columns.json": False,
        "cdc.json": False,
        "change_tracking.json": True,
        "sensitivity.json": True,
        "object_types.json": False,
        "definitions.json": False,
        "proc_params.json": False,
        "indexed_views.json": False,
    }
    assert dmf_calls == [
        ("P", "proc_dmf.json"),
        ("V", "view_dmf.json"),
        ("FN", "func_dmf.json"),
    ]


def test_sqlserver_query_specs_scope_regular_queries_to_selected_schemas() -> None:
    from shared.sqlserver_extract import _sqlserver_query_specs

    for spec in _sqlserver_query_specs():
        sql = spec.sql_factory(["dbo", "gold"])
        assert "SCHEMA_NAME(" in sql
        assert " IN ('dbo', 'gold')" in sql


def test_sqlserver_indexed_views_spec_writes_lowercase_fqns(tmp_path: Path) -> None:
    from shared import sqlserver_extract

    conn = _FakeSqlConn()
    indexed_views_spec = next(
        spec
        for spec in sqlserver_extract._sqlserver_query_specs()
        if spec.filename == "indexed_views.json"
    )

    with patch.object(
        sqlserver_extract,
        "_rows_to_dicts",
        return_value=[
            {"schema_name": "Gold", "name": "IndexedSales"},
            {"schema_name": "dbo", "name": "DimCustomer"},
        ],
    ):
        sqlserver_extract._run_sqlserver_query_spec(
            conn,
            indexed_views_spec,
            tmp_path,
            ["dbo"],
        )

    assert json.loads((tmp_path / "indexed_views.json").read_text(encoding="utf-8")) == [
        "gold.indexedsales",
        "dbo.dimcustomer",
    ]


@pytest.mark.parametrize(
    "message",
    [
        "Invalid object name 'sys.change_tracking_tables'",
        "Change tracking is not supported",
        "Full text metadata is not supported in this version",
        "Could not find stored procedure 'sys.sp_describe_first_result_set'",
        "Cannot find the object 'sys.sensitivity_classifications'",
    ],
)
def test_sqlserver_optional_metadata_known_absence_markers_write_empty_file(
    tmp_path: Path,
    message: str,
) -> None:
    from shared import sqlserver_extract

    conn = _FakeSqlConn({"SELECT optional_metadata": RuntimeError(message)})
    spec = sqlserver_extract._SqlServerQuerySpec(
        "optional.json",
        lambda _schemas: "SELECT optional_metadata",
        optional=True,
    )

    sqlserver_extract._run_sqlserver_query_spec(conn, spec, tmp_path, ["dbo"])

    assert json.loads((tmp_path / "optional.json").read_text(encoding="utf-8")) == []


def test_sqlserver_optional_metadata_unexpected_error_raises(tmp_path: Path) -> None:
    from shared import sqlserver_extract

    conn = _FakeSqlConn(
        {"FROM sys.change_tracking_tables": RuntimeError("permission denied")}
    )

    with (
        patch.object(sqlserver_extract, "_sql_server_connect", return_value=conn),
        patch.object(sqlserver_extract, "_rows_to_dicts", return_value=[]),
    ):
        with pytest.raises(RuntimeError, match="permission denied"):
            sqlserver_extract.run_sqlserver_extraction(tmp_path, database="MigrationTest", schemas=["dbo"])


# ── Unit: extract arg validation ─────────────────────────────────────────────


class TestExtractValidation:
    def test_missing_schemas_errors(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--project-root", str(tmp_path),
        ])
        assert result.returncode != 0

    def test_missing_database_errors_for_sql_server(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ])
        assert result.returncode != 0
        assert "database" in result.stderr.lower()

    def test_missing_manifest_errors(self, tmp_path):
        result = _run_cli([
            "extract",
            "--database", "SomeDB",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ])
        assert result.returncode != 0

    def test_database_not_required_for_oracle(self, tmp_path):
        """Oracle extract should not error on missing --database (validated differently)."""
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        # Without ORACLE_* env vars set, this will fail at connection — not at arg validation
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ])
        # Should NOT fail with exit code 1 (arg validation error)
        # It will fail with exit code 2 (connection error) or succeed
        assert "database is required" not in result.stderr.lower()
