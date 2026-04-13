"""Integration tests for diff-aware catalog reexport — requires Docker SQL Server.

Validates hash stability and diff-aware behavior against real OBJECT_DEFINITION()
output and sys.columns metadata, which have whitespace patterns that hand-crafted
staging data cannot replicate.

Run with: cd lib && uv run pytest ../tests/integration/sql_server/catalog_diff -v -k test_catalog_diff
Requires: SA_PASSWORD env var (Docker SQL Server with MigrationTest DB).
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import SHARED_LIB_DIR
from tests.integration.runtime_helpers import (
    SQL_SERVER_MIGRATION_DATABASE,
    build_sql_server_connection_string,
    ensure_sql_server_migration_test_materialized,
    sql_server_is_available,
)

pyodbc = pytest.importorskip("pyodbc", reason="pyodbc not installed — skipping integration tests")

pytestmark = pytest.mark.integration


def _have_mssql_env() -> bool:
    return sql_server_is_available(pyodbc)


def _connect() -> pyodbc.Connection:
    ensure_sql_server_migration_test_materialized()
    return pyodbc.connect(build_sql_server_connection_string(), autocommit=True)


def _query_rows(conn: pyodbc.Connection, sql: str) -> list[dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _extract_staging(conn: pyodbc.Connection, staging_dir: Path, database: str, schemas: str) -> None:
    """Extract real staging data from SQL Server into JSON files."""
    schema_filter = ", ".join(f"'{s}'" for s in schemas.split(","))

    # table_columns (CAST seed_value/increment_value to avoid SQL_VARIANT pyodbc error)
    rows = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name,
               t.name AS table_name, c.name AS column_name, c.column_id,
               tp.name AS type_name, c.max_length, c.precision, c.scale,
               c.is_nullable, c.is_identity,
               CAST(ic.seed_value AS BIGINT) AS seed_value,
               CAST(ic.increment_value AS BIGINT) AS increment_value
        FROM sys.tables t
        JOIN sys.columns c ON c.object_id = t.object_id
        JOIN sys.types tp ON tp.user_type_id = c.user_type_id
        LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE SCHEMA_NAME(t.schema_id) IN ({schema_filter})
        ORDER BY t.name, c.column_id
    """)
    _write_json(staging_dir / "table_columns.json", rows)

    # object_types
    rows = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name,
               RTRIM(o.type) AS type
        FROM sys.objects o
        WHERE o.type IN ('U','P','V','FN','IF','TF')
          AND SCHEMA_NAME(o.schema_id) IN ({schema_filter})
    """)
    _write_json(staging_dir / "object_types.json", rows)

    # definitions (procs, views, functions)
    rows = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name,
               o.name AS object_name,
               OBJECT_DEFINITION(o.object_id) AS definition
        FROM sys.objects o
        WHERE o.type IN ('P','V','FN','IF','TF')
          AND SCHEMA_NAME(o.schema_id) IN ({schema_filter})
    """)
    _write_json(staging_dir / "definitions.json", rows)

    # pk_unique
    rows = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               i.name AS index_name, i.is_unique, i.is_primary_key,
               c.name AS column_name, ic.key_ordinal
        FROM sys.tables t
        JOIN sys.indexes i ON i.object_id = t.object_id
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE (i.is_unique = 1 OR i.is_primary_key = 1)
          AND SCHEMA_NAME(t.schema_id) IN ({schema_filter})
        ORDER BY schema_name, table_name, i.index_id, ic.key_ordinal
    """)
    _write_json(staging_dir / "pk_unique.json", rows)

    # foreign_keys
    rows = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               fk.name AS constraint_name,
               COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
               SCHEMA_NAME(rt.schema_id) AS ref_schema, rt.name AS ref_table,
               COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
        FROM sys.foreign_keys fk
        JOIN sys.tables t ON t.object_id = fk.parent_object_id
        JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        WHERE SCHEMA_NAME(t.schema_id) IN ({schema_filter})
        ORDER BY schema_name, table_name, fk.name, fkc.constraint_column_id
    """)
    _write_json(staging_dir / "foreign_keys.json", rows)

    # identity_columns (CAST to avoid SQL_VARIANT pyodbc error)
    rows = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               c.name AS column_name,
               CAST(ic.seed_value AS BIGINT) AS seed_value,
               CAST(ic.increment_value AS BIGINT) AS increment_value
        FROM sys.identity_columns ic
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.tables t ON t.object_id = ic.object_id
        WHERE SCHEMA_NAME(t.schema_id) IN ({schema_filter})
    """)
    _write_json(staging_dir / "identity_columns.json", rows)

    # DMF references for procs
    proc_dmf: list[dict[str, Any]] = []
    procs = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name
        FROM sys.objects o
        WHERE o.type = 'P' AND SCHEMA_NAME(o.schema_id) IN ({schema_filter})
    """)
    for proc in procs:
        try:
            refs = _query_rows(conn, f"""
                SELECT '{proc["schema_name"]}' AS referencing_schema,
                       '{proc["object_name"]}' AS referencing_name,
                       r.referenced_schema_name AS referenced_schema,
                       r.referenced_entity_name AS referenced_entity,
                       ISNULL(r.referenced_minor_name, '') AS referenced_minor_name,
                       r.referenced_class_desc,
                       r.is_selected, r.is_updated, r.is_select_all,
                       CAST(0 AS BIT) AS is_insert_all,
                       r.is_all_columns_found,
                       r.is_caller_dependent, r.is_ambiguous
                FROM sys.dm_sql_referenced_entities(
                    '{proc["schema_name"]}.{proc["object_name"]}', 'OBJECT') r
                WHERE r.referenced_entity_name IS NOT NULL
            """)
            proc_dmf.extend(refs)
        except pyodbc.ProgrammingError:
            pass
    _write_json(staging_dir / "proc_dmf.json", proc_dmf)

    # Empty DMF for views/functions (sufficient for this test)
    _write_json(staging_dir / "view_dmf.json", [])
    _write_json(staging_dir / "func_dmf.json", [])

    # proc_params
    rows = _query_rows(conn, f"""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS proc_name,
               p.name AS param_name, tp.name AS type_name,
               p.max_length, p.precision, p.scale,
               p.is_output, p.has_default_value
        FROM sys.parameters p
        JOIN sys.objects o ON o.object_id = p.object_id
        JOIN sys.types tp ON tp.user_type_id = p.user_type_id
        WHERE o.type = 'P' AND SCHEMA_NAME(o.schema_id) IN ({schema_filter})
          AND p.parameter_id > 0
    """)
    _write_json(staging_dir / "proc_params.json", rows)

    # cdc, change_tracking, sensitivity — empty for most test DBs
    _write_json(staging_dir / "cdc.json", [])
    _write_json(staging_dir / "change_tracking.json", [])
    _write_json(staging_dir / "sensitivity.json", [])


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def _run_write_catalog(staging_dir: Path, project_root: Path, database: str) -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, "-m", "shared.setup_ddl", "write-catalog",
         "--staging-dir", str(staging_dir),
         "--project-root", str(project_root),
         "--database", database],
        cwd=str(SHARED_LIB_DIR),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"write-catalog failed: {result.stderr}"
    return json.loads(result.stdout)


skip_no_mssql = pytest.mark.skipif(
    not _have_mssql_env(),
    reason="MSSQL integration DB not reachable (MSSQL_HOST, SA_PASSWORD and a listening server required)",
)


@skip_no_mssql
class TestDiffAwareReexportIntegration:
    """End-to-end diff-aware reexport against a real SQL Server."""

    def _get_schemas(self, conn: pyodbc.Connection) -> str:
        """Find schemas with at least one procedure."""
        rows = _query_rows(conn, """
            SELECT DISTINCT SCHEMA_NAME(o.schema_id) AS s
            FROM sys.objects o
            WHERE o.type = 'P'
              AND SCHEMA_NAME(o.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
        """)
        schemas = [r["s"] for r in rows]
        assert schemas, "No user schemas with procedures found in test DB"
        return ",".join(schemas)

    def test_first_run_produces_hashes_from_real_ddl(self) -> None:
        """Verify ddl_hash is written for real OBJECT_DEFINITION() output."""
        conn = _connect()
        database = SQL_SERVER_MIGRATION_DATABASE
        schemas = self._get_schemas(conn)

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp) / "staging"
            output = Path(tmp) / "output"
            _extract_staging(conn, staging, database, schemas)

            counts = _run_write_catalog(staging, output, database)

            assert counts["new"] > 0, "Expected at least one new object"
            assert counts["unchanged"] == 0
            assert counts["changed"] == 0

            # Verify at least one catalog file has a ddl_hash
            found_hash = False
            for bucket in ("tables", "procedures", "views", "functions"):
                bucket_dir = output / "catalog" / bucket
                if not bucket_dir.exists():
                    continue
                for f in bucket_dir.glob("*.json"):
                    data = json.loads(f.read_text())
                    if "ddl_hash" in data:
                        assert len(data["ddl_hash"]) == 64
                        found_hash = True
            assert found_hash, "No catalog file has ddl_hash"

        conn.close()

    def test_identical_reextraction_produces_all_unchanged(self) -> None:
        """Extract twice from the same DB state — everything should be unchanged."""
        conn = _connect()
        database = SQL_SERVER_MIGRATION_DATABASE
        schemas = self._get_schemas(conn)

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp) / "staging"
            output = Path(tmp) / "output"

            # First extraction + write
            _extract_staging(conn, staging, database, schemas)
            counts1 = _run_write_catalog(staging, output, database)
            total_first = counts1["tables"] + counts1["procedures"] + counts1["views"] + counts1["functions"]

            # Inject fake LLM section into a table catalog to prove preservation
            table_files = list((output / "catalog" / "tables").glob("*.json"))
            enriched_table = None
            if table_files:
                enriched_table = table_files[0]
                data = json.loads(enriched_table.read_text())
                data["scoping"] = {"status": "resolved", "selected_writer": "test.usp_fake"}
                enriched_table.write_text(json.dumps(data, indent=2) + "\n")

            # Second extraction from same DB state + write
            _extract_staging(conn, staging, database, schemas)
            counts2 = _run_write_catalog(staging, output, database)

            assert counts2["unchanged"] == counts1["new"], (
                f"Expected all {counts1['new']} objects unchanged, "
                f"got {counts2['unchanged']} unchanged, {counts2['changed']} changed"
            )
            assert counts2["changed"] == 0
            assert counts2["new"] == 0
            assert counts2["removed"] == 0

            # Verify LLM section survived
            if enriched_table is not None:
                data = json.loads(enriched_table.read_text())
                assert data.get("scoping", {}).get("status") == "resolved", (
                    "LLM-enriched scoping section was destroyed by reexport"
                )

        conn.close()

    def test_altered_proc_detected_as_changed(self) -> None:
        """Alter a procedure, re-extract, verify it's classified as changed."""
        conn = _connect()
        database = SQL_SERVER_MIGRATION_DATABASE
        schemas = self._get_schemas(conn)

        # Find a procedure to alter
        procs = _query_rows(conn, f"""
            SELECT TOP 1 SCHEMA_NAME(o.schema_id) AS schema_name,
                   o.name AS object_name,
                   OBJECT_DEFINITION(o.object_id) AS definition
            FROM sys.objects o
            WHERE o.type = 'P'
              AND SCHEMA_NAME(o.schema_id) IN ({",".join(f"'{s}'" for s in schemas.split(","))})
              AND OBJECT_DEFINITION(o.object_id) IS NOT NULL
            ORDER BY o.name
        """)
        if not procs:
            pytest.skip("No alterable procedures found")

        proc = procs[0]
        original_def = proc["definition"]

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp) / "staging"
            output = Path(tmp) / "output"

            # First extraction + write
            _extract_staging(conn, staging, database, schemas)
            _run_write_catalog(staging, output, database)

            # ALTER the procedure (add a harmless comment)
            altered_def = original_def.replace("CREATE", "ALTER", 1)
            # Add a unique comment to change the definition
            altered_def += "\n-- diff_test_marker"
            cursor = conn.cursor()
            try:
                cursor.execute(altered_def)

                # Second extraction + write
                _extract_staging(conn, staging, database, schemas)
                counts2 = _run_write_catalog(staging, output, database)

                assert counts2["changed"] >= 1, (
                    f"Expected at least 1 changed object after ALTER, got {counts2['changed']}"
                )
            finally:
                # Restore original definition
                restore_def = original_def.replace("CREATE", "ALTER", 1)
                cursor.execute(restore_def)

        conn.close()


@skip_no_mssql
class TestViewCatalogEnrichmentIntegration:
    """Integration tests for view catalog sql+columns enrichment via setup-ddl."""

    def test_view_catalog_has_sql_and_columns(self) -> None:
        """setup-ddl write-catalog writes sql and columns into view catalog JSON.

        Creates a temporary test view in the MigrationTest DB, runs write-catalog
        with view_columns.json extracted via sys.columns, then asserts the view
        catalog at catalog/views/<fqn>.json has non-empty sql and columns fields.
        """
        conn = _connect()
        database = SQL_SERVER_MIGRATION_DATABASE
        view_schema = "dbo"
        view_name = "vw_integration_test_view"
        fqn = f"{view_schema}.{view_name}"

        # Find a table to base the view on
        tables = _query_rows(conn, f"""
            SELECT TOP 1 SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
            FROM sys.tables t
            WHERE SCHEMA_NAME(t.schema_id) = 'dbo'
              AND t.is_ms_shipped = 0
        """)
        if not tables:
            pytest.skip("No dbo tables found in test DB — cannot create test view")

        source_table = f"[{tables[0]['schema_name']}].[{tables[0]['table_name']}]"
        cursor = conn.cursor()

        # Drop any leftover view from a previous failed run.
        cursor.execute(f"DROP VIEW IF EXISTS [{view_schema}].[{view_name}]")

        # Create a simple test view
        cursor.execute(f"""
            CREATE VIEW [{view_schema}].[{view_name}] AS
            SELECT TOP 1 * FROM {source_table}
        """)

        try:
            with tempfile.TemporaryDirectory() as tmp:
                staging = Path(tmp) / "staging"
                output = Path(tmp) / "output"

                schemas = view_schema

                # Extract staging data including view_columns.json
                _extract_staging(conn, staging, database, schemas)

                # Extract view_columns.json (the new staging file added by this feature)
                schema_filter = f"'{view_schema}'"
                view_cols = _query_rows(conn, f"""
                    SELECT SCHEMA_NAME(v.schema_id) AS schema_name,
                           v.name AS view_name,
                           c.name AS column_name, c.column_id,
                           tp.name AS type_name, c.max_length, c.precision, c.scale,
                           c.is_nullable
                    FROM sys.views v
                    JOIN sys.columns c ON c.object_id = v.object_id
                    JOIN sys.types tp ON tp.user_type_id = c.user_type_id
                    WHERE v.is_ms_shipped = 0
                      AND SCHEMA_NAME(v.schema_id) IN ({schema_filter})
                    ORDER BY schema_name, view_name, c.column_id
                """)
                _write_json(staging / "view_columns.json", view_cols)

                counts = _run_write_catalog(staging, output, database)
                assert counts["views"] > 0, "Expected at least one view catalog to be written"

                # Verify the test view's catalog has sql and columns
                norm_fqn = f"{view_schema}.{view_name}".lower()
                cat_path = output / "catalog" / "views" / f"{norm_fqn}.json"
                assert cat_path.exists(), f"View catalog not found at {cat_path}"

                cat = json.loads(cat_path.read_text(encoding="utf-8"))
                assert "sql" in cat, "View catalog missing 'sql' field"
                assert cat["sql"], "View catalog 'sql' field is empty"
                assert view_name.lower() in cat["sql"].lower(), (
                    f"View name not found in sql field: {cat['sql'][:200]}"
                )
                assert "columns" in cat, "View catalog missing 'columns' field"
                assert len(cat["columns"]) > 0, "View catalog 'columns' list is empty"
                for col in cat["columns"]:
                    assert "name" in col, "Column entry missing 'name'"
                    assert "sql_type" in col, "Column entry missing 'sql_type'"
                    assert "is_nullable" in col, "Column entry missing 'is_nullable'"
        finally:
            cursor.execute(f"DROP VIEW IF EXISTS [{view_schema}].[{view_name}]")
            conn.close()
