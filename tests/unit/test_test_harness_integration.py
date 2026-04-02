"""Integration tests for test harness — requires Docker SQL Server with MigrationTest DB.

Run with: uv run --project lib pytest -m integration -v
Requires: MSSQL_HOST, SA_PASSWORD, MSSQL_DB env vars (or Docker 'aw-sql' on localhost:1433).
"""

from __future__ import annotations

import os
import uuid

import pytest

from shared.sandbox.sql_server import SqlServerSandbox

pytestmark = pytest.mark.integration


def _have_mssql_env() -> bool:
    return bool(os.environ.get("SA_PASSWORD"))


def _make_backend() -> SqlServerSandbox:
    return SqlServerSandbox(
        host=os.environ.get("MSSQL_HOST", "localhost"),
        port=os.environ.get("MSSQL_PORT", "1433"),
        database=os.environ.get("MSSQL_DB", "MigrationTest"),
        password=os.environ.get("SA_PASSWORD", ""),
        user=os.environ.get("MSSQL_USER", "sa"),
        driver=os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
    )


skip_no_mssql = pytest.mark.skipif(
    not _have_mssql_env(),
    reason="MSSQL env vars not set (SA_PASSWORD required)",
)


@skip_no_mssql
class TestSandboxLifecycle:
    """Full sandbox create → verify → teardown against a real SQL Server."""

    def _unique_run_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def test_sandbox_up_creates_and_clones(self) -> None:
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            result = backend.sandbox_up(run_id=run_id, schemas=["silver"])

            assert result["status"] in ("ok", "partial")
            assert result["run_id"] == run_id
            assert result["sandbox_database"] == backend.sandbox_db_name(run_id)
            assert len(result["tables_cloned"]) > 0
            assert any("DimCurrency" in t for t in result["tables_cloned"])
            assert len(result["procedures_cloned"]) > 0
            assert any("usp_load_DimCurrency" in p for p in result["procedures_cloned"])

            # Verify the sandbox DB actually exists
            sandbox_db = result["sandbox_database"]
            with backend._connect(database=sandbox_db) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_TYPE = 'BASE TABLE'"
                )
                table_count = cursor.fetchone()[0]
                assert table_count > 0
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_sandbox_down_drops_database(self) -> None:
        backend = _make_backend()
        run_id = self._unique_run_id()

        backend.sandbox_up(run_id=run_id, schemas=["silver"])
        result = backend.sandbox_down(run_id=run_id)

        assert result["status"] == "ok"

        # Verify the DB is gone
        with backend._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DB_ID(?)", backend.sandbox_db_name(run_id))
            assert cursor.fetchone()[0] is None

    def test_sandbox_down_idempotent(self) -> None:
        backend = _make_backend()
        run_id = self._unique_run_id()

        # Down on a non-existent sandbox should succeed
        result = backend.sandbox_down(run_id=run_id)
        assert result["status"] == "ok"

    def test_sandbox_up_multiple_schemas(self) -> None:
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            result = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])

            assert result["status"] in ("ok", "partial")
            schemas_seen = {t.split(".")[0] for t in result["tables_cloned"]}
            assert "bronze" in schemas_seen
            assert "silver" in schemas_seen
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_sandbox_up_idempotent(self) -> None:
        """Calling sandbox_up twice with the same run_id recreates cleanly."""
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            result1 = backend.sandbox_up(run_id=run_id, schemas=["silver"])
            result2 = backend.sandbox_up(run_id=run_id, schemas=["silver"])

            assert result1["status"] in ("ok", "partial")
            assert result2["status"] in ("ok", "partial")
            assert result1["tables_cloned"] == result2["tables_cloned"]
        finally:
            backend.sandbox_down(run_id=run_id)


@skip_no_mssql
class TestExecuteScenario:
    """Execute a real scenario against a sandbox database."""

    def _unique_run_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _create_temp_proc(self, backend: SqlServerSandbox, proc_name: str, body: str) -> None:
        with backend._connect(database=backend.database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                CREATE OR ALTER PROCEDURE [silver].[{proc_name}]
                AS
                BEGIN
                    {body}
                END
                """
            )

    def _drop_temp_proc(self, backend: SqlServerSandbox, proc_name: str) -> None:
        with backend._connect(database=backend.database) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP PROCEDURE IF EXISTS [silver].[{proc_name}]")

    def test_execute_inserts_and_captures_ground_truth(self) -> None:
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up_result = backend.sandbox_up(
                run_id=run_id, schemas=["bronze", "silver"],
            )
            assert up_result["status"] in ("ok", "partial")
            assert any("DimCurrency" in t for t in up_result["tables_cloned"])

            scenario = {
                "name": "test_load_dim_currency",
                "target_table": "[silver].[DimCurrency]",
                "procedure": "[silver].[usp_load_DimCurrency]",
                "given": [
                    {
                        "table": "[bronze].[Currency]",
                        "rows": [
                            {"CurrencyCode": "TST", "CurrencyName": "Test Currency",
                             "ModifiedDate": "2024-01-01"},
                        ],
                    },
                ],
            }

            result = backend.execute_scenario(run_id=run_id, scenario=scenario)

            assert result["status"] == "ok"
            assert result["scenario_name"] == "test_load_dim_currency"
            assert result["row_count"] >= 1
            assert isinstance(result["ground_truth_rows"], list)
            assert len(result["ground_truth_rows"]) >= 1
            assert result["errors"] == []
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_execute_rolls_back_fixture_data(self) -> None:
        """Verify fixture data is rolled back after scenario execution."""
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            sandbox_db = backend.sandbox_db_name(run_id)

            scenario = {
                "name": "test_rollback",
                "target_table": "[silver].[DimCurrency]",
                "procedure": "[silver].[usp_load_DimCurrency]",
                "given": [
                    {
                        "table": "[bronze].[Currency]",
                        "rows": [
                            {"CurrencyCode": "RBK", "Name": "Rollback Test"},
                        ],
                    },
                ],
            }

            backend.execute_scenario(run_id=run_id, scenario=scenario)

            # Verify the fixture data was rolled back
            with backend._connect(database=sandbox_db) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM [bronze].[Currency] "
                    "WHERE CurrencyCode = 'RBK'"
                )
                count = cursor.fetchone()[0]
                assert count == 0, "Fixture data should be rolled back after execution"
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_execute_empty_fixtures(self) -> None:
        """Scenario with no fixture rows still runs the procedure."""
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])

            scenario = {
                "name": "test_empty_fixtures",
                "target_table": "[silver].[DimCurrency]",
                "procedure": "[silver].[usp_load_DimCurrency]",
                "given": [
                    {"table": "[bronze].[Currency]", "rows": []},
                ],
            }

            result = backend.execute_scenario(run_id=run_id, scenario=scenario)

            assert result["status"] == "ok"
            assert result["row_count"] >= 0
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_execute_cross_database_exec_returns_clear_error(self) -> None:
        backend = _make_backend()
        run_id = self._unique_run_id()
        proc_name = f"usp_remote_cross_db_{self._unique_run_id()}"

        self._create_temp_proc(backend, proc_name, "EXEC OtherDB.dbo.usp_Load;")

        try:
            up_result = backend.sandbox_up(run_id=run_id, schemas=["silver"])
            assert up_result["status"] in ("ok", "partial")

            result = backend.execute_scenario(
                run_id=run_id,
                scenario={
                    "name": "test_cross_database_exec",
                    "target_table": "[silver].[DimCurrency]",
                    "procedure": f"[silver].[{proc_name}]",
                    "given": [],
                },
            )

            assert result["status"] == "error"
            assert result["errors"] == [{
                "code": "REMOTE_EXEC_UNSUPPORTED",
                "message": (
                    "Sandbox cannot execute cross-database procedure call "
                    f"OtherDB.dbo.usp_Load from [silver].[{proc_name}]. "
                    "The sandbox only clones objects from the source database."
                ),
            }]
        finally:
            backend.sandbox_down(run_id=run_id)
            self._drop_temp_proc(backend, proc_name)

    def test_execute_linked_server_exec_returns_clear_error(self) -> None:
        backend = _make_backend()
        run_id = self._unique_run_id()
        proc_name = f"usp_remote_linked_server_{self._unique_run_id()}"

        self._create_temp_proc(backend, proc_name, "EXEC [LinkedServer].db.dbo.usp_Load;")

        try:
            up_result = backend.sandbox_up(run_id=run_id, schemas=["silver"])
            assert up_result["status"] in ("ok", "partial")

            result = backend.execute_scenario(
                run_id=run_id,
                scenario={
                    "name": "test_linked_server_exec",
                    "target_table": "[silver].[DimCurrency]",
                    "procedure": f"[silver].[{proc_name}]",
                    "given": [],
                },
            )

            assert result["status"] == "error"
            assert result["errors"] == [{
                "code": "REMOTE_EXEC_UNSUPPORTED",
                "message": (
                    "Sandbox cannot execute linked-server procedure call "
                    f"[LinkedServer].db.dbo.usp_Load from [silver].[{proc_name}]. "
                    "The sandbox only clones objects from the source database."
                ),
            }]
        finally:
            backend.sandbox_down(run_id=run_id)
            self._drop_temp_proc(backend, proc_name)
