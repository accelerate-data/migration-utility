"""Integration tests for test harness against the canonical SQL Server fixture.

Run with: uv run --project lib pytest -m integration -v
Requires: MSSQL_HOST and SA_PASSWORD env vars (or Docker 'sql-test' on localhost:1433).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

pyodbc = pytest.importorskip("pyodbc", reason="pyodbc not installed — skipping integration tests")

from shared.sandbox.sql_server import SqlServerSandbox
from tests.integration.runtime_helpers import (
    build_sql_server_sandbox_manifest,
    ensure_sql_server_migration_test_materialized,
    sql_server_is_available,
)

pytestmark = pytest.mark.integration


def _have_mssql_env() -> bool:
    return sql_server_is_available(pyodbc)


def _make_backend() -> SqlServerSandbox:
    ensure_sql_server_migration_test_materialized()
    return SqlServerSandbox.from_env(build_sql_server_sandbox_manifest())


skip_no_mssql = pytest.mark.skipif(
    not _have_mssql_env(),
    reason="MSSQL integration DB not reachable (MSSQL_HOST, SA_PASSWORD and a listening server required)",
)


@skip_no_mssql
class TestSandboxLifecycle:
    """Full sandbox create → verify → teardown against a real SQL Server."""

    def test_sandbox_up_creates_and_clones(self) -> None:
        backend = _make_backend()

        try:
            result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = result.sandbox_database

            assert result.status in ("ok", "partial")
            assert sandbox_db.startswith("__test_")
            assert len(result.tables_cloned) > 0
            assert any("DimCurrency" in t for t in result.tables_cloned)
            assert len(result.procedures_cloned) > 0
            assert any("usp_load_DimCurrency" in p for p in result.procedures_cloned)

            # Verify the sandbox DB actually exists
            with backend._connect(database=sandbox_db) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_TYPE = 'BASE TABLE'"
                )
                table_count = cursor.fetchone()[0]
                assert table_count > 0
        finally:
            backend.sandbox_down(sandbox_db=result.sandbox_database)

    def test_sandbox_down_drops_database(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=["silver"])
        sandbox_db = result.sandbox_database
        down_result = backend.sandbox_down(sandbox_db=sandbox_db)

        assert down_result.status == "ok"

        # Verify the DB is gone
        with backend._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DB_ID(?)", sandbox_db)
            assert cursor.fetchone()[0] is None

    def test_sandbox_down_idempotent(self) -> None:
        backend = _make_backend()

        # Down on a non-existent sandbox should succeed
        result = backend.sandbox_down(sandbox_db="__test_nonexistent99")
        assert result.status == "ok"

    def test_sandbox_up_multiple_schemas(self) -> None:
        backend = _make_backend()

        try:
            result = backend.sandbox_up(schemas=["bronze", "silver"])
            sandbox_db = result.sandbox_database

            assert result.status in ("ok", "partial")
            schemas_seen = {t.split(".")[0] for t in result.tables_cloned}
            assert "bronze" in schemas_seen
            assert "silver" in schemas_seen
        finally:
            backend.sandbox_down(sandbox_db=result.sandbox_database)

    def test_sandbox_up_idempotent(self) -> None:
        """Calling sandbox_up twice creates two different databases."""
        backend = _make_backend()

        try:
            result1 = backend.sandbox_up(schemas=["silver"])
            result2 = backend.sandbox_up(schemas=["silver"])

            assert result1.status in ("ok", "partial")
            assert result2.status in ("ok", "partial")
            # Each call generates a new database name
            assert result1.sandbox_database != result2.sandbox_database
            assert result1.tables_cloned == result2.tables_cloned
        finally:
            backend.sandbox_down(sandbox_db=result1.sandbox_database)
            backend.sandbox_down(sandbox_db=result2.sandbox_database)


@skip_no_mssql
class TestExecuteScenario:
    """Execute a real scenario against a sandbox database."""

    def _create_temp_proc(self, backend: SqlServerSandbox, proc_name: str, body: str) -> None:
        with backend._connect_source(database=backend.source_database) as conn:
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
        with backend._connect_source(database=backend.source_database) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP PROCEDURE IF EXISTS [silver].[{proc_name}]")

    def test_execute_inserts_and_captures_ground_truth(self) -> None:
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["bronze", "silver"])
            sandbox_db = up_result.sandbox_database
            assert up_result.status in ("ok", "partial")
            assert any("DimCurrency" in t for t in up_result.tables_cloned)

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

            result = backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario)

            assert result.status == "ok"
            assert result.scenario_name == "test_load_dim_currency"
            assert result.row_count >= 1
            assert isinstance(result.ground_truth_rows, list)
            assert len(result.ground_truth_rows) >= 1
            assert result.errors == []
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_rolls_back_fixture_data(self) -> None:
        """Verify fixture data is rolled back after scenario execution."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["bronze", "silver"])
            sandbox_db = up_result.sandbox_database

            scenario = {
                "name": "test_rollback",
                "target_table": "[silver].[DimCurrency]",
                "procedure": "[silver].[usp_load_DimCurrency]",
                "given": [
                    {
                        "table": "[bronze].[Currency]",
                        "rows": [
                            {"CurrencyCode": "RBK", "CurrencyName": "Rollback Test"},
                        ],
                    },
                ],
            }

            backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario)

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
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_empty_fixtures(self) -> None:
        """Scenario with no fixture rows still runs the procedure."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["bronze", "silver"])
            sandbox_db = up_result.sandbox_database

            scenario = {
                "name": "test_empty_fixtures",
                "target_table": "[silver].[DimCurrency]",
                "procedure": "[silver].[usp_load_DimCurrency]",
                "given": [
                    {"table": "[bronze].[Currency]", "rows": []},
                ],
            }

            result = backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario)

            assert result.status == "ok"
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_cross_database_exec_returns_clear_error(self) -> None:
        backend = _make_backend()
        proc_name = f"usp_remote_cross_db_{uuid.uuid4().hex[:12]}"

        self._create_temp_proc(backend, proc_name, "EXEC OtherDB.dbo.usp_Load;")

        try:
            up_result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = up_result.sandbox_database
            assert up_result.status in ("ok", "partial")

            result = backend.execute_scenario(
                sandbox_db=sandbox_db,
                scenario={
                    "name": "test_cross_database_exec",
                    "target_table": "[silver].[DimCurrency]",
                    "procedure": f"[silver].[{proc_name}]",
                    "given": [],
                },
            )

            assert result.status == "error"
            assert len(result.errors) == 1
            assert result.errors[0].code == "REMOTE_EXEC_UNSUPPORTED"
            assert "cross-database procedure call" in result.errors[0].message
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            self._drop_temp_proc(backend, proc_name)

    def test_execute_linked_server_exec_returns_clear_error(self) -> None:
        backend = _make_backend()
        proc_name = f"usp_remote_linked_server_{uuid.uuid4().hex[:12]}"

        self._create_temp_proc(backend, proc_name, "EXEC [LinkedServer].db.dbo.usp_Load;")

        try:
            up_result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = up_result.sandbox_database
            assert up_result.status in ("ok", "partial")

            result = backend.execute_scenario(
                sandbox_db=sandbox_db,
                scenario={
                    "name": "test_linked_server_exec",
                    "target_table": "[silver].[DimCurrency]",
                    "procedure": f"[silver].[{proc_name}]",
                    "given": [],
                },
            )

            assert result.status == "error"
            assert len(result.errors) == 1
            assert result.errors[0].code == "REMOTE_EXEC_UNSUPPORTED"
            assert "linked-server procedure call" in result.errors[0].message
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            self._drop_temp_proc(backend, proc_name)

    def test_execute_money_columns_return_decimal_strings(self) -> None:
        """MONEY/SMALLMONEY columns must be decimal strings, not base64/bytes."""
        backend = _make_backend()
        table_name = f"__money_test_{uuid.uuid4().hex[:12]}"
        proc_name = f"usp_money_test_{uuid.uuid4().hex[:12]}"

        try:
            # Create a temp table with MONEY columns in the source DB
            with backend._connect_source(database=backend.source_database) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"CREATE TABLE silver.[{table_name}] ("
                    "  id INT PRIMARY KEY,"
                    "  price MONEY,"
                    "  fee SMALLMONEY"
                    ")"
                )
                cursor.execute(
                    f"CREATE PROCEDURE silver.[{proc_name}] AS BEGIN "
                    f"  INSERT INTO silver.[{table_name}] (id, price, fee) "
                    "  VALUES (1, 42.50, 9.99) "
                    "END"
                )

            up_result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = up_result.sandbox_database
            assert up_result.status in ("ok", "partial")

            result = backend.execute_scenario(
                sandbox_db=sandbox_db,
                scenario={
                    "name": "test_money_columns",
                    "target_table": f"[silver].[{table_name}]",
                    "procedure": f"[silver].[{proc_name}]",
                    "given": [],
                },
            )

            assert result.status == "ok"
            assert result.row_count == 1
            row = result.ground_truth_rows[0]

            # Values must be parseable as Decimal, not base64 garbage
            from decimal import Decimal
            price = Decimal(row["price"])
            fee = Decimal(row["fee"])
            assert price == Decimal("42.50")
            assert fee == Decimal("9.99")
        finally:
            if up_result:
                backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            with backend._connect_source(database=backend.source_database) as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP PROCEDURE IF EXISTS silver.[{proc_name}]")
                cursor.execute(f"DROP TABLE IF EXISTS silver.[{table_name}]")


# ── IDENTITY_INSERT integration ──────────────────────────────────────────────


@skip_no_mssql
class TestIdentityInsertIntegration:
    """Verify IDENTITY_INSERT toggling against a real SQL Server."""

    def test_explicit_identity_value_succeeds(self) -> None:
        """Insert a fixture row with an explicit identity column value.

        silver.DimProduct has ProductKey INT IDENTITY(1,1). Before the fix
        this would fail with 'Cannot insert explicit value for identity column'.
        """
        backend = _make_backend()

        try:
            up = backend.sandbox_up(schemas=["bronze", "silver"])
            sandbox_db = up.sandbox_database
            assert up.status in ("ok", "partial")

            result = backend.execute_scenario(
                sandbox_db=sandbox_db,
                scenario={
                    "name": "test_identity_explicit",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[silver].[DimProduct]",
                            "rows": [
                                {
                                    "ProductKey": 999,
                                    "EnglishProductName": "Identity Test",
                                    "Color": "Red",
                                }
                            ],
                        },
                        {
                            "table": "[bronze].[Product]",
                            "rows": [
                                {
                                    "ProductID": 9999,
                                    "ProductName": "New Widget",
                                    "ProductNumber": "NW-0001",
                                    "MakeFlag": 1,
                                    "FinishedGoodsFlag": 1,
                                    "Color": "Blue",
                                    "SafetyStockLevel": 100,
                                    "ReorderPoint": 50,
                                    "StandardCost": 10.00,
                                    "ListPrice": 20.00,
                                    "DaysToManufacture": 1,
                                    "SellStartDate": "2024-01-01",
                                    "ModifiedDate": "2024-01-01",
                                }
                            ],
                        },
                    ],
                },
            )

            assert result.status == "ok", f"Expected ok, got: {result.errors}"
            assert result.row_count >= 1
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_mixed_identity_and_non_identity_tables(self) -> None:
        """Insert into tables where one has identity and one does not.

        silver.DimProduct → identity (ProductKey)
        bronze.Currency → no identity column
        """
        backend = _make_backend()

        try:
            up = backend.sandbox_up(schemas=["bronze", "silver"])
            sandbox_db = up.sandbox_database
            assert up.status in ("ok", "partial")

            result = backend.execute_scenario(
                sandbox_db=sandbox_db,
                scenario={
                    "name": "test_mixed_identity",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[silver].[DimProduct]",
                            "rows": [
                                {
                                    "ProductKey": 888,
                                    "EnglishProductName": "Mixed Test",
                                    "Color": "Green",
                                }
                            ],
                        },
                        {
                            "table": "[bronze].[Currency]",
                            "rows": [
                                {
                                    "CurrencyCode": "MXD",
                                    "CurrencyName": "Mixed Currency",
                                    "ModifiedDate": "2024-01-01",
                                }
                            ],
                        },
                        {
                            "table": "[bronze].[Product]",
                            "rows": [
                                {
                                    "ProductID": 8888,
                                    "ProductName": "Mixed Widget",
                                    "ProductNumber": "MW-0001",
                                    "MakeFlag": 1,
                                    "FinishedGoodsFlag": 1,
                                    "Color": "Green",
                                    "SafetyStockLevel": 50,
                                    "ReorderPoint": 25,
                                    "StandardCost": 5.00,
                                    "ListPrice": 10.00,
                                    "DaysToManufacture": 1,
                                    "SellStartDate": "2024-01-01",
                                    "ModifiedDate": "2024-01-01",
                                }
                            ],
                        },
                    ],
                },
            )

            assert result.status == "ok", f"Expected ok, got: {result.errors}"
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)


@skip_no_mssql
class TestEnsureViewTablesIntegration:
    """Verify that view-sourced fixtures are materialised end-to-end in a real SQL Server sandbox."""

    def _create_view(self, backend: SqlServerSandbox, view_name: str) -> None:
        with backend._connect_source(database=backend.source_database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"CREATE OR ALTER VIEW [silver].[{view_name}] "
                f"AS SELECT CurrencyCode, CurrencyName FROM [bronze].[Currency]"
            )

    def _drop_view(self, backend: SqlServerSandbox, view_name: str) -> None:
        with backend._connect_source(database=backend.source_database) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP VIEW IF EXISTS [silver].[{view_name}]")

    def _create_proc(
        self, backend: SqlServerSandbox, proc_name: str, view_name: str
    ) -> None:
        with backend._connect_source(database=backend.source_database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                CREATE OR ALTER PROCEDURE [silver].[{proc_name}]
                AS
                BEGIN
                    -- Reads from view fixture; ground truth is whatever remains in target
                    DECLARE @cnt INT;
                    SELECT @cnt = COUNT(*) FROM [silver].[{view_name}];
                END
                """
            )

    def _drop_proc(self, backend: SqlServerSandbox, proc_name: str) -> None:
        with backend._connect_source(database=backend.source_database) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP PROCEDURE IF EXISTS [silver].[{proc_name}]")

    def test_view_fixture_executes_without_error(self) -> None:
        """execute_scenario succeeds when a fixture source is a view in the source DB."""
        backend = _make_backend()
        view_name = f"vw_test_{uuid.uuid4().hex[:10]}"
        proc_name = f"usp_fromview_{uuid.uuid4().hex[:10]}"

        self._create_view(backend, view_name)
        self._create_proc(backend, proc_name, view_name)

        up_result: dict = {}
        try:
            up_result = backend.sandbox_up(schemas=["bronze", "silver"])
            sandbox_db = up_result.sandbox_database
            assert up_result.status in ("ok", "partial")

            scenario = {
                "name": "test_view_fixture",
                "target_table": "[silver].[DimCurrency]",
                "procedure": f"[silver].[{proc_name}]",
                "given": [
                    {
                        "table": f"[silver].[{view_name}]",
                        "rows": [
                            {"CurrencyCode": "VWT", "CurrencyName": "View Test"},
                        ],
                    }
                ],
            }

            result = backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario)

            assert result.status == "ok", result.errors
            assert result.errors == []
        finally:
            if up_result is not None:
                backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            self._drop_proc(backend, proc_name)
            self._drop_view(backend, view_name)


@skip_no_mssql
class TestExecuteSelectIntegration:
    """execute_select against a real SQL Server sandbox."""

    def test_execute_select_returns_fixture_rows(self) -> None:
        """execute_select seeds fixtures, runs SELECT, returns correct rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = up_result.sandbox_database
            assert up_result.status in ("ok", "partial")

            fixtures = [
                {
                    "table": "[silver].[DimCurrency]",
                    "rows": [
                        {"CurrencyAlternateKey": "USD", "CurrencyName": "US Dollar"},
                        {"CurrencyAlternateKey": "EUR", "CurrencyName": "Euro"},
                    ],
                },
            ]
            sql = (
                "SELECT CurrencyAlternateKey, CurrencyName "
                "FROM [silver].[DimCurrency] "
                "ORDER BY CurrencyAlternateKey"
            )

            result = backend.execute_select(
                sandbox_db=sandbox_db, sql=sql, fixtures=fixtures,
            )

            assert result.status == "ok", result.errors
            assert result.row_count == 2
            assert result.errors == []
            rows = result.ground_truth_rows
            codes = {r["CurrencyAlternateKey"] for r in rows}
            assert codes == {"USD", "EUR"}
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_compare_two_sql_returns_equivalent_for_same_result_set(self) -> None:
        """compare_two_sql reports equivalent when both SQLs return the same rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = up_result.sandbox_database
            fixtures = [
                {
                    "table": "[silver].[DimCurrency]",
                    "rows": [
                        {"CurrencyAlternateKey": "USD", "CurrencyName": "US Dollar"},
                        {"CurrencyAlternateKey": "EUR", "CurrencyName": "Euro"},
                    ],
                },
            ]
            sql_a = (
                "SELECT CurrencyAlternateKey, CurrencyName "
                "FROM [silver].[DimCurrency]"
            )
            sql_b = (
                "WITH src AS ("
                "  SELECT CurrencyAlternateKey, CurrencyName "
                "  FROM [silver].[DimCurrency]"
                ") "
                "SELECT CurrencyAlternateKey, CurrencyName FROM src"
            )

            result = backend.compare_two_sql(
                sandbox_db=sandbox_db,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )

            assert result["status"] == "ok", result["errors"]
            assert result["equivalent"] is True
            assert result["a_minus_b"] == []
            assert result["b_minus_a"] == []
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_select_empty_fixtures(self) -> None:
        """execute_select with no fixture rows returns 0 rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = up_result.sandbox_database

            result = backend.execute_select(
                sandbox_db=sandbox_db,
                sql="SELECT CurrencyAlternateKey FROM [silver].[DimCurrency]",
                fixtures=[],
            )

            assert result.status == "ok"
            assert result.row_count == 0
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_select_rolls_back_fixtures(self) -> None:
        """Fixture rows are rolled back after execute_select."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["silver"])
            sandbox_db = up_result.sandbox_database

            fixtures = [
                {
                    "table": "[silver].[DimCurrency]",
                    "rows": [
                        {"CurrencyAlternateKey": "ZZZ", "CurrencyName": "Rollback Test"},
                    ],
                },
            ]
            backend.execute_select(
                sandbox_db=sandbox_db,
                sql="SELECT CurrencyAlternateKey FROM [silver].[DimCurrency]",
                fixtures=fixtures,
            )

            # Verify fixture row was rolled back
            with backend._connect(database=sandbox_db) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM [silver].[DimCurrency] "
                    "WHERE CurrencyAlternateKey = 'ZZZ'"
                )
                assert cursor.fetchone()[0] == 0, "Fixture row should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
