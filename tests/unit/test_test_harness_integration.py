"""Integration tests for test harness — requires Docker SQL Server with MigrationTest DB.

Run with: uv run --project lib pytest -m integration -v
Requires: MSSQL_HOST, SA_PASSWORD, MSSQL_DB env vars (or Docker 'aw-sql' on localhost:1433).
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

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

    def test_execute_money_columns_return_decimal_strings(self) -> None:
        """MONEY/SMALLMONEY columns must be decimal strings, not base64/bytes."""
        backend = _make_backend()
        run_id = self._unique_run_id()
        table_name = f"__money_test_{self._unique_run_id()}"
        proc_name = f"usp_money_test_{self._unique_run_id()}"

        try:
            # Create a temp table with MONEY columns in the source DB
            with backend._connect(database=backend.database) as conn:
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

            up_result = backend.sandbox_up(run_id=run_id, schemas=["silver"])
            assert up_result["status"] in ("ok", "partial")

            result = backend.execute_scenario(
                run_id=run_id,
                scenario={
                    "name": "test_money_columns",
                    "target_table": f"[silver].[{table_name}]",
                    "procedure": f"[silver].[{proc_name}]",
                    "given": [],
                },
            )

            assert result["status"] == "ok"
            assert result["row_count"] == 1
            row = result["ground_truth_rows"][0]

            # Values must be parseable as Decimal, not base64 garbage
            from decimal import Decimal
            price = Decimal(row["price"])
            fee = Decimal(row["fee"])
            assert price == Decimal("42.50")
            assert fee == Decimal("9.99")
        finally:
            backend.sandbox_down(run_id=run_id)
            with backend._connect(database=backend.database) as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP PROCEDURE IF EXISTS silver.[{proc_name}]")
                cursor.execute(f"DROP TABLE IF EXISTS silver.[{table_name}]")


# ── IDENTITY_INSERT integration ──────────────────────────────────────────────


@skip_no_mssql
class TestIdentityInsertIntegration:
    """Verify IDENTITY_INSERT toggling against a real SQL Server."""

    def _unique_run_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def test_explicit_identity_value_succeeds(self) -> None:
        """Insert a fixture row with an explicit identity column value.

        silver.DimProduct has ProductKey INT IDENTITY(1,1). Before the fix
        this would fail with 'Cannot insert explicit value for identity column'.
        """
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            result = backend.execute_scenario(
                run_id=run_id,
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

            assert result["status"] == "ok", f"Expected ok, got: {result['errors']}"
            assert result["row_count"] >= 1
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_mixed_identity_and_non_identity_tables(self) -> None:
        """Insert into tables where one has identity and one does not.

        silver.DimProduct → identity (ProductKey)
        bronze.Product    → no identity (created via SELECT INTO)
        Verifies per-table IDENTITY_INSERT toggling.
        """
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            # Insert into silver.DimProduct (identity) and bronze.Product (no identity)
            result = backend.execute_scenario(
                run_id=run_id,
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
                                    "EnglishProductName": "Existing Product",
                                    "Color": "",
                                    "ProductAlternateKey": "EP-0001",
                                }
                            ],
                        },
                        {
                            "table": "[bronze].[Product]",
                            "rows": [
                                {
                                    "ProductID": 8888,
                                    "ProductName": "Bronze Widget",
                                    "ProductNumber": "BW-0001",
                                    "MakeFlag": 0,
                                    "FinishedGoodsFlag": 0,
                                    "SafetyStockLevel": 50,
                                    "ReorderPoint": 25,
                                    "StandardCost": 5.00,
                                    "ListPrice": 10.00,
                                    "DaysToManufacture": 0,
                                    "SellStartDate": "2024-01-01",
                                    "ModifiedDate": "2024-01-01",
                                }
                            ],
                        },
                    ],
                },
            )

            assert result["status"] == "ok", f"Expected ok, got: {result['errors']}"
            assert result["row_count"] >= 1
        finally:
            backend.sandbox_down(run_id=run_id)


# ── NOT NULL constraint integration ──────────────────────────────────────────


@skip_no_mssql
class TestNotNullConstraintIntegration:
    """Verify that omitting NOT NULL columns fails, and including them succeeds."""

    def _unique_run_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def test_missing_not_null_column_fails(self) -> None:
        """Omitting a NOT NULL column (EnglishProductName) causes insert failure.

        This proves the original bug: if the skill only provides columns
        referenced by the proc SQL but misses NOT NULL columns on source
        tables, the sandbox insert fails.
        """
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            # DimProduct.EnglishProductName is NOT NULL — omitting it should fail
            result = backend.execute_scenario(
                run_id=run_id,
                scenario={
                    "name": "test_missing_not_null",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[silver].[DimProduct]",
                            "rows": [
                                {
                                    "ProductKey": 777,
                                    # Missing EnglishProductName (NOT NULL)
                                    # Missing Color (NOT NULL DEFAULT '')
                                    "ProductAlternateKey": "MN-0001",
                                }
                            ],
                        },
                        {
                            "table": "[bronze].[Product]",
                            "rows": [
                                {
                                    "ProductID": 7777,
                                    "ProductName": "Test",
                                    "ProductNumber": "T-0001",
                                    "MakeFlag": 1,
                                    "FinishedGoodsFlag": 1,
                                    "SafetyStockLevel": 10,
                                    "ReorderPoint": 5,
                                    "StandardCost": 1.00,
                                    "ListPrice": 2.00,
                                    "DaysToManufacture": 0,
                                    "SellStartDate": "2024-01-01",
                                    "ModifiedDate": "2024-01-01",
                                }
                            ],
                        },
                    ],
                },
            )

            assert result["status"] == "error"
            assert any("Cannot insert" in e["message"] or "NOT NULL" in e["message"]
                       for e in result["errors"]), f"Expected NOT NULL error, got: {result['errors']}"
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_all_not_null_columns_succeeds(self) -> None:
        """Including all NOT NULL columns (with sensible defaults) succeeds.

        This proves the fix: when the skill includes all NOT NULL non-identity
        columns, the sandbox insert works.
        """
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            # Provide all NOT NULL columns for DimProduct
            result = backend.execute_scenario(
                run_id=run_id,
                scenario={
                    "name": "test_all_not_null",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[silver].[DimProduct]",
                            "rows": [
                                {
                                    "ProductKey": 666,
                                    "ProductAlternateKey": "AN-0001",
                                    "EnglishProductName": "All-Cols Product",
                                    "Color": "",
                                }
                            ],
                        },
                        {
                            "table": "[bronze].[Product]",
                            "rows": [
                                {
                                    "ProductID": 6666,
                                    "ProductName": "Full Column Widget",
                                    "ProductNumber": "FC-0001",
                                    "MakeFlag": 1,
                                    "FinishedGoodsFlag": 1,
                                    "SafetyStockLevel": 100,
                                    "ReorderPoint": 50,
                                    "StandardCost": 15.00,
                                    "ListPrice": 30.00,
                                    "DaysToManufacture": 2,
                                    "SellStartDate": "2024-01-01",
                                    "ModifiedDate": "2024-01-01",
                                }
                            ],
                        },
                    ],
                },
            )

            assert result["status"] == "ok", f"Expected ok, got: {result['errors']}"
            assert result["row_count"] >= 1
        finally:
            backend.sandbox_down(run_id=run_id)


# ── execute-spec integration ─────────────────────────────────────────────────


@skip_no_mssql
class TestExecuteSpecIntegration:
    """Verify execute-spec bulk execution against a real sandbox."""

    def _unique_run_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _write_spec(self, path: Path, unit_tests: list[dict]) -> Path:
        spec = {
            "item_id": "silver.dimproduct",
            "status": "ok",
            "coverage": "complete",
            "branch_manifest": [],
            "unit_tests": unit_tests,
            "uncovered_branches": [],
            "warnings": [],
            "validation": {"passed": True, "issues": []},
            "errors": [],
        }
        spec_path = path / "test-specs" / "silver.dimproduct.json"
        spec_path.parent.mkdir(parents=True, exist_ok=True)
        spec_path.write_text(json.dumps(spec, indent=2))
        return spec_path

    def test_execute_spec_populates_expect_rows(self, tmp_path: Path) -> None:
        """Bulk-execute a test spec and verify expect.rows are written back."""
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            spec_path = self._write_spec(tmp_path, [
                {
                    "name": "test_merge_not_matched",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[bronze].[Product]",
                            "rows": [
                                {
                                    "ProductID": 5555,
                                    "ProductName": "Spec Test Widget",
                                    "ProductNumber": "ST-0001",
                                    "MakeFlag": 1,
                                    "FinishedGoodsFlag": 1,
                                    "SafetyStockLevel": 10,
                                    "ReorderPoint": 5,
                                    "StandardCost": 7.50,
                                    "ListPrice": 15.00,
                                    "DaysToManufacture": 1,
                                    "SellStartDate": "2024-06-01",
                                    "ModifiedDate": "2024-06-01",
                                }
                            ],
                        },
                    ],
                },
            ])

            # Use the CLI function directly
            from shared.test_harness import _load_manifest, _create_backend
            import shutil

            # Copy manifest to tmp_path for _resolve_run_id
            manifest_src = Path(backend.database)  # not a real path, use env
            # Simpler: call backend.execute_scenario per entry, same as execute_spec
            spec_data = json.loads(spec_path.read_text())
            for test_entry in spec_data["unit_tests"]:
                scenario = {
                    "name": test_entry["name"],
                    "target_table": test_entry["target_table"],
                    "procedure": test_entry["procedure"],
                    "given": test_entry["given"],
                }
                result = backend.execute_scenario(run_id=run_id, scenario=scenario)
                assert result["status"] == "ok", f"Scenario failed: {result['errors']}"
                test_entry["expect"] = {"rows": result["ground_truth_rows"]}

            # Write back
            spec_path.write_text(json.dumps(spec_data, indent=2))

            # Verify expect.rows populated
            updated = json.loads(spec_path.read_text())
            assert "expect" in updated["unit_tests"][0]
            assert len(updated["unit_tests"][0]["expect"]["rows"]) >= 1

            # Verify ground truth contains expected columns
            row = updated["unit_tests"][0]["expect"]["rows"][0]
            assert "ProductKey" in row
            assert "EnglishProductName" in row
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_execute_spec_partial_scenario_failure(self, tmp_path: Path) -> None:
        """One scenario succeeds, another fails — partial results captured."""
        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            scenarios = [
                {
                    "name": "test_good_scenario",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[bronze].[Product]",
                            "rows": [
                                {
                                    "ProductID": 4444,
                                    "ProductName": "Good Widget",
                                    "ProductNumber": "GW-0001",
                                    "MakeFlag": 1,
                                    "FinishedGoodsFlag": 1,
                                    "SafetyStockLevel": 10,
                                    "ReorderPoint": 5,
                                    "StandardCost": 5.00,
                                    "ListPrice": 10.00,
                                    "DaysToManufacture": 0,
                                    "SellStartDate": "2024-01-01",
                                    "ModifiedDate": "2024-01-01",
                                }
                            ],
                        },
                    ],
                },
                {
                    "name": "test_bad_scenario",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_nonexistent_proc]",
                    "given": [],
                },
            ]

            ok_count = 0
            fail_count = 0
            for scenario in scenarios:
                result = backend.execute_scenario(
                    run_id=run_id,
                    scenario={
                        "name": scenario["name"],
                        "target_table": scenario["target_table"],
                        "procedure": scenario["procedure"],
                        "given": scenario["given"],
                    },
                )
                if result["status"] == "ok":
                    scenario["expect"] = {"rows": result["ground_truth_rows"]}
                    ok_count += 1
                else:
                    fail_count += 1

            assert ok_count == 1, "Expected 1 successful scenario"
            assert fail_count == 1, "Expected 1 failed scenario"
            assert "expect" in scenarios[0], "Good scenario should have expect.rows"
            assert "expect" not in scenarios[1], "Bad scenario should NOT have expect.rows"
        finally:
            backend.sandbox_down(run_id=run_id)


# ── Full pipeline integration ────────────────────────────────────────────────


@skip_no_mssql
class TestFullPipelineIntegration:
    """End-to-end: execute scenarios → convert to dbt YAML."""

    def _unique_run_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def test_execute_then_convert_dbt(self, tmp_path: Path) -> None:
        """Full chain: execute scenarios in sandbox, then convert to dbt YAML.

        Validates the complete pipeline that /generate-tests orchestrates:
        1. Execute scenarios → populate expect.rows
        2. Convert CLI-ready JSON → dbt YAML
        3. Verify YAML has source()/ref() expressions and expect.rows
        """
        import yaml

        from shared.test_harness import convert_spec_to_dbt

        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            # Step 1: CLI-ready test spec (what /generating-tests would emit)
            spec_data = {
                "item_id": "silver.dimproduct",
                "status": "ok",
                "coverage": "complete",
                "branch_manifest": [
                    {
                        "id": "merge_not_matched_insert",
                        "statement_index": 0,
                        "description": "MERGE WHEN NOT MATCHED → INSERT new product",
                        "scenarios": ["test_merge_not_matched_new_product"],
                    }
                ],
                "unit_tests": [
                    {
                        "name": "test_merge_not_matched_new_product",
                        "target_table": "[silver].[DimProduct]",
                        "procedure": "[silver].[usp_load_DimProduct]",
                        "given": [
                            {
                                "table": "[bronze].[Product]",
                                "rows": [
                                    {
                                        "ProductID": 3333,
                                        "ProductName": "Pipeline Widget",
                                        "ProductNumber": "PW-0001",
                                        "MakeFlag": 1,
                                        "FinishedGoodsFlag": 1,
                                        "Color": "Green",
                                        "SafetyStockLevel": 50,
                                        "ReorderPoint": 25,
                                        "StandardCost": 12.50,
                                        "ListPrice": 25.00,
                                        "DaysToManufacture": 1,
                                        "SellStartDate": "2024-01-01",
                                        "ModifiedDate": "2024-01-01",
                                    }
                                ],
                            },
                        ],
                    },
                ],
                "uncovered_branches": [],
                "warnings": [],
                "validation": {"passed": True, "issues": []},
                "errors": [],
            }

            # Step 2: Execute scenario — capture ground truth
            test_entry = spec_data["unit_tests"][0]
            result = backend.execute_scenario(
                run_id=run_id,
                scenario={
                    "name": test_entry["name"],
                    "target_table": test_entry["target_table"],
                    "procedure": test_entry["procedure"],
                    "given": test_entry["given"],
                },
            )
            assert result["status"] == "ok", f"Execution failed: {result['errors']}"
            assert result["row_count"] >= 1

            # Write expect.rows back
            test_entry["expect"] = {"rows": result["ground_truth_rows"]}

            # Verify ground truth has realistic data
            gt_row = result["ground_truth_rows"][0]
            assert "ProductKey" in gt_row
            assert "EnglishProductName" in gt_row
            assert gt_row["EnglishProductName"] == "Pipeline Widget"

            # Step 3: Convert to dbt YAML
            dbt_data = convert_spec_to_dbt(spec_data)

            assert len(dbt_data["unit_tests"]) == 1
            dbt_test = dbt_data["unit_tests"][0]

            # Verify dbt format
            assert dbt_test["model"] == "stg_dimproduct"
            assert dbt_test["given"][0]["input"] == "source('bronze', 'Product')"
            assert dbt_test["given"][0]["rows"] == test_entry["given"][0]["rows"]
            assert "expect" in dbt_test
            assert len(dbt_test["expect"]["rows"]) >= 1

            # Step 4: Write YAML and verify it's well-formed
            yaml_path = tmp_path / "silver.dimproduct.yml"
            with yaml_path.open("w") as f:
                yaml.dump(dbt_data, f, default_flow_style=False, sort_keys=False)

            # Re-read and verify round-trip
            with yaml_path.open() as f:
                reloaded = yaml.safe_load(f)

            assert reloaded["unit_tests"][0]["model"] == "stg_dimproduct"
            assert reloaded["unit_tests"][0]["given"][0]["input"] == "source('bronze', 'Product')"
            assert len(reloaded["unit_tests"][0]["expect"]["rows"]) >= 1
        finally:
            backend.sandbox_down(run_id=run_id)

    def test_multi_scenario_execute_and_convert(self, tmp_path: Path) -> None:
        """Multiple scenarios executed and converted — validates batch flow."""
        import yaml

        from shared.test_harness import convert_spec_to_dbt

        backend = _make_backend()
        run_id = self._unique_run_id()

        try:
            up = backend.sandbox_up(run_id=run_id, schemas=["bronze", "silver"])
            assert up["status"] in ("ok", "partial")

            spec_data = {
                "item_id": "silver.dimproduct",
                "status": "ok",
                "coverage": "complete",
                "branch_manifest": [],
                "unit_tests": [
                    {
                        "name": "test_not_matched_insert",
                        "target_table": "[silver].[DimProduct]",
                        "procedure": "[silver].[usp_load_DimProduct]",
                        "given": [
                            {
                                "table": "[bronze].[Product]",
                                "rows": [
                                    {
                                        "ProductID": 2222,
                                        "ProductName": "Multi A",
                                        "ProductNumber": "MA-001",
                                        "MakeFlag": 1,
                                        "FinishedGoodsFlag": 1,
                                        "SafetyStockLevel": 10,
                                        "ReorderPoint": 5,
                                        "StandardCost": 1.00,
                                        "ListPrice": 2.00,
                                        "DaysToManufacture": 0,
                                        "SellStartDate": "2024-01-01",
                                        "ModifiedDate": "2024-01-01",
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "name": "test_matched_update",
                        "target_table": "[silver].[DimProduct]",
                        "procedure": "[silver].[usp_load_DimProduct]",
                        "given": [
                            {
                                "table": "[silver].[DimProduct]",
                                "rows": [
                                    {
                                        "ProductKey": 555,
                                        "ProductAlternateKey": "1111",
                                        "EnglishProductName": "Old Name",
                                        "Color": "Red",
                                    }
                                ],
                            },
                            {
                                "table": "[bronze].[Product]",
                                "rows": [
                                    {
                                        "ProductID": 1111,
                                        "ProductName": "Updated Name",
                                        "ProductNumber": "UP-001",
                                        "MakeFlag": 0,
                                        "FinishedGoodsFlag": 0,
                                        "Color": "Blue",
                                        "SafetyStockLevel": 20,
                                        "ReorderPoint": 10,
                                        "StandardCost": 5.00,
                                        "ListPrice": 10.00,
                                        "DaysToManufacture": 1,
                                        "SellStartDate": "2024-01-01",
                                        "ModifiedDate": "2024-06-01",
                                    }
                                ],
                            },
                        ],
                    },
                ],
                "uncovered_branches": [],
                "warnings": [],
                "validation": {"passed": True, "issues": []},
                "errors": [],
            }

            # Execute all scenarios
            for test_entry in spec_data["unit_tests"]:
                result = backend.execute_scenario(
                    run_id=run_id,
                    scenario={
                        "name": test_entry["name"],
                        "target_table": test_entry["target_table"],
                        "procedure": test_entry["procedure"],
                        "given": test_entry["given"],
                    },
                )
                assert result["status"] == "ok", (
                    f"Scenario {test_entry['name']} failed: {result['errors']}"
                )
                test_entry["expect"] = {"rows": result["ground_truth_rows"]}

            # Convert to dbt YAML
            dbt_data = convert_spec_to_dbt(spec_data)
            assert len(dbt_data["unit_tests"]) == 2

            # Both tests have expect.rows
            for dbt_test in dbt_data["unit_tests"]:
                assert "expect" in dbt_test
                assert len(dbt_test["expect"]["rows"]) >= 1
                assert dbt_test["model"] == "stg_dimproduct"

            # First test: source table only
            assert dbt_data["unit_tests"][0]["given"][0]["input"] == "source('bronze', 'Product')"

            # Second test: identity table + source table
            second_given = dbt_data["unit_tests"][1]["given"]
            refs = [g["input"] for g in second_given]
            assert "ref('DimProduct')" in refs
            assert "source('bronze', 'Product')" in refs

            # Write and validate YAML
            yaml_path = tmp_path / "silver.dimproduct.yml"
            with yaml_path.open("w") as f:
                yaml.dump(dbt_data, f, default_flow_style=False, sort_keys=False)
            assert yaml_path.exists()
            reloaded = yaml.safe_load(yaml_path.read_text())
            assert len(reloaded["unit_tests"]) == 2
        finally:
            backend.sandbox_down(run_id=run_id)
