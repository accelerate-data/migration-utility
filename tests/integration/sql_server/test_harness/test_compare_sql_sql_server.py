"""Integration tests for compare_two_sql against the schema-level MigrationTest contract on SQL Server.

Tests the full compare_two_sql workflow: seed fixtures, run two SELECTs, symmetric diff.
Covers DML extraction patterns (INSERT, MERGE, UPDATE, DELETE), identity columns,
FK constraints, NULL handling, MONEY types, and transaction rollback.

Run with: cd lib && uv run pytest ../tests/integration/sql_server/test_harness -v -k compare_sql
Requires: MSSQL_HOST, SA_PASSWORD, MSSQL_DB env vars (or Docker 'sql-test' on localhost:1433).
"""

from __future__ import annotations
import os
from pathlib import Path
from typing import Any

import pytest

pyodbc = pytest.importorskip("pyodbc", reason="pyodbc not installed — skipping integration tests")

from shared.fixture_materialization import materialize_migration_test
from shared.sandbox.sql_server import SqlServerSandbox
from shared.runtime_config_models import RuntimeConnection, RuntimeRole
from tests.helpers import REPO_ROOT, SQL_SERVER_FIXTURE_DATABASE, SQL_SERVER_FIXTURE_SCHEMA
from tests.integration.runtime_helpers import (
    sql_server_is_available,
)

pytestmark = pytest.mark.integration

_SQL_SERVER_FIXTURE_READY = False

BRONZE_CURRENCY = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_currency]"
BRONZE_PRODUCT = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_product]"
BRONZE_PROMOTION = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_promotion]"
BRONZE_CUSTOMER = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_customer]"
BRONZE_PERSON = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_person]"
BRONZE_SALESORDERHEADER = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_salesorderheader]"
BRONZE_SALESORDERDETAIL = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_salesorderdetail]"
BRONZE_SALESTERRITORY = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[bronze_salesterritory]"
SILVER_DIMCURRENCY = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[silver_dimcurrency]"
SILVER_DIMPRODUCT = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[silver_dimproduct]"
SILVER_CONFIG = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[silver_config]"
SILVER_USP_LOAD_DIMPRODUCT = f"[{SQL_SERVER_FIXTURE_SCHEMA}].[silver_usp_load_dimproduct]"


def _have_mssql_env() -> bool:
    return sql_server_is_available(pyodbc)


def _ensure_sql_server_fixture_materialized() -> None:
    global _SQL_SERVER_FIXTURE_READY
    if _SQL_SERVER_FIXTURE_READY:
        return

    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ.get("MSSQL_HOST", "localhost"),
            port=os.environ.get("MSSQL_PORT", "1433"),
            database=SQL_SERVER_FIXTURE_DATABASE,
            schema=SQL_SERVER_FIXTURE_SCHEMA,
            user=os.environ.get("MSSQL_USER", "sa"),
            driver=os.environ.get("MSSQL_DRIVER", "FreeTDS"),
            password_env="SA_PASSWORD",
        ),
    )
    result = materialize_migration_test(role, REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(
            "SQL Server MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _SQL_SERVER_FIXTURE_READY = True


def _make_backend() -> SqlServerSandbox:
    _ensure_sql_server_fixture_materialized()
    return SqlServerSandbox.from_env({
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("MSSQL_HOST", "localhost"),
                    "port": os.environ.get("MSSQL_PORT", "1433"),
                    "database": SQL_SERVER_FIXTURE_DATABASE,
                    "user": os.environ.get("MSSQL_USER", "sa"),
                    "driver": os.environ.get("MSSQL_DRIVER", "FreeTDS"),
                    "password_env": "SA_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("MSSQL_HOST", "localhost"),
                    "port": os.environ.get("MSSQL_PORT", "1433"),
                    "user": os.environ.get("MSSQL_USER", "sa"),
                    "driver": os.environ.get("MSSQL_DRIVER", "FreeTDS"),
                    "password_env": "SA_PASSWORD",
                },
            },
        }
    })


skip_no_mssql = pytest.mark.skipif(
    not _have_mssql_env(),
    reason="MSSQL integration DB not reachable (MSSQL_HOST, SA_PASSWORD and a listening server required)",
)


# ── Shared fixture data ──────────────────────────────────────────────────────
# The sandbox clones table structure but NOT data. All tests that query
# bronze/silver tables must seed fixture rows.

_CURRENCY_FIXTURES: list[dict[str, Any]] = [
    {
        "table": BRONZE_CURRENCY,
        "rows": [
            {"CurrencyCode": "USD", "CurrencyName": "US Dollar"},
            {"CurrencyCode": "EUR", "CurrencyName": "Euro"},
            {"CurrencyCode": "GBP", "CurrencyName": "British Pound"},
            {"CurrencyCode": "JPY", "CurrencyName": "Japanese Yen"},
        ],
    },
]

_PRODUCT_FIXTURES: list[dict[str, Any]] = [
    {
        "table": BRONZE_PRODUCT,
        "rows": [
            {"ProductID": 1, "ProductName": "Widget A", "Color": "Red", "StandardCost": 10.00, "ListPrice": 20.00, "SellStartDate": "2024-01-01"},
            {"ProductID": 2, "ProductName": "Widget B", "Color": "Blue", "StandardCost": 15.00, "ListPrice": 30.00, "SellStartDate": "2024-01-01"},
            {"ProductID": 3, "ProductName": "Widget C", "Color": None, "StandardCost": 5.00, "ListPrice": 10.00, "SellStartDate": "2023-01-01"},
        ],
    },
]

_PROMOTION_FIXTURES: list[dict[str, Any]] = [
    {
        "table": BRONZE_PROMOTION,
        "rows": [
            {"PromotionID": 1, "Description": "Summer Sale", "DiscountPct": 0.10, "PromotionType": "Discount", "PromotionCategory": "Seasonal", "StartDate": "2024-06-01"},
            {"PromotionID": 2, "Description": "No Discount", "DiscountPct": 0.0, "PromotionType": "None", "PromotionCategory": "None", "StartDate": "2024-01-01"},
        ],
    },
]

_CUSTOMER_PERSON_FIXTURES: list[dict[str, Any]] = [
    {
        "table": BRONZE_CUSTOMER,
        "rows": [
            {"CustomerID": 1, "PersonID": 10, "TerritoryID": 1},
            {"CustomerID": 2, "PersonID": 20, "TerritoryID": 2},
        ],
    },
    {
        "table": BRONZE_PERSON,
        "rows": [
            {"BusinessEntityID": 10, "FirstName": "Alice", "LastName": "Smith", "EmailPromotion": 1},
            {"BusinessEntityID": 20, "FirstName": "Bob", "LastName": "Jones", "EmailPromotion": 0},
        ],
    },
    {
        "table": BRONZE_SALESORDERHEADER,
        "rows": [
            {"SalesOrderID": 100, "SalesOrderNumber": "SO100", "CustomerID": 1, "OrderDate": "2024-03-15"},
        ],
    },
]

_DIM_PRODUCT_FIXTURES: list[dict[str, Any]] = [
    {
        "table": SILVER_DIMPRODUCT,
        "rows": [
            {"ProductKey": 1, "ProductAlternateKey": "1", "EnglishProductName": "Widget A", "StandardCost": 10.00, "ListPrice": 20.00, "Color": "Red"},
            {"ProductKey": 2, "ProductAlternateKey": "2", "EnglishProductName": "Widget B", "StandardCost": 15.00, "ListPrice": 30.00, "Color": "Blue"},
            {"ProductKey": 3, "ProductAlternateKey": "3", "EnglishProductName": "Widget C", "StandardCost": 5.00, "ListPrice": 10.00, "Color": ""},
        ],
    },
]

_SALES_ORDER_FIXTURES: list[dict[str, Any]] = [
    {
        "table": BRONZE_SALESORDERHEADER,
        "rows": [
            {"SalesOrderID": 1, "SalesOrderNumber": "SO001", "CustomerID": 1, "TerritoryID": 1, "OrderDate": "2024-03-15", "TaxAmt": 40.00, "Freight": 10.00},
        ],
    },
    {
        "table": BRONZE_SALESORDERDETAIL,
        "rows": [
            {"SalesOrderID": 1, "SalesOrderDetailID": 1, "OrderQty": 2, "ProductID": 1, "UnitPrice": 20.00, "LineTotal": 40.00},
            {"SalesOrderID": 1, "SalesOrderDetailID": 2, "OrderQty": 3, "ProductID": 2, "UnitPrice": 30.00, "LineTotal": 90.00},
        ],
    },
]

_TERRITORY_FIXTURES: list[dict[str, Any]] = [
    {
        "table": BRONZE_SALESTERRITORY,
        "rows": [
            {"TerritoryID": 1, "TerritoryName": "Northwest", "CountryRegionCode": "US", "TerritoryGroup": "North America", "SalesYTD": 1000000.00},
            {"TerritoryID": 2, "TerritoryName": "Northeast", "CountryRegionCode": "US", "TerritoryGroup": "North America", "SalesYTD": 2000000.00},
            {"TerritoryID": 3, "TerritoryName": "Central", "CountryRegionCode": "US", "TerritoryGroup": "North America", "SalesYTD": 1500000.00},
        ],
    },
]


@skip_no_mssql
class TestCompareTwoSqlEquivalent:
    """Scenarios where both SELECTs should produce identical results."""

    def _run(
        self, sql_a: str, sql_b: str, fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            return backend.compare_two_sql(
                sandbox_db=up.sandbox_database,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_insert_select_extraction(self) -> None:
        """INSERT...SELECT: extracted SELECT == CTE refactored SELECT."""
        # Mirrors MigrationTest.silver_usp_load_dimpromotion.
        extracted = (
            "SELECT p.PromotionID, p.Description, p.DiscountPct, "
            "p.PromotionType, p.PromotionCategory, p.StartDate, p.EndDate, p.MinQty, p.MaxQty "
            f"FROM {BRONZE_PROMOTION} p"
        )
        refactored = (
            "WITH source_promotion AS ("
            f"    SELECT * FROM {BRONZE_PROMOTION}"
            ") "
            "SELECT PromotionID, Description, DiscountPct, "
            "PromotionType, PromotionCategory, StartDate, EndDate, MinQty, MaxQty "
            "FROM source_promotion"
        )
        result = self._run(extracted, refactored, _PROMOTION_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_merge_using_extraction(self) -> None:
        """MERGE: extracted USING clause == CTE refactored SELECT."""
        # Mirrors MigrationTest.silver_usp_load_dimproduct's USING clause.
        extracted = (
            "SELECT "
            "    CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, "
            "    ProductName AS EnglishProductName, "
            "    StandardCost, ListPrice, "
            "    ISNULL(Color, '') AS Color, "
            "    Size, ProductLine, Class, Style, "
            "    SellStartDate AS StartDate, "
            "    SellEndDate AS EndDate, "
            "    CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete' "
            "         WHEN SellEndDate IS NOT NULL THEN 'Outdated' "
            "         ELSE 'Current' END AS Status "
            f"FROM {BRONZE_PRODUCT}"
        )
        refactored = (
            "WITH source_product AS ("
            f"    SELECT * FROM {BRONZE_PRODUCT}"
            "), "
            "transformed_product AS ("
            "    SELECT "
            "        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, "
            "        ProductName AS EnglishProductName, "
            "        StandardCost, ListPrice, "
            "        ISNULL(Color, '') AS Color, "
            "        Size, ProductLine, Class, Style, "
            "        SellStartDate AS StartDate, "
            "        SellEndDate AS EndDate, "
            "        CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete' "
            "             WHEN SellEndDate IS NOT NULL THEN 'Outdated' "
            "             ELSE 'Current' END AS Status "
            "    FROM source_product"
            ") "
            "SELECT * FROM transformed_product"
        )
        result = self._run(extracted, refactored, _PRODUCT_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_join_with_outer_apply(self) -> None:
        """Multi-join with OUTER APPLY: both sides produce same result."""
        # Mirrors MigrationTest.silver_usp_load_dimcustomer_full.
        sql = (
            "SELECT "
            "    CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey, "
            "    p.FirstName, p.MiddleName, p.LastName, p.Title, "
            "    NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion, "
            "    CAST(h.MinOrderDate AS DATE) AS DateFirstPurchase "
            f"FROM {BRONZE_CUSTOMER} c "
            f"JOIN {BRONZE_PERSON} p ON c.PersonID = p.BusinessEntityID "
            "OUTER APPLY ("
            "    SELECT MIN(OrderDate) AS MinOrderDate "
            f"    FROM {BRONZE_SALESORDERHEADER} sh "
            "    WHERE sh.CustomerID = c.CustomerID"
            ") h"
        )
        refactored = (
            f"WITH source_customer AS (SELECT * FROM {BRONZE_CUSTOMER}), "
            f"source_person AS (SELECT * FROM {BRONZE_PERSON}), "
            f"source_orders AS (SELECT * FROM {BRONZE_SALESORDERHEADER}), "
            "customer_first_purchase AS ("
            "    SELECT CustomerID, MIN(OrderDate) AS MinOrderDate "
            "    FROM source_orders GROUP BY CustomerID"
            "), "
            "final AS ("
            "    SELECT "
            "        CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey, "
            "        p.FirstName, p.MiddleName, p.LastName, p.Title, "
            "        NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion, "
            "        CAST(fp.MinOrderDate AS DATE) AS DateFirstPurchase "
            "    FROM source_customer c "
            "    JOIN source_person p ON c.PersonID = p.BusinessEntityID "
            "    LEFT JOIN customer_first_purchase fp ON fp.CustomerID = c.CustomerID"
            ") "
            "SELECT * FROM final"
        )
        result = self._run(sql, refactored, _CUSTOMER_PERSON_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_union_all(self) -> None:
        """UNION ALL: extracted vs CTE version of dbo.usp_UnionAll."""
        extracted = (
            "SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, "
            "ProductName AS EnglishProductName, ISNULL(Color, '') AS Color "
            f"FROM {BRONZE_PRODUCT} WHERE ProductID <= 250 "
            "UNION ALL "
            "SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '') "
            f"FROM {BRONZE_PRODUCT} WHERE ProductID > 250"
        )
        refactored = (
            "WITH low_ids AS ("
            "    SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, "
            "        ProductName AS EnglishProductName, ISNULL(Color, '') AS Color "
            f"    FROM {BRONZE_PRODUCT} WHERE ProductID <= 250"
            "), "
            "high_ids AS ("
            "    SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, "
            "        ProductName AS EnglishProductName, ISNULL(Color, '') AS Color "
            f"    FROM {BRONZE_PRODUCT} WHERE ProductID > 250"
            "), "
            "final AS ("
            "    SELECT * FROM low_ids UNION ALL SELECT * FROM high_ids"
            ") "
            "SELECT * FROM final"
        )
        result = self._run(extracted, refactored, _PRODUCT_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_update_to_case_extraction(self) -> None:
        """UPDATE SET: extracted as CASE expression matches CTE version."""
        # Mirrors dbo.usp_UpdateWithCTE pattern
        extracted = (
            "SELECT d.ProductAlternateKey, "
            "    CASE WHEN l.ProductName IS NOT NULL THEN l.ProductName "
            "         ELSE d.EnglishProductName END AS EnglishProductName, "
            "    d.StandardCost, d.ListPrice, d.Color "
            f"FROM {SILVER_DIMPRODUCT} d "
            f"LEFT JOIN (SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey, ProductName FROM {BRONZE_PRODUCT}) l "
            "    ON d.ProductAlternateKey = l.AltKey"
        )
        refactored = (
            "WITH source_product AS ("
            f"    SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey, ProductName FROM {BRONZE_PRODUCT}"
            "), "
            "existing_product AS ("
            f"    SELECT * FROM {SILVER_DIMPRODUCT}"
            "), "
            "final AS ("
            "    SELECT d.ProductAlternateKey, "
            "        CASE WHEN l.ProductName IS NOT NULL THEN l.ProductName "
            "             ELSE d.EnglishProductName END AS EnglishProductName, "
            "        d.StandardCost, d.ListPrice, d.Color "
            "    FROM existing_product d "
            "    LEFT JOIN source_product l ON d.ProductAlternateKey = l.AltKey"
            ") "
            "SELECT * FROM final"
        )
        result = self._run(extracted, refactored, _DIM_PRODUCT_FIXTURES + _PRODUCT_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_delete_invert_extraction(self) -> None:
        """DELETE WHERE: extracted as inverted SELECT matches CTE version."""
        # Mirrors dbo.usp_DeleteWithCTE — keep rows where EnglishProductName IS NOT NULL
        extracted = (
            f"SELECT * FROM {SILVER_DIMPRODUCT} "
            "WHERE ProductAlternateKey NOT IN ("
            f"    SELECT ProductAlternateKey FROM {SILVER_DIMPRODUCT} WHERE EnglishProductName IS NULL"
            ")"
        )
        refactored = (
            "WITH all_products AS ("
            f"    SELECT * FROM {SILVER_DIMPRODUCT}"
            "), "
            "surviving AS ("
            "    SELECT * FROM all_products WHERE EnglishProductName IS NOT NULL"
            ") "
            "SELECT * FROM surviving"
        )
        result = self._run(extracted, refactored, _DIM_PRODUCT_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_exists_subquery(self) -> None:
        """EXISTS subquery: extracted vs CTE with JOIN."""
        # Mirrors dbo.usp_ExistsSubquery
        extracted = (
            "SELECT CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey, "
            "    p.FirstName, p.LastName "
            f"FROM {BRONZE_CUSTOMER} c "
            f"JOIN {BRONZE_PERSON} p ON c.PersonID = p.BusinessEntityID "
            "WHERE EXISTS ("
            f"    SELECT 1 FROM {BRONZE_SALESORDERHEADER} h WHERE h.CustomerID = c.CustomerID"
            ")"
        )
        refactored = (
            f"WITH source_customer AS (SELECT * FROM {BRONZE_CUSTOMER}), "
            f"source_person AS (SELECT * FROM {BRONZE_PERSON}), "
            f"source_orders AS (SELECT * FROM {BRONZE_SALESORDERHEADER}), "
            "customers_with_orders AS ("
            "    SELECT DISTINCT CustomerID FROM source_orders"
            "), "
            "final AS ("
            "    SELECT CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey, "
            "        p.FirstName, p.LastName "
            "    FROM source_customer c "
            "    JOIN source_person p ON c.PersonID = p.BusinessEntityID "
            "    JOIN customers_with_orders o ON o.CustomerID = c.CustomerID"
            ") "
            "SELECT * FROM final"
        )
        result = self._run(extracted, refactored, _CUSTOMER_PERSON_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_not_exists_subquery(self) -> None:
        """NOT EXISTS: extracted vs CTE with LEFT JOIN WHERE NULL."""
        # Mirrors dbo.usp_NotExistsSubquery
        extracted = (
            "SELECT cur.CurrencyCode, cur.CurrencyName "
            f"FROM {BRONZE_CURRENCY} cur "
            "WHERE NOT EXISTS ("
            f"    SELECT 1 FROM {SILVER_DIMCURRENCY} d WHERE d.CurrencyAlternateKey = cur.CurrencyCode"
            ")"
        )
        refactored = (
            f"WITH source_currency AS (SELECT * FROM {BRONZE_CURRENCY}), "
            f"existing_currency AS (SELECT * FROM {SILVER_DIMCURRENCY}), "
            "new_currencies AS ("
            "    SELECT s.CurrencyCode, s.CurrencyName "
            "    FROM source_currency s "
            "    LEFT JOIN existing_currency e ON e.CurrencyAlternateKey = s.CurrencyCode "
            "    WHERE e.CurrencyAlternateKey IS NULL"
            ") "
            "SELECT * FROM new_currencies"
        )
        result = self._run(extracted, refactored, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_window_function(self) -> None:
        """Window function (COUNT OVER): extracted vs CTE preserves window semantics."""
        # Mirrors silver.usp_stage_FactInternetSales window functions
        extracted = (
            "SELECT h.SalesOrderNumber, "
            "    CAST(d.SalesOrderDetailID % 127 AS TINYINT) AS SalesOrderLineNumber, "
            "    d.ProductID AS ProductKey, "
            "    h.CustomerID AS CustomerKey, "
            "    CAST(h.TaxAmt / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY) AS TaxAmt "
            f"FROM {BRONZE_SALESORDERHEADER} h "
            f"JOIN {BRONZE_SALESORDERDETAIL} d ON h.SalesOrderID = d.SalesOrderID "
            "WHERE h.SalesOrderID <= 43662"
        )
        refactored = (
            f"WITH source_header AS (SELECT * FROM {BRONZE_SALESORDERHEADER} WHERE SalesOrderID <= 43662), "
            f"source_detail AS (SELECT * FROM {BRONZE_SALESORDERDETAIL}), "
            "joined AS ("
            "    SELECT h.SalesOrderNumber, "
            "        CAST(d.SalesOrderDetailID % 127 AS TINYINT) AS SalesOrderLineNumber, "
            "        d.ProductID AS ProductKey, "
            "        h.CustomerID AS CustomerKey, "
            "        CAST(h.TaxAmt / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY) AS TaxAmt "
            "    FROM source_header h "
            "    JOIN source_detail d ON h.SalesOrderID = d.SalesOrderID"
            ") "
            "SELECT * FROM joined"
        )
        result = self._run(extracted, refactored, _SALES_ORDER_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_pivot(self) -> None:
        """PIVOT: extracted vs CTE version of dbo.usp_Pivot."""
        sql = (
            "SELECT pvt.TerritoryGroup, CAST(pvt.[1] + pvt.[2] + pvt.[3] AS NVARCHAR(50)) AS TotalSales "
            "FROM ("
            "    SELECT st.TerritoryGroup, st.TerritoryID, "
            "        CAST(st.SalesYTD AS MONEY) AS SalesAmt "
            f"    FROM {BRONZE_SALESTERRITORY} st"
            ") src "
            "PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt"
        )
        refactored = (
            "WITH source_territory AS ("
            "    SELECT TerritoryGroup, TerritoryID, CAST(SalesYTD AS MONEY) AS SalesAmt "
            f"    FROM {BRONZE_SALESTERRITORY}"
            "), "
            "pivoted AS ("
            "    SELECT pvt.TerritoryGroup, CAST(pvt.[1] + pvt.[2] + pvt.[3] AS NVARCHAR(50)) AS TotalSales "
            "    FROM source_territory "
            "    PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt"
            ") "
            "SELECT * FROM pivoted"
        )
        result = self._run(sql, refactored, _TERRITORY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_unpivot(self) -> None:
        """UNPIVOT: extracted vs CTE version of dbo.usp_Unpivot."""
        sql = (
            "SELECT CAST(unpvt.ProductID AS NVARCHAR(100)) AS ProductIdStr, unpvt.AttrValue "
            "FROM ("
            "    SELECT ProductID, "
            "        CAST(ISNULL(ProductName, '') AS NVARCHAR(50)) AS ProductName, "
            "        CAST(ISNULL(ProductNumber, '') AS NVARCHAR(50)) AS ProductNumber "
            f"    FROM {BRONZE_PRODUCT} WHERE ProductID <= 10"
            ") src "
            "UNPIVOT (AttrValue FOR AttrName IN (ProductName, ProductNumber)) unpvt"
        )
        refactored = (
            "WITH source_product AS ("
            "    SELECT ProductID, "
            "        CAST(ISNULL(ProductName, '') AS NVARCHAR(50)) AS ProductName, "
            "        CAST(ISNULL(ProductNumber, '') AS NVARCHAR(50)) AS ProductNumber "
            f"    FROM {BRONZE_PRODUCT} WHERE ProductID <= 10"
            "), "
            "unpivoted AS ("
            "    SELECT CAST(unpvt.ProductID AS NVARCHAR(100)) AS ProductIdStr, unpvt.AttrValue "
            "    FROM source_product "
            "    UNPIVOT (AttrValue FOR AttrName IN (ProductName, ProductNumber)) unpvt"
            ") "
            "SELECT * FROM unpivoted"
        )
        result = self._run(sql, refactored, _PRODUCT_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_grouping_sets(self) -> None:
        """GROUPING SETS: extracted vs CTE version."""
        sql = (
            "SELECT COALESCE(Color, 'ALL_COLORS') AS ColorGroup, "
            "    CAST(COUNT(*) AS NVARCHAR(50)) AS Cnt "
            f"FROM {BRONZE_PRODUCT} "
            "GROUP BY GROUPING SETS ((Color), ())"
        )
        refactored = (
            f"WITH source_product AS (SELECT * FROM {BRONZE_PRODUCT}), "
            "grouped AS ("
            "    SELECT COALESCE(Color, 'ALL_COLORS') AS ColorGroup, "
            "        CAST(COUNT(*) AS NVARCHAR(50)) AS Cnt "
            "    FROM source_product "
            "    GROUP BY GROUPING SETS ((Color), ())"
            ") "
            "SELECT * FROM grouped"
        )
        result = self._run(sql, refactored, _PRODUCT_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_scalar_subquery(self) -> None:
        """Scalar subquery in SELECT: extracted vs CTE with LEFT JOIN."""
        extracted = (
            "SELECT CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey, "
            f"    (SELECT TOP 1 p.FirstName FROM {BRONZE_PERSON} p WHERE p.BusinessEntityID = c.PersonID) AS FirstName, "
            f"    (SELECT TOP 1 p.LastName FROM {BRONZE_PERSON} p WHERE p.BusinessEntityID = c.PersonID) AS LastName "
            f"FROM {BRONZE_CUSTOMER} c "
            "WHERE c.PersonID IS NOT NULL"
        )
        refactored = (
            f"WITH source_customer AS (SELECT * FROM {BRONZE_CUSTOMER} WHERE PersonID IS NOT NULL), "
            f"source_person AS (SELECT * FROM {BRONZE_PERSON}), "
            "final AS ("
            "    SELECT CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey, "
            "        p.FirstName, p.LastName "
            "    FROM source_customer c "
            "    LEFT JOIN source_person p ON p.BusinessEntityID = c.PersonID"
            ") "
            "SELECT * FROM final"
        )
        result = self._run(extracted, refactored, _CUSTOMER_PERSON_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_cross_join(self) -> None:
        """CROSS JOIN: extracted vs CTE version."""
        sql = (
            "SELECT CAST(t.TerritoryID AS NVARCHAR(100)) AS TerritoryStr, cur.CurrencyName "
            f"FROM {BRONZE_SALESTERRITORY} t "
            f"CROSS JOIN {BRONZE_CURRENCY} cur "
            "WHERE t.TerritoryID <= 3 AND cur.CurrencyCode IN ('USD', 'EUR', 'GBP')"
        )
        refactored = (
            f"WITH source_territory AS (SELECT * FROM {BRONZE_SALESTERRITORY} WHERE TerritoryID <= 3), "
            f"source_currency AS (SELECT * FROM {BRONZE_CURRENCY} WHERE CurrencyCode IN ('USD', 'EUR', 'GBP')), "
            "crossed AS ("
            "    SELECT CAST(t.TerritoryID AS NVARCHAR(100)) AS TerritoryStr, c.CurrencyName "
            "    FROM source_territory t CROSS JOIN source_currency c"
            ") "
            "SELECT * FROM crossed"
        )
        result = self._run(sql, refactored, _TERRITORY_FIXTURES + _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_intersect(self) -> None:
        """INTERSECT: extracted vs CTE version."""
        sql = (
            f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode <= 'M' "
            "INTERSECT "
            f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode >= 'E'"
        )
        refactored = (
            f"WITH set_a AS (SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode <= 'M'), "
            f"set_b AS (SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode >= 'E'), "
            "intersected AS (SELECT * FROM set_a INTERSECT SELECT * FROM set_b) "
            "SELECT * FROM intersected"
        )
        result = self._run(sql, refactored, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] > 0

    def test_except(self) -> None:
        """EXCEPT: extracted vs CTE version."""
        sql = (
            f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} "
            "EXCEPT "
            f"SELECT CurrencyAlternateKey, CurrencyName FROM {SILVER_DIMCURRENCY}"
        )
        refactored = (
            f"WITH all_bronze AS (SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY}), "
            f"all_silver AS (SELECT CurrencyAlternateKey, CurrencyName FROM {SILVER_DIMCURRENCY}), "
            "new_only AS (SELECT * FROM all_bronze EXCEPT SELECT * FROM all_silver) "
            "SELECT * FROM new_only"
        )
        result = self._run(sql, refactored, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_empty_result_both_sides(self) -> None:
        """Both SELECTs return 0 rows: should be equivalent."""
        sql_a = f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE 1 = 0"
        sql_b = (
            f"WITH empty AS (SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE 1 = 0) "
            "SELECT * FROM empty"
        )
        result = self._run(sql_a, sql_b, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 0
        assert result["b_count"] == 0


@skip_no_mssql
class TestCompareTwoSqlNotEquivalent:
    """Scenarios where the two SELECTs produce different results."""

    def _run(
        self, sql_a: str, sql_b: str, fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            return backend.compare_two_sql(
                sandbox_db=up.sandbox_database,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_missing_column(self) -> None:
        """Refactored SQL drops a column: produces different rows."""
        sql_a = f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY}"
        sql_b = f"SELECT CurrencyCode FROM {BRONZE_CURRENCY}"
        result = self._run(sql_a, sql_b, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is False

    def test_wrong_filter(self) -> None:
        """Refactored SQL has a different WHERE clause: row count mismatch."""
        sql_a = f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY}"
        sql_b = f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'USD'"
        result = self._run(sql_a, sql_b, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is False
        assert len(result["a_minus_b"]) > 0

    def test_extra_rows_in_b(self) -> None:
        """Refactored SQL produces extra rows via UNION."""
        sql_a = f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'USD'"
        sql_b = (
            f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'USD' "
            "UNION ALL "
            f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'EUR'"
        )
        result = self._run(sql_a, sql_b, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is False
        assert len(result["b_minus_a"]) > 0


@skip_no_mssql
class TestCompareTwoSqlWithFixtures:
    """Scenarios that seed fixture data before comparison."""

    def _run(
        self, sql_a: str, sql_b: str, fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            return backend.compare_two_sql(
                sandbox_db=up.sandbox_database,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_fixtures_with_identity_column(self) -> None:
        """Fixtures include explicit identity column values — IDENTITY_INSERT toggled."""
        fixtures = [
            {
                "table": SILVER_DIMPRODUCT,
                "rows": [
                    {"ProductKey": 99999, "EnglishProductName": "Test Product", "Color": "Red"},
                ],
            },
        ]
        sql = f"SELECT ProductKey, EnglishProductName, Color FROM {SILVER_DIMPRODUCT} WHERE ProductKey = 99999"
        result = self._run(sql, sql, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 1

    def test_fixtures_with_fk_constraints(self) -> None:
        """Fixtures reference FK parent keys that may not exist — FK constraints disabled."""
        fixtures = [
            {
                "table": BRONZE_CURRENCY,
                "rows": [
                    {"CurrencyCode": "ZZZ", "CurrencyName": "Test Currency", "ModifiedDate": "2024-01-01"},
                ],
            },
        ]
        sql = f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'ZZZ'"
        result = self._run(sql, sql, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 1

    def test_null_values_in_result(self) -> None:
        """Both SELECTs return NULLs — symmetric diff handles None correctly."""
        fixtures = [
            {
                "table": BRONZE_PRODUCT,
                "rows": [
                    {
                        "ProductID": 77777,
                        "ProductName": "Null Test",
                        "ProductNumber": "NT-001",
                        "MakeFlag": 1,
                        "FinishedGoodsFlag": 1,
                        "Color": None,
                        "SafetyStockLevel": 10,
                        "ReorderPoint": 5,
                        "StandardCost": 0,
                        "ListPrice": 0,
                        "DaysToManufacture": 0,
                        "SellStartDate": "2024-01-01",
                        "ModifiedDate": "2024-01-01",
                    },
                ],
            },
        ]
        sql = (
            "SELECT ProductID, ProductName, Color, Size, ProductLine "
            f"FROM {BRONZE_PRODUCT} WHERE ProductID = 77777"
        )
        result = self._run(sql, sql, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 1

    def test_money_type_precision(self) -> None:
        """MONEY columns are compared correctly after string serialization."""
        fixtures = [
            {
                "table": BRONZE_PRODUCT,
                "rows": [
                    {
                        "ProductID": 88888,
                        "ProductName": "Money Test",
                        "ProductNumber": "MT-001",
                        "MakeFlag": 1,
                        "FinishedGoodsFlag": 1,
                        "Color": "Blue",
                        "SafetyStockLevel": 10,
                        "ReorderPoint": 5,
                        "StandardCost": 42.5000,
                        "ListPrice": 99.9900,
                        "DaysToManufacture": 0,
                        "SellStartDate": "2024-01-01",
                        "ModifiedDate": "2024-01-01",
                    },
                ],
            },
        ]
        sql_a = f"SELECT StandardCost, ListPrice FROM {BRONZE_PRODUCT} WHERE ProductID = 88888"
        sql_b = (
            f"WITH src AS (SELECT * FROM {BRONZE_PRODUCT} WHERE ProductID = 88888) "
            "SELECT StandardCost, ListPrice FROM src"
        )
        result = self._run(sql_a, sql_b, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_duplicate_rows_multiset(self) -> None:
        """Both sides produce duplicate rows — multiset comparison is correct."""
        fixtures = [
            {
                "table": BRONZE_CURRENCY,
                "rows": [
                    {"CurrencyCode": "DUP", "CurrencyName": "Dup Currency", "ModifiedDate": "2024-01-01"},
                    {"CurrencyCode": "DUP", "CurrencyName": "Dup Currency", "ModifiedDate": "2024-01-01"},
                ],
            },
        ]
        sql = f"SELECT CurrencyCode, CurrencyName FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'DUP'"
        result = self._run(sql, sql, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 2


@skip_no_mssql
class TestCompareTwoSqlTransactionRollback:
    """Verify fixture data and results don't persist after compare_two_sql."""

    def test_rollback_leaves_no_residual_data(self) -> None:
        """After compare_two_sql, fixture data is gone (transaction rolled back)."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        sandbox_db = up.sandbox_database

        try:
            fixtures = [
                {
                    "table": BRONZE_CURRENCY,
                    "rows": [
                        {"CurrencyCode": "RBK", "CurrencyName": "Rollback Test", "ModifiedDate": "2024-01-01"},
                    ],
                },
            ]
            sql = f"SELECT CurrencyCode FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'RBK'"

            backend.compare_two_sql(
                sandbox_db=sandbox_db, sql_a=sql, sql_b=sql, fixtures=fixtures,
            )

            # Verify fixture data was rolled back
            with backend._connect(database=sandbox_db) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {BRONZE_CURRENCY} WHERE CurrencyCode = 'RBK'")
                count = cursor.fetchone()[0]
                assert count == 0, "Fixture data should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=sandbox_db)


@skip_no_mssql
class TestCompareTwoSqlValidation:
    """Validation edge cases for compare_two_sql."""

    def test_rejects_write_sql(self) -> None:
        """SQL containing INSERT/UPDATE/DELETE is rejected before execution."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            with pytest.raises(ValueError, match="write operation"):
                backend.compare_two_sql(
                    sandbox_db=up.sandbox_database,
                    sql_a=f"INSERT INTO {BRONZE_CURRENCY} VALUES ('BAD', 'Bad', GETDATE())",
                    sql_b="SELECT 1",
                    fixtures=[],
                )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_rejects_exec_in_sql(self) -> None:
        """SQL containing EXEC is rejected."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            with pytest.raises(ValueError, match="write operation"):
                backend.compare_two_sql(
                    sandbox_db=up.sandbox_database,
                    sql_a="SELECT 1",
                    sql_b=f"EXEC {SILVER_USP_LOAD_DIMPRODUCT}",
                    fixtures=[],
                )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_rejects_invalid_syntax_in_sql_a(self) -> None:
        """Malformed SQL A is caught by PARSEONLY before execution."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            result = backend.compare_two_sql(
                sandbox_db=up.sandbox_database,
                sql_a="SELEC BROKEN SYNTAX FROM",
                sql_b="SELECT 1 AS x",
                fixtures=[],
            )
            assert result["status"] == "error"
            assert result["errors"][0]["code"] == "SQL_SYNTAX_ERROR"
            assert "A" in result["errors"][0]["message"]
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_rejects_invalid_syntax_in_sql_b(self) -> None:
        """Malformed SQL B is caught by PARSEONLY before execution."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            result = backend.compare_two_sql(
                sandbox_db=up.sandbox_database,
                sql_a="SELECT 1 AS x",
                sql_b="WITH broken AS (SELECT FROM WHERE",
                fixtures=[],
            )
            assert result["status"] == "error"
            assert result["errors"][0]["code"] == "SQL_SYNTAX_ERROR"
            assert "B" in result["errors"][0]["message"]
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_rejects_empty_sql(self) -> None:
        """Empty SQL string is rejected."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=[SQL_SERVER_FIXTURE_SCHEMA])
        try:
            with pytest.raises(ValueError, match="empty"):
                backend.compare_two_sql(
                    sandbox_db=up.sandbox_database,
                    sql_a="",
                    sql_b="SELECT 1",
                    fixtures=[],
                )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)
