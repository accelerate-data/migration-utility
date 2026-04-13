"""Integration tests for compare_two_sql — requires Docker SQL Server with MigrationTest DB.

Tests the full compare_two_sql workflow: seed fixtures, run two SELECTs, symmetric diff.
Covers DML extraction patterns (INSERT, MERGE, UPDATE, DELETE), identity columns,
FK constraints, NULL handling, MONEY types, and transaction rollback.

Run with: cd plugin/lib && uv run pytest ../../tests/integration/sql_server/test_harness -v -k compare_sql
Requires: MSSQL_HOST, SA_PASSWORD, MSSQL_DB env vars (or Docker 'sql-test' on localhost:1433).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

pyodbc = pytest.importorskip("pyodbc", reason="pyodbc not installed — skipping integration tests")

from tests.helpers import REPO_ROOT
from shared.fixture_materialization import materialize_migration_test
from shared.sandbox.sql_server import SqlServerSandbox
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

pytestmark = pytest.mark.integration

_FIXTURE_READY = False


def _have_mssql_env() -> bool:
    if not all(os.environ.get(name) for name in ("MSSQL_HOST", "MSSQL_DB", "SA_PASSWORD")):
        return False
    try:
        conn = pyodbc.connect(
            f"DRIVER={{{os.environ.get('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
            f"SERVER={os.environ.get('MSSQL_HOST', 'localhost')},"
            f"{os.environ.get('MSSQL_PORT', '1433')};"
            f"DATABASE={os.environ.get('MSSQL_DB', 'MigrationTest')};"
            f"UID={os.environ.get('MSSQL_USER', 'sa')};"
            f"PWD={os.environ.get('SA_PASSWORD', '')};"
            "TrustServerCertificate=yes;"
            "LoginTimeout=1;",
            autocommit=True,
        )
        conn.close()
        return True
    except pyodbc.Error:
        return False


def _make_backend() -> SqlServerSandbox:
    global _FIXTURE_READY
    if not _FIXTURE_READY:
        role = RuntimeRole(
            technology="sql_server",
            dialect="tsql",
            connection=RuntimeConnection(
                host=os.environ.get("MSSQL_HOST", "localhost"),
                port=os.environ.get("MSSQL_PORT", "1433"),
                database=os.environ.get("MSSQL_DB", "MigrationTest"),
                user=os.environ.get("MSSQL_USER", "sa"),
                driver=os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
                password_env="SA_PASSWORD",
            ),
        )
        materialize_result = materialize_migration_test(role, REPO_ROOT)
        if materialize_result.returncode != 0:
            raise RuntimeError(
                "SQL Server MigrationTest materialization failed:\n"
                f"stdout:\n{materialize_result.stdout}\n"
                f"stderr:\n{materialize_result.stderr}"
            )
        _FIXTURE_READY = True
    manifest = {
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("MSSQL_HOST", "localhost"),
                    "port": os.environ.get("MSSQL_PORT", "1433"),
                    "database": os.environ.get("MSSQL_DB", "MigrationTest"),
                },
            },
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("MSSQL_HOST", "localhost"),
                    "port": os.environ.get("MSSQL_PORT", "1433"),
                    "user": os.environ.get("MSSQL_USER", "sa"),
                    "driver": os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
                    "password_env": "SA_PASSWORD",
                },
            },
        }
    }
    return SqlServerSandbox.from_env(manifest)


skip_no_mssql = pytest.mark.skipif(
    not _have_mssql_env(),
    reason="MSSQL integration DB not reachable (MSSQL_HOST, MSSQL_DB, SA_PASSWORD and a listening server required)",
)


# ── Shared fixture data ──────────────────────────────────────────────────────
# The sandbox clones table structure but NOT data. All tests that query
# bronze/silver tables must seed fixture rows.

_CURRENCY_FIXTURES: list[dict[str, Any]] = [
    {
        "table": "[bronze].[Currency]",
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
        "table": "[bronze].[Product]",
        "rows": [
            {"ProductID": 1, "ProductName": "Widget A", "Color": "Red", "StandardCost": 10.00, "ListPrice": 20.00, "SellStartDate": "2024-01-01"},
            {"ProductID": 2, "ProductName": "Widget B", "Color": "Blue", "StandardCost": 15.00, "ListPrice": 30.00, "SellStartDate": "2024-01-01"},
            {"ProductID": 3, "ProductName": "Widget C", "Color": None, "StandardCost": 5.00, "ListPrice": 10.00, "SellStartDate": "2023-01-01"},
        ],
    },
]

_PROMOTION_FIXTURES: list[dict[str, Any]] = [
    {
        "table": "[bronze].[Promotion]",
        "rows": [
            {"PromotionID": 1, "Description": "Summer Sale", "DiscountPct": 0.10, "PromotionType": "Discount", "PromotionCategory": "Seasonal", "StartDate": "2024-06-01"},
            {"PromotionID": 2, "Description": "No Discount", "DiscountPct": 0.0, "PromotionType": "None", "PromotionCategory": "None", "StartDate": "2024-01-01"},
        ],
    },
]

_CUSTOMER_PERSON_FIXTURES: list[dict[str, Any]] = [
    {
        "table": "[bronze].[Customer]",
        "rows": [
            {"CustomerID": 1, "PersonID": 10, "TerritoryID": 1},
            {"CustomerID": 2, "PersonID": 20, "TerritoryID": 2},
        ],
    },
    {
        "table": "[bronze].[Person]",
        "rows": [
            {"BusinessEntityID": 10, "FirstName": "Alice", "LastName": "Smith", "EmailPromotion": 1},
            {"BusinessEntityID": 20, "FirstName": "Bob", "LastName": "Jones", "EmailPromotion": 0},
        ],
    },
    {
        "table": "[bronze].[SalesOrderHeader]",
        "rows": [
            {"SalesOrderID": 100, "SalesOrderNumber": "SO100", "CustomerID": 1, "OrderDate": "2024-03-15"},
        ],
    },
]

_DIM_PRODUCT_FIXTURES: list[dict[str, Any]] = [
    {
        "table": "[silver].[DimProduct]",
        "rows": [
            {"ProductKey": 1, "ProductAlternateKey": "1", "EnglishProductName": "Widget A", "StandardCost": 10.00, "ListPrice": 20.00, "Color": "Red"},
            {"ProductKey": 2, "ProductAlternateKey": "2", "EnglishProductName": "Widget B", "StandardCost": 15.00, "ListPrice": 30.00, "Color": "Blue"},
            {"ProductKey": 3, "ProductAlternateKey": "3", "EnglishProductName": "Widget C", "StandardCost": 5.00, "ListPrice": 10.00, "Color": ""},
        ],
    },
]

_SALES_ORDER_FIXTURES: list[dict[str, Any]] = [
    {
        "table": "[bronze].[SalesOrderHeader]",
        "rows": [
            {"SalesOrderID": 1, "SalesOrderNumber": "SO001", "CustomerID": 1, "TerritoryID": 1, "OrderDate": "2024-03-15", "TaxAmt": 40.00, "Freight": 10.00},
        ],
    },
    {
        "table": "[bronze].[SalesOrderDetail]",
        "rows": [
            {"SalesOrderID": 1, "SalesOrderDetailID": 1, "OrderQty": 2, "ProductID": 1, "UnitPrice": 20.00, "LineTotal": 40.00},
            {"SalesOrderID": 1, "SalesOrderDetailID": 2, "OrderQty": 3, "ProductID": 2, "UnitPrice": 30.00, "LineTotal": 90.00},
        ],
    },
]

_TERRITORY_FIXTURES: list[dict[str, Any]] = [
    {
        "table": "[bronze].[SalesTerritory]",
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
        up = backend.sandbox_up(schemas=["bronze", "silver"])
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
        # Mirrors silver.usp_load_DimPromotion which does INSERT...SELECT from bronze.Promotion
        extracted = (
            "SELECT p.PromotionID, p.Description, p.DiscountPct, "
            "p.PromotionType, p.PromotionCategory, p.StartDate, p.EndDate, p.MinQty, p.MaxQty "
            "FROM [bronze].[Promotion] p"
        )
        refactored = (
            "WITH source_promotion AS ("
            "    SELECT * FROM [bronze].[Promotion]"
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
        # Mirrors silver.usp_load_DimProduct's USING clause
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
            "FROM bronze.Product"
        )
        refactored = (
            "WITH source_product AS ("
            "    SELECT * FROM [bronze].[Product]"
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
        # Mirrors silver.usp_load_DimCustomer_Full
        sql = (
            "SELECT "
            "    CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey, "
            "    p.FirstName, p.MiddleName, p.LastName, p.Title, "
            "    NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion, "
            "    CAST(h.MinOrderDate AS DATE) AS DateFirstPurchase "
            "FROM bronze.Customer c "
            "JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID "
            "OUTER APPLY ("
            "    SELECT MIN(OrderDate) AS MinOrderDate "
            "    FROM bronze.SalesOrderHeader sh "
            "    WHERE sh.CustomerID = c.CustomerID"
            ") h"
        )
        refactored = (
            "WITH source_customer AS (SELECT * FROM [bronze].[Customer]), "
            "source_person AS (SELECT * FROM [bronze].[Person]), "
            "source_orders AS (SELECT * FROM [bronze].[SalesOrderHeader]), "
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
            "FROM bronze.Product WHERE ProductID <= 250 "
            "UNION ALL "
            "SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '') "
            "FROM bronze.Product WHERE ProductID > 250"
        )
        refactored = (
            "WITH low_ids AS ("
            "    SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, "
            "        ProductName AS EnglishProductName, ISNULL(Color, '') AS Color "
            "    FROM bronze.Product WHERE ProductID <= 250"
            "), "
            "high_ids AS ("
            "    SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, "
            "        ProductName AS EnglishProductName, ISNULL(Color, '') AS Color "
            "    FROM bronze.Product WHERE ProductID > 250"
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
            "FROM silver.DimProduct d "
            "LEFT JOIN (SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey, ProductName FROM bronze.Product) l "
            "    ON d.ProductAlternateKey = l.AltKey"
        )
        refactored = (
            "WITH source_product AS ("
            "    SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey, ProductName FROM [bronze].[Product]"
            "), "
            "existing_product AS ("
            "    SELECT * FROM [silver].[DimProduct]"
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
            "SELECT * FROM silver.DimProduct "
            "WHERE ProductAlternateKey NOT IN ("
            "    SELECT ProductAlternateKey FROM silver.DimProduct WHERE EnglishProductName IS NULL"
            ")"
        )
        refactored = (
            "WITH all_products AS ("
            "    SELECT * FROM [silver].[DimProduct]"
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
            "FROM bronze.Customer c "
            "JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID "
            "WHERE EXISTS ("
            "    SELECT 1 FROM bronze.SalesOrderHeader h WHERE h.CustomerID = c.CustomerID"
            ")"
        )
        refactored = (
            "WITH source_customer AS (SELECT * FROM [bronze].[Customer]), "
            "source_person AS (SELECT * FROM [bronze].[Person]), "
            "source_orders AS (SELECT * FROM [bronze].[SalesOrderHeader]), "
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
            "FROM bronze.Currency cur "
            "WHERE NOT EXISTS ("
            "    SELECT 1 FROM silver.DimCurrency d WHERE d.CurrencyAlternateKey = cur.CurrencyCode"
            ")"
        )
        refactored = (
            "WITH source_currency AS (SELECT * FROM [bronze].[Currency]), "
            "existing_currency AS (SELECT * FROM [silver].[DimCurrency]), "
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
            "FROM bronze.SalesOrderHeader h "
            "JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID "
            "WHERE h.SalesOrderID <= 43662"
        )
        refactored = (
            "WITH source_header AS (SELECT * FROM [bronze].[SalesOrderHeader] WHERE SalesOrderID <= 43662), "
            "source_detail AS (SELECT * FROM [bronze].[SalesOrderDetail]), "
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
            "    FROM bronze.SalesTerritory st"
            ") src "
            "PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt"
        )
        refactored = (
            "WITH source_territory AS ("
            "    SELECT TerritoryGroup, TerritoryID, CAST(SalesYTD AS MONEY) AS SalesAmt "
            "    FROM [bronze].[SalesTerritory]"
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
            "    FROM bronze.Product WHERE ProductID <= 10"
            ") src "
            "UNPIVOT (AttrValue FOR AttrName IN (ProductName, ProductNumber)) unpvt"
        )
        refactored = (
            "WITH source_product AS ("
            "    SELECT ProductID, "
            "        CAST(ISNULL(ProductName, '') AS NVARCHAR(50)) AS ProductName, "
            "        CAST(ISNULL(ProductNumber, '') AS NVARCHAR(50)) AS ProductNumber "
            "    FROM [bronze].[Product] WHERE ProductID <= 10"
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
            "FROM bronze.Product "
            "GROUP BY GROUPING SETS ((Color), ())"
        )
        refactored = (
            "WITH source_product AS (SELECT * FROM [bronze].[Product]), "
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
            "    (SELECT TOP 1 p.FirstName FROM bronze.Person p WHERE p.BusinessEntityID = c.PersonID) AS FirstName, "
            "    (SELECT TOP 1 p.LastName FROM bronze.Person p WHERE p.BusinessEntityID = c.PersonID) AS LastName "
            "FROM bronze.Customer c "
            "WHERE c.PersonID IS NOT NULL"
        )
        refactored = (
            "WITH source_customer AS (SELECT * FROM [bronze].[Customer] WHERE PersonID IS NOT NULL), "
            "source_person AS (SELECT * FROM [bronze].[Person]), "
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
            "FROM bronze.SalesTerritory t "
            "CROSS JOIN bronze.Currency cur "
            "WHERE t.TerritoryID <= 3 AND cur.CurrencyCode IN ('USD', 'EUR', 'GBP')"
        )
        refactored = (
            "WITH source_territory AS (SELECT * FROM [bronze].[SalesTerritory] WHERE TerritoryID <= 3), "
            "source_currency AS (SELECT * FROM [bronze].[Currency] WHERE CurrencyCode IN ('USD', 'EUR', 'GBP')), "
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
            "SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode <= 'M' "
            "INTERSECT "
            "SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode >= 'E'"
        )
        refactored = (
            "WITH set_a AS (SELECT CurrencyCode, CurrencyName FROM [bronze].[Currency] WHERE CurrencyCode <= 'M'), "
            "set_b AS (SELECT CurrencyCode, CurrencyName FROM [bronze].[Currency] WHERE CurrencyCode >= 'E'), "
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
            "SELECT CurrencyCode, CurrencyName FROM bronze.Currency "
            "EXCEPT "
            "SELECT CurrencyAlternateKey, CurrencyName FROM silver.DimCurrency"
        )
        refactored = (
            "WITH all_bronze AS (SELECT CurrencyCode, CurrencyName FROM [bronze].[Currency]), "
            "all_silver AS (SELECT CurrencyAlternateKey, CurrencyName FROM [silver].[DimCurrency]), "
            "new_only AS (SELECT * FROM all_bronze EXCEPT SELECT * FROM all_silver) "
            "SELECT * FROM new_only"
        )
        result = self._run(sql, refactored, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_empty_result_both_sides(self) -> None:
        """Both SELECTs return 0 rows: should be equivalent."""
        sql_a = "SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE 1 = 0"
        sql_b = "WITH empty AS (SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE 1 = 0) SELECT * FROM empty"
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
        up = backend.sandbox_up(schemas=["bronze", "silver"])
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
        sql_a = "SELECT CurrencyCode, CurrencyName FROM bronze.Currency"
        sql_b = "SELECT CurrencyCode FROM bronze.Currency"
        result = self._run(sql_a, sql_b, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is False

    def test_wrong_filter(self) -> None:
        """Refactored SQL has a different WHERE clause: row count mismatch."""
        sql_a = "SELECT CurrencyCode, CurrencyName FROM bronze.Currency"
        sql_b = "SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode = 'USD'"
        result = self._run(sql_a, sql_b, _CURRENCY_FIXTURES)
        assert result["status"] == "ok"
        assert result["equivalent"] is False
        assert len(result["a_minus_b"]) > 0

    def test_extra_rows_in_b(self) -> None:
        """Refactored SQL produces extra rows via UNION."""
        sql_a = "SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode = 'USD'"
        sql_b = (
            "SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode = 'USD' "
            "UNION ALL "
            "SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode = 'EUR'"
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
        up = backend.sandbox_up(schemas=["bronze", "silver"])
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
                "table": "[silver].[DimProduct]",
                "rows": [
                    {"ProductKey": 99999, "EnglishProductName": "Test Product", "Color": "Red"},
                ],
            },
        ]
        sql = "SELECT ProductKey, EnglishProductName, Color FROM [silver].[DimProduct] WHERE ProductKey = 99999"
        result = self._run(sql, sql, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 1

    def test_fixtures_with_fk_constraints(self) -> None:
        """Fixtures reference FK parent keys that may not exist — FK constraints disabled."""
        fixtures = [
            {
                "table": "[bronze].[Currency]",
                "rows": [
                    {"CurrencyCode": "ZZZ", "CurrencyName": "Test Currency", "ModifiedDate": "2024-01-01"},
                ],
            },
        ]
        sql = "SELECT CurrencyCode, CurrencyName FROM [bronze].[Currency] WHERE CurrencyCode = 'ZZZ'"
        result = self._run(sql, sql, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 1

    def test_null_values_in_result(self) -> None:
        """Both SELECTs return NULLs — symmetric diff handles None correctly."""
        fixtures = [
            {
                "table": "[bronze].[Product]",
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
            "FROM [bronze].[Product] WHERE ProductID = 77777"
        )
        result = self._run(sql, sql, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True
        row_a = result  # just check it didn't crash on NULLs
        assert result["a_count"] == 1

    def test_money_type_precision(self) -> None:
        """MONEY columns are compared correctly after string serialization."""
        fixtures = [
            {
                "table": "[bronze].[Product]",
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
        sql_a = "SELECT StandardCost, ListPrice FROM [bronze].[Product] WHERE ProductID = 88888"
        sql_b = (
            "WITH src AS (SELECT * FROM [bronze].[Product] WHERE ProductID = 88888) "
            "SELECT StandardCost, ListPrice FROM src"
        )
        result = self._run(sql_a, sql_b, fixtures)
        assert result["status"] == "ok"
        assert result["equivalent"] is True

    def test_duplicate_rows_multiset(self) -> None:
        """Both sides produce duplicate rows — multiset comparison is correct."""
        fixtures = [
            {
                "table": "[bronze].[Currency]",
                "rows": [
                    {"CurrencyCode": "DUP", "CurrencyName": "Dup Currency", "ModifiedDate": "2024-01-01"},
                    {"CurrencyCode": "DUP", "CurrencyName": "Dup Currency", "ModifiedDate": "2024-01-01"},
                ],
            },
        ]
        sql = "SELECT CurrencyCode, CurrencyName FROM [bronze].[Currency] WHERE CurrencyCode = 'DUP'"
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
        up = backend.sandbox_up(schemas=["bronze", "silver"])
        sandbox_db = up.sandbox_database

        try:
            fixtures = [
                {
                    "table": "[bronze].[Currency]",
                    "rows": [
                        {"CurrencyCode": "RBK", "CurrencyName": "Rollback Test", "ModifiedDate": "2024-01-01"},
                    ],
                },
            ]
            sql = "SELECT CurrencyCode FROM [bronze].[Currency] WHERE CurrencyCode = 'RBK'"

            backend.compare_two_sql(
                sandbox_db=sandbox_db, sql_a=sql, sql_b=sql, fixtures=fixtures,
            )

            # Verify fixture data was rolled back
            with backend._connect(database=sandbox_db) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM [bronze].[Currency] WHERE CurrencyCode = 'RBK'")
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
        up = backend.sandbox_up(schemas=["bronze"])
        try:
            with pytest.raises(ValueError, match="write operation"):
                backend.compare_two_sql(
                    sandbox_db=up.sandbox_database,
                    sql_a="INSERT INTO bronze.Currency VALUES ('BAD', 'Bad', GETDATE())",
                    sql_b="SELECT 1",
                    fixtures=[],
                )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_rejects_exec_in_sql(self) -> None:
        """SQL containing EXEC is rejected."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=["bronze"])
        try:
            with pytest.raises(ValueError, match="write operation"):
                backend.compare_two_sql(
                    sandbox_db=up.sandbox_database,
                    sql_a="SELECT 1",
                    sql_b="EXEC silver.usp_load_DimProduct",
                    fixtures=[],
                )
        finally:
            backend.sandbox_down(sandbox_db=up.sandbox_database)

    def test_rejects_invalid_syntax_in_sql_a(self) -> None:
        """Malformed SQL A is caught by PARSEONLY before execution."""
        backend = _make_backend()
        up = backend.sandbox_up(schemas=["bronze"])
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
        up = backend.sandbox_up(schemas=["bronze"])
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
        up = backend.sandbox_up(schemas=["bronze"])
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
